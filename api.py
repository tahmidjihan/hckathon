"""Slim wildfire API backed by local slim Parquet (fast) with R2 fallback.

Endpoints:
  GET /health
  GET /years
  GET /fires?year=&bbox=&from=&to=&lat=&lon=&radius_km=&limit=&offset=&sort=&order=
  GET /fires/{fire_id}
  GET /fires/{fire_id}/events
  GET /events?year=&bbox=&from=&to=&limit=&offset=
  GET /events/{event_id}
  GET /stats/overview?...
  GET /stats/timeseries?interval=year|month&...
  GET /stats/sources?...
  GET /stats/top-fires?metric=max_frp|n_events|n_detections|duration&...
  GET /                           -> frontend
"""
from __future__ import annotations

import json
import math
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

app = FastAPI(title="Slim Wildfire API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ROOT = Path(__file__).parent
FRONTEND_DIR = ROOT / "frontend"
LOCAL_SLIM = ROOT / "data" / "unified_slim"
BUCKET = "nasa-hackathon"

_lock = threading.Lock()
_con: duckdb.DuckDBPyConnection | None = None
_use_local = (LOCAL_SLIM / "fires").exists() and (LOCAL_SLIM / "events").exists()
_stats_cache: dict[str, Any] = {}
_stats_lock = threading.Lock()


def _load_env():
    vals = {}
    env_path = ROOT / ".env"
    if not env_path.exists():
        return vals
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()
    return vals


def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is not None:
        return _con
    with _lock:
        if _con is not None:
            return _con
        con = duckdb.connect()
        if not _use_local:
            env = _load_env()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("SET s3_region='auto';")
            if "s3_endpoint" in env:
                con.execute(f"SET s3_endpoint='{env['s3_endpoint']}';")
            if "access_key" in env:
                con.execute(f"SET s3_access_key_id='{env['access_key']}';")
            if "secret_key" in env:
                con.execute(f"SET s3_secret_access_key='{env['secret_key']}';")
            con.execute("SET s3_url_style='path';")
        _con = con
        return _con


def _query(sql: str, params=None):
    con = get_con()
    with _lock:
        cur = con.execute(sql, params or [])
        rows = cur.fetchall()
        names = [d[0] for d in cur.description] if cur.description else []
    return names, rows


def fires_path(year: Optional[int] = None) -> str:
    if _use_local:
        base = (LOCAL_SLIM / "fires").as_posix()
        return f"{base}/fires_*.parquet" if year is None else f"{base}/fires_{year}.parquet"
    return f"s3://{BUCKET}/fires/fires_*.parquet" if year is None else f"s3://{BUCKET}/fires/fires_{year}.parquet"


def events_path(year: Optional[int] = None) -> str:
    if _use_local:
        base = (LOCAL_SLIM / "events").as_posix()
        return f"{base}/events_*.parquet" if year is None else f"{base}/events_{year}.parquet"
    return f"s3://{BUCKET}/events/events_*.parquet" if year is None else f"s3://{BUCKET}/events/events_{year}.parquet"


def year_from_id(idstr: str) -> int:
    try:
        return int(idstr.split("-")[1])
    except Exception:
        raise HTTPException(400, "ID must look like FIRE-2024-000001 or EVT-2024-0000001")


def _bbox_params(bbox: Optional[str]):
    if not bbox:
        return None
    try:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError
        return parts  # minLat,minLon,maxLat,maxLon
    except Exception:
        raise HTTPException(400, "bbox must be minLat,minLon,maxLat,maxLon")


def _narrow_path(base_fn, year, from_date, to_date):
    path = base_fn(year)
    if year is None and from_date and to_date and from_date[:4] == to_date[:4]:
        path = base_fn(int(from_date[:4]))
    elif year is None and from_date and not to_date:
        try:
            path = base_fn(int(from_date[:4]))
        except Exception:
            pass
    return path


def _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km):
    b = _bbox_params(bbox)
    if b is None and lat is not None and lon is not None and radius_km is not None:
        dlat = radius_km / 111.0
        dlon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))
        b = [lat - dlat, lon - dlon, lat + dlat, lon + dlon]
    where, params = [], []
    if b:
        where.append("latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?")
        params.extend([b[0], b[2], b[1], b[3]])
    if from_date:
        where.append("start_date >= CAST(? AS DATE)")
        params.append(from_date)
    if to_date:
        where.append("start_date <= CAST(? AS DATE)")
        params.append(to_date)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    path = _narrow_path(fires_path, year, from_date, to_date)
    return path, clause, params, b


def _cache_get(key):
    with _stats_lock:
        return _stats_cache.get(key)


def _cache_set(key, val):
    with _stats_lock:
        if len(_stats_cache) > 256:
            _stats_cache.clear()
        _stats_cache[key] = val
    return val


@app.get("/health")
def health():
    return {"status": "ok", "source": "local" if _use_local else "r2", "bucket": BUCKET}


@lru_cache(maxsize=1)
def _years_cached():
    out = []
    for y in range(2000, 2027):
        try:
            _, rows = _query(f"SELECT count(*) FROM read_parquet('{fires_path(y)}')")
            fc = rows[0][0]
        except Exception:
            fc = 0
        try:
            _, rows = _query(f"SELECT count(*) FROM read_parquet('{events_path(y)}')")
            ec = rows[0][0]
        except Exception:
            ec = 0
        if fc or ec:
            out.append({"year": y, "fires": fc, "events": ec})
    return tuple(out)


@app.get("/years")
def years():
    return list(_years_cached())


SORT_MAP = {
    "start_date": "start_date",
    "end_date": "end_date",
    "max_frp": "max_frp",
    "sum_frp": "sum_frp",
    "n_events": "n_events",
    "n_detections": "n_detections",
    "fire_id": "fire_id",
    "duration": "(end_date - start_date)",
}


@app.get("/fires")
def list_fires(
    year: Optional[int] = None,
    limit: int = Query(200, le=5000),
    offset: int = 0,
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
    sort: str = Query("start_date"),
    order: str = Query("desc"),
):
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, lat_min, lat_max, lon_min, lon_max, max_frp, sum_frp, sources"
    sort_col = SORT_MAP.get(sort, "start_date")
    ord_sql = "ASC" if str(order).lower() == "asc" else "DESC"
    sql = f"SELECT {cols} FROM read_parquet('{path}') {clause} ORDER BY {sort_col} {ord_sql} NULLS LAST, fire_id LIMIT ? OFFSET ?"
    qparams = params + [limit, offset]
    names, rows = _query(sql, qparams)
    return {"count": len(rows), "results": [dict(zip(names, r)) for r in rows], "limit": limit, "offset": offset}


@app.get("/fires/{fire_id}")
def get_fire(fire_id: str):
    y = year_from_id(fire_id)
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, lat_min, lat_max, lon_min, lon_max, max_frp, sum_frp, sources"
    for path in [fires_path(y), fires_path(None)]:
        try:
            names, rows = _query(f"SELECT {cols} FROM read_parquet('{path}') WHERE fire_id = ?", [fire_id])
            if rows:
                return dict(zip(names, rows[0]))
        except Exception:
            continue
    raise HTTPException(404, "fire not found")


@app.get("/fires/{fire_id}/events")
def fire_events(fire_id: str, limit: int = Query(1000, le=10000)):
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    names, rows = _query(
        f"SELECT {cols} FROM read_parquet('{events_path(None)}') WHERE fire_id = ? ORDER BY first_dt LIMIT ?",
        [fire_id, limit],
    )
    return [dict(zip(names, r)) for r in rows]


@app.get("/events")
def list_events(
    year: Optional[int] = None,
    limit: int = Query(200, le=5000),
    offset: int = 0,
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    b = _bbox_params(bbox)
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    where, params = [], []
    if b:
        where.append("latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?")
        params.extend([b[0], b[2], b[1], b[3]])
    if from_date:
        where.append("event_date >= CAST(? AS DATE)")
        params.append(from_date)
    if to_date:
        where.append("event_date <= CAST(? AS DATE)")
        params.append(to_date)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    path = _narrow_path(events_path, year, from_date, to_date)
    sql = f"SELECT {cols} FROM read_parquet('{path}') {clause} ORDER BY event_date DESC, event_id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    names, rows = _query(sql, params)
    return {"count": len(rows), "results": [dict(zip(names, r)) for r in rows], "limit": limit, "offset": offset}


@app.get("/events/{event_id}")
def get_event(event_id: str):
    y = year_from_id(event_id)
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    for path in [events_path(y), events_path(None)]:
        try:
            names, rows = _query(f"SELECT {cols} FROM read_parquet('{path}') WHERE event_id = ?", [event_id])
            if rows:
                return dict(zip(names, rows[0]))
        except Exception:
            continue
    raise HTTPException(404, "event not found")


# --- Stats endpoints ---

@app.get("/stats/overview")
def stats_overview(
    year: Optional[int] = None,
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["overview", year, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    sql = f"""
        SELECT
          count(*) AS fires,
          coalesce(sum(n_events),0) AS events,
          coalesce(sum(n_detections),0) AS detections,
          max(max_frp) AS max_frp,
          coalesce(sum(sum_frp),0) AS sum_frp,
          min(start_date) AS min_date,
          max(end_date) AS max_date
        FROM read_parquet('{path}') {clause}
    """
    names, rows = _query(sql, params)
    data = dict(zip(names, rows[0])) if rows else {}
    data.update({"source": "local" if _use_local else "r2", "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}})
    return _cache_set(key, data)


@app.get("/stats/timeseries")
def stats_timeseries(
    year: Optional[int] = None,
    interval: str = Query("month", pattern="^(year|month)$"),
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["timeseries", year, interval, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    if interval == "year":
        bucket = "CAST(year(start_date) AS VARCHAR)"
    else:
        bucket = "strftime(start_date, '%Y-%m')"
    sql = f"""
        SELECT {bucket} AS period,
               count(*) AS fires,
               coalesce(sum(n_events),0) AS events,
               coalesce(sum(n_detections),0) AS detections,
               max(max_frp) AS max_frp,
               coalesce(sum(sum_frp),0) AS sum_frp
        FROM read_parquet('{path}') {clause}
        GROUP BY period ORDER BY period
    """
    names, rows = _query(sql, params)
    data = {"interval": interval, "series": [dict(zip(names, r)) for r in rows], "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
    return _cache_set(key, data)


@app.get("/stats/sources")
def stats_sources(
    year: Optional[int] = None,
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["sources", year, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    sql = f"""
        SELECT token AS source, count(*) AS fires
        FROM (
          SELECT unnest(string_split(sources, ',')) AS token
          FROM read_parquet('{path}') {clause}
        )
        GROUP BY token ORDER BY fires DESC
    """
    names, rows = _query(sql, params)
    data = {"sources": [dict(zip(names, r)) for r in rows], "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
    return _cache_set(key, data)


@app.get("/stats/top-fires")
def stats_top_fires(
    year: Optional[int] = None,
    metric: str = Query("max_frp", pattern="^(max_frp|n_events|n_detections|sum_frp|duration)$"),
    limit: int = Query(10, le=100),
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["top", year, metric, limit, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    metric_expr = "(end_date - start_date)" if metric == "duration" else metric
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, max_frp, sum_frp, sources"
    sql = f"SELECT {cols}, {metric_expr} AS rank_metric FROM read_parquet('{path}') {clause} ORDER BY rank_metric DESC NULLS LAST LIMIT ?"
    names, rows = _query(sql, params + [limit])
    data = {"metric": metric, "results": [dict(zip(names, r)) for r in rows], "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
    return _cache_set(key, data)


@app.get("/stats/distribution")
def stats_distribution(
    year: Optional[int] = None,
    metric: str = Query("max_frp", pattern="^(max_frp|n_events|n_detections|sum_frp|duration)$"),
    bins: int = Query(20, ge=5, le=100),
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["dist", year, metric, bins, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    metric_expr = "(CAST(end_date AS DATE) - CAST(start_date AS DATE))" if metric == "duration" else metric
    # compute min/max then bin
    stats_sql = f"SELECT min({metric_expr}) AS mn, max({metric_expr}) AS mx, count(*) AS n FROM read_parquet('{path}') {clause}"
    names, rows = _query(stats_sql, params)
    mn, mx, n = rows[0] if rows else (0, 0, 0)
    if n == 0 or mn is None or mx is None or float(mx) == float(mn):
        data = {"metric": metric, "bins": [], "min": mn, "max": mx, "count": n, "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
        return _cache_set(key, data)
    mnf, mxf = float(mn), float(mx)
    width = (mxf - mnf) / bins
    # width_bucket-like via floor
    sql = f"""
        SELECT
          least({bins-1}, greatest(0, CAST(floor(({metric_expr} - ?) / ?) AS INTEGER))) AS bucket,
          count(*) AS fires
        FROM read_parquet('{path}') {clause}
        GROUP BY bucket ORDER BY bucket
    """
    qparams = [mnf, width if width > 0 else 1.0] + params
    _, brows = _query(sql, qparams)
    counts = {int(r[0]): int(r[1]) for r in brows}
    out_bins = []
    for i in range(bins):
        lo = mnf + i * width
        hi = mnf + (i + 1) * width
        out_bins.append({"bin": i, "lo": lo, "hi": hi, "label": f"{lo:.2g}-{hi:.2g}", "fires": counts.get(i, 0)})
    data = {"metric": metric, "bins": out_bins, "min": mnf, "max": mxf, "count": n, "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
    return _cache_set(key, data)


@app.get("/stats/seasonality")
def stats_seasonality(
    year: Optional[int] = None,
    bbox: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
):
    key = json.dumps(["season", year, bbox, from_date, to_date, lat, lon, radius_km], sort_keys=True)
    cached = _cache_get(key)
    if cached:
        return cached
    path, clause, params, b = _fire_filters(year, bbox, from_date, to_date, lat, lon, radius_km)
    sql = f"""
        SELECT month(start_date) AS month,
               count(*) AS fires,
               coalesce(sum(n_events),0) AS events,
               max(max_frp) AS max_frp,
               coalesce(sum(sum_frp),0) AS sum_frp
        FROM read_parquet('{path}') {clause}
        GROUP BY month ORDER BY month
    """
    names, rows = _query(sql, params)
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    by_m = {int(r[0]): dict(zip(names, r)) for r in rows}
    series = []
    for m in range(1,13):
        d = by_m.get(m, {"month": m, "fires": 0, "events": 0, "max_frp": None, "sum_frp": 0})
        d["label"] = months[m-1]
        series.append(d)
    data = {"series": series, "filters": {"year": year, "bbox": b, "from": from_date, "to": to_date}}
    return _cache_set(key, data)



@app.get("/")
def index():
    p = FRONTEND_DIR / "index.html"
    if p.exists():
        return FileResponse(p)
    return {"message": "Frontend not found. Open /docs for API."}
