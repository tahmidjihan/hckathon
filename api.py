"""Slim wildfire API backed by local slim Parquet (fast) with R2 fallback.

Endpoints:
  GET /health
  GET /years
  GET /fires?year=&bbox=&from=&to=&lat=&lon=&radius_km=&limit=&offset=
  GET /fires/{fire_id}
  GET /fires/{fire_id}/events
  GET /events?year=&bbox=&from=&to=&limit=&offset=
  GET /events/{event_id}
  GET /                           -> frontend
"""
from __future__ import annotations

import math
import threading
from functools import lru_cache
from pathlib import Path
from typing import Optional

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
        names = [d[0] for d in cur.description]
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
    return tuple(out)  # hashable for cache; returned as list below


@app.get("/years")
def years():
    return list(_years_cached())


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
):
    b = _bbox_params(bbox)
    if b is None and lat is not None and lon is not None and radius_km is not None:
        dlat = radius_km / 111.0
        dlon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))
        b = [lat - dlat, lon - dlon, lat + dlat, lon + dlon]
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, lat_min, lat_max, lon_min, lon_max, max_frp, sum_frp, sources"
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
    # if user gave from/to but no year, try to narrow to year range for speed
    path = fires_path(year)
    if year is None and from_date and to_date and from_date[:4] == to_date[:4]:
        path = fires_path(int(from_date[:4]))
    elif year is None and from_date and not to_date:
        path = fires_path(int(from_date[:4]))
    sql = f"SELECT {cols} FROM read_parquet('{path}') {clause} ORDER BY start_date DESC, fire_id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    names, rows = _query(sql, params)
    return {"count": len(rows), "results": [dict(zip(names, r)) for r in rows]}


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
    path = events_path(year)
    if year is None and from_date and to_date and from_date[:4] == to_date[:4]:
        path = events_path(int(from_date[:4]))
    elif year is None and from_date and not to_date:
        path = events_path(int(from_date[:4]))
    sql = f"SELECT {cols} FROM read_parquet('{path}') {clause} ORDER BY event_date DESC, event_id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    names, rows = _query(sql, params)
    return {"count": len(rows), "results": [dict(zip(names, r)) for r in rows]}


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


@app.get("/")
def index():
    p = FRONTEND_DIR / "index.html"
    if p.exists():
        return FileResponse(p)
    return {"message": "Frontend not found. Open /docs for API."}
