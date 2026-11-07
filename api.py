"""Slim wildfire API backed by Cloudflare R2 Parquet (DuckDB httpfs).

Endpoints:
  GET /health
  GET /years                          -> row counts per year
  GET /fires?year=2024&limit=100      -> list fires for a year (or all)
  GET /fires/{fire_id}                -> one fire (routes by year in ID)
  GET /fires/{fire_id}/events         -> drill-down events
  GET /events?year=2024&limit=100
  GET /events/{event_id}
"""
from pathlib import Path
import duckdb
from fastapi import FastAPI, HTTPException, Query
from typing import Optional

app = FastAPI(title="Slim Wildfire API")

BUCKET = "nasa-hackathon"

def _load_env():
    vals = {}
    for line in Path(".env").read_text().splitlines():
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()
    return vals

_ENV = _load_env()

def get_con():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='enam';")
    con.execute(f"SET s3_endpoint='{_ENV['s3_endpoint']}';")
    con.execute(f"SET s3_access_key_id='{_ENV['access_key']}';")
    con.execute(f"SET s3_secret_access_key='{_ENV['secret_key']}';")
    con.execute("SET s3_url_style='path';")
    return con

def fires_path(year: Optional[int] = None):
    return f"s3://{BUCKET}/fires/fires_*.parquet" if year is None else f"s3://{BUCKET}/fires/fires_{year}.parquet"

def events_path(year: Optional[int] = None):
    return f"s3://{BUCKET}/events/events_*.parquet" if year is None else f"s3://{BUCKET}/events/events_{year}.parquet"

def year_from_id(idstr: str) -> int:
    try:
        return int(idstr.split("-")[1])
    except Exception:
        raise HTTPException(400, "ID must look like FIRE-2024-000001 or EVT-2024-0000001")

@app.get("/health")
def health():
    return {"status": "ok", "bucket": BUCKET}

@app.get("/years")
def years():
    con = get_con()
    out = []
    for y in range(2000, 2027):
        try:
            fc = con.execute(f"SELECT count(*) FROM read_parquet('{fires_path(y)}')").fetchone()[0]
        except Exception:
            fc = 0
        try:
            ec = con.execute(f"SELECT count(*) FROM read_parquet('{events_path(y)}')").fetchone()[0]
        except Exception:
            ec = 0
        out.append({"year": y, "fires": fc, "events": ec})
    return out

@app.get("/fires")
def list_fires(year: Optional[int] = None, limit: int = Query(100, le=10000), offset: int = 0):
    con = get_con()
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, lat_min, lat_max, lon_min, lon_max, max_frp, sum_frp, sources"
    rows = con.execute(f"SELECT {cols} FROM read_parquet('{fires_path(year)}') ORDER BY start_date, fire_id LIMIT ? OFFSET ?", [limit, offset]).fetchall()
    names = [d[0] for d in con.description]
    return [dict(zip(names, r)) for r in rows]

@app.get("/fires/{fire_id}")
def get_fire(fire_id: str):
    y = year_from_id(fire_id)
    con = get_con()
    cols = "fire_id, start_date, end_date, n_events, n_detections, latitude, longitude, lat_min, lat_max, lon_min, lon_max, max_frp, sum_frp, sources"
    # Search the start-year file first, then all years if not found
    for path in [fires_path(y), fires_path(None)]:
        rows = con.execute(f"SELECT {cols} FROM read_parquet('{path}') WHERE fire_id = ?", [fire_id]).fetchall()
        if rows:
            names = [d[0] for d in con.description]
            return dict(zip(names, rows[0]))
    raise HTTPException(404, "fire not found")

@app.get("/fires/{fire_id}/events")
def fire_events(fire_id: str, limit: int = Query(1000, le=10000)):
    con = get_con()
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    # Events can span years, so scan all event files (fast via predicates on fire_id)
    rows = con.execute(f"SELECT {cols} FROM read_parquet('{events_path(None)}') WHERE fire_id = ? ORDER BY first_dt LIMIT ?", [fire_id, limit]).fetchall()
    names = [d[0] for d in con.description]
    return [dict(zip(names, r)) for r in rows]

@app.get("/events")
def list_events(year: Optional[int] = None, limit: int = Query(100, le=10000), offset: int = 0):
    con = get_con()
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    rows = con.execute(f"SELECT {cols} FROM read_parquet('{events_path(year)}') ORDER BY event_date, event_id LIMIT ? OFFSET ?", [limit, offset]).fetchall()
    names = [d[0] for d in con.description]
    return [dict(zip(names, r)) for r in rows]

@app.get("/events/{event_id}")
def get_event(event_id: str):
    y = year_from_id(event_id)
    con = get_con()
    cols = "event_id, fire_id, event_date, first_dt, last_dt, latitude, longitude, n_detections, detected_by, sources, max_frp, sum_frp, daynight"
    for path in [events_path(y), events_path(None)]:
        rows = con.execute(f"SELECT {cols} FROM read_parquet('{path}') WHERE event_id = ?", [event_id]).fetchall()
        if rows:
            names = [d[0] for d in con.description]
            return dict(zip(names, rows[0]))
    raise HTTPException(404, "event not found")
