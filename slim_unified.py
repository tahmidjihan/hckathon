#!/usr/bin/env python3
"""Build slim unified dataset (separate events + fires, with bbox) from data/unified."""
from pathlib import Path
import duckdb, json, sys, time

SRC_EVENTS = Path("data/unified/events")
SRC_FIRES = Path("data/unified/fires")
DST = Path("data/unified_slim")
DST_EVENTS = DST / "events"
DST_FIRES = DST / "fires"
DST_EVENTS.mkdir(parents=True, exist_ok=True)
DST_FIRES.mkdir(parents=True, exist_ok=True)

EVENTS_PROJ = """event_id, fire_id, event_date, first_dt, last_dt,
  CAST(latitude AS FLOAT) AS latitude, CAST(longitude AS FLOAT) AS longitude,
  n_detections, detected_by, sources, max_frp, sum_frp, daynight"""
FIRES_PROJ = """fire_id, start_date, end_date, n_events, n_detections,
  CAST(latitude AS FLOAT) AS latitude, CAST(longitude AS FLOAT) AS longitude,
  CAST(lat_min AS FLOAT) AS lat_min, CAST(lat_max AS FLOAT) AS lat_max,
  CAST(lon_min AS FLOAT) AS lon_min, CAST(lon_max AS FLOAT) AS lon_max,
  max_frp, sum_frp, sources"""

def build_table(src_dir: Path, dst_dir: Path, proj: str, prefix: str):
    con = duckdb.connect()
    files = sorted(src_dir.glob(f"{prefix}_*.parquet"))
    total_src = 0
    total_dst = 0
    rows_total = 0
    for src in files:
        year = src.stem.split("_")[1]
        dst = dst_dir / f"{prefix}_{year}.parquet"
        t0 = time.time()
        con.execute(f"""COPY (SELECT {proj} FROM '{src}') TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 19, ROW_GROUP_SIZE 200000)""")
        rows = con.execute(f"SELECT count(*) FROM '{dst}'").fetchone()[0]
        s = src.stat().st_size
        d = dst.stat().st_size
        total_src += s; total_dst += d; rows_total += rows
        print(f"{prefix} {year}: {rows} rows, {s/1e6:.1f} MB -> {d/1e6:.1f} MB, {time.time()-t0:.1f}s", flush=True)
    print(f"{prefix} TOTAL: {rows_total} rows, {total_src/1e9:.2f} GB -> {total_dst/1e9:.2f} GB", flush=True)
    return rows_total, total_src, total_dst

def build_catalog():
    con = duckdb.connect(str(DST / "wildfire.duckdb"))
    con.execute(f"CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{DST_EVENTS}/events_*.parquet')")
    con.execute(f"CREATE OR REPLACE VIEW fires AS SELECT * FROM read_parquet('{DST_FIRES}/fires_*.parquet')")
    con.execute("CREATE OR REPLACE VIEW meta AS SELECT (SELECT count(*) FROM events) AS events, (SELECT count(*) FROM fires) AS fires")
    con.execute("CREATE OR REPLACE MACRO find_event(id) AS TABLE SELECT * FROM events WHERE event_id = id")
    con.execute("CREATE OR REPLACE MACRO find_fire(id) AS TABLE SELECT * FROM fires WHERE fire_id = id")
    con.execute("CREATE OR REPLACE MACRO events_for_fire(id) AS TABLE SELECT * FROM events WHERE fire_id = id ORDER BY first_dt")
    con.close()


if __name__ == "__main__":
    t0 = time.time()
    re_, rs, rd = build_table(SRC_EVENTS, DST_EVENTS, EVENTS_PROJ, "events")
    rf, fs, fd = build_table(SRC_FIRES, DST_FIRES, FIRES_PROJ, "fires")
    build_catalog()
    verify = duckdb.connect(str(DST / "wildfire.duckdb"), read_only=True)
    ve = verify.execute("SELECT count(*) FROM events").fetchone()[0]
    vf = verify.execute("SELECT count(*) FROM fires").fetchone()[0]
    print(f"VERIFY events {ve} fires {vf}")
    manifest = {
        "generated_at": datetime_now(),
        "source": "data/unified",
        "events_rows": ve, "fires_rows": vf,
        "events_schema": ["event_id","fire_id","event_date","first_dt","last_dt","latitude","longitude","n_detections","detected_by","sources","max_frp","sum_frp","daynight"],
        "fires_schema": ["fire_id","start_date","end_date","n_events","n_detections","latitude","longitude","lat_min","lat_max","lon_min","lon_max","max_frp","sum_frp","sources"],
        "compression": "ZSTD level 19, row_group_size 200000",
        "note": "Coordinates/bbox stored as FLOAT (float32), FRP kept as DOUBLE for precision.",
        "sizes": {"events_src": rs, "events_dst": rd, "fires_src": fs, "fires_dst": fd},
        "elapsed_seconds": round(time.time()-t0,1),
    }
    (DST / "build_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))

def datetime_now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
