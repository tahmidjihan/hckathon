#!/usr/bin/env python3
"""
Unified Global Wildfire Dataset Builder
=======================================

Builds a single, de-duplicated wildfire dataset from MODIS (Terra/Aqua) and
VIIRS (Suomi-NPP, NOAA-20, NOAA-21) yearly CSVs.

Three layers
------------
1. staging/  : normalized, exact-deduplicated raw detections (Parquet).
2. events/   : one row per near-simultaneous fire observation cluster
               (1.5 km / 4 h, capped at one UTC day), with `event_id`.
3. fires/    : one row per persistent fire, linking events across days
               (5 km / 3-day gap), with `fire_id`.

The DuckDB catalog exposes `detections`, `events`, and `fires` tables, plus
`find_event(id)` / `find_fire(id)` / `events_for_fire(id)` helpers.

Usage
-----
    python3 build_unified_wildfires.py --all
    python3 build_unified_wildfires.py --stage staging --years 2024 --force
    python3 build_unified_wildfires.py --stage events --years 2024 --force
    python3 build_unified_wildfires.py --stage fires --years 2024
    python3 build_unified_wildfires.py --stage catalog

Stages are resumable: staging and event files are skipped when they already
exist unless --force is supplied.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
EARTH_RADIUS_KM = 6371.0
KM_PER_DEG_LAT = 111.32
EVENT_RADIUS_KM = 1.5
EVENT_WINDOW_HOURS = 4.0
FIRE_RADIUS_KM = 5.0
FIRE_GAP_DAYS = 3

SOURCE_NAMES = [
    "MODIS_Terra",
    "MODIS_Aqua",
    "VIIRS_SNPP",
    "VIIRS_NOAA20",
    "VIIRS_NOAA21",
]
SOURCE_RANK = {name: i for i, name in enumerate(SOURCE_NAMES)}
SOURCE_SHORT = {
    "MODIS_Terra": "MODIS Terra",
    "MODIS_Aqua": "MODIS Aqua",
    "VIIRS_SNPP": "VIIRS Suomi-NPP",
    "VIIRS_NOAA20": "VIIRS NOAA-20",
    "VIIRS_NOAA21": "VIIRS NOAA-21",
}

# Neighbour offsets: centre first, then orthogonal, then diagonals.
OFFSETS = [
    (0, 0),
    (0, -1), (0, 1), (-1, 0), (1, 0),
    (-1, -1), (-1, 1), (1, -1), (1, 1),
]

CSV_COLUMNS = {
    "latitude": "DOUBLE",
    "longitude": "DOUBLE",
    "brightness": "DOUBLE",
    "scan": "DOUBLE",
    "track": "DOUBLE",
    "acq_date": "VARCHAR",
    "acq_time": "VARCHAR",
    "satellite": "VARCHAR",
    "instrument": "VARCHAR",
    "confidence": "VARCHAR",
    "version": "VARCHAR",
    "bright_t31": "DOUBLE",
    "frp": "DOUBLE",
    "daynight": "VARCHAR",
    "type": "INTEGER",
    "year": "INTEGER",
}

EVENT_COMMON_FIELDS = [
    ("event_id", pa.string()),
    ("event_date", pa.date32()),
    ("first_dt", pa.timestamp("us")),
    ("last_dt", pa.timestamp("us")),
    ("duration_minutes", pa.float32()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("n_detections", pa.int32()),
    ("n_sensors", pa.int8()),
    ("detected_by", pa.string()),
    ("sources", pa.string()),
    ("modis_terra_detections", pa.int32()),
    ("modis_aqua_detections", pa.int32()),
    ("viirs_snpp_detections", pa.int32()),
    ("viirs_noaa20_detections", pa.int32()),
    ("viirs_noaa21_detections", pa.int32()),
    ("max_frp", pa.float64()),
    ("mean_frp", pa.float64()),
    ("sum_frp", pa.float64()),
    ("max_brightness", pa.float64()),
    ("max_bright_t31", pa.float64()),
    ("mean_modis_confidence", pa.float64()),
    ("viirs_confidence_labels", pa.string()),
    ("daynight", pa.string()),
    ("type", pa.int32()),
]
EVENT_RAW_SCHEMA = pa.schema(EVENT_COMMON_FIELDS)
EVENT_SCHEMA = pa.schema(
    [("event_id", pa.string()), ("fire_id", pa.string())]
    + EVENT_COMMON_FIELDS[1:]
)

FIRE_SCHEMA = pa.schema([
    ("fire_id", pa.string()),
    ("start_date", pa.date32()),
    ("end_date", pa.date32()),
    ("duration_days", pa.int32()),
    ("n_events", pa.int32()),
    ("n_detections", pa.int32()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("lat_min", pa.float64()),
    ("lat_max", pa.float64()),
    ("lon_min", pa.float64()),
    ("lon_max", pa.float64()),
    ("max_frp", pa.float64()),
    ("mean_frp", pa.float64()),
    ("sum_frp", pa.float64()),
    ("n_sensors", pa.int8()),
    ("sources", pa.string()),
    ("modis_detections", pa.int32()),
    ("viirs_detections", pa.int32()),
    ("first_event_id", pa.string()),
    ("last_event_id", pa.string()),
    ("representative_event_id", pa.string()),
    ("representative_frp", pa.float64()),
])


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_years(spec: str) -> List[int]:
    years: List[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            years.extend(range(int(a), int(b) + 1))
        else:
            years.append(int(part))
    return sorted(set(years))


def discover_files(data_root: Path) -> Dict[int, List[Path]]:
    """Maps calendar year -> CSV files, using the trailing _YYYY.csv name."""
    by_year: Dict[int, List[Path]] = {}
    pat = re.compile(r"_(\d{4})\.csv$", re.IGNORECASE)
    for path in sorted(data_root.rglob("*.csv")):
        m = pat.search(path.name)
        if not m:
            continue
        year = int(m.group(1))
        by_year.setdefault(year, []).append(path)
    return by_year


def parquet_row_count(path: Path) -> int:
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return 0


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


class BatchParquetWriter:
    """Buffered Parquet writer with an explicit schema."""

    def __init__(self, path: Path, schema: pa.Schema, buffer_rows: int = 50_000):
        self.path = path
        self.schema = schema
        self.buffer_rows = buffer_rows
        self.rows: List[Dict[str, Any]] = []
        path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(str(path), schema, compression="zstd")

    def add(self, row: Dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.buffer_rows:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        self.writer.write_table(table)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        self.writer.close()


# ---------------------------------------------------------------------------
# Stage 1: staging (DuckDB CSV -> normalized Parquet)
# ---------------------------------------------------------------------------
def build_staging(
    years: Sequence[int],
    files_by_year: Dict[int, List[Path]],
    out_dir: Path,
    force: bool,
    memory_limit: str,
    threads: int,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Dict[str, Any]:
    assert duckdb is not None and pa is not None
    staging_root = out_dir / "staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_dir / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit={sql_quote(memory_limit)}")
    con.execute(f"SET temp_directory={sql_quote(str(tmp_dir))}")
    if threads > 0:
        con.execute(f"SET threads={threads}")

    time4_expr = (
        "CASE WHEN regexp_replace(CAST(acq_time AS VARCHAR), '[^0-9]', '', 'g') = '' "
        "THEN NULL ELSE lpad(substr(regexp_replace(CAST(acq_time AS VARCHAR), "
        "'[^0-9]', '', 'g'), 1, 4), 4, '0') END"
    )
    source_expr = (
        "CASE "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) = 'terra' THEN 'MODIS_Terra' "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) = 'aqua' THEN 'MODIS_Aqua' "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('snpp','suomi-npp','suomi_npp','npp') THEN 'VIIRS_SNPP' "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('n20','noaa-20','noaa20','jpss-1','jpss1') THEN 'VIIRS_NOAA20' "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('n21','noaa-21','noaa21','jpss-2','jpss2') THEN 'VIIRS_NOAA21' "
        "END"
    )
    source_rank_expr = (
        "CASE "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) = 'terra' THEN 0 "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) = 'aqua' THEN 1 "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('snpp','suomi-npp','suomi_npp','npp') THEN 2 "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('n20','noaa-20','noaa20','jpss-1','jpss1') THEN 3 "
        "WHEN lower(trim(CAST(satellite AS VARCHAR))) IN "
        "('n21','noaa-21','noaa21','jpss-2','jpss2') THEN 4 "
        "END"
    )

    date_filter = ""
    if start_date:
        date_filter += f" AND event_date >= DATE {sql_quote(start_date)}"
    if end_date:
        date_filter += f" AND event_date <= DATE {sql_quote(end_date)}"

    summary: Dict[str, Any] = {}
    for year in years:
        files = files_by_year.get(year, [])
        if not files:
            log(f"staging: no input files for {year}")
            continue

        out_path = staging_root / f"year={year}" / "detections.parquet"
        if out_path.exists() and not force:
            rows = parquet_row_count(out_path)
            summary[str(year)] = {
                "rows": rows,
                "bytes": file_size(out_path),
                "skipped": True,
                "input_files": [str(p) for p in files],
            }
            log(f"staging: {year} already exists ({rows:,} rows), skipping")
            continue

        out_path.parent.mkdir(parents=True, exist_ok=True)
        # DuckDB accepts a map literal for the columns option.
        col_map = ", ".join(f"'{k}': '{v}'" for k, v in CSV_COLUMNS.items())
        rel = (
            "read_csv([" + ", ".join(sql_quote(str(p)) for p in files) +
            f"], header=true, columns={{{col_map}}}, ignore_errors=true)"
        )

        sql = f"""
        COPY (
            WITH raw AS (SELECT * FROM {rel}),
            base AS (
                SELECT
                    {source_expr} AS source,
                    {source_rank_expr} AS source_rank,
                    CAST(satellite AS VARCHAR) AS satellite,
                    {time4_expr} AS acq_time4,
                    TRY_CAST(latitude AS DOUBLE) AS latitude,
                    TRY_CAST(longitude AS DOUBLE) AS longitude,
                    TRY_CAST(acq_date AS DATE) AS event_date,
                    TRY_CAST(acq_date AS VARCHAR) AS acq_date,
                    TRY_CAST(frp AS DOUBLE) AS frp,
                    TRY_CAST(brightness AS DOUBLE) AS brightness,
                    TRY_CAST(bright_t31 AS DOUBLE) AS bright_t31,
                    TRY_CAST(confidence AS DOUBLE) AS confidence_num,
                    lower(trim(CAST(confidence AS VARCHAR))) AS confidence_raw,
                    upper(trim(CAST(daynight AS VARCHAR))) AS daynight,
                    TRY_CAST(type AS INTEGER) AS type
                FROM raw
            ),
            norm AS (
                SELECT
                    source,
                    source_rank,
                    satellite,
                    latitude,
                    longitude,
                    event_date,
                    acq_date,
                    acq_time4 AS acq_time,
                    TRY_STRPTIME(
                        acq_date || ' ' || acq_time4, '%Y-%m-%d %H%M'
                    ) AS dt,
                    frp,
                    brightness,
                    bright_t31,
                    CASE WHEN source LIKE 'MODIS%' THEN confidence_num END
                        AS modis_confidence_pct,
                    CASE WHEN confidence_raw IN ('l', 'n', 'h')
                         THEN upper(confidence_raw) END AS viirs_confidence_label,
                    CASE WHEN daynight IN ('D', 'N') THEN daynight END AS daynight,
                    type
                FROM base
                WHERE source IS NOT NULL
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                  AND latitude BETWEEN -90 AND 90
                  AND longitude BETWEEN -180 AND 180
                  AND event_date IS NOT NULL
                  AND acq_time4 IS NOT NULL
                  AND type = 0
                  AND year(event_date) = {year}
                  {date_filter}
            )
            SELECT
                source,
                satellite,
                CASE WHEN source LIKE 'MODIS%' THEN 'MODIS' ELSE 'VIIRS' END
                    AS instrument,
                latitude, longitude, dt, event_date, acq_date, acq_time,
                frp, brightness, bright_t31,
                modis_confidence_pct, viirs_confidence_label, daynight, type
            FROM norm
            WHERE dt IS NOT NULL
            QUALIFY row_number() OVER (
                PARTITION BY source, dt, latitude, longitude
                ORDER BY frp DESC NULLS LAST, brightness DESC NULLS LAST
            ) = 1
            ORDER BY dt, source_rank, latitude, longitude,
                     frp DESC NULLS LAST, brightness DESC NULLS LAST
        ) TO {sql_quote(str(out_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)
        """
        t0 = time.time()
        log(f"staging: building {year} from {len(files)} files -> {out_path}")
        con.execute(sql)
        rows = parquet_row_count(out_path)
        elapsed = time.time() - t0
        summary[str(year)] = {
            "rows": rows,
            "bytes": file_size(out_path),
            "seconds": round(elapsed, 1),
            "input_files": [str(p) for p in files],
        }
        log(f"staging: {year} done: {rows:,} rows, {file_size(out_path)/1e6:.1f} MB, {elapsed:.1f}s")

    con.close()
    return summary


# ---------------------------------------------------------------------------
# Stage 2: events (streaming 1.5 km / 4 h clustering)
# ---------------------------------------------------------------------------
class EventAgg:
    __slots__ = (
        "eid", "event_date", "first_dt", "last_dt", "lat_sum", "lon_sum",
        "n", "max_frp", "sum_frp", "frp_n", "max_b", "max_t31",
        "counts", "modis_conf_sum", "modis_conf_n", "viirs_labels",
        "daynight", "ftype",
    )

    def __init__(self, eid: str, event_date: date, dt: datetime, lat: float, lon: float):
        self.eid = eid
        self.event_date = event_date
        self.first_dt = dt
        self.last_dt = dt
        self.lat_sum = 0.0
        self.lon_sum = 0.0
        self.n = 0
        self.max_frp: Optional[float] = None
        self.sum_frp = 0.0
        self.frp_n = 0
        self.max_b: Optional[float] = None
        self.max_t31: Optional[float] = None
        self.counts = [0] * 5
        self.modis_conf_sum = 0.0
        self.modis_conf_n = 0
        self.viirs_labels = set()
        self.daynight: Optional[str] = None
        self.ftype: Optional[int] = None

    def add(
        self,
        rank: int,
        dt: datetime,
        lat: float,
        lon: float,
        frp: Optional[float],
        brightness: Optional[float],
        t31: Optional[float],
        conf_pct: Optional[float],
        conf_label: Optional[str],
        daynight: Optional[str],
        ftype: Optional[int],
    ) -> None:
        self.lat_sum += lat
        self.lon_sum += lon
        self.n += 1
        if dt < self.first_dt:
            self.first_dt = dt
        if dt > self.last_dt:
            self.last_dt = dt
        if frp is not None:
            self.sum_frp += frp
            self.frp_n += 1
            if self.max_frp is None or frp > self.max_frp:
                self.max_frp = frp
        if brightness is not None and (self.max_b is None or brightness > self.max_b):
            self.max_b = brightness
        if t31 is not None and (self.max_t31 is None or t31 > self.max_t31):
            self.max_t31 = t31
        self.counts[rank] += 1
        if conf_pct is not None:
            self.modis_conf_sum += conf_pct
            self.modis_conf_n += 1
        if conf_label:
            self.viirs_labels.add(conf_label)
        if daynight == "D":
            self.daynight = "D"
        elif self.daynight is None and daynight == "N":
            self.daynight = "N"
        if self.ftype is None and ftype is not None:
            self.ftype = ftype

    def to_row(self) -> Dict[str, Any]:
        n = max(1, self.n)
        counts = self.counts
        has_modis = (counts[0] + counts[1]) > 0
        has_viirs = (counts[2] + counts[3] + counts[4]) > 0
        detected_by = (
            "MODIS+VIIRS" if has_modis and has_viirs
            else "MODIS" if has_modis
            else "VIIRS"
        )
        sources = [SOURCE_NAMES[i] for i, c in enumerate(counts) if c > 0]
        n_sensors = sum(1 for c in counts if c > 0)
        return {
            "event_id": self.eid,
            "event_date": self.event_date,
            "first_dt": self.first_dt,
            "last_dt": self.last_dt,
            "duration_minutes": round(
                (self.last_dt - self.first_dt).total_seconds() / 60.0, 1
            ),
            "latitude": round(self.lat_sum / n, 6),
            "longitude": round(self.lon_sum / n, 6),
            "n_detections": self.n,
            "n_sensors": n_sensors,
            "detected_by": detected_by,
            "sources": ",".join(sources),
            "modis_terra_detections": counts[0],
            "modis_aqua_detections": counts[1],
            "viirs_snpp_detections": counts[2],
            "viirs_noaa20_detections": counts[3],
            "viirs_noaa21_detections": counts[4],
            "max_frp": round(self.max_frp, 2) if self.max_frp is not None else None,
            "mean_frp": round(self.sum_frp / self.frp_n, 2) if self.frp_n else None,
            "sum_frp": round(self.sum_frp, 2) if self.frp_n else None,
            "max_brightness": round(self.max_b, 2) if self.max_b is not None else None,
            "max_bright_t31": round(self.max_t31, 2) if self.max_t31 is not None else None,
            "mean_modis_confidence": (
                round(self.modis_conf_sum / self.modis_conf_n, 2)
                if self.modis_conf_n else None
            ),
            "viirs_confidence_labels": ",".join(sorted(self.viirs_labels)) or None,
            "daynight": self.daynight,
            "type": self.ftype,
        }


def build_events_year(
    staging_path: Path,
    out_path: Path,
    radius_km: float,
    window_hours: float,
) -> Dict[str, Any]:
    """Clusters one year of staging detections into events."""
    t0 = time.time()
    window = timedelta(hours=window_hours)
    radius_sq = radius_km * radius_km
    cell_deg = radius_km / KM_PER_DEG_LAT

    writer = BatchParquetWriter(out_path, EVENT_RAW_SCHEMA, buffer_rows=50_000)

    active: Dict[str, EventAgg] = {}
    grid: Dict[Tuple[int, int], Dict[int, Tuple]] = {}
    expiry: deque = deque()
    pid = 0
    seq = 0
    year: Optional[int] = None
    current_date: Optional[date] = None
    detections = 0

    def flush_day() -> None:
        for ev in active.values():
            writer.add(ev.to_row())

    pf = pq.ParquetFile(str(staging_path))
    for batch in pf.iter_batches(batch_size=150_000):
        d = batch.to_pydict()
        lats = d["latitude"]
        lons = d["longitude"]
        dts = d["dt"]
        dates = d["event_date"]
        sources = d["source"]
        frps = d["frp"]
        brights = d["brightness"]
        t31s = d["bright_t31"]
        confs = d["modis_confidence_pct"]
        labels = d["viirs_confidence_label"]
        daynights = d["daynight"]
        types = d["type"]
        n = len(lats)

        for i in range(n):
            lat = lats[i]
            lon = lons[i]
            dt = dts[i]
            edate = dates[i]
            src = sources[i]
            detections += 1

            if current_date is None:
                current_date = edate
                year = edate.year
            elif edate != current_date:
                flush_day()
                active.clear()
                grid.clear()
                expiry.clear()
                current_date = edate
                if edate.year != year:
                    year = edate.year
                    seq = 0

            cutoff = dt - window
            while expiry and expiry[0][0] < cutoff:
                _, cellkey, p = expiry.popleft()
                cell = grid.get(cellkey)
                if cell is not None:
                    cell.pop(p[4], None)

            coslat = max(0.05, math.cos(math.radians(lat)))
            lon_cell_deg = cell_deg / coslat
            cy = int(math.floor(lat / cell_deg))
            cx = int(math.floor(lon / lon_cell_deg))

            found_eid: Optional[str] = None
            for oy, ox in OFFSETS:
                cell = grid.get((cy + oy, cx + ox))
                if not cell:
                    continue
                for p in cell.values():
                    if dt - p[0] > window:
                        continue
                    dlat = (lat - p[2]) * KM_PER_DEG_LAT
                    dlon = (lon - p[3]) * KM_PER_DEG_LAT * coslat
                    if dlat * dlat + dlon * dlon <= radius_sq:
                        found_eid = p[1]
                        break
                if found_eid is not None:
                    break

            if found_eid is None:
                seq += 1
                found_eid = f"EVT-{year}-{seq:07d}"
                ev = EventAgg(found_eid, edate, dt, lat, lon)
                active[found_eid] = ev
            else:
                ev = active[found_eid]

            ev.add(
                SOURCE_RANK[src], dt, lat, lon,
                frps[i], brights[i], t31s[i],
                confs[i], labels[i], daynights[i], types[i],
            )

            pid += 1
            cellkey = (cy, cx)
            cell = grid.get(cellkey)
            if cell is None:
                cell = {}
                grid[cellkey] = cell
            point = (dt, found_eid, lat, lon, pid)
            cell[pid] = point
            expiry.append((dt, cellkey, point))

    flush_day()
    active.clear()
    grid.clear()
    expiry.clear()
    writer.close()

    rows = parquet_row_count(out_path)
    elapsed = time.time() - t0
    return {
        "detections": detections,
        "events": rows,
        "bytes": file_size(out_path),
        "seconds": round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# Stage 3: fires (streaming 5 km / 3-day link across events)
# ---------------------------------------------------------------------------
class FireAgg:
    __slots__ = (
        "fid", "start_date", "last_date", "lat_sum", "lon_sum", "n_det",
        "n_events", "max_frp", "sum_frp", "counts",
        "lat_min", "lat_max", "lon_min", "lon_max",
        "first_event", "last_event", "rep_event", "rep_frp", "cellkey",
    )

    def __init__(self, fid: str, event_date: date, lat: float, lon: float,
                 n_det: int, event_id: str, max_frp: Optional[float],
                 sum_frp: float, counts: Sequence[int]):
        self.fid = fid
        self.start_date = event_date
        self.last_date = event_date
        self.lat_sum = lat * n_det
        self.lon_sum = lon * n_det
        self.n_det = n_det
        self.n_events = 1
        self.max_frp = max_frp
        self.sum_frp = sum_frp
        self.counts = list(counts)
        self.lat_min = self.lat_max = lat
        self.lon_min = self.lon_max = lon
        self.first_event = event_id
        self.last_event = event_id
        self.rep_event = event_id
        self.rep_frp = max_frp
        self.cellkey: Optional[Tuple[int, int]] = None

    def add_event(self, event_date: date, lat: float, lon: float, n_det: int,
                  event_id: str, max_frp: Optional[float], sum_frp: float,
                  counts: Sequence[int]) -> None:
        self.last_date = event_date
        self.lat_sum += lat * n_det
        self.lon_sum += lon * n_det
        self.n_det += n_det
        self.n_events += 1
        if max_frp is not None and (self.max_frp is None or max_frp > self.max_frp):
            self.max_frp = max_frp
        self.sum_frp += sum_frp
        for i in range(5):
            self.counts[i] += counts[i]
        if lat < self.lat_min:
            self.lat_min = lat
        if lat > self.lat_max:
            self.lat_max = lat
        if lon < self.lon_min:
            self.lon_min = lon
        if lon > self.lon_max:
            self.lon_max = lon
        self.last_event = event_id
        if max_frp is not None and (self.rep_frp is None or max_frp > self.rep_frp):
            self.rep_event = event_id
            self.rep_frp = max_frp

    @property
    def centroid(self) -> Tuple[float, float]:
        n = max(1, self.n_det)
        return self.lat_sum / n, self.lon_sum / n

    def to_row(self) -> Dict[str, Any]:
        lat, lon = self.centroid
        counts = self.counts
        modis = counts[0] + counts[1]
        viirs = counts[2] + counts[3] + counts[4]
        sources = [SOURCE_NAMES[i] for i, c in enumerate(counts) if c > 0]
        return {
            "fire_id": self.fid,
            "start_date": self.start_date,
            "end_date": self.last_date,
            "duration_days": (self.last_date - self.start_date).days + 1,
            "n_events": self.n_events,
            "n_detections": self.n_det,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "lat_min": round(self.lat_min, 6),
            "lat_max": round(self.lat_max, 6),
            "lon_min": round(self.lon_min, 6),
            "lon_max": round(self.lon_max, 6),
            "max_frp": round(self.max_frp, 2) if self.max_frp is not None else None,
            "mean_frp": round(self.sum_frp / self.n_det, 2) if self.n_det else None,
            "sum_frp": round(self.sum_frp, 2),
            "n_sensors": sum(1 for c in counts if c > 0),
            "sources": ",".join(sources),
            "modis_detections": modis,
            "viirs_detections": viirs,
            "first_event_id": self.first_event,
            "last_event_id": self.last_event,
            "representative_event_id": self.rep_event,
            "representative_frp": (
                round(self.rep_frp, 2) if self.rep_frp is not None else None
            ),
        }


def build_fires(
    events_by_year: Sequence[Tuple[int, Path]],
    out_dir: Path,
    radius_km: float,
    gap_days: int,
    force: bool,
) -> Dict[str, Any]:
    t0 = time.time()
    events_out = out_dir / "events"
    fires_out = out_dir / "fires"
    events_out.mkdir(parents=True, exist_ok=True)
    fires_out.mkdir(parents=True, exist_ok=True)

    event_writers: Dict[int, BatchParquetWriter] = {}
    fire_writers: Dict[int, BatchParquetWriter] = {}
    active: Dict[str, FireAgg] = {}
    grid: Dict[Tuple[int, int], Dict[str, None]] = {}
    expiry: deque = deque()
    seq_by_year: Dict[int, int] = {}
    cell_deg = radius_km / KM_PER_DEG_LAT
    radius_sq = radius_km * radius_km
    gap = timedelta(days=gap_days)
    n_events = 0
    n_fires_finalized = 0

    def writer_for_event_year(year: int) -> BatchParquetWriter:
        if year not in event_writers:
            path = events_out / f"events_{year}.parquet"
            if path.exists():
                path.unlink()
            event_writers[year] = BatchParquetWriter(path, EVENT_SCHEMA)
        return event_writers[year]

    def writer_for_fire_year(year: int) -> BatchParquetWriter:
        if year not in fire_writers:
            path = fires_out / f"fires_{year}.parquet"
            if path.exists():
                path.unlink()
            fire_writers[year] = BatchParquetWriter(path, FIRE_SCHEMA)
        return fire_writers[year]

    def finalize_fire(fid: str) -> None:
        nonlocal n_fires_finalized
        fire = active.pop(fid, None)
        if fire is None:
            return
        if fire.cellkey is not None:
            cell = grid.get(fire.cellkey)
            if cell is not None:
                cell.pop(fid, None)
        writer_for_fire_year(fire.start_date.year).add(fire.to_row())
        n_fires_finalized += 1

    for year, path in events_by_year:
        if not path.exists():
            continue
        pf = pq.ParquetFile(str(path))
        for batch in pf.iter_batches(batch_size=100_000):
            d = batch.to_pydict()
            n = len(d["event_id"])
            for i in range(n):
                edate = d["event_date"][i]
                lat = d["latitude"][i]
                lon = d["longitude"][i]
                n_det = d["n_detections"][i]
                eid = d["event_id"][i]
                max_frp = d["max_frp"][i]
                sum_frp = d["sum_frp"][i] or 0.0
                counts = (
                    d["modis_terra_detections"][i],
                    d["modis_aqua_detections"][i],
                    d["viirs_snpp_detections"][i],
                    d["viirs_noaa20_detections"][i],
                    d["viirs_noaa21_detections"][i],
                )

                cutoff = edate - gap
                while expiry and expiry[0][0] < cutoff:
                    popped_date, fid = expiry.popleft()
                    fire = active.get(fid)
                    if fire is not None and fire.last_date == popped_date:
                        finalize_fire(fid)

                coslat = max(0.05, math.cos(math.radians(lat)))
                lon_cell_deg = cell_deg / coslat
                cy = int(math.floor(lat / cell_deg))
                cx = int(math.floor(lon / lon_cell_deg))

                best_fid: Optional[str] = None
                best_d2 = radius_sq
                for oy, ox in OFFSETS:
                    cell = grid.get((cy + oy, cx + ox))
                    if not cell:
                        continue
                    for fid in cell.keys():
                        fire = active.get(fid)
                        if fire is None or fire.last_date < cutoff:
                            continue
                        flat, flon = fire.centroid
                        dlat = (lat - flat) * KM_PER_DEG_LAT
                        dlon = (lon - flon) * KM_PER_DEG_LAT * coslat
                        d2 = dlat * dlat + dlon * dlon
                        if d2 <= best_d2:
                            best_d2 = d2
                            best_fid = fid

                if best_fid is None:
                    seq_by_year[edate.year] = seq_by_year.get(edate.year, 0) + 1
                    fid = f"FIRE-{edate.year}-{seq_by_year[edate.year]:06d}"
                    fire = FireAgg(
                        fid, edate, lat, lon, n_det, eid, max_frp,
                        sum_frp, counts,
                    )
                    active[fid] = fire
                else:
                    fid = best_fid
                    fire = active[fid]
                    fire.add_event(
                        edate, lat, lon, n_det, eid, max_frp, sum_frp, counts
                    )

                # re-index the fire if its centroid moved to another cell
                new_lat, new_lon = fire.centroid
                new_cy = int(math.floor(new_lat / cell_deg))
                new_cx = int(math.floor(new_lon / (
                    cell_deg / max(0.05, math.cos(math.radians(new_lat)))
                )))
                new_key = (new_cy, new_cx)
                if fire.cellkey != new_key:
                    if fire.cellkey is not None:
                        old_cell = grid.get(fire.cellkey)
                        if old_cell is not None:
                            old_cell.pop(fid, None)
                    cell = grid.get(new_key)
                    if cell is None:
                        cell = {}
                        grid[new_key] = cell
                    cell[fid] = None
                    fire.cellkey = new_key

                expiry.append((fire.last_date, fid))

                row = {k: d[k][i] for k, _ in EVENT_COMMON_FIELDS if k in d}
                row["fire_id"] = fid
                writer_for_event_year(edate.year).add(row)
                n_events += 1

    for fid in list(active.keys()):
        finalize_fire(fid)

    for writer in event_writers.values():
        writer.close()
    for writer in fire_writers.values():
        writer.close()

    elapsed = time.time() - t0
    return {
        "events_processed": n_events,
        "fires_finalized": n_fires_finalized,
        "seconds": round(elapsed, 1),
        "event_files": {str(y): file_size(p) for y, p in sorted(
            (y, events_out / f"events_{y}.parquet") for y in event_writers
        )},
        "fire_files": {str(y): file_size(p) for y, p in sorted(
            (y, fires_out / f"fires_{y}.parquet") for y in fire_writers
        )},
    }


# ---------------------------------------------------------------------------
# Stage 4: DuckDB catalog
# ---------------------------------------------------------------------------
def build_catalog(
    out_dir: Path,
    memory_limit: str,
    threads: int,
    force: bool,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    assert duckdb is not None
    db_path = out_dir / "wildfire.duckdb"
    if db_path.exists():
        if not force:
            log(f"catalog: {db_path} exists (use --force to rebuild)")
            return {"path": str(db_path), "skipped": True}
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute(f"SET memory_limit={sql_quote(memory_limit)}")
    con.execute(f"SET temp_directory={sql_quote(str(out_dir / '_tmp'))}")
    if threads > 0:
        con.execute(f"SET threads={threads}")

    events_glob = str(out_dir / "events" / "events_*.parquet")
    fires_glob = str(out_dir / "fires" / "fires_*.parquet")
    staging_glob = str(out_dir / "staging" / "year=*" / "detections.parquet")

    log("catalog: materializing events table")
    con.execute(
        f"CREATE TABLE events AS SELECT * FROM read_parquet({sql_quote(events_glob)})"
    )
    log("catalog: materializing fires table")
    con.execute(
        f"CREATE TABLE fires AS SELECT * FROM read_parquet({sql_quote(fires_glob)})"
    )

    log("catalog: creating staging view")
    con.execute(
        "CREATE VIEW detections AS SELECT * FROM read_parquet("
        f"{sql_quote(staging_glob)}, hive_partitioning=true)"
    )

    log("catalog: creating indexes")
    con.execute("CREATE INDEX idx_events_event_id ON events(event_id)")
    con.execute("CREATE INDEX idx_events_fire_id ON events(fire_id)")
    con.execute("CREATE INDEX idx_events_date ON events(event_date)")
    con.execute("CREATE INDEX idx_fires_fire_id ON fires(fire_id)")
    con.execute("CREATE INDEX idx_fires_start_date ON fires(start_date)")

    con.execute(
        "CREATE MACRO find_event(eid) AS TABLE "
        "SELECT * FROM events WHERE event_id = eid"
    )
    con.execute(
        "CREATE MACRO find_fire(fid) AS TABLE "
        "SELECT * FROM fires WHERE fire_id = fid"
    )
    con.execute(
        "CREATE MACRO events_for_fire(fid) AS TABLE "
        "SELECT * FROM events WHERE fire_id = fid ORDER BY first_dt"
    )

    con.execute("CREATE TABLE meta(key VARCHAR, value VARCHAR)")
    meta = {
        "dataset": "Unified global wildfire dataset",
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "event_radius_km": str(config["event_radius_km"]),
        "event_window_hours": str(config["event_window_hours"]),
        "fire_radius_km": str(config["fire_radius_km"]),
        "fire_gap_days": str(config["fire_gap_days"]),
        "event_definition": "1.5 km / 4 h / single UTC day clustering",
        "fire_definition": "5 km / 3-day-gap linking of events",
        "event_id_format": "EVT-<year>-<7-digit sequence>",
        "fire_id_format": "FIRE-<start year>-<6-digit sequence>",
    }
    con.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    con.execute("CHECKPOINT")
    con.close()

    return {
        "path": str(db_path),
        "bytes": file_size(db_path),
        "events": parquet_row_count_files(out_dir / "events", "events_*.parquet"),
        "fires": parquet_row_count_files(out_dir / "fires", "fires_*.parquet"),
    }


def parquet_row_count_files(directory: Path, pattern: str) -> int:
    return sum(parquet_row_count(p) for p in sorted(directory.glob(pattern)))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def load_manifest(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def save_manifest(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a single de-duplicated global wildfire dataset "
                    "from MODIS + VIIRS yearly CSVs."
    )
    parser.add_argument("--data-root", default="data/tu")
    parser.add_argument("--out-dir", default="data/unified")
    parser.add_argument(
        "--stage", choices=["all", "staging", "events", "fires", "catalog"],
        default="all",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Run every stage (same as --stage all)",
    )
    parser.add_argument("--years", default=None, help="e.g. 2000-2026 or 2024")
    parser.add_argument("--start-date", default=None, help="staging date filter")
    parser.add_argument("--end-date", default=None, help="staging date filter")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild outputs even when they exist")
    parser.add_argument("--event-radius-km", type=float, default=EVENT_RADIUS_KM)
    parser.add_argument("--event-window-hours", type=float, default=EVENT_WINDOW_HOURS)
    parser.add_argument("--fire-radius-km", type=float, default=FIRE_RADIUS_KM)
    parser.add_argument("--fire-gap-days", type=int, default=FIRE_GAP_DAYS)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--threads", type=int, default=0)
    args = parser.parse_args()

    if duckdb is None or pa is None or pq is None:
        log("ERROR: duckdb and pyarrow are required. "
            "Install with: python3 -m pip install --user duckdb pyarrow")
        sys.exit(1)

    data_root = Path(args.data_root).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "build_manifest.json"
    manifest = load_manifest(manifest_path)
    manifest.setdefault("config", {})
    manifest["config"].update({
        "data_root": str(data_root),
        "out_dir": str(out_dir),
        "event_radius_km": args.event_radius_km,
        "event_window_hours": args.event_window_hours,
        "fire_radius_km": args.fire_radius_km,
        "fire_gap_days": args.fire_gap_days,
    })

    files_by_year = discover_files(data_root)
    all_years = sorted(files_by_year)
    years = parse_years(args.years) if args.years else all_years
    log(f"input: {len(files_by_year)} years, {len(all_years)} total; "
        f"selected {years[0] if years else '-'}..{years[-1] if years else '-'}")

    stages = ["staging", "events", "fires", "catalog"] if args.stage == "all" else [args.stage]

    if "staging" in stages:
        manifest["staging"] = build_staging(
            years, files_by_year, out_dir, args.force,
            args.memory_limit, args.threads, args.start_date, args.end_date,
        )
        save_manifest(manifest_path, manifest)

    if "events" in stages:
        staging_dir = out_dir / "staging"
        events_raw = out_dir / "events_raw"
        events_raw.mkdir(parents=True, exist_ok=True)
        summary: Dict[str, Any] = {}
        for year in years:
            start = staging_dir / f"year={year}" / "detections.parquet"
            if not start.exists():
                continue
            dst = events_raw / f"events_{year}.parquet"
            if dst.exists() and not args.force:
                rows = parquet_row_count(dst)
                summary[str(year)] = {"events": rows, "skipped": True,
                                      "bytes": file_size(dst)}
                log(f"events: {year} exists ({rows:,} events), skipping")
                continue
            log(f"events: clustering {year} ...")
            stats = build_events_year(
                start, dst, args.event_radius_km, args.event_window_hours
            )
            summary[str(year)] = stats
            log(f"events: {year} done: {stats['detections']:,} detections -> "
                f"{stats['events']:,} events in {stats['seconds']:.0f}s")
            manifest["events"] = summary
            save_manifest(manifest_path, manifest)
        manifest["events"] = summary
        save_manifest(manifest_path, manifest)

    if "fires" in stages:
        events_raw = out_dir / "events_raw"
        # Link every available event year so fires can span year boundaries.
        pairs = []
        for path in sorted(events_raw.glob("events_*.parquet")):
            try:
                pairs.append((int(path.stem.split("_")[-1]), path))
            except ValueError:
                continue
        log(f"fires: linking {len(pairs)} event files "
            f"({pairs[0][0]}..{pairs[-1][0]})" if pairs else "fires: no event files")
        manifest["fires"] = build_fires(
            pairs, out_dir, args.fire_radius_km, args.fire_gap_days, args.force
        )
        save_manifest(manifest_path, manifest)

    if "catalog" in stages:
        manifest["catalog"] = build_catalog(
            out_dir, args.memory_limit, args.threads, args.force, manifest["config"]
        )
        manifest["built_at"] = datetime.now().isoformat(timespec="seconds")
        save_manifest(manifest_path, manifest)

    log(f"manifest: {manifest_path}")
    log("done")


if __name__ == "__main__":
    main()
