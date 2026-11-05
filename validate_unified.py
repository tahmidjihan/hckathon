#!/usr/bin/env python3
"""
Validate the unified wildfire dataset.

Checks
------
1. event_id and fire_id are unique.
2. Every event points at an existing fire.
3. Fire event/detection totals match the events table.
4. Detection conservation: per year, sum(events.n_detections) == staging rows.
   The trimmed final dataset has no staging layer, so this falls back to the
   per-year staging counts recorded in build_manifest.json.
5. Coordinates and dates are in valid ranges.
6. Fire date ranges contain their events.

Usage
-----
    python3 validate_unified.py
    python3 validate_unified.py --out-dir data/unified --db data/unified/wildfire.duckdb
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb is required (python3 -m pip install --user duckdb)")
    sys.exit(1)


def q(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()


def add(results: List[Dict[str, Any]], name: str, value: Any, expected: Any,
        passed: bool, detail: str = "") -> None:
    results.append({
        "check": name,
        "value": value,
        "expected": expected,
        "passed": bool(passed),
        "detail": detail,
    })


def staging_relation(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    prefer_catalog: bool,
) -> Optional[str]:
    """Return a SQL relation for staging/detections, or None if unavailable."""
    if prefer_catalog:
        try:
            con.execute("SELECT 1 FROM detections LIMIT 1")
            return "detections"
        except Exception:
            pass

    staging_files = list((out_dir / "staging").glob("year=*/detections.parquet"))
    if not staging_files:
        return None

    glob = str(out_dir / "staging" / "year=*" / "detections.parquet")
    return f"read_parquet('{glob}', hive_partitioning=true)"


def conservation_from_manifest(
    con: duckdb.DuckDBPyConnection,
    events_src: str,
    manifest: Dict[str, Any],
) -> List[Tuple[int, int, int, int, int]]:
    """Compare per-year event detections with staging counts stored in the manifest."""
    staging_rows = {
        int(year): int(row["rows"])
        for year, row in (manifest.get("staging") or {}).items()
        if isinstance(row, dict) and "rows" in row
    }
    if not staging_rows:
        return []

    event_rows = con.execute(
        f"""
        SELECT year(event_date) AS year, count(*) AS events,
               sum(n_detections) AS event_dets
        FROM {events_src}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    event_map = {int(r[0]): (int(r[1]), int(r[2])) for r in event_rows}

    years = sorted(set(staging_rows) | set(event_map))
    rows: List[Tuple[int, int, int, int, int]] = []
    for year in years:
        staging = staging_rows.get(year, 0)
        n_events, event_dets = event_map.get(year, (0, 0))
        rows.append((year, staging, n_events, event_dets, event_dets - staging))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the unified wildfire dataset")
    parser.add_argument("--out-dir", default="data/unified")
    parser.add_argument("--db", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    db_path = Path(args.db).resolve() if args.db else out_dir / "wildfire.duckdb"
    results: List[Dict[str, Any]] = []

    events_glob = str(out_dir / "events" / "events_*.parquet")
    fires_glob = str(out_dir / "fires" / "fires_*.parquet")

    catalog_mode = db_path.exists()
    if catalog_mode:
        con = duckdb.connect(str(db_path), read_only=True)
        events_src = "events"
        fires_src = "fires"
        try:
            views = {
                r[0] for r in con.execute(
                    "SELECT view_name FROM duckdb_views()"
                ).fetchall()
            }
            tables = {
                r[0] for r in con.execute(
                    "SELECT table_name FROM duckdb_tables()"
                ).fetchall()
            }
            if {"events", "fires"} <= views:
                catalog_kind = "view-based"
            elif {"events", "fires"} <= tables:
                catalog_kind = "materialized"
            else:
                catalog_kind = "catalog"
        except Exception:
            catalog_kind = "catalog"
        print(f"validating DuckDB catalog: {db_path}")
    else:
        con = duckdb.connect()
        events_src = f"read_parquet('{events_glob}')"
        fires_src = f"read_parquet('{fires_glob}')"
        catalog_kind = "parquet-direct"
        print(f"catalog not found, validating Parquet directly: {out_dir}")

    staging_src = staging_relation(con, out_dir, prefer_catalog=catalog_mode)
    use_staging = staging_src is not None

    manifest_path = out_dir / "build_manifest.json"
    manifest: Dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception as exc:  # pragma: no cover - defensive
            print(f"WARNING: could not read {manifest_path}: {exc}")

    # --- basic counts -------------------------------------------------------
    n_events = q(con, f"SELECT count(*) FROM {events_src}")[0]
    n_fires = q(con, f"SELECT count(*) FROM {fires_src}")[0]

    if use_staging:
        n_detections = q(con, f"SELECT count(*) FROM {staging_src}")[0]
        detections_basis = "staging files"
        add(results, "detections present", n_detections, "> 0", n_detections > 0)
    else:
        n_detections = q(con, f"SELECT sum(n_detections) FROM {events_src}")[0] or 0
        detections_basis = "sum(events.n_detections); staging removed"
        add(
            results, "detections present", n_detections, "> 0", n_detections > 0,
            detail="staging layer absent; using event detection totals",
        )

    add(results, "events present", n_events, "> 0", n_events > 0)
    add(results, "fires present", n_fires, "> 0", n_fires > 0)

    # --- uniqueness ---------------------------------------------------------
    dup_events = q(
        con, f"SELECT count(*) - count(DISTINCT event_id) FROM {events_src}"
    )[0]
    add(results, "event_id unique", dup_events, 0, dup_events == 0)

    dup_fires = q(
        con, f"SELECT count(*) - count(DISTINCT fire_id) FROM {fires_src}"
    )[0]
    add(results, "fire_id unique", dup_fires, 0, dup_fires == 0)

    # --- referential integrity ---------------------------------------------
    orphan_events = q(
        con,
        f"SELECT count(*) FROM {events_src} e "
        f"LEFT JOIN {fires_src} f USING (fire_id) "
        "WHERE e.fire_id IS NULL OR f.fire_id IS NULL"
    )[0]
    add(results, "every event has a fire", orphan_events, 0, orphan_events == 0)

    # --- fire totals vs events ---------------------------------------------
    mismatch = q(
        con,
        f"""
        WITH agg AS (
            SELECT fire_id, count(*) AS n_events, sum(n_detections) AS n_dets
            FROM {events_src} GROUP BY fire_id
        )
        SELECT count(*) FROM {fires_src} f JOIN agg a USING (fire_id)
        WHERE f.n_events <> a.n_events OR f.n_detections <> a.n_dets
        """,
    )[0]
    add(results, "fire totals match events", mismatch, 0, mismatch == 0)

    # --- detection conservation per year -----------------------------------
    conservation: List[Tuple[int, int, int, int, int]] = []
    conservation_basis = "unavailable"
    if use_staging:
        conservation = con.execute(
            f"""
            WITH s AS (
                SELECT year, count(*) AS staging_rows
                FROM {staging_src} GROUP BY year
            ),
            e AS (
                SELECT year(event_date) AS year, count(*) AS events,
                       sum(n_detections) AS event_dets
                FROM {events_src} GROUP BY year(event_date)
            )
            SELECT s.year, s.staging_rows, e.events, e.event_dets,
                   e.event_dets - s.staging_rows AS delta
            FROM s LEFT JOIN e USING (year)
            ORDER BY s.year
            """
        ).fetchall()
        conservation_basis = "staging files"
    else:
        conservation = conservation_from_manifest(con, events_src, manifest)
        if conservation:
            conservation_basis = "build_manifest.json staging rows"

    if conservation:
        bad_years = [r for r in conservation if r[4] != 0 or r[1] != r[3]]
        add(
            results, "detection conservation", len(bad_years), 0,
            not bad_years,
            detail="; ".join(
                f"{r[0]}: staging={r[1]:,} events_dets={r[3]:,} delta={r[4]:,}"
                for r in bad_years[:10]
            ),
        )
    else:
        add(
            results, "detection conservation", "skipped", "n/a", True,
            detail="staging removed and no per-year staging counts in build_manifest.json",
        )

    # --- ranges -------------------------------------------------------------
    invalid_events = q(
        con,
        f"""
        SELECT count(*) FROM {events_src}
        WHERE latitude NOT BETWEEN -90 AND 90
           OR longitude NOT BETWEEN -180 AND 180
           OR n_detections < 1
           OR first_dt IS NULL
           OR last_dt IS NULL
           OR last_dt < first_dt
        """,
    )[0]
    add(results, "event coords/times valid", invalid_events, 0, invalid_events == 0)

    invalid_fires = q(
        con,
        f"""
        SELECT count(*) FROM {fires_src}
        WHERE latitude NOT BETWEEN -90 AND 90
           OR longitude NOT BETWEEN -180 AND 180
           OR end_date < start_date
           OR n_events < 1
           OR n_detections < 1
        """,
    )[0]
    add(results, "fire coords/times valid", invalid_fires, 0, invalid_fires == 0)

    out_of_range = q(
        con,
        f"""
        SELECT count(*) FROM {events_src} e
        JOIN {fires_src} f USING (fire_id)
        WHERE e.event_date < f.start_date OR e.event_date > f.end_date
        """,
    )[0]
    add(results, "events within fire date range", out_of_range, 0, out_of_range == 0)

    # --- print --------------------------------------------------------------
    print()
    print(f"{'check':<34} {'value':>16} {'expected':>10}  result")
    print("-" * 78)
    for r in results:
        flag = "PASS" if r["passed"] else "FAIL"
        print(f"{r['check']:<34} {str(r['value']):>16} {str(r['expected']):>10}  {flag}")
        if r["detail"]:
            print(f"    {r['detail']}")

    conservation_json = [
        {
            "year": r[0],
            "staging_rows": r[1],
            "events": r[2],
            "event_detections": r[3],
            "delta": r[4],
        }
        for r in conservation
    ]
    report = {
        "out_dir": str(out_dir),
        "catalog": str(db_path) if catalog_mode else None,
        "catalog_mode": catalog_kind,
        "staging_present": use_staging,
        "totals": {
            "detections": n_detections,
            "events": n_events,
            "fires": n_fires,
        },
        "detections_basis": detections_basis,
        "conservation_basis": conservation_basis,
        "conservation_by_year": conservation_json,
        "checks": results,
        "passed": all(r["passed"] for r in results),
    }
    report_path = out_dir / "validation_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print()
    print(f"report: {report_path}")
    if not report["passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
