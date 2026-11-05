#!/usr/bin/env bash
# Post-build finalizer for data/unified.
#
# 1. Wait for the running build to exit.
# 2. Validate the dataset. Abort if anything fails.
# 3. Replace the materialized DuckDB catalog with a tiny view-based catalog.
# 4. Remove intermediate layers and the raw CSV inputs.
#
# Kept: events/, fires/, wildfire.duckdb (views), build_manifest.json,
#       build.log, validation_report.json, and data/tu/**/*.py.
#
# Set KEEP_RAW=1 to keep the raw CSV inputs. Set BUILD_PID=<pid> for a new build.
set -uo pipefail
cd "$(dirname "$0")"

OUT="data/unified"
PID="${BUILD_PID:-21743}"
LOG="$OUT/finalize.log"

exec > >(tee -a "$LOG") 2>&1

echo "[$(date)] waiting for build PID $PID"
while kill -0 "$PID" 2>/dev/null; do
  sleep 30
done
echo "[$(date)] build exited"
tail -30 "$OUT/build.log"

if ! python3 validate_unified.py --out-dir "$OUT"; then
  echo "[$(date)] VALIDATION FAILED - keeping every file"
  exit 1
fi

echo "[$(date)] validation passed - rebuilding view-based catalog"
python3 - <<'PY'
import json
import os
from datetime import datetime
from pathlib import Path
import duckdb

out = Path("data/unified").resolve()
db = out / "wildfire.duckdb"
if db.exists():
    db.unlink()

events = str(out / "events" / "events_*.parquet")
fires = str(out / "fires" / "fires_*.parquet")

con = duckdb.connect(str(db))
con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events}')")
con.execute(f"CREATE VIEW fires AS SELECT * FROM read_parquet('{fires}')")
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
con.executemany("INSERT INTO meta VALUES (?, ?)", [
    ("dataset", "Unified global wildfire dataset"),
    ("catalog", "view-based over Parquet"),
    ("event_id_format", "EVT-<year>-<7-digit sequence>"),
    ("fire_id_format", "FIRE-<start year>-<6-digit sequence>"),
    ("notes", "staging/events_raw and raw CSVs removed after validation"),
])
con.execute("CHECKPOINT")
con.close()

manifest_path = out / "build_manifest.json"
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text())
else:
    manifest = {}

n_events = sum(
    int(v["events"]) for v in (manifest.get("events") or {}).values()
    if isinstance(v, dict) and "events" in v
)
n_fires = int((manifest.get("fires") or {}).get("fires_finalized") or 0)
n_detections = sum(
    int(v["rows"]) for v in (manifest.get("staging") or {}).values()
    if isinstance(v, dict) and "rows" in v
)
raw_removed = os.environ.get("KEEP_RAW", "0") != "1"
removed = ["staging/", "events_raw/", "_tmp/", "materialized DuckDB tables"]
if raw_removed:
    removed.append("data/tu/**/*.csv")

manifest["catalog"] = {
    "path": str(db),
    "bytes": db.stat().st_size,
    "mode": "view-based over Parquet",
    "events": n_events,
    "fires": n_fires,
}
manifest["final"] = {
    "built_at": manifest.get("built_at"),
    "finalized_at": datetime.now().isoformat(timespec="seconds"),
    "detections": n_detections,
    "events": n_events,
    "fires": n_fires,
    "sizes_bytes": {
        "events": sum(f.stat().st_size for f in (out / "events").glob("*.parquet")),
        "fires": sum(f.stat().st_size for f in (out / "fires").glob("*.parquet")),
        "catalog": db.stat().st_size,
    },
    "kept": [
        "events/",
        "fires/",
        "wildfire.duckdb (views)",
        "build_manifest.json",
        "validation_report.json",
        "data/tu/**/*.py",
    ],
    "removed": removed,
}
manifest["cleanup"] = {
    "at": datetime.now().isoformat(timespec="seconds"),
    "removed": removed,
    "kept": manifest["final"]["kept"],
}
manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
print("view catalog written")
PY

# DuckDB 2.x sometimes leaves an empty temp directory beside the database.
rm -rf "$OUT/wildfire.duckdb.tmp"

echo "[$(date)] removing intermediate layers"
rm -rf "$OUT/staging" "$OUT/events_raw" "$OUT/_tmp"

if [ "${KEEP_RAW:-0}" = "1" ]; then
  echo "[$(date)] KEEP_RAW=1 - leaving raw CSV inputs in place"
else
  echo "[$(date)] removing raw CSV inputs (keeping Python cleaning scripts)"
  find data/tu -type f -name '*.csv' -delete
fi

echo "[$(date)] final sizes"
du -sh "$OUT"/* 2>/dev/null
echo "[$(date)] disk"
df -h . | tail -1
echo "[$(date)] done"
