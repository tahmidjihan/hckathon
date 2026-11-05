# Unified Wildfire Dataset

One de-duplicated global dataset built from all MODIS (Terra/Aqua) and VIIRS
(Suomi-NPP, NOAA-20, NOAA-21) yearly CSVs that were under `data/tu/`.

Built by `build_unified_wildfires.py`. For the pipeline architecture and
scientific rules of the older per-pass harmonizer, see `AGENTS.md`.

The current build is **trimmed to the query layers only**:

- `data/unified/events/` — final event-level Parquet
- `data/unified/fires/` — final fire-level Parquet
- `data/unified/wildfire.duckdb` — tiny **view-based** catalog

The raw yearly CSVs and the intermediate `staging/` + `events_raw/` layers
were removed after validation. Only the one-off cleaning scripts remain under
`data/tu/**/*.py`.

## Final build

| Item | Value |
| --- | --- |
| Build date | 2026-11-05 |
| Coverage | 2000–2026 (2025 and 2026 are partial) |
| Raw detections | 458,262,571 |
| Events | 164,025,421 |
| Fires | 60,445,399 |
| Final size on disk | ~11 GB |
| `events/` | 5.7 GB |
| `fires/` | 4.9 GB |
| `wildfire.duckdb` | ~524 KB (views only) |
| Validation | all checks pass; see `validation_report.json` (per-year conservation uses staging counts recorded in `build_manifest.json`) |

Validation covers: unique `event_id` / `fire_id`, every event linked to a
fire, fire totals matching events, per-year detection conservation, valid
coordinates/times, and events inside their fire's date range.

## Layers

| Layer | Grain | Key | Status / where |
| --- | --- | --- | --- |
| Detections | One raw normalized observation | — | Rebuild-time only. Removed after validation; not queryable. |
| Events | One near-simultaneous fire observation cluster | `event_id` | Final: `data/unified/events/events_YYYY.parquet` |
| Fires | One persistent fire, linked across days | `fire_id` | Final: `data/unified/fires/fires_YYYY.parquet` |
| Catalog | DuckDB views over the Parquet files | `event_id`, `fire_id` | `data/unified/wildfire.duckdb` |

`events` is the "one row per fire observation, detected by both or one sensor"
table. `fires` is the higher-level incident table for dashboards and trend
analysis. `fires` is the primary API list; `events` is the drill-down layer.

The catalog contains **views only** — it does not duplicate the Parquet data.
That is why the whole dataset is ~11 GB instead of ~36 GB.

## Definitions

- **Detection** — a single MODIS or VIIRS active-fire pixel after exact
  duplicate removal.
- **Event** — detections chained within **1.5 km** and **4 hours** into one
  cluster, capped at a single UTC day. Each event has a centroid, first/last
  time, max/mean/sum FRP, per-sensor detection counts, and
  `detected_by = MODIS | VIIRS | MODIS+VIIRS`.
- **Fire** — events linked across consecutive days within **5 km** and a
  **3-day gap**. A fire keeps `start_date`, `end_date`, total events,
  total detections, bounding box, max FRP, and source attribution.

Both links are greedy and deterministic (input order is
`dt, source_rank, latitude, longitude, frp DESC`). They are not a global
transitive closure, so extreme edge cases may produce a different grouping
than an all-pairs clustering.

A fire can start in one year and contain events from the next year. For example,
`EVT-2024-0000001` belongs to `FIRE-2023-3690165`.

## IDs

- Event: `EVT-<year>-<7-digit sequence>`, e.g. `EVT-2024-0000123`
- Fire: `FIRE-<start year>-<6-digit sequence>`, e.g. `FIRE-2024-000045`

IDs are stable for a given input ordering and build configuration. The final
artifacts are not resumable at the staging/event-raw level because those
intermediates were removed; a full rebuild requires restoring the raw CSVs.

## Querying

Python:

```python
import duckdb

con = duckdb.connect("data/unified/wildfire.duckdb", read_only=True)

# Look up one event / fire
con.sql("SELECT * FROM find_event('EVT-2024-0000123')").show()
con.sql("SELECT * FROM find_fire('FIRE-2024-000045')").show()
con.sql("SELECT * FROM events_for_fire('FIRE-2024-000045')").show()

# Map layer for a date range
con.sql("""
    SELECT event_id, event_date, latitude, longitude,
           max_frp, n_detections, detected_by, sources
    FROM events
    WHERE event_date BETWEEN DATE '2024-07-01' AND DATE '2024-07-31'
    ORDER BY event_date, max_frp DESC
""").show()

# Fire incidents active in a window
con.sql("""
    SELECT fire_id, start_date, end_date, latitude, longitude,
           max_frp, n_detections, n_events, sources
    FROM fires
    WHERE start_date <= DATE '2024-08-31'
      AND end_date >= DATE '2024-08-01'
""").show()
```

### API-style area queries

The main API use case is "where, when, and how much fire was in one area".
Use `fires` for incident lists and `events` for daily/map detail.

```python
# Fires that started inside a bounding box and date range
con.sql("""
    SELECT fire_id, start_date, end_date, latitude, longitude,
           max_frp, sum_frp, n_detections, n_events, sources
    FROM fires
    WHERE latitude BETWEEN 35.0 AND 45.0
      AND longitude BETWEEN -125.0 AND -110.0
      AND start_date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'
    ORDER BY max_frp DESC
""").show()

# How much fire in an area over time (daily event totals)
con.sql("""
    SELECT event_date,
           count(*) AS events,
           sum(n_detections) AS detections,
           sum(sum_frp) AS total_frp,
           max(max_frp) AS peak_frp
    FROM events
    WHERE latitude BETWEEN 35.0 AND 45.0
      AND longitude BETWEEN -125.0 AND -110.0
      AND event_date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'
    GROUP BY event_date
    ORDER BY event_date
""").show()

# Which sensors detected the fires in a box
con.sql("""
    SELECT sources, count(*) AS fires, sum(n_detections) AS detections
    FROM fires
    WHERE latitude BETWEEN 35.0 AND 45.0
      AND longitude BETWEEN -125.0 AND -110.0
      AND start_date BETWEEN DATE '2024-06-01' AND DATE '2024-09-30'
    GROUP BY sources
    ORDER BY fires DESC
""").show()
```

CLI:

```bash
python3 - <<'PY'
import duckdb
con = duckdb.connect("data/unified/wildfire.duckdb", read_only=True)
con.sql("SELECT * FROM find_fire('FIRE-2024-000045')").show()
PY
```

`find_event` / `find_fire` scan the yearly Parquet files; they are convenient
but not indexed point lookups. Area/date queries benefit from Parquet row-group
pruning. If you later need very fast ID lookups for an API, add a small
materialized (`event_id`, `year`) index rather than duplicating the full rows.

## Schemas

### `events`

`event_id`, `fire_id`, `event_date`, `first_dt`, `last_dt`,
`duration_minutes`, `latitude`, `longitude`, `n_detections`, `n_sensors`,
`detected_by`, `sources`, `modis_terra_detections`, `modis_aqua_detections`,
`viirs_snpp_detections`, `viirs_noaa20_detections`, `viirs_noaa21_detections`,
`max_frp`, `mean_frp`, `sum_frp`, `max_brightness`, `max_bright_t31`,
`mean_modis_confidence`, `viirs_confidence_labels`, `daynight`, `type`.

`detected_by` is derived from the sensor counts. `sources` is a comma-separated
list such as `MODIS_Terra,MODIS_Aqua,VIIRS_NOAA20`.

### `fires`

`fire_id`, `start_date`, `end_date`, `duration_days`, `n_events`,
`n_detections`, `latitude`, `longitude`, `lat_min`, `lat_max`, `lon_min`,
`lon_max`, `max_frp`, `mean_frp`, `sum_frp`, `n_sensors`, `sources`,
`modis_detections`, `viirs_detections`, `first_event_id`, `last_event_id`,
`representative_event_id`, `representative_frp`.

### Detection-level schema (not in the final catalog)

The old `detections` view exposed the normalized staging schema (`source`,
`satellite`, `instrument`, `latitude`, `longitude`, `dt`, `event_date`,
`acq_date`, `acq_time`, `frp`, `brightness`, `bright_t31`,
`modis_confidence_pct`, `viirs_confidence_label`, `daynight`, `type`, `year`).
It was removed with `staging/` after validation. Use `events` for the final
aggregated observations.

### `meta`

Build configuration and ID/definition notes.

## Important data caveats

- **Confidence filtering happens upstream.** The cleaning scripts already keep
  only MODIS `confidence >= 30` and VIIRS `n/h` (L is removed). The unified
  dataset does not add or restore low-confidence detections.
- **Only `type = 0`** (vegetation fire) rows are included.
- MODIS and VIIRS have no single shared confidence scale. `events` keeps
  `mean_modis_confidence` (0–100) and `viirs_confidence_labels` (`N`/`H`)
  separately instead of inventing a combined score.
- **An event is not a whole wildfire.** A fire burning for weeks produces many
  events; use `fire_id` to follow it across days.
- **Fire chaining can be long.** With a 3-day gap and 5 km radius, a persistent
  fire or a chain of nearby fires can span weeks or months. Tune
  `--fire-gap-days` / `--fire-radius-km` when rebuilding if you want stricter
  incidents.
- Timestamps are naive UTC (as supplied by the source CSVs). Events are capped
  at UTC day boundaries.
- 2025 and 2026 are partial years in the source data.
- **Raw inputs are gone.** The cleaned yearly CSVs that fed the build were
  deleted after validation. A full rebuild requires restoring them first.
- **The view catalog stores absolute Parquet paths.** If you move
  `data/unified`, rebuild or edit the catalog views before querying.

## Rebuilding

The current build is final. `staging/`, `events_raw/`, and the raw CSVs are
deleted, so the current `--stage fires` command cannot run as-is. To rebuild
from scratch:

1. Restore the cleaned yearly CSVs to their original paths under
   `data/tu/` (see `AGENTS.md` for the historical layout).
2. Run the full build:

```bash
python3 build_unified_wildfires.py --all --out-dir data/unified --force
```

3. Validate:

```bash
python3 validate_unified.py --out-dir data/unified
```

4. Replace the materialized catalog with the tiny view-based catalog and remove
   intermediates:

```bash
# The finalizer waits for a build PID and aborts if validation fails.
# KEEP_RAW=1 leaves the raw CSVs in place.
BUILD_PID=<pid> bash finalize_unified.sh
```

A one-year/one-day smoke build still works after restoring input data:

```bash
python3 build_unified_wildfires.py --stage staging --years 2024 \
  --start-date 2024-07-15 --end-date 2024-07-15 --out-dir /tmp/wf_test --force
python3 build_unified_wildfires.py --stage events --years 2024 \
  --out-dir /tmp/wf_test --force
```

Staging and event stages skip existing year files unless `--force` is given.
The fire-linking and catalog stages always rebuild their outputs.

To change fire-linking thresholds without restoring the raw CSVs, you would
need to reconstruct an `events_raw` layer from the final `events/` Parquet by
dropping `fire_id`. The builder does not currently expose a flag for that; it is
a straightforward projection if you need it.
