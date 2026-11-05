# AGENTS.md — Project Entry Point

> Read this first. It is the onboarding map for this repo: what the project is,
> which files do what, how to run the pipeline, and which rules must not be broken.

## 1. What this project is

A **scientific cross-sensor active-fire harmonization pipeline** in pure Python (stdlib only,
with optional `pyarrow` for Parquet). It merges satellite wildfire detections from:

- **MODIS** (Terra and Aqua) — 1 km thermal-anomaly product.
- **VIIRS** (Suomi-NPP, NOAA-20, NOAA-21) — 375 m thermal-anomaly product.

There are two distinct matching problems:

1. **VIIRS ↔ VIIRS unification** (`unify_viirs.py`): collapse duplicate detections of the same
   physical fire seen by different VIIRS satellites into one **unified event**.
2. **MODIS ↔ unified-VIIRS harmonization** (`modis_viirs_harmonization.py`): match MODIS
   detections against unified VIIRS events and emit matched pairs / sensor-only populations.

The project is a research/data-engineering artifact, not a web app. There is no
`requirements.txt` and no automated test suite in the workspace. A Git repository
may be present; data outputs are ignored.

## 2. File map

| Path | Role |
| --- | --- |
| `build_unified_wildfires.py` | **Current main entry point.** Builds the single de-duplicated MODIS+VIIRS dataset: staging → events → fires → DuckDB catalog. In the final build, `staging/` and `events_raw/` were removed after validation. See `UNIFIED_DATA.md`. |
| `validate_unified.py` | Integrity and conservation checks against the DuckDB catalog. Writes `validation_report.json`. |
| `finalize_unified.sh` | Post-build finalizer: waits for a build PID, runs validation, rebuilds the tiny view-based catalog, then removes intermediates and raw CSVs. |
| `UNIFIED_DATA.md` | Schema, ID format, final counts, definitions, and API/query examples for the unified dataset. |
| `modis_viirs_harmonization.py` | **Legacy per-pass pipeline** (1,426 lines). Loads MODIS + unified VIIRS, runs Terra/Aqua matching, optional calibration, writes CSVs + year-wise Parquet + `run_manifest.json`. Still useful for the documented orbital-drift policy and diagnostics. |
| `unify_viirs.py` | **Stage 1 pipeline** (698 lines). Reads per-satellite VIIRS CSVs, performs two-pass anchor matching, writes one unified-events CSV. |
| `harmonization_architecture.py` | **Documentation only** — two module docstrings describing the architecture and the orbital-drift revision. Contains no executable code. |
| `message.txt` | Stale CRLF paste of an earlier `modis_viirs_harmonization.py`. Reference/backup only. |
| `message (2).txt` | Stale CRLF paste of an earlier `unify_viirs.py`. Reference/backup only. |
| `message (1).txt` | Stale CRLF paste of `harmonization_architecture.py`. Reference only. |
| `data/unified/events/events_<year>.parquet` | **Final event layer** — 164,025,421 events, partitioned by year. |
| `data/unified/fires/fires_<year>.parquet` | **Final fire layer** — 60,445,399 fires, partitioned by start year. |
| `data/unified/wildfire.duckdb` | View-based DuckDB catalog: `events`, `fires`, `meta`, and lookup macros. No materialized copy. |
| `data/unified/build_manifest.json` | Build config, per-year counts, and the cleanup record. |
| `data/unified/validation_report.json` | Results of the final integrity/conservation checks. |
| `data/tu/*/{clean.py,cleanup.py}` | One-off pandas cleaning scripts that produced the old yearly CSVs. Not part of the runtime pipeline. |
| `data/tu/**/output/*.csv` (historical) | Cleaned MODIS/VIIRS yearly inputs. **Deleted after the 2026-11-05 build;** restore them to rebuild. |

`data/unified` is ~11 GB: `events/` 5.7 GB + `fires/` 4.9 GB + a ~524 KB view
catalog. The raw `data/tu` CSVs are gone; only the cleaning `.py` scripts remain.
The cleaning scripts ship with `INPUT_DIRECTORY = ""` — they must be edited
before reuse.

## 3. Unified dataset (current objective)

`build_unified_wildfires.py` is the current production path. It built one
de-duplicated dataset from every MODIS/VIIRS yearly CSV under `data/tu/`:

- **staging** — normalized exact-deduplicated raw detections. Rebuild-time only;
  removed after validation.
- **events** — one row per near-simultaneous fire observation cluster
  (1.5 km / 4 h, capped at one UTC day), keyed by `event_id`. Final layer kept.
- **fires** — one row per persistent fire, linking events across days
  (5 km / 3-day gap), keyed by `fire_id`. Final layer kept.
- **catalog** — `data/unified/wildfire.duckdb`, now **view-based** over the
  Parquet files, with `events`, `fires`, `meta`, and `find_event(id)` /
  `find_fire(id)` / `events_for_fire(id)` helpers. There is **no `detections`
  view** (staging was removed).

Final build (2026-11-05):

| Item | Value |
| --- | --- |
| Raw detections | 458,262,571 |
| Events | 164,025,421 |
| Fires | 60,445,399 |
| On-disk size | ~11 GB |
| Validation | all checks pass (`validation_report.json`) |

The raw CSVs, `staging/`, and `events_raw/` were deleted after validation, so a
full rebuild requires restoring the cleaned yearly CSVs first. The old
`--stage fires` path cannot run until that `events_raw/` layer is recreated.

Build/validate/finalize (after restoring inputs):

```bash
python3 build_unified_wildfires.py --all --out-dir data/unified --force
python3 validate_unified.py --out-dir data/unified
BUILD_PID=<pid> bash finalize_unified.sh
```

Definitions, schemas, caveats, and query examples are in `UNIFIED_DATA.md`.
The older per-pass policy below (orbital-drift windows, Terra disabled after
2022) is intentionally **not** used by the unified builder; the unified builder
uses one consistent 1.5 km / 4 h rule across all years and sensors.

## 4. Canonical run order (legacy per-pass pipeline)

> **Historical reference only.** The cleaned yearly CSVs under `data/tu/*/output/`
> were deleted after the 2026-11-05 unified build. These commands require
> restoring those inputs first. The current production path is
> `build_unified_wildfires.py` in section 3.

```text
raw FIRMS-style CSVs
      │  (clean.py / cleanup.py — pandas, manual)
      ▼
data/tu/*/output/*.csv          # yearly cleaned per-satellite files
      │  unify_viirs.py        # VIIRS-only → unified events
      ▼
unification-<region>-<date>.csv
      │  modis_viirs_harmonization.py --pass both   # Terra + Aqua
      ▼
<harmonized output dir>/
  Phase1_Terra/{terra_only_modis,terra_only_viirs,terra_matched_pairs}.csv
  Phase2_Aqua/{aqua_only_modis,aqua_only_viirs,aqua_matched_pairs}.csv
  modis-viirs-{terra,aqua}-<region>-<date>.csv
  Yearly/harmonized_<year>.parquet           # authoritative Aqua product
  Yearly/terra_harmonized_<year>.parquet     # Terra diagnostic product
  calibration.json                           # only when --fit-calibration is used
  run_manifest.json                          # provenance: inputs, hashes, counts, policy
```

### Stage 1 — unify VIIRS

```bash
python3 unify_viirs.py \
  --data-dir "data/tu/VIIRS-C2/SUOMI VIIRS/output" \
  --region quebec \
  --start-date 2024-01-01 --end-date 2024-12-31 \
  --output data/unified/viirs_unified_2024.csv
```

- Defaults: radius `750 m`, pass-1 window `65 min` (NOAA-20 anchor), pass-2 window `30 min`
  (NOAA-21 anchor leftovers).
- Inputs must be **VIIRS only**. `--data-dir` is scanned recursively and any non-VIIRS
  satellite label (`Aqua`, `Terra`, ...) raises `ValueError: Unrecognized satellite label`.
  Do **not** point it at `data/tu/` — point it at one VIIRS `output/` directory, or merge the
  three VIIRS CSVs into a staging directory first.
- Missing required columns (`acq_date`, `acq_time`, `bright_ti4`, `frp`, `confidence`,
  `satellite`) are fatal for that file. Confidence must be one of `l/n/h` (or long forms).
- Exact duplicates on `(satellite, lat, lon, date, time)` are dropped; every parsed detection
  must survive into exactly one event (conservation assertion at the end).

### Stage 2 — harmonize MODIS against unified VIIRS

```bash
python3 modis_viirs_harmonization.py \
  --pass both \
  --modis-dir data/tu/MODRIS/MODRIS/output \
  --viirs-file data/unified/viirs_unified_2024.csv \
  --region quebec \
  --start-date 2024-01-01 --end-date 2024-12-31 \
  --output-dir Data/Processed/quebec/2024/Harmonized
```

- `--pass` is required: `terra`, `aqua`, or `both`.
- `--modis-dir` may be a file or directory (recursive `.csv`/`.txt`).
- Defaults: radius `1.5 km`, cutoff `2012-01-19`, Terra milestone `2022-10-01`,
  Aqua milestone `2023-02-01`.
- `--dry-run` prints counts and planned paths without writing.
- `--fit-calibration` / `--calibrate` fits Aqua confidence calibration from the current
  matched pairs; `--calibration-file` / `--apply-calibration` select the JSON path.
- Parquet is written **only if `pyarrow` is installed**; otherwise the pipeline silently skips
  Parquet and still writes CSVs + manifest. Current environment: `pyarrow` 26.0.1 and
  `duckdb` 2.0.0 are installed; `pandas` is **not**. The legacy harmonizer itself only needs
  the standard library.

### Stage 3 — optional legacy low-confidence filter

Phase 4 filtering is **removed by default**. All three populations are retained. To re-enable
the old behaviour, pass `--apply-low-confidence-filter` together with a valid calibration whose
`l_threshold` exists. Rules when enabled:

1. Matched pairs: MODIS confidence `< L-threshold` → filtered; `>=` → retained; missing → filtered.
2. Unmatched MODIS: same rule.
3. Unmatched VIIRS: label `L` → filtered; `N`/`H` → retained; missing/unknown → filtered.

## 5. Scientific rules and invariants (do not silently change)

### Orbital-drift time-window policy (`get_matching_window_policy`)

| Pass | Date | Matching | Window |
| --- | --- | --- | --- |
| Terra | `< 2022-10-01` | enabled | 240 min (4 h, diagnostic persistence) |
| Terra | `>= 2022-10-01` | **disabled** (Terra exits matching) | 0 |
| Aqua | `< 2023-02-01` | enabled | 60 min (near-coincident) |
| Aqua | `>= 2023-02-01` | enabled | 135 min (2 h 15 min) |

The 135-minute Aqua window is an **implementation rule**, explicitly documented as not yet
empirically validated. Treat it as a policy constant, not a scientific guarantee.

### Matching semantics

- MODIS is always the **anchor**: canonical `latitude`, `longitude`, `acq_date`, `acq_time`
  come from MODIS. A centroid is also emitted but is not the canonical coordinate.
- Search is **MODIS-centered**, radius fixed at `1.5 km`.
- Time comparison uses real `datetime` arithmetic on actual acquisition time; no midnight
  defaulting, no synthetic timestamps.
- Candidate selection: sort by `(distance_km, time_delta_minutes, source_viirs_id)` and take the
  first — deterministic nearest-neighbour with ID tie-break. All candidates are recorded in
  `viirs_contributor_ids`, count in `candidate_count`/`viirs_pixel_count`, and the ambiguity
  flag is set when more than one candidate exists.
- Radiometry is merged with `max(MODIS, VIIRS)` per field (`frp`, `brightness`, `bright_t31`).
  **Never sum** MODIS and VIIRS values; no synthetic integration.
- `viirs_total_frp` currently equals the single matched VIIRS FRP (not a multi-pixel sum).
- Missing measurements stay `None`/empty — do not invent defaults (especially confidence).

### Populations and outputs

- Terra output is **diagnostic**: parquet contains `matched + unmatched_modis` only;
  unmatched VIIRS is intentionally excluded from the Terra parquet.
- Aqua output is the **primary harmonized product**: all three populations are retained
  (unless the legacy filter is explicitly enabled).
- Each record carries provenance: `match_id`, `dataset_type`, `match_status`,
  `processing_pass`, `processing_year`, `source_modis_id`, `source_viirs_id`,
  `time_window_minutes`, `orbital_period`, `policy_note`, `filter_status`, `filter_reason`.
- `run_manifest.json` is the audit trail: run timestamp, region, date scope, SHA-256 of the
  unified VIIRS input, per-file MODIS hashes, policy constants, per-phase counts, output paths.

### Calibration (`calibrate_aqua_confidence`)

- Computes MODIS confidence centroids for VIIRS `L`, `N`, `H` classes from **unfiltered**
  Aqua matched pairs; `l_threshold = mean(MODIS confidence % | VIIRS L)`.
- If no VIIRS `L` samples exist, calibration must be accepted cleanly with status
  `accepted_without_l`, not raise.
- Frozen calibration is scope-validated on reuse: requested dates must fall inside the
  calibration's `date_scope`.

## 6. Data formats

> **Historical input format.** The cleaned yearly CSVs described here were
> deleted after the 2026-11-05 build. Restore them before running the legacy
> per-pass pipeline or a full rebuild.

Cleaned inputs (FIRMS-style, header names vary slightly):

```text
MODIS: latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,
       confidence,version,bright_t31,frp,daynight,type,year
       confidence = 0..100 (numeric); only >= 30 and type == 0 survive cleaning
VIIRS: latitude,longitude,brightness,scan,track,acq_date,acq_time,satellite,instrument,
       confidence,version,bright_t31,frp,daynight,type,year
       confidence = l/n/h; only n/h and type == 0 survive cleaning
```

Unified VIIRS event columns (`build_event`): `latitude`, `longitude`, `acq_date`, `acq_time`,
`first_acq_date`, `first_acq_time`, `frp`, `bright_ti4`, `bright_ti5`, `satellite` (JSON list),
`instrument` (JSON list), `confidence` (JSON `[sat, label]` pairs), `satellite_confidences`
(JSON map), `num_satellites`, `multi_satellite`, `canonical_*`, `event_detection_count`, `type`.

Harmonized record columns are listed in `harmonize_pass` (coordinates, per-sensor radiometry,
confidence, diagnostics, provenance). `write_csv_file` falls back to a 20-column header set for
empty datasets, so schema can differ between empty and non-empty CSV outputs.

## 7. Known traps / things to verify before changing behaviour

- `harmonization_architecture.py` looks importable but only contains docstrings; the real logic
  lives in `modis_viirs_harmonization.py`.
- The three `message*.txt` files are stale copies (CRLF, older versions). Do not edit them as
  if they were source; edit the `.py` files.
- `unify_viirs.py` cannot consume MODIS CSVs; keep VIIRS input directories separate.
- The default output paths are `Data/Processed/...` (capital `D`) while the dataset lives under
  `data/`. Linux is case-sensitive — pass `--output-dir` explicitly.
- Current environment: `pyarrow` 26.0.1 and `duckdb` 2.0.0 are installed; `pandas` is **not**.
  Install `pandas` only if you need the cleaning scripts.
- **The final dataset is trimmed.** `staging/`, `events_raw/`, and the raw CSVs are deleted.
  `--stage fires` cannot run until `events_raw/` is recreated from restored inputs.
- `wildfire.duckdb` is **view-based**; there is no `detections` view. `find_event` /
  `find_fire` scan the yearly Parquet files rather than using a B-tree index.
- If you need to change fire thresholds, rebuild from restored raw CSVs. The final `events/`
  Parquet can be projected to an `events_raw` layer by dropping `fire_id`, but the builder
  does not expose a flag for that.
- `finalize_unified.sh` deletes `staging/`, `events_raw/`, and raw CSVs when validation passes.
  Use `BUILD_PID=<pid>` for a new build, `KEEP_RAW=1` to retain the raw CSVs, or run the
  steps manually if you want to keep the intermediates.
- Dates are compared as `YYYY-MM-DD` strings against milestone constants. Keep that format.
- `--date` is mutually exclusive with `--start-date`/`--end-date`; the two range flags must be
  supplied together. Same rules apply in both CLIs.
- There are no automated tests. Validate changes with a small dated slice and `--dry-run`, then
  inspect `run_manifest.json` counts and the conservation assertion in `run_unification`.

## 8. Quick smoke checks

```bash
# Unified dataset is present and queryable
python3 - <<'PY'
import duckdb
con = duckdb.connect("data/unified/wildfire.duckdb", read_only=True)
print(con.execute("SELECT count(*) FROM events").fetchone())
print(con.execute("SELECT count(*) FROM fires").fetchone())
print(con.execute("SELECT * FROM find_fire('FIRE-2024-000001')").fetchone())
PY

# Integrity report (uses the catalog; no raw inputs needed)
python3 validate_unified.py --out-dir data/unified

# CLI surface + syntax
python3 build_unified_wildfires.py --help
python3 unify_viirs.py --help
python3 modis_viirs_harmonization.py --help
python3 -m py_compile build_unified_wildfires.py validate_unified.py \
  unify_viirs.py modis_viirs_harmonization.py
```

The legacy dry run below requires the restored cleaned CSVs:

```bash
python3 modis_viirs_harmonization.py --pass aqua \
  --modis-dir data/tu/MODRIS/MODRIS/output/wildfires_2024.csv \
  --viirs-file <unified_viirs.csv> --region quebec --date 2024-06-01 --dry-run
```

When in doubt, prefer **not** changing the scientific constants (radius, windows, milestones,
max-radiometry rule, anchor semantics) and record any intentional policy change in
`harmonization_architecture.py` plus `run_manifest.json` output fields.
