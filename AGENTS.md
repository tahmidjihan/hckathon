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

The project is a research/data-engineering artifact, not a web app. There is no `git` repo,
no `requirements.txt`, and no test suite in the current workspace.

## 2. File map

| Path | Role |
| --- | --- |
| `modis_viirs_harmonization.py` | **Main pipeline** (1,426 lines). Loads MODIS + unified VIIRS, runs Terra/Aqua matching, optional calibration, writes CSVs + year-wise Parquet + `run_manifest.json`. |
| `unify_viirs.py` | **Stage 1 pipeline** (698 lines). Reads per-satellite VIIRS CSVs, performs two-pass anchor matching, writes one unified-events CSV. |
| `harmonization_architecture.py` | **Documentation only** — two module docstrings describing the architecture and the orbital-drift revision. Contains no executable code. |
| `message.txt` | Stale CRLF paste of an earlier `modis_viirs_harmonization.py`. Reference/backup only. |
| `message (2).txt` | Stale CRLF paste of an earlier `unify_viirs.py`. Reference/backup only. |
| `message (1).txt` | Stale CRLF paste of `harmonization_architecture.py`. Reference only. |
| `data/tu/MODRIS/MODRIS/output/wildfires_<year>.csv` | Cleaned MODIS yearly detections, 2000–2026. |
| `data/tu/VIIRS-C2/SUOMI VIIRS/output/viirs_wildfires_<year>.csv` | Cleaned Suomi-NPP detections, 2012–2026. |
| `data/tu/VIIRS-J1/output/viirs_wildfires_<year>.csv` | Cleaned NOAA-20 detections, 2018–2026. |
| `data/tu/VIIRS-J2/output/viirs_wildfires_<year>.csv` | Cleaned NOAA-21 detections, 2023–2026. |
| `data/tu/*/{clean.py,cleanup.py}` | One-off pandas cleaning scripts that produced the `output/` CSVs. Not part of the runtime pipeline. |

`data/` is ~37 GB. Only cleaned yearly outputs are present; original raw granules are absent.
The cleaning scripts ship with `INPUT_DIRECTORY = ""` — they must be edited before reuse.

## 3. Canonical run order

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
  Parquet and still writes CSVs + manifest. `pyarrow` is currently **not installed** in this
  environment (nor is `pandas`). The runtime pipeline itself only needs the standard library.

### Stage 3 — optional legacy low-confidence filter

Phase 4 filtering is **removed by default**. All three populations are retained. To re-enable
the old behaviour, pass `--apply-low-confidence-filter` together with a valid calibration whose
`l_threshold` exists. Rules when enabled:

1. Matched pairs: MODIS confidence `< L-threshold` → filtered; `>=` → retained; missing → filtered.
2. Unmatched MODIS: same rule.
3. Unmatched VIIRS: label `L` → filtered; `N`/`H` → retained; missing/unknown → filtered.

## 4. Scientific rules and invariants (do not silently change)

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

## 5. Data formats

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

## 6. Known traps / things to verify before changing behaviour

- `harmonization_architecture.py` looks importable but only contains docstrings; the real logic
  lives in `modis_viirs_harmonization.py`.
- The three `message*.txt` files are stale copies (CRLF, older versions). Do not edit them as
  if they were source; edit the `.py` files.
- `unify_viirs.py` cannot consume MODIS CSVs; keep VIIRS input directories separate.
- The default output paths are `Data/Processed/...` (capital `D`) while the dataset lives under
  `data/`. Linux is case-sensitive — pass `--output-dir` explicitly.
- No `pyarrow`/`pandas` in the current Python 3.14 environment. Install if Parquet or the
  cleaning scripts are needed; the core harmonizer runs without them.
- Dates are compared as `YYYY-MM-DD` strings against milestone constants. Keep that format.
- `--date` is mutually exclusive with `--start-date`/`--end-date`; the two range flags must be
  supplied together. Same rules apply in both CLIs.
- There are no automated tests. Validate changes with a small dated slice and `--dry-run`, then
  inspect `run_manifest.json` counts and the conservation assertion in `run_unification`.

## 7. Quick smoke checks

```bash
# CLI surface
python3 unify_viirs.py --help
python3 modis_viirs_harmonization.py --help

# Syntax check
python3 -m py_compile unify_viirs.py modis_viirs_harmonization.py

# Small dry run (adjust dates to an available slice)
python3 modis_viirs_harmonization.py --pass aqua \
  --modis-dir data/tu/MODRIS/MODRIS/output/wildfires_2024.csv \
  --viirs-file <unified_viirs.csv> --region quebec --date 2024-06-01 --dry-run
```

When in doubt, prefer **not** changing the scientific constants (radius, windows, milestones,
max-radiometry rule, anchor semantics) and record any intentional policy change in
`harmonization_architecture.py` plus `run_manifest.json` output fields.
