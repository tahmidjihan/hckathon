import os
import sys
import csv
import math
import json
import hashlib
import argparse
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Any, Optional, Set

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    pa = None
    pq = None
    HAS_PYARROW = False

EARTH_RADIUS_KM = 6371.0
# Aligned with operational availability of Suomi-NPP VIIRS (launched Oct 2011, operational Jan 2012)
DEFAULT_DATE_CUTOFF = "2012-01-19"

# Orbital-drift milestones defining historical cutoffs
TERRA_ORBITAL_CUTOFF = "2022-10-01"   # Oct 2022 Terra cutoff (exits matching workflow)
AQUA_ORBITAL_CUTOFF = "2023-02-01"    # Feb 2023 Aqua cutoff (adopts new 2h 15m window)

# Time-window policy durations (in minutes)
AQUA_PRE_CUTOFF_WINDOW_MIN = 60.0     # 60 minutes
AQUA_POST_CUTOFF_WINDOW_MIN = 135.0   # 2 hours 15 minutes (135.0 min)
TERRA_PRE_CUTOFF_WINDOW_MIN = 240.0   # 4 hours (240.0 min)


def parse_optional_float(val: Any) -> Optional[float]:
    """Parses optional numeric values without inserting artificial defaults."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in ("nan", "null", "none"):
        return None
    try:
        f = float(s)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def compute_file_sha256(filepath: str) -> str:
    """Computes SHA-256 hash of a file for deterministic provenance tracking."""
    if not os.path.isfile(filepath):
        return ""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Computes great-circle distance between two points in kilometers."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return EARTH_RADIUS_KM * c


def parse_datetime(date_str: Any, time_str: Any) -> Optional[datetime]:
    """
    Parses date and time strings into a datetime object.
    Requires a valid acquisition time (does not silently default to midnight).
    """
    if date_str is None or time_str is None:
        return None
    d_str = str(date_str).strip()[:10]
    t_raw = str(time_str).strip().replace(":", "")
    if len(t_raw) == 0:
        return None
    t_clean = t_raw.zfill(4)[:4]
    try:
        return datetime.strptime(f"{d_str} {t_clean}", "%Y-%m-%d %H%M")
    except Exception:
        try:
            return datetime.fromisoformat(str(date_str).strip())
        except Exception:
            return None


# -------------------------------------------------------------------------
# Readers & Input File Validation
# -------------------------------------------------------------------------
def load_modis_records(
    modis_path: str,
    target_pass: str,
    cutoff_date: str = DEFAULT_DATE_CUTOFF,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Loads and normalizes MODIS fire detections from a file or directory.
    Validates coordinates, timestamps, confidence ranges, and date bounds.
    Preserves missing scientific measurements as None (does not fabricate defaults).
    """
    records: List[Dict[str, Any]] = []
    stats = {
        "files_scanned": 0,
        "rows_read": 0,
        "rows_valid": 0,
        "rows_invalid_coords": 0,
        "rows_invalid_time": 0,
        "rows_before_cutoff": 0,
        "rows_out_of_date_range": 0,
        "rows_wrong_platform": 0,
        "file_details": [],
    }

    if not os.path.exists(modis_path):
        return records, stats

    file_paths: List[str] = []
    if os.path.isfile(modis_path):
        file_paths = [modis_path]
    else:
        for root, _, files in os.walk(modis_path):
            for f in files:
                if f.lower().endswith((".csv", ".txt")):
                    file_paths.append(os.path.join(root, f))

    cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")

    for fpath in sorted(file_paths):
        file_read = 0
        file_valid = 0
        stats["files_scanned"] += 1
        try:
            with open(fpath, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stats["rows_read"] += 1
                    file_read += 1
                    clean_row = {k.strip().lower().lstrip("\ufeff"): v for k, v in row.items() if k}

                    try:
                        lat_val = clean_row.get("latitude", clean_row.get("lat"))
                        lon_val = clean_row.get("longitude", clean_row.get("lon"))
                        if lat_val is None or lon_val is None:
                            stats["rows_invalid_coords"] += 1
                            continue
                        lat = float(lat_val)
                        lon = float(lon_val)
                        if math.isnan(lat) or math.isnan(lon) or abs(lat) > 90.0 or abs(lon) > 180.0:
                            stats["rows_invalid_coords"] += 1
                            continue
                    except (ValueError, TypeError):
                        stats["rows_invalid_coords"] += 1
                        continue

                    raw_d = clean_row.get("acq_date", clean_row.get("date"))
                    raw_t = clean_row.get("acq_time", clean_row.get("time"))
                    if not raw_d or not raw_t or str(raw_t).strip() == "":
                        stats["rows_invalid_time"] += 1
                        continue

                    acq_d = str(raw_d).strip()[:10]
                    acq_t = str(raw_t).strip()
                    dt = parse_datetime(acq_d, acq_t)
                    if not dt:
                        stats["rows_invalid_time"] += 1
                        continue

                    if dt < cutoff_dt:
                        stats["rows_before_cutoff"] += 1
                        continue

                    if start_date and acq_d < start_date:
                        stats["rows_out_of_date_range"] += 1
                        continue
                    if end_date and acq_d > end_date:
                        stats["rows_out_of_date_range"] += 1
                        continue

                    sat_val = str(clean_row.get("satellite", clean_row.get("platform", ""))).strip().lower()
                    if sat_val:
                        if target_pass == "aqua" and "terra" in sat_val and "aqua" not in sat_val:
                            stats["rows_wrong_platform"] += 1
                            continue
                        if target_pass == "terra" and "aqua" in sat_val and "terra" not in sat_val:
                            stats["rows_wrong_platform"] += 1
                            continue

                    raw_conf = parse_optional_float(clean_row.get("confidence"))
                    if raw_conf is not None and (raw_conf < 0.0 or raw_conf > 100.0):
                        raw_conf = None

                    frp_val = parse_optional_float(clean_row.get("frp", clean_row.get("power")))
                    bright_val = parse_optional_float(clean_row.get("brightness", clean_row.get("bright_ti4")))
                    t31_val = parse_optional_float(clean_row.get("bright_t31", clean_row.get("bright_ti5")))

                    daynight = str(clean_row.get("daynight", "D")).strip().upper()
                    if daynight not in ("D", "N"):
                        daynight = "D"

                    fire_type = clean_row.get("type", None)
                    if fire_type is not None:
                        try:
                            fire_type = int(fire_type)
                        except (ValueError, TypeError):
                            fire_type = None

                    records.append({
                        "source_modis_id": f"MODIS_{stats['rows_valid']}",
                        "latitude": lat,
                        "longitude": lon,
                        "dt": dt,
                        "acq_date": acq_d,
                        "acq_time": dt.strftime("%H%M"),
                        "confidence_pct": raw_conf,
                        "frp": frp_val,
                        "brightness": bright_val,
                        "bright_t31": t31_val,
                        "daynight": daynight,
                        "type": fire_type,
                        "platform": "Aqua" if target_pass == "aqua" else "Terra",
                        "instrument": "MODIS",
                        "file": os.path.basename(fpath),
                    })
                    stats["rows_valid"] += 1
                    file_valid += 1
        except Exception as e:
            print(f"[WARN] Failed to read MODIS file {fpath}: {e}")
            continue

        stats["file_details"].append({
            "path": fpath,
            "sha256": compute_file_sha256(fpath),
            "rows_read": file_read,
            "rows_valid": file_valid,
        })

    return records, stats


def load_unified_viirs_records(
    viirs_csv_path: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Loads unified VIIRS event records produced by unify_viirs.py.
    Validates coordinates, timestamps, and preserves optional measurements as None.
    Does not default absent confidence to nominal ('N').
    """
    records: List[Dict[str, Any]] = []
    stats = {
        "rows_read": 0,
        "rows_valid": 0,
        "rows_invalid": 0,
        "rows_out_of_date_range": 0,
    }

    if not os.path.exists(viirs_csv_path):
        return records, stats

    with open(viirs_csv_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            stats["rows_read"] += 1
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                if math.isnan(lat) or math.isnan(lon) or abs(lat) > 90.0 or abs(lon) > 180.0:
                    stats["rows_invalid"] += 1
                    continue

                raw_d = str(row.get("acq_date", "")).strip()[:10]
                raw_t = str(row.get("acq_time", "")).strip()
                if not raw_d or not raw_t:
                    stats["rows_invalid"] += 1
                    continue

                dt = parse_datetime(raw_d, raw_t)
                if not dt:
                    stats["rows_invalid"] += 1
                    continue

                if start_date and raw_d < start_date:
                    stats["rows_out_of_date_range"] += 1
                    continue
                if end_date and raw_d > end_date:
                    stats["rows_out_of_date_range"] += 1
                    continue

                frp = parse_optional_float(row.get("frp"))
                brightness = parse_optional_float(row.get("brightness", row.get("bright_ti4")))
                bright_ti4 = parse_optional_float(row.get("bright_ti4", row.get("brightness")))
                bright_t31 = parse_optional_float(row.get("bright_t31", row.get("bright_ti5")))
                bright_ti5 = parse_optional_float(row.get("bright_ti5", row.get("bright_t31")))

                # Determine VIIRS confidence label strictly without inventing nominal defaults
                conf_label = None
                raw_label = row.get("canonical_confidence_label") or row.get("confidence_label")
                if raw_label:
                    lbl = str(raw_label).strip().upper()
                    if lbl in ("L", "N", "H"):
                        conf_label = lbl
                if conf_label is None:
                    raw_c = row.get("canonical_confidence") or row.get("confidence")
                    if raw_c:
                        c_str = str(raw_c).strip().lower()
                        if "high" in c_str or c_str == "h":
                            conf_label = "H"
                        elif "low" in c_str or c_str == "l":
                            conf_label = "L"
                        elif "nominal" in c_str or c_str == "n":
                            conf_label = "N"

                sat = row.get("canonical_satellite") or row.get("satellite") or "VIIRS"
                inst = row.get("canonical_instrument") or row.get("instrument") or "VIIRS"
                daynight = str(row.get("canonical_daynight", row.get("daynight", "D"))).strip().upper()
                if daynight not in ("D", "N"):
                    daynight = "D"

                fire_type = row.get("type", None)
                if fire_type is not None:
                    try:
                        fire_type = int(fire_type)
                    except (ValueError, TypeError):
                        fire_type = None

                records.append({
                    "source_viirs_id": f"VIIRS_{idx}",
                    "latitude": lat,
                    "longitude": lon,
                    "dt": dt,
                    "acq_date": raw_d,
                    "acq_time": dt.strftime("%H%M"),
                    "frp": frp,
                    "brightness": brightness,
                    "bright_t31": bright_t31,
                    "bright_ti4": bright_ti4,
                    "bright_ti5": bright_ti5,
                    "confidence_label": conf_label,
                    "confidence": row.get("canonical_confidence", row.get("confidence")),
                    "daynight": daynight,
                    "satellite": str(sat),
                    "instrument": str(inst),
                    "num_satellites": int(row.get("num_satellites", 1)),
                    "multi_satellite": str(row.get("multi_satellite", "false")).lower() in ("true", "1"),
                    "type": fire_type,
                })
                stats["rows_valid"] += 1
            except Exception:
                stats["rows_invalid"] += 1

    return records, stats


# -------------------------------------------------------------------------
# Harmonization Matching Engine with Orbital-Drift Milestone Windows
# -------------------------------------------------------------------------
def get_matching_window_policy(
    pass_name: str,
    acq_date_str: str,
    terra_cutoff: str = TERRA_ORBITAL_CUTOFF,
    aqua_cutoff: str = AQUA_ORBITAL_CUTOFF,
    aqua_pre_window_min: float = AQUA_PRE_CUTOFF_WINDOW_MIN,
    aqua_post_window_min: float = AQUA_POST_CUTOFF_WINDOW_MIN,
    terra_pre_window_min: float = TERRA_PRE_CUTOFF_WINDOW_MIN,
) -> Tuple[bool, float, str, str]:
    """
    Evaluates the orbital-drift milestone policy for a detection date.

    Orbital Milestones:
      - Oct 2022 (Terra Cutoff):
          Terra exits matching workflow.
          * Pre-cutoff (< terra_cutoff): 4-hour window (240 min diagnostic persistence pass).
          * Post-cutoff (>= terra_cutoff): Terra–VIIRS matching is disabled in this harmonization stage.
      - Feb 2023 (Aqua Cutoff):
          Aqua adopts new matching window.
          * Pre-cutoff (< aqua_cutoff): 60-minute near-coincident window.
          * Post-cutoff (>= aqua_cutoff): 2-hour-15-minute (135.0 min) window.
          (Note: The 2h 15m window is an implementation rule, not yet an empirically validated guarantee).

    Returns:
      (is_matching_enabled, time_window_minutes, orbital_period, policy_note)
    """
    clean_date = str(acq_date_str).strip()[:10]
    p_lower = pass_name.lower()

    if p_lower == "terra":
        if clean_date >= terra_cutoff:
            return (
                False,
                0.0,
                "post_cutoff",
                f"Terra matching disabled post-milestone (acq_date {clean_date} >= {terra_cutoff}). Terra exits matching workflow.",
            )
        else:
            return (
                True,
                terra_pre_window_min,
                "pre_cutoff",
                f"Terra pre-cutoff pass: 4-hour window ({terra_pre_window_min} min diagnostic persistence).",
            )
    elif p_lower == "aqua":
        if clean_date >= aqua_cutoff:
            return (
                True,
                aqua_post_window_min,
                "post_cutoff",
                f"Aqua post-cutoff (>= {aqua_cutoff}): 2h 15m window ({aqua_post_window_min} min rule applied - implementation rule, unvalidated guarantee).",
            )
        else:
            return (
                True,
                aqua_pre_window_min,
                "pre_cutoff",
                f"Aqua pre-cutoff pass: 60-minute window ({aqua_pre_window_min} min near-coincident).",
            )
    else:
        return (True, 60.0, "standard", "Standard 60-minute window")


def harmonize_pass(
    modis_records: List[Dict[str, Any]],
    viirs_records: List[Dict[str, Any]],
    pass_name: str,
    radius_km: float = 1.5,
    terra_cutoff: str = TERRA_ORBITAL_CUTOFF,
    aqua_cutoff: str = AQUA_ORBITAL_CUTOFF,
    aqua_pre_window_min: float = AQUA_PRE_CUTOFF_WINDOW_MIN,
    aqua_post_window_min: float = AQUA_POST_CUTOFF_WINDOW_MIN,
    terra_pre_window_min: float = TERRA_PRE_CUTOFF_WINDOW_MIN,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Matches MODIS detections against unified VIIRS records based on orbital-drift milestone cutoffs:
      - Oct 2022 Terra cutoff: Terra exits matching workflow after milestone. Pre-cutoff: 4hr window; post-cutoff: matching disabled.
      - Feb 2023 Aqua cutoff: Pre-cutoff: 60-minute window; post-cutoff: 2-hour-15-minute (135 min) window.
      - Spatial search radius: Fixed 1.5 km centered on MODIS.
      - Preserves actual acquisition timestamps and compares via proper datetime arithmetic.
      - Retains anchor coordinates, deterministic nearest-candidate selection, and max radiometry.
    """
    matched_pairs: List[Dict[str, Any]] = []
    matched_viirs_ids: Set[str] = set()
    unmatched_modis: List[Dict[str, Any]] = []

    for m in modis_records:
        enabled, window_min, period, policy_note = get_matching_window_policy(
            pass_name,
            m["acq_date"],
            terra_cutoff=terra_cutoff,
            aqua_cutoff=aqua_cutoff,
            aqua_pre_window_min=aqua_pre_window_min,
            aqua_post_window_min=aqua_post_window_min,
            terra_pre_window_min=terra_pre_window_min,
        )

        if not enabled:
            # Terra post-cutoff: Terra exits matching workflow
            unmatched_modis.append({
                "match_id": None,
                "dataset_type": "only_modis",
                "match_status": "disabled_post_terra_cutoff",
                "processing_pass": pass_name,
                "processing_year": int(m["acq_date"][:4]),
                "modis_platform": m.get("platform", "Terra"),
                "modis_instrument": "MODIS",
                "viirs_satellite": None,
                "viirs_instrument": None,
                "viirs_present": False,
                "latitude": round(m["latitude"], 5),
                "longitude": round(m["longitude"], 5),
                "acq_date": m["acq_date"],
                "acq_time": m["acq_time"],
                "modis_latitude": round(m["latitude"], 5),
                "modis_longitude": round(m["longitude"], 5),
                "viirs_latitude": None,
                "viirs_longitude": None,
                "centroid_latitude": round(m["latitude"], 5),
                "centroid_longitude": round(m["longitude"], 5),
                "modis_acq_date": m["acq_date"],
                "modis_acq_time": m["acq_time"],
                "viirs_acq_date": None,
                "viirs_acq_time": None,
                "frp": round(m["frp"], 2) if m.get("frp") is not None else None,
                "modis_frp": round(m["frp"], 2) if m.get("frp") is not None else None,
                "viirs_frp": None,
                "viirs_total_frp": None,
                "brightness": round(m["brightness"], 2) if m.get("brightness") is not None else None,
                "modis_brightness": round(m["brightness"], 2) if m.get("brightness") is not None else None,
                "viirs_bright_ti4": None,
                "bright_t31": round(m["bright_t31"], 2) if m.get("bright_t31") is not None else None,
                "modis_bright_t31": round(m["bright_t31"], 2) if m.get("bright_t31") is not None else None,
                "viirs_bright_ti5": None,
                "modis_confidence_pct": round(m["confidence_pct"], 1) if m.get("confidence_pct") is not None else None,
                "viirs_confidence_label": None,
                "match_distance_km": None,
                "match_time_delta_minutes": None,
                "candidate_count": 0,
                "is_ambiguous": False,
                "selection_rule": None,
                "viirs_pixel_count": 0,
                "viirs_contributor_ids": None,
                "modis_type": m.get("type"),
                "viirs_type": None,
                "type_mismatch": False,
                "daynight_mismatch": False,
                "source_modis_id": m["source_modis_id"],
                "source_viirs_id": None,
                "time_window_minutes": 0.0,
                "orbital_period": period,
                "policy_note": policy_note,
                "filter_status": "retained",
                "filter_reason": "phase_4_filtering_removed",
            })
            continue

        time_window = timedelta(minutes=window_min)
        candidates = []
        for v in viirs_records:
            t_diff = abs(m["dt"] - v["dt"])
            if t_diff <= time_window:
                dist_km = haversine_distance_km(m["latitude"], m["longitude"], v["latitude"], v["longitude"])
                if dist_km <= radius_km:
                    candidates.append((v, dist_km, t_diff.total_seconds() / 60.0))

        if candidates:
            # Deterministic sorting: distance -> time delta -> stable source_viirs_id
            candidates.sort(key=lambda c: (c[1], c[2], str(c[0].get("source_viirs_id", ""))))

            cand_count = len(candidates)
            is_ambiguous = (cand_count > 1)
            best_viirs, best_dist, best_tdelta = candidates[0]

            matched_viirs_ids.add(best_viirs["source_viirs_id"])

            # Radiometric integration: max of available sensor values (no multi-candidate summation)
            m_frp = m.get("frp")
            v_frp = best_viirs.get("frp")
            if m_frp is not None and v_frp is not None:
                integrated_frp = max(m_frp, v_frp)
            elif m_frp is not None:
                integrated_frp = m_frp
            elif v_frp is not None:
                integrated_frp = v_frp
            else:
                integrated_frp = None

            m_b = m.get("brightness")
            v_b = best_viirs.get("brightness")
            if m_b is not None and v_b is not None:
                max_brightness = max(m_b, v_b)
            elif m_b is not None:
                max_brightness = m_b
            elif v_b is not None:
                max_brightness = v_b
            else:
                max_brightness = None

            m_t31 = m.get("bright_t31")
            v_t31 = best_viirs.get("bright_ti5") or best_viirs.get("bright_t31")
            if m_t31 is not None and v_t31 is not None:
                max_t31 = max(m_t31, v_t31)
            elif m_t31 is not None:
                max_t31 = m_t31
            elif v_t31 is not None:
                max_t31 = v_t31
            else:
                max_t31 = None

            # Type and Day/Night comparisons
            type_mismatch = False
            m_type = m.get("type")
            v_type = best_viirs.get("type")
            if m_type is not None and v_type is not None:
                type_mismatch = (m_type != v_type)

            daynight_mismatch = (m.get("daynight") != best_viirs.get("daynight"))

            stable_match_id = f"MATCH_{pass_name.upper()}_{m['source_modis_id']}_{best_viirs['source_viirs_id']}"
            all_candidate_ids = ";".join(c[0]["source_viirs_id"] for c in candidates)

            matched_pairs.append({
                "match_id": stable_match_id,
                "dataset_type": "matched_pairs",
                "match_status": "matched",
                "processing_pass": pass_name,
                "processing_year": int(m["acq_date"][:4]),
                # Canonical coordinates & timestamps are MODIS's (anchor consistency)
                "latitude": round(m["latitude"], 5),
                "longitude": round(m["longitude"], 5),
                "acq_date": m["acq_date"],
                "acq_time": m["acq_time"],
                # Sensor-specific explicit coordinates
                "modis_latitude": round(m["latitude"], 5),
                "modis_longitude": round(m["longitude"], 5),
                "viirs_latitude": round(best_viirs["latitude"], 5),
                "viirs_longitude": round(best_viirs["longitude"], 5),
                "centroid_latitude": round((m["latitude"] + best_viirs["latitude"]) / 2.0, 5),
                "centroid_longitude": round((m["longitude"] + best_viirs["longitude"]) / 2.0, 5),
                # Sensor-specific explicit timestamps
                "modis_acq_date": m["acq_date"],
                "modis_acq_time": m["acq_time"],
                "viirs_acq_date": best_viirs["acq_date"],
                "viirs_acq_time": best_viirs["acq_time"],
                # Radiometric measurements
                "frp": round(integrated_frp, 2) if integrated_frp is not None else None,
                "modis_frp": round(m_frp, 2) if m_frp is not None else None,
                "viirs_frp": round(v_frp, 2) if v_frp is not None else None,
                "viirs_total_frp": round(v_frp, 2) if v_frp is not None else None,
                "brightness": round(max_brightness, 2) if max_brightness is not None else None,
                "modis_brightness": round(m_b, 2) if m_b is not None else None,
                "viirs_bright_ti4": round(best_viirs.get("bright_ti4", v_b), 2) if best_viirs.get("bright_ti4", v_b) is not None else None,
                "bright_t31": round(max_t31, 2) if max_t31 is not None else None,
                "modis_bright_t31": round(m_t31, 2) if m_t31 is not None else None,
                "viirs_bright_ti5": round(best_viirs.get("bright_ti5", v_t31), 2) if best_viirs.get("bright_ti5", v_t31) is not None else None,
                # Confidence values
                "modis_confidence_pct": round(m["confidence_pct"], 1) if m.get("confidence_pct") is not None else None,
                "viirs_confidence_label": best_viirs.get("confidence_label"),
                # Provenance & Platform identification
                "modis_platform": m.get("platform", "Aqua" if pass_name == "aqua" else "Terra"),
                "modis_instrument": "MODIS",
                "viirs_satellite": best_viirs.get("satellite", "VIIRS"),
                "viirs_instrument": best_viirs.get("instrument", "VIIRS"),
                "viirs_present": True,
                # Match Diagnostics & Audit fields
                "match_distance_km": round(best_dist, 3),
                "match_time_delta_minutes": round(best_tdelta, 1),
                "time_window_minutes": round(window_min, 1),
                "orbital_period": period,
                "policy_note": policy_note,
                "candidate_count": cand_count,
                "is_ambiguous": is_ambiguous,
                "selection_rule": "nearest_spatial_temporal_id_tiebreak",
                "viirs_pixel_count": cand_count,
                "viirs_contributor_ids": all_candidate_ids,
                "modis_type": m_type,
                "viirs_type": v_type,
                "type_mismatch": type_mismatch,
                "daynight_mismatch": daynight_mismatch,
                "source_modis_id": m["source_modis_id"],
                "source_viirs_id": best_viirs["source_viirs_id"],
                "filter_status": "retained",
                "filter_reason": "phase_4_filtering_removed",
            })
        else:
            unmatched_modis.append({
                "match_id": None,
                "dataset_type": "only_modis",
                "match_status": "unmatched_modis",
                "processing_pass": pass_name,
                "processing_year": int(m["acq_date"][:4]),
                "modis_platform": m.get("platform", "Aqua" if pass_name == "aqua" else "Terra"),
                "modis_instrument": "MODIS",
                "viirs_satellite": None,
                "viirs_instrument": None,
                "viirs_present": False,
                "latitude": round(m["latitude"], 5),
                "longitude": round(m["longitude"], 5),
                "acq_date": m["acq_date"],
                "acq_time": m["acq_time"],
                "modis_latitude": round(m["latitude"], 5),
                "modis_longitude": round(m["longitude"], 5),
                "viirs_latitude": None,
                "viirs_longitude": None,
                "centroid_latitude": round(m["latitude"], 5),
                "centroid_longitude": round(m["longitude"], 5),
                "modis_acq_date": m["acq_date"],
                "modis_acq_time": m["acq_time"],
                "viirs_acq_date": None,
                "viirs_acq_time": None,
                "frp": round(m["frp"], 2) if m.get("frp") is not None else None,
                "modis_frp": round(m["frp"], 2) if m.get("frp") is not None else None,
                "viirs_frp": None,
                "viirs_total_frp": None,
                "brightness": round(m["brightness"], 2) if m.get("brightness") is not None else None,
                "modis_brightness": round(m["brightness"], 2) if m.get("brightness") is not None else None,
                "viirs_bright_ti4": None,
                "bright_t31": round(m["bright_t31"], 2) if m.get("bright_t31") is not None else None,
                "modis_bright_t31": round(m["bright_t31"], 2) if m.get("bright_t31") is not None else None,
                "viirs_bright_ti5": None,
                "modis_confidence_pct": round(m["confidence_pct"], 1) if m.get("confidence_pct") is not None else None,
                "viirs_confidence_label": None,
                "match_distance_km": None,
                "match_time_delta_minutes": None,
                "time_window_minutes": round(window_min, 1),
                "orbital_period": period,
                "policy_note": policy_note,
                "candidate_count": 0,
                "is_ambiguous": False,
                "selection_rule": None,
                "viirs_pixel_count": 0,
                "viirs_contributor_ids": None,
                "modis_type": m.get("type"),
                "viirs_type": None,
                "type_mismatch": False,
                "daynight_mismatch": False,
                "source_modis_id": m["source_modis_id"],
                "source_viirs_id": None,
                "filter_status": "retained",
                "filter_reason": "phase_4_filtering_removed",
            })

    unmatched_viirs: List[Dict[str, Any]] = []
    for v in viirs_records:
        if v["source_viirs_id"] not in matched_viirs_ids:
            v_b = v.get("brightness") or v.get("bright_ti4")
            v_t31 = v.get("bright_ti5") or v.get("bright_t31")
            unmatched_viirs.append({
                "match_id": None,
                "dataset_type": "only_viirs",
                "match_status": "unmatched_viirs",
                "processing_pass": pass_name,
                "processing_year": int(v["acq_date"][:4]),
                "modis_platform": "Aqua" if pass_name == "aqua" else "Terra",
                "modis_instrument": "MODIS",
                "viirs_satellite": v.get("satellite", "VIIRS"),
                "viirs_instrument": v.get("instrument", "VIIRS"),
                "viirs_present": True,
                "latitude": round(v["latitude"], 5),
                "longitude": round(v["longitude"], 5),
                "acq_date": v["acq_date"],
                "acq_time": v["acq_time"],
                "modis_latitude": None,
                "modis_longitude": None,
                "viirs_latitude": round(v["latitude"], 5),
                "viirs_longitude": round(v["longitude"], 5),
                "centroid_latitude": round(v["latitude"], 5),
                "centroid_longitude": round(v["longitude"], 5),
                "modis_acq_date": None,
                "modis_acq_time": None,
                "viirs_acq_date": v["acq_date"],
                "viirs_acq_time": v["acq_time"],
                "frp": round(v["frp"], 2) if v.get("frp") is not None else None,
                "modis_frp": None,
                "viirs_frp": round(v["frp"], 2) if v.get("frp") is not None else None,
                "viirs_total_frp": round(v["frp"], 2) if v.get("frp") is not None else None,
                "brightness": round(v_b, 2) if v_b is not None else None,
                "modis_brightness": None,
                "viirs_bright_ti4": round(v.get("bright_ti4", v_b), 2) if v.get("bright_ti4", v_b) is not None else None,
                "bright_t31": round(v_t31, 2) if v_t31 is not None else None,
                "modis_bright_t31": None,
                "viirs_bright_ti5": round(v.get("bright_ti5", v_t31), 2) if v.get("bright_ti5", v_t31) is not None else None,
                "modis_confidence_pct": None,
                "viirs_confidence_label": v.get("confidence_label"),
                "match_distance_km": None,
                "match_time_delta_minutes": None,
                "time_window_minutes": None,
                "orbital_period": None,
                "policy_note": None,
                "candidate_count": 0,
                "is_ambiguous": False,
                "selection_rule": None,
                "viirs_pixel_count": 1,
                "viirs_contributor_ids": v["source_viirs_id"],
                "modis_type": None,
                "viirs_type": v.get("type"),
                "type_mismatch": False,
                "daynight_mismatch": False,
                "source_modis_id": None,
                "source_viirs_id": v["source_viirs_id"],
                "filter_status": "retained",
                "filter_reason": "phase_4_filtering_removed",
            })

    return matched_pairs, unmatched_modis, unmatched_viirs



# -------------------------------------------------------------------------
# Global Confidence Calibration (Aqua Only)
# -------------------------------------------------------------------------
def calibrate_aqua_confidence(
    matched_aqua_pairs: List[Dict[str, Any]],
    min_class_samples: int = 3,
    scope_start: Optional[str] = None,
    scope_end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Computes empirical MODIS confidence percentage centroids (means) for VIIRS L, N, and H classes.
    Uses the full, unfiltered Aqua matched-pair population across the declared range.
    Requires each class to meet min_class_samples; marks invalid/incomplete if any class is sparse or absent.
    """
    classes: Dict[str, List[float]] = {"L": [], "N": [], "H": []}
    dates = []

    for m in matched_aqua_pairs:
        label = m.get("viirs_confidence_label")
        conf_pct = m.get("modis_confidence_pct")
        if conf_pct is not None and label in classes:
            classes[label].append(float(conf_pct))
        d = m.get("acq_date")
        if d:
            dates.append(str(d)[:10])

    sorted_dates = sorted(dates) if dates else []
    resolved_start = scope_start or (sorted_dates[0] if sorted_dates else None)
    resolved_end = scope_end or (sorted_dates[-1] if sorted_dates else None)

    centroids: Dict[str, Optional[float]] = {}
    sample_sizes: Dict[str, int] = {}
    for label, vals in classes.items():
        sample_sizes[label] = len(vals)
        centroids[label] = round(float(sum(vals) / len(vals)), 2) if vals else None

    l_threshold = centroids.get("L")
    has_l_data = (sample_sizes["L"] > 0)

    if not has_l_data:
        # VIIRS has NO L-confidence data in this sample: accept it cleanly without throwing an error!
        # If N and/or H are present, check their sample sizes or presence
        is_complete = any(sample_sizes[c] >= min_class_samples for c in ["N", "H"]) or (len(matched_aqua_pairs) > 0)
        is_valid = len(matched_aqua_pairs) > 0
        status = "accepted_without_l"
        status_reason = "Accepted: VIIRS data contains no L-confidence detections. Calibration accepted without error with available N/H classes."
    else:
        # L-confidence data IS present: continue standard validation
        is_complete = all(sample_sizes[c] >= min_class_samples for c in ["L", "N", "H"])
        is_valid = is_complete and (l_threshold is not None)
        status = "valid"
        status_reason = "Complete L, N, H classes with sufficient sample sizes"
        if not is_valid:
            sparse_classes = [c for c in ["L", "N", "H"] if sample_sizes[c] < min_class_samples]
            status = "insufficient_samples"
            status_reason = f"Classes with insufficient samples (< {min_class_samples}): {sparse_classes}"

    calib_id = f"CALIB_AQUA_{resolved_start or 'UNKNOWN'}_{resolved_end or 'UNKNOWN'}_{hashlib.sha256(json.dumps(centroids, sort_keys=True).encode()).hexdigest()[:8]}"

    return {
        "calibration_id": calib_id,
        "modis_platform": "Aqua",
        "date_scope": {
            "start_date": resolved_start,
            "end_date": resolved_end,
        },
        "total_matched_pairs": len(matched_aqua_pairs),
        "centroids": centroids,
        "sample_sizes": sample_sizes,
        "l_threshold": l_threshold,
        "has_l_data": has_l_data,
        "min_class_samples": min_class_samples,
        "is_complete": is_complete,
        "is_valid": is_valid,
        "status": status,
        "status_reason": status_reason,
    }


def save_calibration(calibration_data: Dict[str, Any], filepath: str):
    """Persists a frozen calibration JSON to disk."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(calibration_data, f, indent=2)


def load_and_validate_calibration(
    filepath: str,
    requested_start_date: Optional[str] = None,
    requested_end_date: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Loads and validates a persisted calibration JSON.
    Verifies that the calibration is valid, covers the requested date scope,
    and cleanly handles absent L-confidence without error.
    """
    if not os.path.isfile(filepath):
        return None, f"Calibration file not found at: {filepath}"

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            calib = json.load(f)
    except Exception as e:
        return None, f"Corrupted or unreadable calibration JSON: {e}"

    if not calib.get("is_valid"):
        return None, f"Calibration is marked invalid: {calib.get('status_reason', 'insufficient samples')}"

    l_thresh = calib.get("l_threshold")
    sample_sizes = calib.get("sample_sizes", {})
    if sample_sizes.get("L", 0) > 0 and (l_thresh is None or not isinstance(l_thresh, (int, float))):
        return None, "Missing or invalid l_threshold in calibration with present L class"

    # Validate date scope containment
    scope = calib.get("date_scope") or {}
    c_start = scope.get("start_date")
    c_end = scope.get("end_date")

    if requested_start_date and c_start and requested_start_date < c_start:
        return None, f"Out of scope: requested start date {requested_start_date} is earlier than calibration start {c_start}"

    if requested_end_date and c_end and requested_end_date > c_end:
        return None, f"Out of scope: requested end date {requested_end_date} is later than calibration end {c_end}"

    return calib, "valid"



# -------------------------------------------------------------------------
# Three-Population Low-Confidence Filtering
# -------------------------------------------------------------------------
def filter_harmonized_records(
    matched: List[Dict[str, Any]],
    unmatched_modis: List[Dict[str, Any]],
    unmatched_viirs: List[Dict[str, Any]],
    l_threshold: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    Applies the three distinct population filtering rules:
      1. Matched pairs: strictly below L-threshold is discarded; >= L-threshold is retained.
      2. Unmatched MODIS: strictly below L-threshold is discarded; >= L-threshold is retained.
      3. Unmatched VIIRS: 'L' is discarded; 'N' and 'H' are retained. Missing/unknown confidence is discarded.
    Attaches explicit filter_status and filter_reason to every record.
    Returns (retained_records, filtered_out_records, filter_counts).
    """
    retained: List[Dict[str, Any]] = []
    filtered_out: List[Dict[str, Any]] = []
    counts = {
        "matched_retained": 0,
        "matched_filtered": 0,
        "unmatched_modis_retained": 0,
        "unmatched_modis_filtered": 0,
        "unmatched_viirs_retained": 0,
        "unmatched_viirs_filtered": 0,
        "total_retained": 0,
        "total_filtered": 0,
    }

    # 1. Matched Pairs
    for m in matched:
        m_copy = dict(m)
        conf = m.get("modis_confidence_pct")
        if conf is not None and conf < l_threshold:
            m_copy["filter_status"] = "filtered"
            m_copy["filter_reason"] = f"modis_confidence_{conf}_strictly_below_l_thresh_{l_threshold}"
            counts["matched_filtered"] += 1
            filtered_out.append(m_copy)
        elif conf is not None:
            m_copy["filter_status"] = "retained"
            m_copy["filter_reason"] = f"modis_confidence_{conf}_gte_l_thresh_{l_threshold}"
            counts["matched_retained"] += 1
            retained.append(m_copy)
        else:
            m_copy["filter_status"] = "filtered"
            m_copy["filter_reason"] = "missing_modis_confidence"
            counts["matched_filtered"] += 1
            filtered_out.append(m_copy)

    # 2. Unmatched MODIS
    for um in unmatched_modis:
        um_copy = dict(um)
        conf = um.get("modis_confidence_pct")
        if conf is not None and conf < l_threshold:
            um_copy["filter_status"] = "filtered"
            um_copy["filter_reason"] = f"modis_confidence_{conf}_strictly_below_l_thresh_{l_threshold}"
            counts["unmatched_modis_filtered"] += 1
            filtered_out.append(um_copy)
        elif conf is not None:
            um_copy["filter_status"] = "retained"
            um_copy["filter_reason"] = f"modis_confidence_{conf}_gte_l_thresh_{l_threshold}"
            counts["unmatched_modis_retained"] += 1
            retained.append(um_copy)
        else:
            um_copy["filter_status"] = "filtered"
            um_copy["filter_reason"] = "missing_modis_confidence"
            counts["unmatched_modis_filtered"] += 1
            filtered_out.append(um_copy)

    # 3. Unmatched VIIRS
    for uv in unmatched_viirs:
        uv_copy = dict(uv)
        label = uv.get("viirs_confidence_label")
        if label in ("N", "H"):
            uv_copy["filter_status"] = "retained"
            uv_copy["filter_reason"] = f"viirs_confidence_{label}_retained"
            counts["unmatched_viirs_retained"] += 1
            retained.append(uv_copy)
        elif label == "L":
            uv_copy["filter_status"] = "filtered"
            uv_copy["filter_reason"] = "unmatched_viirs_l_confidence_filtered"
            counts["unmatched_viirs_filtered"] += 1
            filtered_out.append(uv_copy)
        else:
            uv_copy["filter_status"] = "filtered"
            uv_copy["filter_reason"] = "missing_or_invalid_viirs_confidence"
            counts["unmatched_viirs_filtered"] += 1
            filtered_out.append(uv_copy)

    counts["total_retained"] = len(retained)
    counts["total_filtered"] = len(filtered_out)
    return retained, filtered_out, counts


# -------------------------------------------------------------------------
# File Output Writers: Year-wise Parquet & Stable CSV
# -------------------------------------------------------------------------
def write_year_partitioned_parquet(
    records: List[Dict[str, Any]],
    output_dir: str,
    base_filename: str = "harmonized",
) -> List[Dict[str, Any]]:
    """
    Partitions records by canonical MODIS processing year and writes year-wise Parquet files.
    """
    if not records:
        return []

    if not HAS_PYARROW:
        raise RuntimeError("pyarrow is required to write year-wise Parquet files")

    yearly_groups: Dict[int, List[Dict[str, Any]]] = {}
    for r in records:
        yr = r.get("processing_year")
        if yr is None:
            acq_d = r.get("acq_date") or ""
            try:
                yr = int(str(acq_d)[:4])
            except (ValueError, TypeError):
                yr = 9999
        if yr not in yearly_groups:
            yearly_groups[yr] = []
        yearly_groups[yr].append(r)

    os.makedirs(output_dir, exist_ok=True)
    parquet_outputs = []

    for yr, group in sorted(yearly_groups.items()):
        parquet_path = os.path.join(output_dir, f"{base_filename}_{yr}.parquet")
        table = pa.Table.from_pylist(group)
        pq.write_table(table, parquet_path, compression="SNAPPY")
        parquet_outputs.append({
            "year": yr,
            "file_path": parquet_path,
            "row_count": len(group),
            "columns": table.column_names,
            "file_size_bytes": os.path.getsize(parquet_path),
        })

    return parquet_outputs


def write_csv_file(filepath: str, records: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None):
    """
    Safely writes records to CSV, creating parent directories.
    Always writes headers even when the dataset is empty.
    """
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    fields = fieldnames
    if not fields:
        if records:
            fields = list(records[0].keys())
        else:
            fields = [
                "match_id", "dataset_type", "match_status", "processing_pass", "processing_year",
                "latitude", "longitude", "acq_date", "acq_time", "frp", "brightness", "bright_t31",
                "modis_confidence_pct", "viirs_confidence_label", "modis_platform", "modis_instrument",
                "viirs_satellite", "viirs_instrument", "source_modis_id", "source_viirs_id",
            ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        if records:
            writer.writerows(records)


# -------------------------------------------------------------------------
# CLI & Pipeline Execution Orchestrator
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="MODIS–VIIRS 2-Phase Fire Detection Harmonization Pipeline"
    )
    parser.add_argument(
        "--pass",
        dest="target_pass",
        choices=["aqua", "terra", "both"],
        required=True,
        help="Harmonization pass: 'terra' (Phase 1), 'aqua' (Phase 2), or 'both'",
    )
    parser.add_argument(
        "--modis-dir",
        type=str,
        required=True,
        help="Path to MODIS input directory or CSV file",
    )
    parser.add_argument(
        "--viirs-file",
        type=str,
        required=True,
        help="Path to unified VIIRS CSV (from unify_viirs.py)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="quebec",
        help="Region identifier (default: quebec)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Single date identifier YYYY-MM-DD",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Inclusive start date YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Inclusive end date YYYY-MM-DD",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Target output directory (default: Data/Processed/<region>/<date>/Harmonized)",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=1.5,
        help="Matching spatial radius in kilometers (default: 1.5 km)",
    )
    parser.add_argument(
        "--cutoff-date",
        type=str,
        default=DEFAULT_DATE_CUTOFF,
        help="Inclusive cutoff date for MODIS detections (default: 2012-01-19)",
    )
    parser.add_argument(
        "--terra-cutoff",
        type=str,
        default=TERRA_ORBITAL_CUTOFF,
        help=f"Orbital-drift milestone cutoff for Terra (default: {TERRA_ORBITAL_CUTOFF})",
    )
    parser.add_argument(
        "--aqua-cutoff",
        type=str,
        default=AQUA_ORBITAL_CUTOFF,
        help=f"Orbital-drift milestone cutoff for Aqua (default: {AQUA_ORBITAL_CUTOFF})",
    )
    parser.add_argument(
        "--apply-low-confidence-filter",
        action="store_true",
        default=False,
        help="Optional: Apply legacy Phase 4 low-confidence filter (default: False, Phase 4 removed per updated decisions)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Dry run: inspect inputs, windows, and intended output paths without writing outputs",
    )
    parser.add_argument(
        "--fit-calibration", "--calibrate",
        action="store_true",
        default=False,
        help="Fit calibration from the loaded Aqua matched-pair population and persist to --calibration-file",
    )
    parser.add_argument(
        "--apply-calibration",
        type=str,
        default=None,
        help="Path to existing frozen calibration JSON to validate and inspect",
    )
    parser.add_argument(
        "--calibration-file",
        type=str,
        default=None,
        help="Default path for saving or loading calibration JSON",
    )
    parser.add_argument(
        "--min-class-samples",
        type=int,
        default=3,
        help="Minimum required sample count for L, N, and H classes in calibration",
    )

    args = parser.parse_args()

    # Resolve date parameters
    if args.date and (args.start_date or args.end_date):
        parser.error("Cannot specify both --date and --start-date/--end-date")
    if bool(args.start_date) != bool(args.end_date):
        parser.error("Both --start-date and --end-date must be provided together")

    if args.date:
        start_d = args.date
        end_d = args.date
        date_label = args.date
    elif args.start_date and args.end_date:
        if args.start_date > args.end_date:
            parser.error(f"--start-date ({args.start_date}) cannot be after --end-date ({args.end_date})")
        start_d = args.start_date
        end_d = args.end_date
        date_label = f"{args.start_date}_to_{args.end_date}" if args.start_date != args.end_date else args.start_date
    else:
        start_d = None
        end_d = None
        date_label = "all_dates"

    passes_to_run = ["terra", "aqua"] if args.target_pass == "both" else [args.target_pass]
    out_dir = args.output_dir or f"Data/Processed/{args.region}/{date_label}/Harmonized"

    print("=" * 75)
    print(f" MODIS–VIIRS HARMONIZATION PIPELINE: {', '.join(p.upper() for p in passes_to_run)}")
    print(f" Region:           {args.region}")
    print(f" Date Scope:       {date_label}")
    print(f" Radius:           {args.radius_km} km")
    print(f" Cutoff Date:      {args.cutoff_date}")
    print(f" Terra Cutoff:     {args.terra_cutoff} (matching disabled post-cutoff)")
    print(f" Aqua Cutoff:      {args.aqua_cutoff} (window: 60m pre-cutoff -> 135m post-cutoff)")
    print(f" Phase 4 Filter:   {'ENABLED (Legacy)' if args.apply_low_confidence_filter else 'REMOVED (All Populations Retained)'}")
    print(f" Output Directory: {out_dir}")
    print("=" * 75)

    viirs_records, viirs_stats = load_unified_viirs_records(
        args.viirs_file, start_date=start_d, end_date=end_d
    )
    print(f"[INFO] Loaded {viirs_stats['rows_valid']} valid VIIRS records from {args.viirs_file}")

    manifest_data: Dict[str, Any] = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "region": args.region,
        "date_scope": {"start_date": start_d, "end_date": end_d, "label": date_label},
        "radius_km": args.radius_km,
        "cutoff_date": args.cutoff_date,
        "orbital_milestones": {
            "terra_cutoff": args.terra_cutoff,
            "aqua_cutoff": args.aqua_cutoff,
            "aqua_pre_window_min": AQUA_PRE_CUTOFF_WINDOW_MIN,
            "aqua_post_window_min": AQUA_POST_CUTOFF_WINDOW_MIN,
            "terra_pre_window_min": TERRA_PRE_CUTOFF_WINDOW_MIN,
        },
        "phase_4_filtering_status": "applied" if args.apply_low_confidence_filter else "removed_per_new_decisions",
        "input_files": {
            "viirs_unified_csv": {
                "path": args.viirs_file,
                "sha256": compute_file_sha256(args.viirs_file),
                "rows_read": viirs_stats["rows_read"],
                "rows_valid": viirs_stats["rows_valid"],
            },
            "modis_inputs": [],
        },
        "phases": {},
    }

    calib_file_path = args.apply_calibration or args.calibration_file or os.path.join(out_dir, "calibration.json")

    for p in passes_to_run:
        phase_label = "Phase1_Terra" if p == "terra" else "Phase2_Aqua"
        print(f"\n---> Executing {phase_label} ({p.upper()})")

        modis_records, modis_stats = load_modis_records(
            args.modis_dir, p, args.cutoff_date, start_date=start_d, end_date=end_d
        )
        print(f"[INFO] Loaded {modis_stats['rows_valid']} valid MODIS {p.capitalize()} records")

        for f_det in modis_stats.get("file_details", []):
            if f_det not in manifest_data["input_files"]["modis_inputs"]:
                manifest_data["input_files"]["modis_inputs"].append(f_det)

        matched, unmatched_modis, unmatched_viirs = harmonize_pass(
            modis_records,
            viirs_records,
            p,
            radius_km=args.radius_km,
            terra_cutoff=args.terra_cutoff,
            aqua_cutoff=args.aqua_cutoff,
        )

        phase_dir = os.path.join(out_dir, phase_label)
        dataset1_path = os.path.join(phase_dir, f"{p}_only_modis.csv")
        dataset2_path = os.path.join(phase_dir, f"{p}_only_viirs.csv")
        dataset3_path = os.path.join(phase_dir, f"{p}_matched_pairs.csv")
        legacy_combined_path = os.path.join(out_dir, f"modis-viirs-{p}-{args.region}-{date_label}.csv")

        if args.dry_run:
            print(f"[DRY RUN] Dataset 1: {dataset1_path} ({len(unmatched_modis)} rows)")
            print(f"[DRY RUN] Dataset 2: {dataset2_path} ({len(unmatched_viirs)} rows)")
            print(f"[DRY RUN] Dataset 3: {dataset3_path} ({len(matched)} rows)")
            continue

        # Write diagnostic population CSVs
        write_csv_file(dataset1_path, unmatched_modis)
        write_csv_file(dataset2_path, unmatched_viirs)
        write_csv_file(dataset3_path, matched)

        all_raw_phase = matched + unmatched_modis + unmatched_viirs
        write_csv_file(legacy_combined_path, all_raw_phase)

        if p == "terra":
            # NEW: Terra Parquet writer
            terra_parquet_data = matched + unmatched_modis  # excludes unmatched_viirs per design decision: Terra/VIIRS co-detection rate too low to be meaningful (NASA: Aqua ~73% vs Terra ~66% standalone detection)
            terra_parquet_meta: List[Dict[str, Any]] = []

            if HAS_PYARROW and terra_parquet_data:

                yearly_parquet_dir = os.path.join(out_dir, "Yearly")

                terra_parquet_meta = write_year_partitioned_parquet(
                    terra_parquet_data,
                    yearly_parquet_dir,
                    base_filename="terra_harmonized",
                )
                for pm in terra_parquet_meta:
                    print(f"[INFO] Generated Terra Diagnostic Parquet (Year {pm['year']}): {pm['row_count']:,} rows -> {pm['file_path']}")

            manifest_data["phases"]["phase1_terra"] = {
                "role": "diagnostic_persistence",
                "phase_name": phase_label,
                "modis_input_count": modis_stats["rows_valid"],
                "dataset1_only_modis_count": len(unmatched_modis),
                # "dataset2_only_viirs_count": len(unmatched_viirs),
                "dataset3_matched_pairs_count": len(matched),
                "total_phase_records": len(all_raw_phase),
                
                "diagnostic_parquet_outputs": terra_parquet_meta,

                "matching_policy": (
                    "Terra matching disabled post-Oct 2022 cutoff"
                    if (start_d and start_d >= args.terra_cutoff)
                    else "4-hour pre-cutoff window"
                ),
                "output_datasets": {
                    "dataset1_only_modis": dataset1_path,
                    "dataset2_only_viirs": dataset2_path,
                    "dataset3_matched_pairs": dataset3_path,
                    "legacy_combined_csv": legacy_combined_path,
                },
            }
            print(f"[INFO] Phase 1 (Terra - Diagnostic): Written 3 datasets (Matched: {len(matched)}, Only MODIS: {len(unmatched_modis)}, Only VIIRS: {len(unmatched_viirs)}) -> Total: {len(all_raw_phase):,}")

        elif p == "aqua":
            # Aqua: Primary Harmonized Product
            calib_to_use = None
            if args.fit_calibration:
                print(f"[INFO] Fitting Aqua confidence calibration across {len(matched):,} matched pairs...")
                calib = calibrate_aqua_confidence(
                    matched,
                    min_class_samples=args.min_class_samples,
                    scope_start=start_d,
                    scope_end=end_d,
                )
                save_calibration(calib, calib_file_path)
                print(f"[INFO] Calibration status: {calib['status']} (L-Threshold: {calib.get('l_threshold')}%, Has L data: {calib.get('has_l_data', False)})")
                print(f"[INFO] Saved calibration to: {calib_file_path}")
                calib_to_use = calib if calib["is_valid"] else None
            elif os.path.isfile(calib_file_path):
                calib_to_use, calib_msg = load_and_validate_calibration(
                    calib_file_path, requested_start_date=start_d, requested_end_date=end_d
                )
                if calib_to_use:
                    print(f"[INFO] Reusing validated calibration from {calib_file_path}")

            retained_records: List[Dict[str, Any]] = []
            filtered_out_records: List[Dict[str, Any]] = []
            filter_counts: Dict[str, Any] = {}
            parquet_meta: List[Dict[str, Any]] = []

            # Phase 4 Low-Confidence Filtering: REMOVED from default workflow
            if args.apply_low_confidence_filter and calib_to_use and calib_to_use.get("l_threshold") is not None:
                l_thresh = float(calib_to_use["l_threshold"])
                print(f"[INFO] Applying optional legacy L-threshold {l_thresh}% to Aqua populations...")
                retained_records, filtered_out_records, filter_counts = filter_harmonized_records(
                    matched, unmatched_modis, unmatched_viirs, l_threshold=l_thresh
                )
                filtered_out_csv = os.path.join(phase_dir, "aqua_filtered_out.csv")
                write_csv_file(filtered_out_csv, filtered_out_records)
            else:
                # Default: Retain all three populations without discarding records
                print("[INFO] Phase 4 Low-Confidence Filtering: REMOVED per updated decisions. Retaining all populations (matched pairs, unmatched MODIS, unmatched VIIRS).")
                retained_records = all_raw_phase
                filter_counts = {
                    "matched_retained": len(matched),
                    "unmatched_modis_retained": len(unmatched_modis),
                    "unmatched_viirs_retained": len(unmatched_viirs),
                    "total_retained": len(all_raw_phase),
                    "total_filtered": 0,
                    "filtering_policy": "phase_4_filtering_removed",
                }

            # Generate Year-wise Parquet Product
            if HAS_PYARROW and retained_records:
                yearly_parquet_dir = os.path.join(out_dir, "Yearly")
                parquet_meta = write_year_partitioned_parquet(
                    retained_records, yearly_parquet_dir, base_filename="harmonized"
                )
                for pm in parquet_meta:
                    print(f"[INFO] Generated Primary Parquet (Year {pm['year']}): {pm['row_count']:,} rows -> {pm['file_path']}")

            manifest_data["phases"]["phase2_aqua"] = {
                "role": "primary_harmonized_product",
                "phase_name": phase_label,
                "modis_input_count": modis_stats["rows_valid"],
                "dataset1_only_modis_count": len(unmatched_modis),
                "dataset2_only_viirs_count": len(unmatched_viirs),
                "dataset3_matched_pairs_count": len(matched),
                "total_retained_count": len(retained_records),
                "calibration_applied": calib_to_use,
                "filter_counts": filter_counts,
                "phase_4_filtering": "removed_per_new_decisions" if not args.apply_low_confidence_filter else "applied",
                "primary_parquet_outputs": parquet_meta,
                "output_datasets": {
                    "dataset1_only_modis": dataset1_path,
                    "dataset2_only_viirs": dataset2_path,
                    "dataset3_matched_pairs": dataset3_path,
                    "legacy_combined_csv": legacy_combined_path,
                },
            }

    if not args.dry_run:
        manifest_path = os.path.join(out_dir, "run_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
        print(f"\n[INFO] Complete run manifest saved to: {manifest_path}")


if __name__ == "__main__":
    main()
