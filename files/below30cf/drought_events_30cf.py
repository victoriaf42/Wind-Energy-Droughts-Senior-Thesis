"""
Wind Drought Events — 30% CF Threshold (2020–2024)
====================================================
Applies the CF = 0.30 drought threshold to ERCOT-mapped grid cells for the
2020–2024 study period, joins year-specific installed wind capacity to each
event, and produces two complementary output files per grid cell:

  Output A — Drought event summaries  (wind_results_{lat}_{lon}.csv)
      One row per identified drought event, with duration, severity metrics,
      load zone, and the nameplate capacity installed at that grid cell in
      the year the event occurred.

  Output B — Hourly drought flags      (grid_{lat}_{lon}_hourly.csv)
      One row per hour (2020–2024), recording whether the hour was inside a
      drought event (is_drought = 1) and, if so, the hourly CF shortfall
      below the 0.30 threshold. Used downstream in the price impact analysis.

Why 2020–2024 only
------------------
The full 1950–2024 hazard characterisation is handled by
wind_drought_identification.py. This script is scoped to the period covered
by ERCOT hourly price data, which is required for the PPA financial risk
analysis.

Why CF = 0.30
--------------
0.30 approximates the long-run mean CF across the ERA5 domain (~0.31) and
is consistent with reported onshore wind CFs for Texas in the 2020s. It
represents a meaningful shortfall below expected generation rather than an
arbitrary cutoff. Sensitivity analyses at other thresholds can be run by
changing the THRESHOLD variable below.

Requirements
------------
    pip install numpy pandas xarray netCDF4 openpyxl

Input files required
--------------------
  WIND_CF_DIR    : annual wind CF NetCDF files from wind_cf_pipeline.py
                   named {year}_wind_cf.nc
  MAPPING_FILE   : grid_to_loadzone_mapping.csv from ercot_spatial_grid.py
                   columns: lat_idx, lon_idx, lat, lon, load_zone
  CAPACITY_DIR   : annual installed capacity CSVs and Excel files:
                   {year}_onshore_wind_turbine.csv (columns: lat_idx,
                   lon_idx, Nameplate Capacity (MW)) and
                   {year}_all_plants_with_loadzones.xlsx (columns:
                   Technology, Load Zone, Nameplate Capacity (MW)).
                   Both compiled from the EIA Form 860 dataset.

Note on pct_wind
-----------------
pct_wind is the share of total installed nameplate capacity in a load zone
attributable to onshore wind turbines, calculated annually from EIA Form 860.
It is used to produce two capacity-weighted severity scores:

  weighted_severity_capacity : total_severity x installed_capacity_mw
      Weights by the wind capacity at risk at that specific grid cell.

  weighted_severity_pct_wind : total_severity x pct_wind
      Weights by the wind share of the entire load zone, reflecting the
      zone's overall exposure to wind generation shortfalls.

Output columns — drought event summaries
-----------------------------------------
  event_id                   : sequential event identifier within grid cell/year
  duration                   : consecutive below-threshold hours
  total_severity             : sum of (threshold - CF) over the event
  avg_severity               : total_severity / duration
  pct_severity               : avg_severity / threshold
  start_time                 : timestamp of the first drought hour
  lat_idx, lon_idx           : ERA5 grid cell indices
  lat, lon                   : grid cell coordinates
  load_zone                  : ERCOT load zone (e.g. LZ_WEST)
  installed_capacity_mw      : nameplate wind capacity at this cell in that year
  pct_wind                   : wind share of total load zone capacity (that year)
  weighted_severity_capacity : total_severity x installed_capacity_mw
  weighted_severity_pct_wind : total_severity x pct_wind

Output columns — hourly drought flags
--------------------------------------
  datetime              : hourly timestamp
  is_drought            : 1 if CF < threshold, 0 otherwise
  shortfall_cf          : max(0, threshold − CF); 0 for non-drought hours
  wind_cf               : raw capacity factor value
  lat_idx, lon_idx      : ERA5 grid cell indices
  load_zone             : ERCOT load zone

Usage
-----
    python drought_events_30cf.py

    Update the path variables in the CONFIGURATION block before running.
    Set THRESHOLD to a different value (e.g. 0.10, 0.15) for sensitivity
    analyses without changing any other logic.
"""

import os
import re
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Annual wind CF NetCDF files ({year}_wind_cf.nc) from wind_cf_pipeline.py
WIND_CF_DIR = Path("output/wind_cfs")

# Grid-to-load-zone mapping from ercot_spatial_grid.py
MAPPING_FILE = Path("output/load_zones/grid_to_loadzone_mapping.csv")

# Annual installed capacity files (from EIA Form 860):
#   {year}_onshore_wind_turbine.csv  — grid-cell-level wind capacity
#   {year}_all_plants_with_loadzones.xlsx — all technologies, used for pct_wind
CAPACITY_DIR = Path("data/installed_capacities")

# Output directories
EVENTS_DIR  = Path("output/drought_events_30cf")   # drought event summaries
HOURLY_DIR  = Path("output/drought_hourly_30cf")   # hourly drought flags

# CF drought threshold
THRESHOLD = 0.30

# Study period for financial risk analysis
YEARS = list(range(2020, 2025))


# =============================================================================
# HELPER: drought event identification (same logic as wind_drought_identification.py)
# =============================================================================

def identify_drought_events(
    cf_series: np.ndarray,
    threshold: float,
    start_date: str
) -> pd.DataFrame:
    """
    Identify drought events from a CF time series.

    Returns a DataFrame with one row per event: event_id, duration,
    total_severity, avg_severity, pct_severity, start_time.
    Returns an empty DataFrame if no events are found.
    """
    dates    = pd.date_range(start=start_date, periods=len(cf_series), freq="h")
    severity = np.maximum(0, threshold - cf_series)

    below     = severity > 0
    diff      = np.diff(below.astype(int), prepend=0)
    event_id  = np.cumsum(diff == 1) * below

    df = pd.DataFrame({"date": dates, "severity": severity, "event_id": event_id})
    df_events = df[df["event_id"] > 0]

    if df_events.empty:
        return pd.DataFrame(
            columns=["event_id", "duration", "total_severity",
                     "avg_severity", "pct_severity", "start_time"]
        )

    summary = (
        df_events.groupby("event_id")
        .agg(duration=("severity", "size"),
             total_severity=("severity", "sum"),
             start_time=("date", "first"))
        .reset_index()
    )
    summary["avg_severity"] = summary["total_severity"] / summary["duration"]
    summary["pct_severity"] = summary["avg_severity"] / threshold
    return summary


# =============================================================================
# HELPER: hourly drought flag for a single year
# =============================================================================

def build_hourly_flags(
    cf_series: np.ndarray,
    threshold: float,
    start_date: str,
    lat_idx: int,
    lon_idx: int,
    load_zone: str
) -> pd.DataFrame:
    """
    Build an hourly flag DataFrame for one grid cell / one year.

    Columns: datetime, is_drought, shortfall_cf, wind_cf, lat_idx, lon_idx,
             load_zone.
    """
    dates = pd.date_range(start=start_date, periods=len(cf_series), freq="h")
    is_drought   = (cf_series < threshold).astype(int)
    shortfall_cf = np.maximum(0, threshold - cf_series)

    return pd.DataFrame({
        "datetime":    dates,
        "is_drought":  is_drought,
        "shortfall_cf": shortfall_cf,
        "wind_cf":     cf_series,
        "lat_idx":     lat_idx,
        "lon_idx":     lon_idx,
        "load_zone":   load_zone,
    })


# =============================================================================
# MAIN
# =============================================================================

def main():
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    HOURLY_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load grid mapping ---
    mapping = pd.read_csv(MAPPING_FILE)
    for col in ["lat_idx", "lon_idx"]:
        mapping[col] = mapping[col].astype(int)
    mapping["load_zone"] = mapping["load_zone"].astype(str).str.strip()
    mapping = mapping.dropna(subset=["load_zone"])

    print("Wind Drought Events — CF = 0.30 Threshold (2020–2024)")
    print("=" * 55)
    print(f"Grid cells to process : {len(mapping)}")
    print(f"Years                 : {YEARS[0]}–{YEARS[-1]}")
    print(f"CF threshold          : {THRESHOLD}")
    print(f"Events output         : {EVENTS_DIR}/")
    print(f"Hourly output         : {HOURLY_DIR}/")
    print("=" * 55)

    # --- Load all capacity files once ---
    # Grid-cell-level wind capacity (for installed_capacity_mw per event)
    capacity_by_year = {}
    # Load-zone-level pct_wind (wind share of total zone capacity, for weighting)
    pct_wind_by_year = {}

    for year in YEARS:
        # Grid-cell wind capacity
        cap_path = CAPACITY_DIR / f"{year}_onshore_wind_turbine.csv"
        if not cap_path.exists():
            print(f"  WARNING: {cap_path.name} not found for {year}")
            capacity_by_year[year] = pd.DataFrame(
                columns=["lat_idx", "lon_idx", "installed_capacity_mw"]
            )
        else:
            cap_df = pd.read_csv(cap_path)
            cap_df = (
                cap_df.groupby(["lat_idx", "lon_idx"], as_index=False)
                ["Nameplate Capacity (MW)"].sum()
                .rename(columns={"Nameplate Capacity (MW)": "installed_capacity_mw"})
            )
            capacity_by_year[year] = cap_df

        # Load-zone pct_wind from all-plants file
        # pct_wind = wind nameplate capacity / total nameplate capacity per zone
        all_plants_path = CAPACITY_DIR / f"{year}_all_plants_with_loadzones.xlsx"
        if not all_plants_path.exists():
            print(f"  WARNING: {all_plants_path.name} not found for {year}")
            pct_wind_by_year[year] = {}
        else:
            ap = pd.read_excel(all_plants_path, engine="openpyxl")
            ap["Nameplate Capacity (MW)"] = pd.to_numeric(
                ap["Nameplate Capacity (MW)"], errors="coerce"
            )
            # Detect load zone column
            lz_col = next(
                (c for c in ap.columns
                 if "load" in c.lower() and "zone" in c.lower()), None
            )
            if lz_col is None:
                print(f"  WARNING: no load zone column in {all_plants_path.name}")
                pct_wind_by_year[year] = {}
            else:
                ap["lz"] = (
                    ap[lz_col].astype(str).str.upper()
                    .str.replace("LZ_", "", regex=False)
                )
                total_by_lz = ap.groupby("lz")["Nameplate Capacity (MW)"].sum()
                wind_by_lz  = ap.loc[
                    ap["Technology"] == "Onshore Wind Turbine"
                ].groupby("lz")["Nameplate Capacity (MW)"].sum()
                pct = (wind_by_lz / total_by_lz * 100).fillna(0).round(4)
                pct_wind_by_year[year] = pct.to_dict()

    # --- Process each grid cell ---
    completed = skipped = 0

    for _, row in mapping.iterrows():
        lat_idx   = int(row["lat_idx"])
        lon_idx   = int(row["lon_idx"])
        lat       = row["lat"]
        lon       = row["lon"]
        load_zone = row["load_zone"]

        events_path = EVENTS_DIR / f"wind_results_{lat_idx}_{lon_idx}.csv"
        hourly_path = HOURLY_DIR  / f"grid_{lat_idx}_{lon_idx}_hourly.csv"

        if events_path.exists() and hourly_path.exists():
            skipped += 1
            continue

        all_events = []
        all_hourly = []

        for year in YEARS:
            cf_path = WIND_CF_DIR / f"{year}_wind_cf.nc"
            if not cf_path.exists():
                print(f"  WARNING: {cf_path} not found, skipping year {year}")
                continue

            try:
                with xr.open_dataset(cf_path) as ds:
                    cf_data = (
                        ds["wind_cf"]
                        .isel(latitude=lat_idx, longitude=lon_idx)
                        .load().values
                    )
            except Exception as e:
                print(f"  ERROR reading {cf_path.name} at ({lat_idx},{lon_idx}): {e}")
                continue

            start_date = f"{year}-01-01 00:00"

            # --- Drought events ---
            events = identify_drought_events(cf_data, THRESHOLD, start_date)
            if not events.empty:
                events["lat_idx"]   = lat_idx
                events["lon_idx"]   = lon_idx
                events["lat"]       = lat
                events["lon"]       = lon
                events["load_zone"] = load_zone

                # Join installed capacity for this year
                cap_df = capacity_by_year[year]
                match  = cap_df[
                    (cap_df["lat_idx"] == lat_idx) &
                    (cap_df["lon_idx"] == lon_idx)
                ]
                capacity = match["installed_capacity_mw"].iloc[0]                     if not match.empty else 0.0
                events["installed_capacity_mw"] = capacity

                # Join pct_wind for this load zone and year
                lz_key = load_zone.replace("LZ_", "")
                pct_wind = pct_wind_by_year[year].get(lz_key, 0.0)
                events["pct_wind"] = pct_wind

                # Capacity-weighted severity scores
                events["weighted_severity_capacity"] = (
                    events["total_severity"] * events["installed_capacity_mw"]
                )
                events["weighted_severity_pct_wind"] = (
                    events["total_severity"] * events["pct_wind"]
                )

                all_events.append(events)

            # --- Hourly flags ---
            hourly = build_hourly_flags(
                cf_data, THRESHOLD, start_date, lat_idx, lon_idx, load_zone
            )
            all_hourly.append(hourly)

        # Save outputs
        if all_events:
            pd.concat(all_events, ignore_index=True).to_csv(events_path, index=False)

        if all_hourly:
            pd.concat(all_hourly, ignore_index=True).to_csv(hourly_path, index=False)

        completed += 1
        if completed % 25 == 0:
            print(f"  Progress: {completed} cells completed, {skipped} skipped")

    print(f"\nDone.")
    print(f"  Completed : {completed}")
    print(f"  Skipped   : {skipped} (output already existed)")
    print(f"  Events    : {EVENTS_DIR}/")
    print(f"  Hourly    : {HOURLY_DIR}/")


if __name__ == "__main__":
    main()
