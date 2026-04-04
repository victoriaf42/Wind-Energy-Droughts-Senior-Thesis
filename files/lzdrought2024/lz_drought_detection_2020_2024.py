"""
Load-Zone Wind Drought Detection — Event and Hourly Files (2020–2024)
======================================================================
Produces the load-zone-level drought event and hourly flag files used
throughout the price impact and PPA financial risk analysis.

A drought hour is defined at the load zone level: it occurs when at least
min_capacity_fraction (default 50%) of the zone's installed wind capacity
is simultaneously operating below the CF threshold. This capacity-fraction
trigger avoids flagging isolated single-cell shortfalls as zone-wide droughts
and is consistent with the CapThresh50pct labelling used in all output files.

Two output files are produced per load zone per threshold:

  {ZONE}_CF{threshold}_CapThresh50pct_years_2020_2024_hourly.csv
      One row per hour (2020–2024) with:
        datetime, is_drought, zone_avg_cf, shortfall_cf,
        capacity_below_mw, pct_capacity_below, total_capacity_mw,
        pct_wind, weighted_severity_by_pct_wind

  {ZONE}_CF{threshold}_CapThresh50pct_years_2020_2024_events.csv
      One row per identified drought event with:
        start_time, end_time, year, duration, total_severity, avg_severity,
        max_severity, total_weighted_severity, avg_weighted_severity,
        pct_wind, avg_zone_cf, avg_capacity_below_mw, avg_pct_capacity_below,
        total_capacity_mw, avg_severity_per_hour, pct_severity

These files are required inputs for:
  - price_capacity_prep.py  (merging prices into hourly files)
  - drought_events_30cf.py  (grid-cell level events with price join)
  - exploratory_price_analysis.py

Why 2020–2024 only
-------------------
This script covers the period for which ERCOT hourly price data are available.
The full 1950–2024 historical hazard characterisation is handled separately
by lz_drought_events_historical.py.

Sensitivity analyses
---------------------
The script is designed to run across multiple CF thresholds in one pass.
Set CF_THRESHOLDS to a list (e.g. [0.06, 0.10, 0.15, 0.30]) to produce
output files for each threshold. The primary analysis uses CF = 0.30.

Requirements
------------
    pip install numpy pandas xarray netCDF4 openpyxl

Input files required
--------------------
  NC_DIR           : annual wind CF NetCDF files ({year}_wind_cf.nc)
                     from wind_cf_pipeline.py
  GRID_MAPPING     : grid_to_loadzone_mapping.csv from ercot_spatial_grid.py
                     columns: lat, lon, load_zone
  CAPACITY_SUMMARY : loadzone_capacity_summary.csv from price_capacity_prep.py
                     columns: load_zone, year, pct_wind, total_capacity_mw
  CAPACITY_DIR     : annual EIA Form 860 wind capacity CSVs
                     {year}_onshore_wind_turbine.csv
                     columns: grid_latitude, grid_longitude, Load Zone,
                              Nameplate Capacity (MW)

Usage
-----
    python lz_drought_detection_2020_2024.py

    Update the path variables in the CONFIGURATION block before running.
"""

import os
import re
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from datetime import datetime


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Annual wind CF NetCDF files from wind_cf_pipeline.py
NC_DIR = Path("output/wind_cfs")

# Grid-to-load-zone mapping from ercot_spatial_grid.py
GRID_MAPPING = Path("output/load_zones/grid_to_loadzone_mapping.csv")

# Load-zone capacity summary (pct_wind) from price_capacity_prep.py
CAPACITY_SUMMARY = Path("output/analysis_inputs/loadzone_capacity_summary.csv")

# Annual installed capacity CSVs from EIA Form 860
CAPACITY_DIR = Path("data/installed_capacities")

# Output directory for event and hourly files
OUTPUT_DIR = Path("output/lz_drought_2020_2024")

# CF thresholds to process — primary analysis uses 0.30
# Add others for sensitivity analysis (e.g. [0.06, 0.10, 0.15, 0.30])
CF_THRESHOLDS = [0.30]

# Minimum fraction of zone MW below threshold to trigger a drought hour
MIN_CAPACITY_FRACTION = 0.50

# Study period
START_YEAR = 2020
END_YEAR   = 2024

# Load zones to process
LOAD_ZONES = ["LZ_WEST", "LZ_NORTH", "LZ_SOUTH", "LZ_HOUSTON"]


# =============================================================================
# CORE FUNCTION
# =============================================================================

def process_load_zone(
    load_zone: str,
    cf_threshold: float,
    nc_dir: Path,
    grid_map: pd.DataFrame,
    capacity_summary: pd.DataFrame,
    yearly_capacities: dict,
    output_dir: Path,
    start_year: int,
    end_year: int,
    min_capacity_fraction: float,
) -> None:
    """
    Detect drought events for one load zone across start_year–end_year.

    Writes two CSV files:
      - {ZONE}_CF{threshold}_CapThresh50pct_years_{start}_{end}_hourly.csv
      - {ZONE}_CF{threshold}_CapThresh50pct_years_{start}_{end}_events.csv
    """
    zone_cells = grid_map[grid_map["load_zone"] == load_zone].copy()
    if zone_cells.empty:
        print(f"  [SKIP] No grid cells found for {load_zone}")
        return

    all_hourly = []

    for year in range(start_year, end_year + 1):
        nc_path = nc_dir / f"{year}_wind_cf.nc"
        if not nc_path.exists():
            print(f"  [SKIP] {nc_path.name} not found")
            continue

        # Get year-specific capacity weights
        cap_year = yearly_capacities.get(year)
        if cap_year is None:
            print(f"  [WARN] No capacity data for {year}, skipping")
            continue

        # Match capacity to zone cells
        merged = zone_cells.merge(
            cap_year,
            left_on=["lat", "lon", "load_zone"],
            right_on=["grid_latitude", "grid_longitude", "Load Zone"],
            how="left"
        )
        merged["Nameplate Capacity (MW)"] = (
            merged["Nameplate Capacity (MW)"].fillna(0.0)
        )
        merged = merged[merged["Nameplate Capacity (MW)"] > 0].copy()

        if merged.empty:
            continue

        cap_weights = merged["Nameplate Capacity (MW)"].values
        total_cap   = cap_weights.sum()

        # Get pct_wind for this zone and year
        pct_wind_row = capacity_summary[
            (capacity_summary["load_zone"] == load_zone) &
            (capacity_summary["year"] == year)
        ]
        pct_wind = float(pct_wind_row["pct_wind"].iloc[0]) \
            if not pct_wind_row.empty else 0.0

        # Extract CF matrix for zone cells
        with xr.open_dataset(nc_path) as ds:
            time_dim = next(
                (d for d in ["time", "valid_time"] if d in ds.dims), None
            )
            timestamps = pd.to_datetime(ds[time_dim].values)
            n_hours = len(timestamps)

            cf_matrix = np.full((n_hours, len(merged)), np.nan)
            for col_i, (_, cell) in enumerate(merged.iterrows()):
                cf_matrix[:, col_i] = (
                    ds["wind_cf"]
                    .sel(latitude=cell["lat"], longitude=cell["lon"],
                         method="nearest")
                    .values
                )

        # Capacity-weighted zone average CF
        zone_avg_cf = (cf_matrix * cap_weights).sum(axis=1) / total_cap

        # Fraction of capacity below threshold
        below_mask     = cf_matrix < cf_threshold
        capacity_below = (below_mask * cap_weights).sum(axis=1)
        pct_cap_below  = capacity_below / total_cap

        is_drought   = (pct_cap_below >= min_capacity_fraction).astype(int)
        shortfall_cf = np.maximum(0, cf_threshold - zone_avg_cf)

        year_hourly = pd.DataFrame({
            "datetime":            timestamps,
            "year":                year,
            "is_drought":          is_drought,
            "zone_avg_cf":         zone_avg_cf,
            "shortfall_cf":        shortfall_cf,
            "capacity_below_mw":   capacity_below,
            "pct_capacity_below":  pct_cap_below,
            "total_capacity_mw":   total_cap,
            "pct_wind":            pct_wind,
            "weighted_severity_by_pct_wind": shortfall_cf * pct_wind,
            "load_zone":           load_zone,
        })
        all_hourly.append(year_hourly)

    if not all_hourly:
        print(f"  [WARN] No hourly data produced for {load_zone}")
        return

    hourly_df = pd.concat(all_hourly, ignore_index=True)

    # --- Save hourly file ---
    stub = (
        f"{load_zone}_CF{cf_threshold}_CapThresh50pct"
        f"_years_{start_year}_{end_year}"
    )
    hourly_path = output_dir / f"{stub}_hourly.csv"
    hourly_df.to_csv(hourly_path, index=False)
    print(f"  Saved hourly → {hourly_path.name}")

    # --- Build events from consecutive drought hours ---
    events = []
    diff     = np.diff(hourly_df["is_drought"].values, prepend=0)
    event_id = np.cumsum(diff == 1) * hourly_df["is_drought"].values

    hourly_df["event_id"] = event_id
    drought_hours = hourly_df[hourly_df["event_id"] > 0]

    for eid, grp in drought_hours.groupby("event_id"):
        severity_series = np.maximum(0, cf_threshold - grp["zone_avg_cf"].values)
        weighted_sev    = severity_series * grp["pct_wind"].values

        events.append({
            "start_time":               grp["datetime"].iloc[0],
            "end_time":                 grp["datetime"].iloc[-1],
            "year":                     grp["year"].iloc[0],
            "duration":                 len(grp),
            "total_severity":           severity_series.sum(),
            "avg_severity":             severity_series.mean(),
            "max_severity":             severity_series.max(),
            "total_weighted_severity":  weighted_sev.sum(),
            "avg_weighted_severity":    weighted_sev.mean(),
            "pct_wind":                 grp["pct_wind"].iloc[0],
            "avg_zone_cf":              grp["zone_avg_cf"].mean(),
            "avg_capacity_below_mw":    grp["capacity_below_mw"].mean(),
            "avg_pct_capacity_below":   grp["pct_capacity_below"].mean(),
            "total_capacity_mw":        grp["total_capacity_mw"].iloc[0],
            "avg_severity_per_hour":    severity_series.mean(),
            "pct_severity":             severity_series.mean() / cf_threshold,
            "load_zone":                load_zone,
        })

    events_df = pd.DataFrame(events)
    events_path = output_dir / f"{stub}_events.csv"
    events_df.to_csv(events_path, index=False)
    print(f"  Saved {len(events_df)} events → {events_path.name}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Load-Zone Wind Drought Detection (2020–2024)")
    print("=" * 55)
    print(f"CF thresholds         : {CF_THRESHOLDS}")
    print(f"Capacity trigger      : >= {MIN_CAPACITY_FRACTION*100:.0f}% of zone MW below threshold")
    print(f"Period                : {START_YEAR}–{END_YEAR}")
    print(f"Load zones            : {LOAD_ZONES}")
    print(f"Output                : {OUTPUT_DIR}/")
    print("=" * 55)

    # Load shared inputs once
    grid_map = pd.read_csv(GRID_MAPPING)
    grid_map["load_zone"] = (
        grid_map["load_zone"].astype(str).str.strip().str.upper()
    )

    capacity_summary = pd.read_csv(CAPACITY_SUMMARY)
    capacity_summary["load_zone"] = (
        capacity_summary["load_zone"].astype(str).str.strip().str.upper()
    )

    # Load all annual capacity files once
    yearly_capacities = {}
    for year in range(START_YEAR, END_YEAR + 1):
        cap_path = CAPACITY_DIR / f"{year}_onshore_wind_turbine.csv"
        if not cap_path.exists():
            print(f"  [WARN] {cap_path.name} not found")
            continue
        cap = (
            pd.read_csv(cap_path)
            .groupby(["grid_latitude", "grid_longitude", "Load Zone"],
                     as_index=False)
            ["Nameplate Capacity (MW)"].sum()
        )
        yearly_capacities[year] = cap

    # Run for each threshold × zone combination
    for cf_threshold in CF_THRESHOLDS:
        print(f"\n{'='*55}")
        print(f"CF threshold = {cf_threshold}")
        print(f"{'='*55}")

        for zone in LOAD_ZONES:
            print(f"\nProcessing {zone}...")
            process_load_zone(
                load_zone=zone,
                cf_threshold=cf_threshold,
                nc_dir=NC_DIR,
                grid_map=grid_map,
                capacity_summary=capacity_summary,
                yearly_capacities=yearly_capacities,
                output_dir=OUTPUT_DIR,
                start_year=START_YEAR,
                end_year=END_YEAR,
                min_capacity_fraction=MIN_CAPACITY_FRACTION,
            )

    print("\nDone.")


if __name__ == "__main__":
    main()
