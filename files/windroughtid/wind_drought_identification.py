"""
Wind Energy Drought Identification
====================================
Identifies wind energy drought events from ERA5-derived capacity factor (CF)
time series, applied individually to each ERA5 grid cell across the full
study period (1950–2024).

A wind energy drought is defined as any consecutive sequence of hours during
which the grid-cell CF falls below a user-defined threshold. For each event,
the function computes:

    - duration        : number of consecutive below-threshold hours
    - total_severity  : sum of hourly shortfalls (threshold − CF) over the event
    - avg_severity    : total_severity / duration
    - pct_severity    : avg_severity expressed as a fraction of the threshold
    - start_time      : timestamp of the first hour of the event

This threshold-based design allows sensitivity analyses by varying the trigger
level (e.g. CF thresholds of 0.10, 0.15, 0.30) without modifying the
underlying detection logic.

Output
------
One CSV file per grid cell, named wind_drought_{lat_idx}_{lon_idx}.csv,
saved to OUTPUT_DIR. Each file contains one row per identified drought event
across the full study period.

Requirements
------------
    pip install numpy pandas xarray netcdf4

Input
-----
Annual NetCDF files of ERA5-derived wind capacity factors, named:
    {year}_wind_cf.nc
Each file must contain a variable named 'wind_cf' with dimensions
(latitude, longitude, time).

These files are produced by applying the Vestas V90-2MW power curve to ERA5
100m wind speeds — see the companion capacity factor estimation script.

Usage
-----
    python wind_drought_identification.py

Update the DATA_DIR, OUTPUT_DIR, and THRESHOLD variables below before running.

Notes
-----
- The script skips grid cells whose output file already exists, making it
  safe to interrupt and resume.
- File naming follows the convention {year}_wind_cf.nc; the year is parsed
  from the filename prefix before the first underscore.
- A single hour above the threshold embedded within a prolonged drought will
  split it into two separate events. This is a known limitation of the
  threshold-based approach; see thesis methodology for discussion.
"""

import os
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path


# =============================================================================
# CONFIGURATION — update before running
# =============================================================================

# Directory containing annual wind CF NetCDF files ({year}_wind_cf.nc)
DATA_DIR = Path("data/wind_cfs")

# Directory where per-grid-cell drought event CSVs will be saved
OUTPUT_DIR = Path("output/drought_events")

# CF threshold that defines a drought hour.
# The study uses 0.30 as the primary threshold; adjust for sensitivity analyses.
THRESHOLD = 0.30


# =============================================================================
# CORE FUNCTION: drought event identification
# =============================================================================

def identify_drought_events(
    cf_series: np.ndarray,
    threshold: float,
    start_date: str
) -> pd.DataFrame:
    """
    Identify wind energy drought events from a capacity factor time series.

    A drought event is a consecutive sequence of hours where CF < threshold.
    For each event, computes duration, severity metrics, and start timestamp.

    Parameters
    ----------
    cf_series : np.ndarray
        1D array of hourly capacity factor values for a single grid cell.
    threshold : float
        CF threshold below which an hour is classified as a drought hour.
        The study uses 0.30 as the primary threshold.
    start_date : str
        ISO 8601 datetime string for the first hour of the series,
        e.g. '1950-01-01 00:00'.

    Returns
    -------
    pd.DataFrame
        One row per identified drought event, with columns:
        event_id, duration, total_severity, avg_severity, pct_severity,
        start_time.
        Returns an empty DataFrame if no events are found.
    """
    # Build hourly timestamp index
    dates = pd.date_range(start=start_date, periods=len(cf_series), freq="h")

    # Per-hour severity: how far CF fell below the threshold (0 if above)
    severity = np.maximum(0, threshold - cf_series)

    # Assign consecutive below-threshold hours to events using change detection
    below_threshold = severity > 0
    transitions = np.diff(below_threshold.astype(int), prepend=0)
    event_id = np.cumsum(transitions == 1) * below_threshold

    df = pd.DataFrame({
        "date":     dates,
        "severity": severity,
        "event_id": event_id
    })

    # Retain only hours that belong to an event
    df_events = df[df["event_id"] > 0]

    if df_events.empty:
        return pd.DataFrame(
            columns=["event_id", "duration", "total_severity",
                     "avg_severity", "pct_severity", "start_time"]
        )

    # Summarise each event
    summary = (
        df_events
        .groupby("event_id")
        .agg(
            duration     = ("severity", "size"),
            total_severity = ("severity", "sum"),
            start_time   = ("date",     "first")
        )
        .reset_index()
    )

    summary["avg_severity"] = summary["total_severity"] / summary["duration"]
    summary["pct_severity"] = summary["avg_severity"] / threshold

    return summary


# =============================================================================
# MAIN: iterate over all grid cells and years
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Collect annual NetCDF files, sorted chronologically
    nc_files = sorted(DATA_DIR.glob("*_wind_cf.nc"))
    if not nc_files:
        raise FileNotFoundError(
            f"No '*_wind_cf.nc' files found in {DATA_DIR}. "
            "Check DATA_DIR and file naming convention."
        )

    print(f"Wind Drought Identification")
    print(f"=" * 50)
    print(f"CF threshold : {THRESHOLD}")
    print(f"Years found  : {len(nc_files)} "
          f"({nc_files[0].stem.split('_')[0]}–{nc_files[-1].stem.split('_')[0]})")
    print(f"Output dir   : {OUTPUT_DIR}")
    print(f"=" * 50)

    # Extract lat/lon grid from the first file
    with xr.open_dataset(nc_files[0]) as ds_ref:
        lats = ds_ref.latitude.values
        lons = ds_ref.longitude.values

    print(f"Grid size    : {len(lats)} lats × {len(lons)} lons "
          f"= {len(lats) * len(lons):,} cells\n")

    completed = skipped = errors = 0

    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):

            output_path = OUTPUT_DIR / f"wind_drought_{i}_{j}.csv"

            # Resume support: skip cells already processed
            if output_path.exists():
                skipped += 1
                continue

            all_years = []

            for nc_file in nc_files:
                year = nc_file.stem.split("_")[0]

                try:
                    with xr.open_dataset(nc_file) as ds:
                        cf_data = (
                            ds["wind_cf"]
                            .sel(latitude=lat, longitude=lon)
                            .load()
                            .values
                        )

                    events = identify_drought_events(
                        cf_data, THRESHOLD, f"{year}-01-01 00:00"
                    )
                    all_years.append(events)

                except Exception as e:
                    print(f"  [ERROR] {nc_file.name} at ({i},{j}): {e}")
                    errors += 1
                    continue

            if all_years:
                result = pd.concat(all_years, ignore_index=True)
                result.to_csv(output_path, index=False)

            completed += 1
            if completed % 50 == 0:
                print(f"  Progress: {completed} cells completed, "
                      f"{skipped} skipped, {errors} errors")

    print(f"\nDone.")
    print(f"  Completed : {completed}")
    print(f"  Skipped   : {skipped} (output already existed)")
    print(f"  Errors    : {errors}")
    print(f"  Output    : {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
