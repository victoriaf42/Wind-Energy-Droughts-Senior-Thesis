"""
Load-Zone Wind Drought Event Identification (CF = 0.30, 1950–2024)
====================================================================
Identifies wind energy drought events at the ERCOT load zone level across
the full 1950–2024 study period, using a capacity-weighted trigger to define
when a zone is in drought.

This script is the load-zone-level counterpart to wind_drought_identification.py
(which operates at the individual grid cell level). Rather than flagging a
drought at a single ERA5 grid cell when its CF falls below 0.30, this script
asks whether enough of a zone's installed wind capacity is simultaneously
below the threshold to constitute a meaningful zone-wide generation shortfall.

Drought trigger definition
---------------------------
A load zone hour is classified as a drought hour if the share of the zone's
2024 installed wind capacity whose grid cell CF falls below 0.30 is >= 50%.

In other words: at least 50% of the zone's nameplate wind capacity must be
experiencing below-threshold generation simultaneously for the hour to count.
This "capacity fraction" trigger was chosen to avoid flagging isolated
single-cell shortfalls as zone-wide droughts and is consistent with the
CapThresh50pct labelling used throughout the thesis.

Why 2024 capacity weights for the full historical period
---------------------------------------------------------
The 1950–2024 hazard characterisation uses 2024 installed capacity locations
as fixed spatial weights throughout. This is a deliberate design choice:
the goal is to characterise the meteorological hazard that the *current*
(2024) wind fleet would face, not to reconstruct what a historically changing
fleet would have experienced. This makes the historical drought statistics
directly comparable to the 2020–2024 financial risk analysis.

Output
------
  ALL_ZONES_events_all_1950_2024_CF0.3_cap50pct.csv
      One row per identified drought event per load zone, with columns:
        load_zone      : ERCOT load zone (e.g. LZ_WEST)
        start_time     : timestamp of the first drought hour
        end_time       : timestamp of the last drought hour
        duration       : event length in hours
        avg_zone_cf    : capacity-weighted average CF across the zone during
                         the event (weighted by 2024 nameplate capacity)
        total_severity : sum of hourly (threshold − avg_zone_cf) over the event
        avg_severity   : total_severity / duration
        pct_severity   : avg_severity / threshold

Requirements
------------
    pip install numpy pandas xarray netCDF4

Input files required
--------------------
  NC_DIR            : annual wind CF NetCDF files ({year}_wind_cf.nc)
                      from wind_cf_pipeline.py
  GRID_MAPPING_PATH : grid_to_loadzone_mapping.csv from ercot_spatial_grid.py
                      columns: lat_idx, lon_idx, lat, lon, load_zone
  CAPACITY_2024_PATH: 2024_onshore_wind_turbine.csv
                      columns: grid_latitude, grid_longitude, Load Zone,
                               Nameplate Capacity (MW)
                      compiled from EIA Form 860

Note on the capacity file
--------------------------
The 2024_onshore_wind_turbine.csv file contains the locations and nameplate
capacities of all onshore wind turbines operating in ERCOT in 2024, matched
to ERA5 grid cells by nearest-neighbour lookup. Grid cells with no wind
capacity are excluded from the zone-level averaging. This file is not
included in the repository; see README for data source details.

Usage
-----
    python lz_drought_events_historical.py

    Update the path variables in the CONFIGURATION block before running.
    The script prints progress every 10 years and is safe to re-run —
    if the output file already exists it will be overwritten.

Runtime note
------------
Processing 75 years × 4 zones × ~123 grid cells requires reading ~75 NetCDF
files and may take 20–60 minutes depending on hardware. The script reads each
year's full CF array once and extracts all grid cells in a single matrix
operation to minimise I/O overhead.
"""

import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Annual wind CF NetCDF files ({year}_wind_cf.nc) from wind_cf_pipeline.py
NC_DIR = Path("output/wind_cfs")

# Grid-to-load-zone mapping from ercot_spatial_grid.py
GRID_MAPPING_PATH = Path("output/load_zones/grid_to_loadzone_mapping.csv")

# 2024 installed wind capacity file (EIA Form 860)
CAPACITY_2024_PATH = Path("data/installed_capacities/2024_onshore_wind_turbine.csv")

# Output directory and filename
OUTPUT_DIR  = Path("output/historical_drought_events")
OUTPUT_FILE = OUTPUT_DIR / "ALL_ZONES_events_all_1950_2024_CF0.3_cap50pct.csv"

# Drought parameters
CF_THRESHOLD          = 0.30   # capacity factor threshold
MIN_CAPACITY_FRACTION = 0.50   # fraction of zone MW below threshold to trigger drought

# Study period
START_YEAR = 1950
END_YEAR   = 2024

# NetCDF variable name for capacity factor
CF_VAR_NAME = "wind_cf"


# =============================================================================
# HELPER: build capacity-weighted grid cell table per load zone
# =============================================================================

def build_capacity_weights(
    grid_map: pd.DataFrame,
    cap2024_path: Path
) -> pd.DataFrame:
    """
    Join 2024 installed wind capacity to the grid-to-load-zone mapping.

    Only grid cells with positive installed capacity are retained. These
    cells are used as the spatial basis for zone-level CF averaging and
    the capacity-fraction drought trigger.

    Returns a DataFrame with columns:
        load_zone, lat, lon, lat_idx, lon_idx, cap_mw
    """
    cap = pd.read_csv(cap2024_path)

    # Aggregate to grid cell level (multiple turbines may share a cell)
    cap_agg = (
        cap.groupby(["grid_latitude", "grid_longitude", "Load Zone"], as_index=False)
        ["Nameplate Capacity (MW)"].sum()
    )

    merged = grid_map.merge(
        cap_agg,
        left_on=["lat", "lon", "load_zone"],
        right_on=["grid_latitude", "grid_longitude", "Load Zone"],
        how="left"
    )

    merged["Nameplate Capacity (MW)"] = merged["Nameplate Capacity (MW)"].fillna(0.0)
    merged = merged[merged["Nameplate Capacity (MW)"] > 0].copy()
    merged = merged.rename(columns={"Nameplate Capacity (MW)": "cap_mw"})

    # Sum capacity where multiple turbines map to the same cell
    merged = (
        merged.groupby(["load_zone", "lat", "lon", "lat_idx", "lon_idx"], as_index=False)
        ["cap_mw"].sum()
    )

    return merged


# =============================================================================
# HELPER: extract CF matrix for all weighted grid cells from one year's NetCDF
# =============================================================================

def extract_cf_matrix(
    ds: xr.Dataset,
    cells: pd.DataFrame
) -> np.ndarray:
    """
    Extract CF time series for all grid cells in one operation.

    Parameters
    ----------
    ds    : open xarray Dataset for one year
    cells : DataFrame with lat_idx, lon_idx columns (one row per grid cell)

    Returns
    -------
    np.ndarray of shape (n_hours, n_cells)
    """
    cf_var = ds[CF_VAR_NAME]
    n_cells = len(cells)
    n_hours = cf_var.shape[0]

    matrix = np.full((n_hours, n_cells), np.nan)

    for col_idx, (_, row) in enumerate(cells.iterrows()):
        matrix[:, col_idx] = (
            cf_var
            .isel(latitude=int(row["lat_idx"]), longitude=int(row["lon_idx"]))
            .values
        )

        if (col_idx + 1) % 50 == 0 or (col_idx + 1) == n_cells:
            print(f"      extracted {col_idx + 1}/{n_cells} cells")

    return matrix


# =============================================================================
# HELPER: identify drought events from a zone CF time series
# =============================================================================

def identify_zone_drought_events(
    zone_cf: np.ndarray,
    below_fraction: np.ndarray,
    timestamps: pd.DatetimeIndex,
    threshold: float,
    min_cap_fraction: float,
    load_zone: str
) -> pd.DataFrame:
    """
    Identify drought events for one load zone in one year.

    A drought hour is defined as: below_fraction >= min_cap_fraction,
    i.e. at least min_cap_fraction of the zone's MW is below threshold.

    Parameters
    ----------
    zone_cf          : capacity-weighted average CF across the zone, hourly
    below_fraction   : fraction of zone MW below threshold, hourly
    timestamps       : hourly datetime index
    threshold        : CF drought threshold (e.g. 0.30)
    min_cap_fraction : minimum fraction of MW below threshold to trigger (e.g. 0.50)
    load_zone        : load zone label

    Returns
    -------
    DataFrame with one row per event, or empty DataFrame if no events found.
    """
    is_drought = below_fraction >= min_cap_fraction

    # Identify event boundaries using change detection
    diff     = np.diff(is_drought.astype(int), prepend=0)
    event_id = np.cumsum(diff == 1) * is_drought

    if event_id.max() == 0:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": timestamps,
        "zone_cf":   zone_cf,
        "severity":  np.maximum(0, threshold - zone_cf),
        "event_id":  event_id,
    })
    df_ev = df[df["event_id"] > 0]

    summary = (
        df_ev.groupby("event_id")
        .agg(
            start_time    = ("timestamp", "first"),
            end_time      = ("timestamp", "last"),
            duration      = ("zone_cf",  "size"),
            avg_zone_cf   = ("zone_cf",  "mean"),
            total_severity= ("severity", "sum"),
        )
        .reset_index(drop=True)
    )

    summary["avg_severity"] = summary["total_severity"] / summary["duration"]
    summary["pct_severity"] = summary["avg_severity"] / threshold
    summary["load_zone"]    = load_zone

    return summary


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Load grid mapping ---
    grid_map = pd.read_csv(GRID_MAPPING_PATH)
    grid_map["lat_idx"] = grid_map["lat_idx"].astype(int)
    grid_map["lon_idx"] = grid_map["lon_idx"].astype(int)
    grid_map["load_zone"] = grid_map["load_zone"].astype(str).str.strip().str.upper()
    grid_map = grid_map.dropna(subset=["load_zone"])

    print("Load-Zone Wind Drought Event Identification")
    print("=" * 60)
    print(f"CF threshold          : {CF_THRESHOLD}")
    print(f"Capacity trigger      : >= {MIN_CAPACITY_FRACTION*100:.0f}% of zone MW below threshold")
    print(f"Period                : {START_YEAR}–{END_YEAR}")
    print(f"Capacity weights      : 2024 installed capacity (fixed)")
    print(f"Output                : {OUTPUT_FILE}")
    print("=" * 60)

    # --- Build capacity weights ---
    print("\nBuilding 2024 capacity weights...")
    weighted_cells = build_capacity_weights(grid_map, CAPACITY_2024_PATH)
    load_zones = sorted(weighted_cells["load_zone"].unique())
    print(f"Load zones  : {load_zones}")
    print(f"Grid cells  : {len(weighted_cells)} (with positive 2024 capacity)")

    all_events = []
    years = list(range(START_YEAR, END_YEAR + 1))

    for year in years:
        nc_path = NC_DIR / f"{year}_wind_cf.nc"
        if not nc_path.exists():
            print(f"  [SKIP] {nc_path.name} not found")
            continue

        if year == START_YEAR or year % 10 == 0:
            print(f"\n[{year}] opening {nc_path.name}")

        try:
            with xr.open_dataset(nc_path) as ds:
                # Build timestamp index
                time_dim = next(
                    (d for d in ["time", "valid_time"] if d in ds.dims), None
                )
                if time_dim is None:
                    print(f"  [WARN] no time dimension found in {nc_path.name}")
                    continue
                timestamps = pd.to_datetime(ds[time_dim].values)

                if year == START_YEAR or year % 10 == 0:
                    print(f"    Extracting CF for {len(weighted_cells)} cells...")

                cf_matrix = extract_cf_matrix(ds, weighted_cells)

        except Exception as e:
            print(f"  [ERROR] {nc_path.name}: {e}")
            continue

        # --- Process each load zone ---
        for z_idx, zone in enumerate(load_zones):
            zone_cells = weighted_cells[weighted_cells["load_zone"] == zone].reset_index(drop=True)
            zone_col_mask = weighted_cells["load_zone"].values == zone

            zone_cf_matrix = cf_matrix[:, zone_col_mask]   # (n_hours, n_zone_cells)
            zone_cap = zone_cells["cap_mw"].values           # (n_zone_cells,)
            total_cap = zone_cap.sum()

            if total_cap == 0:
                continue

            # Capacity-weighted average CF
            zone_cf_weighted = (zone_cf_matrix * zone_cap).sum(axis=1) / total_cap

            # Fraction of zone MW below threshold each hour
            below_mask     = zone_cf_matrix < CF_THRESHOLD
            below_fraction = (below_mask * zone_cap).sum(axis=1) / total_cap

            events = identify_zone_drought_events(
                zone_cf   = zone_cf_weighted,
                below_fraction = below_fraction,
                timestamps= timestamps,
                threshold = CF_THRESHOLD,
                min_cap_fraction = MIN_CAPACITY_FRACTION,
                load_zone = zone,
            )

            if not events.empty:
                all_events.append(events)

            if year == START_YEAR or year % 10 == 0:
                print(f"    [{year}] {zone}: {len(events)} events identified")

    # --- Save combined output ---
    if not all_events:
        print("\nNo events identified. Check input files and configuration.")
        return

    combined = pd.concat(all_events, ignore_index=True)
    col_order = [
        "load_zone", "start_time", "end_time", "duration",
        "avg_zone_cf", "total_severity", "avg_severity", "pct_severity"
    ]
    combined = combined[[c for c in col_order if c in combined.columns]]
    combined = combined.sort_values(["load_zone", "start_time"]).reset_index(drop=True)
    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"\nDone.")
    print(f"  Total events : {len(combined):,}")
    print(f"  Period       : {combined['start_time'].min()} to {combined['start_time'].max()}")
    print(f"  Output       : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
