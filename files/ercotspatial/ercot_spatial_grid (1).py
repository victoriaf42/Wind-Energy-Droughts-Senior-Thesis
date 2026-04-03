"""
ERCOT Load Zone Spatial Grid Assignment
========================================
This script performs three steps:

1. Generate ERA5 grid index
   Creates a complete DataFrame of all ERA5 grid cells covering the Texas
   domain (26°–36°N, 94°–107°W) at 0.25° resolution, with integer row/column
   indices used throughout the analysis pipeline.

2. Validate grid alignment
   Cross-checks that the wind CF grid cell indices match the full ERA5 grid
   coordinates within a floating-point tolerance, confirming no indexing errors
   were introduced during processing.

3. Assign ERCOT load zones via spatial join
   Converts grid cell coordinates to point geometries, loads a GeoJSON of
   Texas counties manually classified to ERCOT load zones (produced in QGIS),
   and assigns each ERA5 grid cell to its corresponding load zone using a
   point-in-polygon spatial join.

Output: ercot_grid_with_loadzones.csv — one row per ERA5 grid cell, with
        load zone assignment and integer indices for use in downstream analysis.

Requirements
------------
    pip install numpy pandas geopandas shapely

Input Files
-----------
    - Texas_County_LoadZones.geojson  : ERCOT load zone polygons (from QGIS)
    - grid_to_loadzone_mapping.csv    : existing wind CF grid cell index file
                                        (used for validation only)

Note on boundary accuracy
--------------------------
ERCOT load zone boundaries were approximated by manually classifying Texas
counties to load zones in QGIS. Minor boundary errors may exist near the
service territory edge; up to two ERA5 grid cells near the boundary may have
been incorrectly excluded. See thesis methodology for discussion.

Usage
-----
    python ercot_spatial_grid.py

    Update the INPUT_DIR and OUTPUT_DIR paths below before running.
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Directory containing input files.
# Place Texas_County_LoadZones.geojson and grid_to_loadzone_mapping.csv here.
# Default assumes a 'data/' subfolder relative to this script.
# Update this path if your files are stored elsewhere.
INPUT_DIR = Path("data")

# Path to ERCOT load zone polygons (GeoJSON exported from QGIS)
LOADZONES_GEOJSON = INPUT_DIR / "Texas_County_LoadZones.geojson"

# Path to existing wind CF grid mapping (used for validation only)
WIND_GRID_CSV = INPUT_DIR / "grid_to_loadzone_mapping.csv"

# Output file
OUTPUT_CSV = INPUT_DIR / "ercot_grid_with_loadzones.csv"

# ERA5 grid parameters
LAT_MIN, LAT_MAX = 26, 36
LON_MIN, LON_MAX = -107, -94
RESOLUTION = 0.25  # degrees

# Tolerance for coordinate matching validation
MATCH_TOLERANCE = 1e-4


# =============================================================================
# STEP 1: Generate ERA5 grid index
# =============================================================================

def generate_era5_grid() -> pd.DataFrame:
    """
    Generate a DataFrame of all ERA5 grid cells covering the Texas domain.

    Latitudes are ordered north-to-south (descending) to match the ERA5
    NetCDF dimension convention. Each cell is assigned integer lat_idx and
    lon_idx values used as keys throughout the analysis pipeline.

    Returns
    -------
    pd.DataFrame with columns: lat_idx, lon_idx, latitude, longitude
    """
    # ERA5 latitudes run north-to-south
    latitudes  = np.arange(LAT_MAX, LAT_MIN - RESOLUTION, -RESOLUTION)
    longitudes = np.arange(LON_MIN, LON_MAX + RESOLUTION,  RESOLUTION)

    lat_idx, lon_idx = np.meshgrid(
        range(len(latitudes)),
        range(len(longitudes)),
        indexing="ij"
    )

    grid_df = pd.DataFrame({
        "lat_idx":   lat_idx.flatten(),
        "lon_idx":   lon_idx.flatten(),
        "latitude":  np.repeat(latitudes, len(longitudes)),
        "longitude": np.tile(longitudes, len(latitudes)),
    })

    print(f"[Step 1] ERA5 grid generated: {len(grid_df):,} cells")
    print(grid_df.head())
    return grid_df


# =============================================================================
# STEP 2: Validate grid alignment
# =============================================================================

def validate_grid_alignment(grid_df: pd.DataFrame) -> None:
    """
    Cross-check that the wind CF grid indices align with the full ERA5 grid.

    Merges the wind CF mapping file (which contains lat/lon coordinates)
    against the full ERA5 grid on integer indices, and flags any cells where
    the coordinates differ by more than MATCH_TOLERANCE degrees.

    Parameters
    ----------
    grid_df : pd.DataFrame
        Full ERA5 grid produced by generate_era5_grid().
    """
    if not WIND_GRID_CSV.exists():
        print(f"[Step 2] Validation file not found ({WIND_GRID_CSV}), skipping.")
        return

    wind_grid = pd.read_csv(WIND_GRID_CSV)
    print(f"\n[Step 2] Validating grid alignment")
    print(f"  Full ERA5 grid cells : {len(grid_df):,}")
    print(f"  Wind CF grid cells   : {len(wind_grid):,}")

    merged = pd.merge(
        wind_grid, grid_df,
        on=["lat_idx", "lon_idx"],
        how="left",
        suffixes=("_wind", "_full")
    )

    merged["lat_diff"] = np.abs(merged["lat"] - merged["latitude"])
    merged["lon_diff"] = np.abs(merged["lon"] - merged["longitude"])

    mismatches = merged[
        (merged["lat_diff"] > MATCH_TOLERANCE) |
        (merged["lon_diff"] > MATCH_TOLERANCE)
    ]

    n_matched = len(merged) - len(mismatches)
    print(f"  Matched  : {n_matched} / {len(merged)}")
    print(f"  Mismatches: {len(mismatches)}")

    if len(mismatches) > 0:
        print("  WARNING — coordinate mismatches detected:")
        print(mismatches[
            ["lat_idx", "lon_idx", "lat", "latitude", "lon", "longitude",
             "lat_diff", "lon_diff"]
        ].head())
    else:
        print("  All latitude/longitude values match within tolerance.")


# =============================================================================
# STEP 3: Assign ERCOT load zones via spatial join
# =============================================================================

def assign_load_zones(grid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign each ERA5 grid cell to an ERCOT load zone via point-in-polygon join.

    Converts grid cell coordinates to Shapely Point geometries, then performs
    a spatial join against the ERCOT load zone polygons. Grid cells outside
    all polygons (i.e. outside the ERCOT service territory) receive NaN for
    the load zone column.

    Parameters
    ----------
    grid_df : pd.DataFrame
        Full ERA5 grid produced by generate_era5_grid().

    Returns
    -------
    pd.DataFrame
        Grid cells with load zone columns appended.
    """
    print(f"\n[Step 3] Loading load zone polygons from {LOADZONES_GEOJSON}")
    loadzones_gdf = gpd.read_file(LOADZONES_GEOJSON)

    # Convert grid cells to GeoDataFrame (point geometries)
    geometry = [Point(lon, lat) for lon, lat in
                zip(grid_df["longitude"], grid_df["latitude"])]
    grid_gdf = gpd.GeoDataFrame(grid_df, geometry=geometry, crs=loadzones_gdf.crs)

    # Point-in-polygon spatial join
    joined = gpd.sjoin(grid_gdf, loadzones_gdf, how="left", predicate="within")

    matched = joined["index_right"].notnull().sum()
    print(f"  Grid cells assigned to a load zone: {matched:,} / {len(joined):,}")

    # Warn if result is outside expected range (~900 cells for this domain)
    if not (890 <= matched <= 910):
        print(f"  WARNING — unexpected match count ({matched}). "
              "Check CRS alignment or GeoJSON boundary definitions.")

    # Drop spatial helper columns before saving
    result = joined.drop(columns=["geometry", "index_right"], errors="ignore")
    return result


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("ERCOT Spatial Grid Assignment")
    print("=" * 50)

    # Step 1: generate grid
    grid_df = generate_era5_grid()

    # Step 2: validate alignment against wind CF mapping
    validate_grid_alignment(grid_df)

    # Step 3: spatial join to load zones
    result = assign_load_zones(grid_df)

    # Save output
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"\n[Done] Grid with load zones saved to: {OUTPUT_CSV}")
    print(f"       {len(result):,} rows, {len(result.columns)} columns")


if __name__ == "__main__":
    main()
