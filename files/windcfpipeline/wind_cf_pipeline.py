"""
Wind Capacity Factor Pipeline
================================
Converts ERA5 GRIB files into load-zone-level wind capacity factor (CF)
time series for the ERCOT service territory. Runs in four sequential stages:

  Stage 1 — Wind Speeds
      Reads annual ERA5 GRIB files, extracts the 100m u- and v-wind components,
      computes resultant wind speed (sqrt(u² + v²)), and saves one NetCDF file
      per year.

  Stage 2 — Capacity Factors
      Applies the Vestas V90-2MW power curve to each hourly wind speed value,
      producing CF estimates in the range [0, 1]. Uses Numba JIT compilation
      for performance across the full 1950-2024 domain.

  Stage 3 — Validation
      Computes summary statistics and a CF distribution breakdown across all
      years to verify the output is physically plausible before proceeding.

  Stage 4 — Load Zone Aggregation
      Maps each ERA5 grid cell to an ERCOT load zone via spatial join, then
      computes capacity-weighted average CF time series per load zone for
      2020-2024 using installed wind plant data.

Pipeline data flow
------------------
  ERA5 GRIB files  →  [Stage 1]  →  {year}_wind_speed.nc
  wind_speed.nc    →  [Stage 2]  →  {year}_wind_cf.nc
  wind_cf.nc       →  [Stage 3]  →  wind_cf_validation_summary.csv
  wind_cf.nc       →  [Stage 4]  →  grid_cell_weights_by_lz.csv
                                     grid_to_loadzone_mapping.csv
                                     lz_cf_timeseries.csv

Requirements
------------
    pip install numpy pandas xarray cfgrib geopandas shapely numba netCDF4
                openpyxl matplotlib

Input files required
--------------------
  GRIB_DIR         : annual ERA5 GRIB files named {year}.grib
  WIND_SPEED_DIR   : written by Stage 1, read by Stage 2
  WIND_CF_DIR      : written by Stage 2, read by Stages 3 and 4
  LOADZONES_GEOJSON: county-to-load-zone boundary file (see note below)
  CAPACITY_DIR     : annual Excel files of installed wind plants named
                     {year}_plants.xlsx with columns:
                     Technology, Lat, Lon, Nameplate Capacity (MW), LZ

Note on LOADZONES_GEOJSON
--------------------------
The GeoJSON used in this study was produced by manually classifying Texas
counties to ERCOT load zones in QGIS. This file is not included in the
repository. To reproduce, create a county-to-load-zone classification using
Census TIGER shapefiles and ERCOT load zone definitions. Export as GeoJSON.
The file must contain a column named 'Load Zones' and a geometry column.

Note on Vestas V90-2MW power curve
------------------------------------
The polynomial coefficients used in Stage 2 represent the Vestas V90-2MW
turbine (rated capacity 2 MW). Wind output is zero below cut-in (3 m/s) and
above cut-out (25 m/s), and is capped at rated output between 12.5 m/s and
the cut-out speed. More recently deployed turbines achieve higher CFs; results
should be interpreted accordingly.

Usage
-----
    python wind_cf_pipeline.py

    To run only specific stages, set the RUN_STAGES list in the configuration
    block below, e.g. RUN_STAGES = [1, 2] skips validation and aggregation.
"""

import os
import re
import sys
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# Numba is used for Stage 2 — import with a clear error message if missing
try:
    import numba
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False
    print("WARNING: numba not installed. Stage 2 will fall back to plain NumPy "
          "(correct results, but slower). Install with: pip install numba")


# =============================================================================
# CONFIGURATION — update all paths before running
# =============================================================================

# Which stages to run (1 = wind speeds, 2 = CFs, 3 = validation, 4 = aggregation)
RUN_STAGES = [1, 2, 3, 4]

# --- Input ---
# Annual ERA5 GRIB files, named {year}.grib
GRIB_DIR = Path("data/era5_grib")

# County-to-load-zone GeoJSON (produced in QGIS — not included in repo)
LOADZONES_GEOJSON = Path("data/Texas_County_LoadZones.geojson")

# Annual installed wind plant Excel files, named {year}_plants.xlsx
CAPACITY_DIR = Path("data/installed_capacities")

# --- Intermediate / output ---
WIND_SPEED_DIR = Path("output/wind_speeds")   # Stage 1 writes here
WIND_CF_DIR    = Path("output/wind_cfs")       # Stage 2 writes here
OUTPUT_DIR     = Path("output/load_zones")     # Stage 4 writes here

# Validation output (Stage 3)
VALIDATION_CSV = WIND_CF_DIR / "wind_cf_validation_summary.csv"

# Years for the full hazard analysis (Stages 1-3)
YEARS_FULL = list(range(1950, 2025))

# Years for load zone aggregation (Stage 4) — limited to ERCOT price data period
YEARS_LZ = list(range(2020, 2025))


# =============================================================================
# STAGE 1: Wind speeds from ERA5 GRIB files
# =============================================================================

def _open_wind_component(grib_file: Path, short_name: str, index_path: Path):
    """
    Try multiple cfgrib filter strategies to extract a single wind component.

    ERA5 GRIB files can encode variable names differently depending on the
    version of the CDS API used to download them. This function tries
    shortName first, then typeOfLevel + level as a fallback.

    Returns an xarray Dataset on success, or None if all methods fail.
    """
    # Method 1: shortName (e.g. '100u', '100v')
    try:
        ds = xr.open_dataset(
            str(grib_file), engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {"shortName": short_name},
                "indexpath": str(index_path),
            }
        )
        if ds.data_vars:
            return ds
        ds.close()
    except Exception:
        pass

    # Method 2: typeOfLevel + level
    level = 100
    type_of_level = "heightAboveGround"
    param = "u" if "u" in short_name else "v"
    try:
        ds = xr.open_dataset(
            str(grib_file), engine="cfgrib",
            backend_kwargs={
                "filter_by_keys": {
                    "typeOfLevel": type_of_level,
                    "level": level,
                    "shortName": param,
                },
                "indexpath": str(index_path),
            }
        )
        if ds.data_vars:
            return ds
        ds.close()
    except Exception:
        pass

    return None


def stage1_wind_speeds():
    """
    Stage 1: Extract 100m u/v wind components from ERA5 GRIB files and compute
    resultant wind speed (sqrt(u² + v²)) for each grid cell and hour.

    Input : {year}.grib files in GRIB_DIR
    Output: {year}_wind_speed.nc files in WIND_SPEED_DIR
    """
    WIND_SPEED_DIR.mkdir(parents=True, exist_ok=True)

    # Clean up stale cfgrib index files that cause read errors on re-runs
    for idx in list(GRIB_DIR.glob("*.idx")) + list(WIND_SPEED_DIR.glob("*.idx")):
        try:
            idx.unlink()
        except OSError:
            pass

    grib_files = sorted(GRIB_DIR.glob("*.grib*"))
    if not grib_files:
        raise FileNotFoundError(f"No GRIB files found in {GRIB_DIR}")

    print(f"\n[Stage 1] Processing {len(grib_files)} GRIB files → {WIND_SPEED_DIR}/")

    for grib_file in grib_files:
        year = grib_file.stem
        output_path = WIND_SPEED_DIR / f"{year}_wind_speed.nc"

        if output_path.exists():
            print(f"  [SKIP] {year} — already exists")
            continue

        print(f"  Processing {grib_file.name}...")

        idx_u = WIND_SPEED_DIR / f"{grib_file.stem}_u.idx"
        idx_v = WIND_SPEED_DIR / f"{grib_file.stem}_v.idx"

        u_ds = _open_wind_component(grib_file, "100u", idx_u)
        v_ds = _open_wind_component(grib_file, "100v", idx_v)

        if u_ds is None or v_ds is None:
            print(f"  [FAIL] Could not extract u or v component from {grib_file.name}")
            continue

        try:
            u_var = list(u_ds.data_vars)[0]
            v_var = list(v_ds.data_vars)[0]
            u = u_ds[u_var].values
            v = v_ds[v_var].values

            if u.shape != v.shape:
                print(f"  [FAIL] Shape mismatch: u={u.shape}, v={v.shape}")
                u_ds.close(); v_ds.close()
                continue

            wind_speed = np.sqrt(u**2 + v**2)

            # Build output dataset preserving spatial/temporal coordinates
            ds_out = xr.Dataset(
                {"wind_speed_100m": (u_ds[u_var].dims, wind_speed)},
                coords={
                    k: u_ds[u_var].coords[k]
                    for k in u_ds[u_var].dims
                    if k in u_ds[u_var].coords
                }
            )
            ds_out["wind_speed_100m"].attrs.update({
                "units": "m s**-1",
                "long_name": "100m resultant wind speed",
                "description": "sqrt(u100^2 + v100^2) from ERA5"
            })
            ds_out.to_netcdf(output_path)

            print(f"  [OK] {year} — shape {wind_speed.shape}, "
                  f"range {wind_speed.min():.2f}–{wind_speed.max():.2f} m/s")
        finally:
            u_ds.close()
            v_ds.close()

    print("[Stage 1] Complete.")


# =============================================================================
# STAGE 2: Capacity factors using the Vestas V90-2MW power curve
# =============================================================================

def _make_power_curve():
    """
    Return a vectorised capacity factor function for the Vestas V90-2MW.

    The power curve is approximated by an 8th-degree polynomial fitted to the
    manufacturer's published curve between cut-in (3 m/s) and rated speed
    (12.5 m/s). Output is normalised by rated capacity (2,000 kW) to give CF
    in [0, 1].

    Uses Numba JIT compilation if available; falls back to plain NumPy otherwise.
    """
    if NUMBA_AVAILABLE:
        @numba.vectorize([numba.float64(numba.float64)], nopython=True, cache=True)
        def _cf(ws):
            if ws <= 3.0 or ws >= 25.0:
                return 0.0
            if ws >= 12.5:
                return 1.0
            return (
                634.228
                - 1248.5   * ws
                + 999.57   * ws**2
                - 426.224  * ws**3
                + 105.617  * ws**4
                - 15.4587  * ws**5
                + 1.3223   * ws**6
                - 0.0609186* ws**7
                + 0.00116265*ws**8
            ) / 2000.0
        return _cf
    else:
        def _cf_numpy(ws_array):
            ws = np.asarray(ws_array, dtype=np.float64)
            cf = np.where(
                (ws <= 3.0) | (ws >= 25.0), 0.0,
                np.where(
                    ws >= 12.5, 1.0,
                    (634.228 - 1248.5*ws + 999.57*ws**2 - 426.224*ws**3
                     + 105.617*ws**4 - 15.4587*ws**5 + 1.3223*ws**6
                     - 0.0609186*ws**7 + 0.00116265*ws**8) / 2000.0
                )
            )
            return cf
        return _cf_numpy


def stage2_capacity_factors():
    """
    Stage 2: Apply the Vestas V90-2MW power curve to produce hourly CF estimates.

    Input : {year}_wind_speed.nc files in WIND_SPEED_DIR
    Output: {year}_wind_cf.nc files in WIND_CF_DIR
    """
    WIND_CF_DIR.mkdir(parents=True, exist_ok=True)

    nc_files = sorted(WIND_SPEED_DIR.glob("*_wind_speed.nc"))
    if not nc_files:
        raise FileNotFoundError(
            f"No wind speed NetCDF files found in {WIND_SPEED_DIR}. "
            "Run Stage 1 first."
        )

    power_curve = _make_power_curve()
    method = "Numba" if NUMBA_AVAILABLE else "NumPy"
    print(f"\n[Stage 2] Converting {len(nc_files)} wind speed files to CF "
          f"({method}) → {WIND_CF_DIR}/")

    for nc_file in nc_files:
        year = nc_file.stem.replace("_wind_speed", "")
        output_path = WIND_CF_DIR / f"{year}_wind_cf.nc"

        if output_path.exists():
            print(f"  [SKIP] {year} — already exists")
            continue

        print(f"  Processing {year}...")

        try:
            with xr.open_dataset(nc_file) as ds:
                ws = ds["wind_speed_100m"]
                cf_values = power_curve(ws.values)

                ds_out = xr.Dataset(
                    {"wind_cf": (ws.dims, cf_values)},
                    coords={k: ws.coords[k] for k in ws.dims if k in ws.coords}
                )
                ds_out["wind_cf"].attrs.update({
                    "units": "dimensionless",
                    "long_name": "Wind capacity factor (Vestas V90-2MW)",
                    "valid_range": [0.0, 1.0],
                    "description": (
                        "Hourly capacity factor derived from ERA5 100m wind speed "
                        "using the Vestas V90-2MW power curve. Zero below cut-in "
                        "(3 m/s) and above cut-out (25 m/s); 1.0 between rated "
                        "speed (12.5 m/s) and cut-out."
                    )
                })
                ds_out.to_netcdf(output_path)

                mean_cf = cf_values.mean()
                print(f"  [OK] {year} — shape {cf_values.shape}, "
                      f"mean CF {mean_cf:.3f}")
        except Exception as e:
            print(f"  [FAIL] {year}: {e}")

    print("[Stage 2] Complete.")


# =============================================================================
# STAGE 3: Validation
# =============================================================================

def stage3_validation():
    """
    Stage 3: Compute summary statistics for all CF files.

    Produces a per-year CSV with mean, median, std, percentiles, and CF
    distribution bins. Used to verify the pipeline output is physically
    plausible before running Stage 4.

    Input : {year}_wind_cf.nc files in WIND_CF_DIR
    Output: wind_cf_validation_summary.csv in WIND_CF_DIR
    """
    nc_files = sorted(WIND_CF_DIR.glob("*_wind_cf.nc"))
    if not nc_files:
        raise FileNotFoundError(
            f"No CF NetCDF files found in {WIND_CF_DIR}. Run Stage 2 first."
        )

    print(f"\n[Stage 3] Validating {len(nc_files)} CF files...")

    rows = []
    bins = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    bin_labels = [f"{int(b*100)}-{int(bins[i+1]*100)}%" for i, b in enumerate(bins[:-1])]

    for nc_file in nc_files:
        m = re.search(r"(19\d{2}|20\d{2})", nc_file.name)
        year = int(m.group(0)) if m else None

        with xr.open_dataset(nc_file) as ds:
            cf = ds["wind_cf"].values.ravel()
            cf = cf[~np.isnan(cf)]

        counts, _ = np.histogram(cf, bins=bins)
        pct = (counts / len(cf) * 100).round(2)

        row = {
            "year": year,
            "n_values": len(cf),
            "mean_cf": round(cf.mean(), 4),
            "median_cf": round(float(np.median(cf)), 4),
            "std_cf": round(cf.std(), 4),
            "p10": round(float(np.percentile(cf, 10)), 4),
            "p90": round(float(np.percentile(cf, 90)), 4),
            "missing": int(np.isnan(ds["wind_cf"].values).sum()),
        }
        for label, p in zip(bin_labels, pct):
            row[f"pct_{label}"] = p

        rows.append(row)
        print(f"  {year}: mean CF = {row['mean_cf']:.3f}, "
              f"median = {row['median_cf']:.3f}")

    summary = pd.DataFrame(rows).sort_values("year")
    summary.to_csv(VALIDATION_CSV, index=False)
    print(f"\n  Summary saved to: {VALIDATION_CSV}")
    print(f"  Overall mean CF (all years): "
          f"{summary['mean_cf'].mean():.4f}")
    print("[Stage 3] Complete.")


# =============================================================================
# STAGE 4: Load zone aggregation
# =============================================================================

def _map_grid_to_loadzones(lats: np.ndarray, lons: np.ndarray) -> gpd.GeoDataFrame:
    """
    Assign each ERA5 grid cell to an ERCOT load zone via point-in-polygon join.

    Parameters
    ----------
    lats, lons : coordinate arrays from the CF NetCDF files

    Returns
    -------
    GeoDataFrame with columns: lat_idx, lon_idx, lat, lon, Load Zones
    """
    gdf_lz = gpd.read_file(LOADZONES_GEOJSON)

    # Detect the load zone column name
    if "Load Zones" in gdf_lz.columns:
        lz_col = "Load Zones"
    elif "LZ" in gdf_lz.columns:
        lz_col = "LZ"
    else:
        raise ValueError(
            f"Cannot find load zone column in GeoJSON. "
            f"Available columns: {gdf_lz.columns.tolist()}"
        )

    # Build point GeoDataFrame for all grid cells
    records = []
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            records.append({
                "lat_idx": i, "lon_idx": j,
                "lat": lat, "lon": lon,
                "geometry": Point(lon, lat)
            })
    grid_gdf = gpd.GeoDataFrame(records, crs=gdf_lz.crs)

    joined = gpd.sjoin(grid_gdf, gdf_lz[[lz_col, "geometry"]],
                       how="left", predicate="within")
    joined = joined.rename(columns={lz_col: "Load Zones"})

    print(f"  Grid cells assigned to load zones: "
          f"{joined['Load Zones'].notna().sum()} / {len(joined)}")
    print("  Cells per load zone:")
    print(joined["Load Zones"].value_counts().to_string(index=True))

    return joined[["lat_idx", "lon_idx", "lat", "lon", "Load Zones"]].copy()


def _find_nearest(value: float, array: np.ndarray) -> int:
    return int(np.abs(array - value).argmin())


def stage4_load_zone_aggregation():
    """
    Stage 4: Compute capacity-weighted average CF per ERCOT load zone.

    For each year in YEARS_LZ:
      1. Load installed wind plant locations and nameplate capacities.
      2. Map each plant to its nearest ERA5 grid cell.
      3. Calculate each grid cell's weight within its load zone (MW share).
      4. Extract the CF time series for each grid cell.

    Outputs three CSV files:
      - grid_cell_weights_by_lz.csv   : grid cell × year weights
      - grid_to_loadzone_mapping.csv  : static grid-to-LZ mapping
      - lz_cf_timeseries.csv          : hourly CF per plant location

    Input : {year}_wind_cf.nc, {year}_plants.xlsx, Texas_County_LoadZones.geojson
    Output: CSV files in OUTPUT_DIR
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build grid from the first available CF file
    cf_files = sorted(WIND_CF_DIR.glob("*_wind_cf.nc"))
    if not cf_files:
        raise FileNotFoundError(
            f"No CF files found in {WIND_CF_DIR}. Run Stage 2 first."
        )
    with xr.open_dataset(cf_files[0]) as ds_ref:
        lats = ds_ref["latitude"].values
        lons = ds_ref["longitude"].values

    print(f"\n[Stage 4] Load zone aggregation for years {YEARS_LZ[0]}–{YEARS_LZ[-1]}...")

    # Map grid cells to load zones (done once — grid is constant)
    print("  Building grid-to-load-zone mapping...")
    grid_with_lz = _map_grid_to_loadzones(lats, lons)

    # Save static mapping for reuse
    mapping_path = OUTPUT_DIR / "grid_to_loadzone_mapping.csv"
    grid_with_lz.to_csv(mapping_path, index=False)
    print(f"  Saved: {mapping_path}")

    all_weights = []
    all_lz_data = []

    for year in YEARS_LZ:
        print(f"\n  Processing {year}...")

        cf_path = WIND_CF_DIR / f"{year}_wind_cf.nc"
        plants_path = CAPACITY_DIR / f"{year}_plants.xlsx"

        if not cf_path.exists():
            print(f"  [SKIP] CF file not found: {cf_path}")
            continue
        if not plants_path.exists():
            print(f"  [SKIP] Plant file not found: {plants_path}")
            continue

        # Load wind plants
        plants = pd.read_excel(plants_path, engine="openpyxl")
        wind_plants = plants[plants["Technology"] == "Onshore Wind Turbine"].copy()
        print(f"  Wind plants: {len(wind_plants)}")

        # Map each plant to nearest grid cell
        wind_plants["lat_idx"] = wind_plants["Lat"].apply(
            lambda x: _find_nearest(x, lats))
        wind_plants["lon_idx"] = wind_plants["Lon"].apply(
            lambda x: _find_nearest(x, lons))
        wind_plants["lat_grid"] = lats[wind_plants["lat_idx"]]
        wind_plants["lon_grid"] = lons[wind_plants["lon_idx"]]

        # Aggregate nameplate capacity by grid cell
        grid_cap = (
            wind_plants
            .groupby(["lat_idx", "lon_idx", "lat_grid", "lon_grid"])
            .agg(capacity_MW=("Nameplate Capacity (MW)", "sum"))
            .reset_index()
        )

        # Merge with load zone assignment
        grid_cap = grid_cap.merge(
            grid_with_lz[["lat_idx", "lon_idx", "Load Zones"]],
            on=["lat_idx", "lon_idx"], how="left"
        )

        # Compute each grid cell's weight within its load zone
        lz_totals = (
            grid_cap.groupby("Load Zones")["capacity_MW"]
            .sum().rename("lz_total_mw")
        )
        grid_cap = grid_cap.join(lz_totals, on="Load Zones")
        grid_cap["weight_in_lz"] = (
            grid_cap["capacity_MW"] / grid_cap["lz_total_mw"]
        )
        grid_cap["year"] = year
        all_weights.append(grid_cap)

        # Extract CF time series per plant location
        ds = xr.open_dataset(cf_path)
        time = ds["time"].values

        for _, plant in wind_plants.iterrows():
            cf_series = (
                ds["wind_cf"]
                .isel(latitude=int(plant["lat_idx"]),
                      longitude=int(plant["lon_idx"]))
                .values
            )
            all_lz_data.append(pd.DataFrame({
                "datetime":        time,
                "capacity_factor": cf_series,
                "load_zone":       plant.get("LZ", np.nan),
                "lat":             plant["Lat"],
                "lon":             plant["Lon"],
                "capacity_MW":     plant["Nameplate Capacity (MW)"],
                "year":            year,
            }))
        ds.close()

    # Save outputs
    if all_weights:
        weights_df = pd.concat(all_weights, ignore_index=True)
        weights_path = OUTPUT_DIR / "grid_cell_weights_by_lz.csv"
        weights_df.to_csv(weights_path, index=False)
        print(f"\n  Saved: {weights_path}")

    if all_lz_data:
        ts_df = pd.concat(all_lz_data, ignore_index=True)
        ts_path = OUTPUT_DIR / "lz_cf_timeseries.csv"
        ts_df.to_csv(ts_path, index=False)
        print(f"  Saved: {ts_path}")

    print("[Stage 4] Complete.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("Wind Capacity Factor Pipeline")
    print("=" * 50)
    print(f"Stages to run : {RUN_STAGES}")
    print(f"Full period   : {YEARS_FULL[0]}–{YEARS_FULL[-1]}")
    print(f"LZ period     : {YEARS_LZ[0]}–{YEARS_LZ[-1]}")
    print("=" * 50)

    if 1 in RUN_STAGES:
        stage1_wind_speeds()

    if 2 in RUN_STAGES:
        stage2_capacity_factors()

    if 3 in RUN_STAGES:
        stage3_validation()

    if 4 in RUN_STAGES:
        stage4_load_zone_aggregation()

    print("\nAll selected stages complete.")


if __name__ == "__main__":
    main()
