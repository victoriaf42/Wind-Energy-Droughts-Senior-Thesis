"""
Capacity Summary by Load Zone and Year
========================================
Computes installed capacity totals and wind share (pct_wind) per ERCOT load
zone per year from EIA Form 860 plant data. Also validates year-over-year
capacity changes at the grid cell level.

This script runs at Step 8 — before load-zone drought detection — because
pct_wind is required by lz_drought_detection_2020_2024.py (Step 9) to
compute capacity-weighted severity scores.

Output files
------------
  loadzone_capacity_summary.csv
      Total, wind, solar, and pct_wind capacity by load zone and year.
      Primary use: joined into drought event files as a severity weight.

  capacity_change_report.csv
      Grid cells where installed wind capacity changed year-over-year.
      Used to confirm year-specific capacity assignments are correct.

Note on installed capacity data
---------------------------------
Annual plant files ({year}_all_plants_with_loadzones.xlsx and
{year}_onshore_wind_turbine.csv) were compiled from the EIA Form 860 dataset
(https://www.eia.gov/electricity/data/eia860/). Plants were filtered to the
ERCOT service territory and matched to ERA5 grid cells by nearest-neighbour
lookup on latitude and longitude. These files are not included in the
repository due to size.

Note on pct_wind
-----------------
pct_wind is the share of total installed nameplate capacity in a load zone
attributable to onshore wind turbines, calculated annually. It is used to
compute capacity-weighted drought severity scores: events in zones with
higher wind shares are assigned greater weight, reflecting their greater
exposure to wind generation shortfalls.

Requirements
------------
    pip install pandas openpyxl

Input files required
--------------------
  CAPACITY_DIR : annual EIA Form 860 plant files:
                 {year}_all_plants_with_loadzones.xlsx
                 {year}_onshore_wind_turbine.csv

Usage
-----
    python capacity_summary.py

    Update CAPACITY_DIR and OUTPUT_DIR before running.
    Run this before lz_drought_detection_2020_2024.py (Step 9).
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Annual EIA Form 860 plant files
CAPACITY_DIR = Path("data/installed_capacities")

# Output directory
OUTPUT_DIR = Path("output/analysis_inputs")

# Load zones to process
ZONES = ["WEST", "SOUTH", "NORTH", "HOUSTON"]

# Years
YEARS = list(range(2020, 2025))


# =============================================================================
# TASK 1: Capacity summary by load zone and year
# =============================================================================

def compute_capacity_summary():
    """
    Compute total installed capacity and wind/solar share per load zone per year.

    Reads annual EIA Form 860 plant Excel files, categorises technologies,
    and calculates:
      - total_capacity_mw   : all technologies
      - wind_capacity_mw    : onshore wind turbines only
      - solar_capacity_mw   : solar photovoltaic only
      - wind_solar_mw       : wind + solar combined
      - pct_wind            : wind share of total (used for weighted severity)
      - pct_wind_solar      : wind + solar share of total

    Output: loadzone_capacity_summary.csv
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[Task 1] Computing capacity summary by load zone and year...")

    all_rows = []

    for year in YEARS:
        plant_file = CAPACITY_DIR / f"{year}_all_plants_with_loadzones.xlsx"
        if not plant_file.exists():
            print(f"  [SKIP] {plant_file.name} not found")
            continue

        df = pd.read_excel(plant_file, engine="openpyxl")
        df["Nameplate Capacity (MW)"] = pd.to_numeric(
            df["Nameplate Capacity (MW)"], errors="coerce"
        )

        # Detect load zone column
        lz_col = next(
            (c for c in df.columns if "load" in c.lower() and "zone" in c.lower()),
            None
        )
        if lz_col is None:
            print(f"  WARNING: Could not find load zone column in {plant_file.name}")
            continue

        lz_filter = [f"LZ_{z}" for z in ZONES] + ZONES
        df = df[df[lz_col].isin(lz_filter)].copy()
        df["lz_clean"] = (
            df[lz_col].astype(str).str.upper().str.replace("LZ_", "", regex=False)
        )

        for zone in ZONES:
            zone_df = df[df["lz_clean"] == zone]

            total = zone_df["Nameplate Capacity (MW)"].sum()
            wind  = zone_df.loc[
                zone_df["Technology"] == "Onshore Wind Turbine",
                "Nameplate Capacity (MW)"
            ].sum()
            solar = zone_df.loc[
                zone_df["Technology"] == "Solar Photovoltaic",
                "Nameplate Capacity (MW)"
            ].sum()

            all_rows.append({
                "year":              year,
                "load_zone":         f"LZ_{zone}",
                "total_capacity_mw": round(total, 1),
                "wind_capacity_mw":  round(wind, 1),
                "solar_capacity_mw": round(solar, 1),
                "wind_solar_mw":     round(wind + solar, 1),
                "pct_wind":          round(wind / total * 100, 2) if total > 0 else 0.0,
                "pct_wind_solar":    round((wind + solar) / total * 100, 2) if total > 0 else 0.0,
            })

    summary = pd.DataFrame(all_rows).sort_values(["year", "load_zone"])
    out_path = OUTPUT_DIR / "loadzone_capacity_summary.csv"
    summary.to_csv(out_path, index=False)

    print(summary.to_string(index=False))
    print(f"\n  Saved: {out_path}")
    print("[Task 1] Complete.")


# =============================================================================
# TASK 2: Validate capacity changes across years
# =============================================================================

def validate_capacity_changes():
    """
    Identify ERA5 grid cells where installed wind capacity changed across years.

    Confirms that year-specific capacity assignments in the drought event files
    correctly reflect additions and retirements rather than a single static value.

    Input  : {year}_onshore_wind_turbine.csv
    Output : capacity_change_report.csv
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[Task 2] Checking for capacity changes across years...")

    all_years = []
    for year in YEARS:
        cap_file = CAPACITY_DIR / f"{year}_onshore_wind_turbine.csv"
        if not cap_file.exists():
            print(f"  [SKIP] {cap_file.name} not found")
            continue
        df = pd.read_csv(cap_file)
        df = (
            df.groupby(["lat_idx", "lon_idx"], as_index=False)
            ["Nameplate Capacity (MW)"].sum()
            .rename(columns={"Nameplate Capacity (MW)": "installed_capacity_mw"})
        )
        df["year"] = year
        all_years.append(df)

    if not all_years:
        print("  No capacity files found.")
        return

    cap_all = pd.concat(all_years, ignore_index=True)
    cap_pivot = cap_all.pivot_table(
        index=["lat_idx", "lon_idx"],
        columns="year",
        values="installed_capacity_mw",
        aggfunc="sum"
    )

    changed = []
    for (lat_idx, lon_idx), row in cap_pivot.iterrows():
        vals = row.dropna().values
        if len(vals) >= 2 and len(set(vals)) > 1:
            changed.append({
                "lat_idx": lat_idx,
                "lon_idx": lon_idx,
                **{str(int(y)): v for y, v in row.dropna().items()}
            })

    change_df = pd.DataFrame(changed)
    out_path = OUTPUT_DIR / "capacity_change_report.csv"
    change_df.to_csv(out_path, index=False)

    print(f"  Grid cells with capacity changes: {len(change_df)}")
    if not change_df.empty:
        print(change_df.to_string(index=False))
    print(f"  Saved: {out_path}")
    print("[Task 2] Complete.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("Capacity Summary by Load Zone and Year")
    print("=" * 50)
    print(f"Zones  : {ZONES}")
    print(f"Years  : {YEARS[0]}–{YEARS[-1]}")
    print(f"Output : {OUTPUT_DIR}/")
    print("=" * 50)

    compute_capacity_summary()
    validate_capacity_changes()

    print("\nAll tasks complete.")
    print(f"Next step: run lz_drought_detection_2020_2024.py (Step 9)")


if __name__ == "__main__":
    main()
