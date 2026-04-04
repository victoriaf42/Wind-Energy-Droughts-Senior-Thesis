"""
Price and Capacity Data Preparation
=====================================
Merges ERCOT hourly electricity prices with the load-zone-level drought
flag files produced by drought_events_30cf.py, adds log-transformed prices,
and computes installed capacity summaries used in the financial risk analysis.

This script covers three tasks:

  Task 1 — Merge prices into hourly drought files
      Joins ERCOT settlement point prices to the hourly drought flag files
      (one per load zone) on datetime and load zone. Also computes
      log(price), which is used as the dependent variable in the
      Welch's ANOVA and z-test analyses.

  Task 2 — Capacity summary by load zone and year
      Reads annual EIA Form 860 plant files, filters to ERCOT load zones,
      and computes total installed capacity and wind/solar share by zone
      and year. The pct_wind column is used to compute capacity-weighted
      severity scores in the drought event files.

  Task 3 — Validate capacity changes across years
      Identifies grid cells where installed wind capacity changed between
      years (e.g. due to new builds or retirements). Useful for confirming
      that year-specific capacity assignments in the event files are correct.

Output files
------------
  LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
      Hourly drought flags + prices + log_prices, one file per load zone.
      These are the primary input files for the Welch's ANOVA and z-tests.

  loadzone_capacity_summary.csv
      Total, wind, solar, and pct_wind capacity by load zone and year.

  capacity_change_report.csv
      Grid cells where installed wind capacity changed across years.

Note on installed capacity data
---------------------------------
Annual plant files ({year}_all_plants_with_loadzones.xlsx and
{year}_onshore_wind_turbine.csv) were compiled from the EIA Form 860 dataset
(https://www.eia.gov/electricity/data/eia860/). Plants were filtered to the
ERCOT service territory and matched to ERA5 grid cells by nearest-neighbour
lookup on latitude and longitude. Load zone assignments follow ERCOT's
published county-to-load-zone mapping. These files are not included in the
repository due to size; download instructions are in the README.

Note on percent wind (pct_wind)
---------------------------------
pct_wind is the share of total installed nameplate capacity in a load zone
that comes from onshore wind turbines, by year. It is used to compute
capacity-weighted drought severity scores: events in zones with higher wind
shares are assigned greater weight, reflecting their greater exposure to
wind generation shortfalls.

Requirements
------------
    pip install numpy pandas openpyxl

Input files required
--------------------
  HOURLY_DIR        : hourly drought flag CSVs from drought_events_30cf.py
                      named grid_{lat}_{lon}_hourly.csv
  LZ_HOURLY_DIR     : load-zone-aggregated hourly drought files
                      named LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
                      (produced by the load zone aggregation step)
  PRICE_FILE        : ERCOT hourly settlement point prices, 2020-2024
                      columns: hour (datetime), load_zone, price
  CAPACITY_DIR      : annual EIA Form 860 plant files
                      {year}_all_plants_with_loadzones.xlsx
                      {year}_onshore_wind_turbine.csv

Usage
-----
    python price_capacity_prep.py

    Update the path variables in the CONFIGURATION block before running.
    Set ZONES to match the load zones in your drought files.
"""

import numpy as np
import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Load-zone-level hourly drought files (LZ_{ZONE}_CF0.3_... .csv)
# These are the files produced by aggregating grid-cell drought flags
# up to the load zone level
LZ_HOURLY_DIR = Path("output/lz_drought_hourly")

# ERCOT hourly settlement point prices (2020-2024)
# Expected columns: hour (datetime), load_zone (e.g. LZ_WEST), price
PRICE_FILE = Path("data/ercot_hourly_aggregated_prices_2020-2024.csv")

# Annual EIA Form 860 plant files
CAPACITY_DIR = Path("data/installed_capacities")

# Output directory
OUTPUT_DIR = Path("output/analysis_inputs")

# Load zones to process
ZONES = ["WEST", "SOUTH", "NORTH", "HOUSTON"]

# Years
YEARS = list(range(2020, 2025))

# Winter Storm Uri dates — excluded from certain price comparisons
# (February 10–20, 2021; peak grid stress period)
URI_DATES = pd.to_datetime([
    "2021-02-10", "2021-02-11", "2021-02-13", "2021-02-14", "2021-02-15",
    "2021-02-16", "2021-02-17", "2021-02-18", "2021-02-19", "2021-02-20"
]).normalize()


# =============================================================================
# TASK 1: Merge ERCOT prices into hourly drought files + add log(price)
# =============================================================================

def task1_merge_prices():
    """
    Join ERCOT settlement point prices to load-zone hourly drought flag files
    and add log(price) as a derived column.

    Log prices are used as the dependent variable in the Welch's ANOVA and
    z-test analyses. Hours with non-positive prices receive NaN for log_prices
    and are excluded from those analyses (counts reported to console).

    Input  : LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
    Output : same filename (overwritten in-place in OUTPUT_DIR)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[Task 1] Merging prices into hourly drought files...")

    prices = pd.read_csv(PRICE_FILE)
    prices["hour"] = pd.to_datetime(prices["hour"])

    # Normalise load_zone format to strip the LZ_ prefix for matching
    prices["load_zone"] = (
        prices["load_zone"].astype(str).str.upper().str.replace("LZ_", "", regex=False)
    )

    # Auto-detect price column name
    price_col = next(
        (c for c in prices.columns if "price" in c.lower() or "lmp" in c.lower()),
        None
    )
    if price_col is None:
        raise ValueError(
            f"Could not find a price column in {PRICE_FILE}. "
            f"Available columns: {prices.columns.tolist()}"
        )
    if price_col != "price":
        prices = prices.rename(columns={price_col: "price"})

    prices = prices[["hour", "load_zone", "price"]]

    for zone in ZONES:
        lz_file = LZ_HOURLY_DIR / (
            f"LZ_{zone}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv"
        )
        if not lz_file.exists():
            print(f"  [SKIP] {lz_file.name} not found")
            continue

        df = pd.read_csv(lz_file)
        df["hour"] = pd.to_datetime(df["datetime"])
        df["load_zone_key"] = zone

        merged = df.merge(
            prices,
            left_on=["hour", "load_zone_key"],
            right_on=["hour", "load_zone"],
            how="left"
        ).drop(columns=["load_zone_key", "load_zone"], errors="ignore")

        # Add log(price) — NaN for non-positive prices
        merged["log_prices"] = np.where(
            merged["price"] > 0,
            np.log(merged["price"]),
            np.nan
        )

        missing   = merged["price"].isna().sum()
        nonpos    = (merged["price"] <= 0).sum()
        log_nan   = merged["log_prices"].isna().sum()

        out_path = OUTPUT_DIR / lz_file.name
        merged.to_csv(out_path, index=False)

        print(f"  {zone}: {len(merged):,} rows | "
              f"missing prices={missing} | "
              f"non-positive prices={nonpos} | "
              f"log_prices NaN={log_nan}")

    print("[Task 1] Complete.")


# =============================================================================
# TASK 2: Capacity summary by load zone and year
# =============================================================================

def task2_capacity_summary():
    """
    Compute total installed capacity and wind/solar share per load zone per year.

    Reads annual EIA Form 860 plant Excel files, categorises technologies,
    and calculates:
      - total_capacity_mw   : all technologies
      - wind_capacity_mw    : onshore wind turbines only
      - solar_capacity_mw   : solar photovoltaic only
      - wind_solar_mw       : wind + solar combined
      - pct_wind            : wind share of total (used for weighted severity)
      - pct_wind_solar      : wind+solar share of total

    Output: loadzone_capacity_summary.csv
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[Task 2] Computing capacity summary by load zone and year...")

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

        # Filter to the four main ERCOT load zones
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

            total    = zone_df["Nameplate Capacity (MW)"].sum()
            wind     = zone_df.loc[
                zone_df["Technology"] == "Onshore Wind Turbine",
                "Nameplate Capacity (MW)"
            ].sum()
            solar    = zone_df.loc[
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
    print("[Task 2] Complete.")


# =============================================================================
# TASK 3: Validate capacity changes across years
# =============================================================================

def task3_capacity_change_report():
    """
    Identify ERA5 grid cells where installed wind capacity changed across years.

    This check confirms that the year-specific capacity assignments in the
    drought event files correctly reflect capacity additions and retirements
    rather than applying a single static value across all years.

    Input  : {year}_onshore_wind_turbine.csv for each year
    Output : capacity_change_report.csv — grid cells with any year-over-year
             change in nameplate capacity
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[Task 3] Checking for capacity changes across years...")

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
    print("[Task 3] Complete.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("Price and Capacity Data Preparation")
    print("=" * 50)
    print(f"Zones : {ZONES}")
    print(f"Years : {YEARS[0]}–{YEARS[-1]}")
    print(f"Output: {OUTPUT_DIR}/")
    print("=" * 50)

    task1_merge_prices()
    task2_capacity_summary()
    task3_capacity_change_report()

    print("\nAll tasks complete.")


if __name__ == "__main__":
    main()
