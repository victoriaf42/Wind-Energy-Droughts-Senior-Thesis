"""
ERCOT Hourly Price Aggregation (2020–2024)
==========================================
Reads raw ERCOT Settlement Point Price Excel files (one file per year,
one sheet per month) and produces a single clean hourly CSV covering
all four main ERCOT load zones for 2020–2024.

This file is a required input for all downstream price analyses:
  - price_capacity_prep.py  (merging prices into drought flag files)
  - exploratory_price_analysis.py
  - Welch's ANOVA and z-test scripts

Data source
-----------
ERCOT publishes Settlement Point Prices (SPPs) through its market data portal:
https://www.ercot.com/mktinfo/prices

The raw files used here are the "Historical RTM Settlement Point Prices"
Excel workbooks, one per calendar year, with one worksheet per month.
Each row represents a 15-minute interval settlement point price; this script
averages across the four 15-minute intervals within each hour to produce
hourly prices.

Relevant columns in the raw files
-----------------------------------
  Settlement Point Name   : load zone identifier (e.g. LZ_WEST)
  Settlement Point Type   : 'LZ' for load zones (other types are filtered out)
  Settlement Point Price  : 15-minute interval price in $/MWh
  Delivery Date           : date of delivery
  Delivery Hour           : hour ending (1 = midnight–1am, 24 = 11pm–midnight)
  Delivery Interval       : 15-minute interval within the hour (1–4)

Output
------
  ercot_hourly_aggregated_prices_2020-2024.csv
    columns: hour (datetime), load_zone (e.g. LZ_WEST), price ($/MWh)
    one row per load zone per hour

Note on load zones
-------------------
This script retains only the four main ERCOT weather/load zones used in the
thesis analysis: LZ_WEST, LZ_NORTH, LZ_SOUTH, LZ_HOUSTON. The four smaller
competitive zones (LZ_AEN, LZ_CPS, LZ_LCRA, LZ_RAYBN) are excluded. To
include all zones, remove the LOAD_ZONES filter below.

Note on Delivery Hour convention
----------------------------------
ERCOT uses a 1–24 hour convention where Hour 1 covers midnight to 1am.
This script converts to standard Python datetime by subtracting 1 from the
delivery hour before constructing the timestamp, so Hour 1 becomes 00:00,
Hour 24 becomes 23:00.

Requirements
------------
    pip install pandas openpyxl

Input
-----
  PRICE_DIR : directory containing annual Excel files named {year}.xlsx
              Each file must have monthly worksheets with the columns listed
              above. Files are downloaded from ERCOT's market data portal.

Usage
-----
    python ercot_price_aggregation.py

    Update PRICE_DIR and OUTPUT_FILE in the CONFIGURATION block below.
    Adjust YEARS if your raw files cover a different period.
"""

import datetime
import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Directory containing raw ERCOT SPP Excel files ({year}.xlsx)
PRICE_DIR = Path("data/ercot_spp_raw")

# Output file path
OUTPUT_FILE = Path("data/ercot_hourly_aggregated_prices_2020-2024.csv")

# Years to process
YEARS = list(range(2020, 2025))

# Load zones to retain (set to None to keep all LZ types)
LOAD_ZONES = {"LZ_WEST", "LZ_NORTH", "LZ_SOUTH", "LZ_HOUSTON"}

# Raw column names in ERCOT Excel files
RAW_COLS = {
    "price":    "Settlement Point Price",
    "type":     "Settlement Point Type",
    "name":     "Settlement Point Name",
    "date":     "Delivery Date",
    "hour":     "Delivery Hour",
    "interval": "Delivery Interval",
}


# =============================================================================
# HELPERS
# =============================================================================

def build_datetime(date, delivery_hour: int) -> datetime.datetime:
    """
    Convert ERCOT Delivery Date + Delivery Hour to a Python datetime.

    ERCOT hours run 1–24. Hour 1 = 00:00–01:00, so we subtract 1 to get
    the start of the hour in standard 0-based notation.
    """
    hour = int(delivery_hour) - 1
    return datetime.datetime.combine(date, datetime.time(hour=hour))


def process_year(year: int, price_dir: Path) -> pd.DataFrame | None:
    """
    Read all monthly sheets from one year's ERCOT SPP Excel file, filter
    to load zone rows, and return a DataFrame of hourly averages.

    Returns None if the file does not exist.
    """
    file_path = price_dir / f"{year}.xlsx"
    if not file_path.exists():
        print(f"  [SKIP] {file_path.name} not found")
        return None

    print(f"  Processing {year}...")
    xls    = pd.ExcelFile(file_path, engine="openpyxl")
    sheets = xls.sheet_names

    yearly_records = []

    for sheet in sheets:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet,
            engine="openpyxl",
            usecols=list(RAW_COLS.values())
        )

        # Keep only load zone settlement points
        df = df[df[RAW_COLS["type"]] == "LZ"].copy()

        # Filter to target load zones if specified
        if LOAD_ZONES:
            df = df[df[RAW_COLS["name"]].isin(LOAD_ZONES)]

        # Drop rows missing critical fields
        df = df.dropna(subset=[
            RAW_COLS["price"], RAW_COLS["name"],
            RAW_COLS["date"],  RAW_COLS["hour"]
        ])

        if df.empty:
            continue

        # Standardise column names
        df = df.rename(columns={
            RAW_COLS["price"]: "price",
            RAW_COLS["name"]:  "load_zone",
            RAW_COLS["date"]:  "delivery_date",
            RAW_COLS["hour"]:  "delivery_hour",
        })

        # Average 15-minute intervals to hourly
        hourly = (
            df.groupby(["delivery_date", "delivery_hour", "load_zone"])
            ["price"].mean()
            .reset_index()
        )

        yearly_records.append(hourly)
        print(f"    {sheet}: {len(hourly):,} hourly records")

    if not yearly_records:
        print(f"  [WARN] No data found for {year}")
        return None

    return pd.concat(yearly_records, ignore_index=True)


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print("ERCOT Hourly Price Aggregation")
    print("=" * 50)
    print(f"Source : {PRICE_DIR}/")
    print(f"Years  : {YEARS[0]}–{YEARS[-1]}")
    print(f"Zones  : {sorted(LOAD_ZONES) if LOAD_ZONES else 'all LZ types'}")
    print(f"Output : {OUTPUT_FILE}")
    print("=" * 50)

    all_years = []

    for year in YEARS:
        year_df = process_year(year, PRICE_DIR)
        if year_df is not None:
            all_years.append(year_df)

    if not all_years:
        raise RuntimeError(
            f"No data loaded. Check that {PRICE_DIR}/ contains "
            f"{YEARS[0]}.xlsx through {YEARS[-1]}.xlsx"
        )

    full_df = pd.concat(all_years, ignore_index=True)

    # Build hourly datetime column
    # ERCOT Hour 1 = 00:00, so subtract 1
    full_df["delivery_date"] = pd.to_datetime(full_df["delivery_date"])
    full_df["hour"] = full_df.apply(
        lambda r: build_datetime(r["delivery_date"].date(), r["delivery_hour"]),
        axis=1
    )

    # Final output: hour, load_zone, price
    out = full_df[["hour", "load_zone", "price"]].sort_values(
        ["load_zone", "hour"]
    ).reset_index(drop=True)

    out.to_csv(OUTPUT_FILE, index=False)

    print(f"\nDone.")
    print(f"  Total hourly records : {len(out):,}")
    print(f"  Date range           : {out['hour'].min()} to {out['hour'].max()}")
    print(f"  Load zones           : {sorted(out['load_zone'].unique())}")
    print(f"  Output               : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
