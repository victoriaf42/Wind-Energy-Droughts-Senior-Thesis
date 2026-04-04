"""
Price Merge — Join ERCOT Prices into Hourly Drought Files
==========================================================
Joins ERCOT hourly settlement point prices to the load-zone hourly drought
flag files produced by lz_drought_detection_2020_2024.py, and adds
log-transformed prices as a derived column.

This script runs at Step 12 — after all drought hourly files have been
produced — because it reads the load-zone hourly files from Step 9
(lz_drought_detection_2020_2024.py) and writes updated versions that include
price and log_price columns.

The merged files are the primary inputs for:
  - Welch's ANOVA (log_prices as dependent variable)
  - Proportions z-tests (price exceedance analysis)
  - Exploratory price impact visualisations

Output
------
  LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
      Updated in-place with additional columns:
        price      : ERCOT settlement point price ($/MWh)
        log_prices : log(price) — NaN for non-positive prices

Note on log prices
-------------------
Log transformation is applied only to hours with price > 0. Hours with
zero or negative prices (which occur in ERCOT due to curtailment incentives
and negative pricing events) receive NaN for log_prices and are excluded
from the Welch's ANOVA and z-test analyses. The count of excluded hours is
reported to the console for transparency.

Requirements
------------
    pip install numpy pandas

Input files required
--------------------
  LZ_HOURLY_DIR : load-zone hourly drought files from lz_drought_detection_2020_2024.py
                  LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
  PRICE_FILE    : ERCOT hourly aggregated prices from ercot_price_aggregation.py
                  columns: hour (datetime), load_zone (e.g. LZ_WEST), price

Usage
-----
    python price_merge.py

    Update LZ_HOURLY_DIR, PRICE_FILE, and OUTPUT_DIR before running.
    Run this after drought_events_30cf.py (Step 11) and before the
    statistical analysis scripts.
"""

import numpy as np
import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Load-zone hourly drought files from lz_drought_detection_2020_2024.py
LZ_HOURLY_DIR = Path("output/lz_drought_2020_2024")

# ERCOT hourly aggregated prices from ercot_price_aggregation.py
PRICE_FILE = Path("data/ercot_hourly_aggregated_prices_2020-2024.csv")

# Output directory — merged files saved here
# Set to same as LZ_HOURLY_DIR to overwrite in-place, or a new path to keep originals
OUTPUT_DIR = Path("output/lz_droughts_with_prices")

# Load zones to process
ZONES = ["WEST", "SOUTH", "NORTH", "HOUSTON"]


# =============================================================================
# MERGE PRICES
# =============================================================================

def merge_prices():
    """
    Join ERCOT prices to load-zone hourly drought files and add log(price).

    For each load zone, reads the hourly drought file, merges prices on
    datetime, computes log(price) where price > 0, and saves the result.

    Reports missing prices, non-positive prices, and resulting log_prices
    NaN counts for each zone.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading ERCOT price data...")
    prices = pd.read_csv(PRICE_FILE)
    prices["hour"] = pd.to_datetime(prices["hour"])

    # Normalise load_zone: strip LZ_ prefix for matching
    prices["load_zone"] = (
        prices["load_zone"].astype(str).str.upper()
        .str.replace("LZ_", "", regex=False)
    )

    # Auto-detect price column
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

    print(f"  Price data loaded: {len(prices):,} rows")
    print(f"  Date range: {prices['hour'].min()} to {prices['hour'].max()}")

    print(f"\nMerging prices into hourly drought files...")

    for zone in ZONES:
        lz_file = LZ_HOURLY_DIR / (
            f"LZ_{zone}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv"
        )
        if not lz_file.exists():
            print(f"  [SKIP] {lz_file.name} not found")
            continue

        df = pd.read_csv(lz_file)
        df["hour"] = pd.to_datetime(df["datetime"])

        merged = df.merge(
            prices[prices["load_zone"] == zone][["hour", "price"]],
            on="hour",
            how="left"
        ).drop(columns=["hour"])

        # Log-transform price — NaN for non-positive values
        merged["log_prices"] = np.where(
            merged["price"] > 0,
            np.log(merged["price"]),
            np.nan
        )

        missing  = merged["price"].isna().sum()
        nonpos   = (merged["price"] <= 0).sum()
        log_nan  = merged["log_prices"].isna().sum()

        out_path = OUTPUT_DIR / lz_file.name
        merged.to_csv(out_path, index=False)

        print(f"  LZ_{zone}: {len(merged):,} rows | "
              f"missing prices={missing} | "
              f"non-positive prices={nonpos} | "
              f"log_prices NaN={log_nan}")
        print(f"    → {out_path.name}")

    print("\nPrice merge complete.")
    print(f"Output: {OUTPUT_DIR}/")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("Price Merge — ERCOT Prices into Hourly Drought Files")
    print("=" * 55)
    print(f"Drought files : {LZ_HOURLY_DIR}/")
    print(f"Price file    : {PRICE_FILE}")
    print(f"Output        : {OUTPUT_DIR}/")
    print(f"Zones         : {ZONES}")
    print("=" * 55)

    merge_prices()


if __name__ == "__main__":
    main()
