"""
ERA5 Wind Data Retrieval Script
================================
Downloads hourly 100m u- and v-components of wind velocity from the
Copernicus Climate Data Store (CDS) for the ERCOT/Texas domain.

Data: ERA5 hourly reanalysis, single levels
Domain: 26°–36°N, 94°–107°W (Texas / ERCOT service territory)
Period: Configurable via YEARS variable below
Output: Annual GRIB files saved to OUTPUT_DIR

Requirements
------------
    pip install cdsapi xarray geopandas netcdf4 shapely cfgrib pandas

CDS API Setup
-------------
1. Register at https://cds.climate.copernicus.eu
2. Accept the ERA5 terms of use
3. Create a file at ~/.cdsapirc with the following content:

    url: https://cds.climate.copernicus.eu/api
    key: <YOUR_API_KEY_HERE>

   Your API key is available at:
   https://cds.climate.copernicus.eu/profile

   Do NOT hard-code your API key in this script.

Usage
-----
    python era5_wind_download.py

References
----------
Hersbach, H. et al. (2023). ERA5 hourly data on single levels from 1940
to present. Copernicus Climate Change Service (C3S) Climate Data Store
(CDS). https://doi.org/10.24381/cds.adbb2d47
"""

import cdsapi
import os
import sys
import time
from datetime import datetime


# =============================================================================
# CONFIGURATION — edit these as needed
# =============================================================================

# Output directory for downloaded GRIB files
OUTPUT_DIR = "era5_wind_data"

# Years to download
YEARS = list(range(1950, 2025))

# CDS dataset identifier
DATASET = "reanalysis-era5-single-levels"

# Variables to retrieve
VARIABLES = [
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
]

# Spatial domain: [north, west, south, east] in degrees
# Covers Texas / ERCOT service territory
AREA = [36, -107, 26, -94]

# =============================================================================
# DOWNLOAD SCRIPT
# =============================================================================

def build_request(year: int) -> dict:
    """Build the CDS API request dictionary for a single year."""
    return {
        "product_type": ["reanalysis"],
        "variable": VARIABLES,
        "year": [str(year)],
        "month": [f"{m:02d}" for m in range(1, 13)],
        "day":   [f"{d:02d}" for d in range(1, 32)],
        "time":  [f"{h:02d}:00" for h in range(24)],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": AREA,
    }


def download_year(client: cdsapi.Client, year: int, output_dir: str) -> bool:
    """
    Download ERA5 wind data for a single year.

    Returns True on success, False on failure.
    """
    output_path = os.path.join(output_dir, f"era5_wind_{year}.grib")

    # Skip if already downloaded
    if os.path.exists(output_path):
        print(f"  [SKIP] {year} — file already exists: {output_path}")
        return True

    print(f"  [DOWNLOAD] {year} — submitting request to CDS API...")
    sys.stdout.flush()

    try:
        start = time.time()
        request = build_request(year)
        result = client.retrieve(DATASET, request)
        result.download(output_path)
        elapsed = (time.time() - start) / 60

        file_size_gb = os.path.getsize(output_path) / (1024 ** 3)
        print(f"  [OK] {year} — {file_size_gb:.2f} GB, {elapsed:.1f} min")
        return True

    except Exception as e:
        print(f"  [FAIL] {year} — {e}")
        # Remove partial file if it exists
        if os.path.exists(output_path):
            os.remove(output_path)
        return False


def main():
    print("ERA5 Wind Data Download")
    print("=" * 50)
    print(f"Domain : {AREA} [N, W, S, E]")
    print(f"Years  : {YEARS[0]}–{YEARS[-1]} ({len(YEARS)} years)")
    print(f"Output : {OUTPUT_DIR}/")
    print(f"Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 50)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialise CDS client — reads credentials from ~/.cdsapirc
    try:
        client = cdsapi.Client()
    except Exception as e:
        print(f"\n[ERROR] Could not initialise CDS API client: {e}")
        print("Ensure ~/.cdsapirc is configured. See script docstring for instructions.")
        sys.exit(1)

    # Download year by year
    successful, failed, skipped = 0, 0, 0
    failed_years = []

    for i, year in enumerate(YEARS, 1):
        print(f"\n[{i}/{len(YEARS)}] Year {year}")
        result = download_year(client, year, OUTPUT_DIR)
        if result:
            if os.path.exists(os.path.join(OUTPUT_DIR, f"era5_wind_{year}.grib")):
                successful += 1
            else:
                skipped += 1
        else:
            failed += 1
            failed_years.append(year)

    # Summary
    print("\n" + "=" * 50)
    print("DOWNLOAD COMPLETE")
    print(f"  Successful : {successful}")
    print(f"  Skipped    : {skipped} (already existed)")
    print(f"  Failed     : {failed}")
    if failed_years:
        print(f"  Failed years: {failed_years}")
        print("  Re-run the script to retry failed years.")
    print(f"  Finished   : {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 50)


if __name__ == "__main__":
    main()
