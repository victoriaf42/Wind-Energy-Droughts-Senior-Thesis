# Wind Energy Droughts — Data Preparation Pipeline

Replication code for the data preparation pipeline used in:

> [Your Name] (2025). *[Thesis Title]*. [Institution].

This repository contains two scripts that together produce the spatial ERA5 wind dataset used in the analysis. Run them in order.

---

## Scripts

### 1. `era5_wind_download.py` — Download ERA5 wind data

Downloads hourly 100m wind velocity components (u and v) from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu) for the Texas/ERCOT domain.

| Field | Value |
|---|---|
| Dataset | ERA5 hourly reanalysis, single levels |
| Variables | `100m_u_component_of_wind`, `100m_v_component_of_wind` |
| Domain | 26°–36°N, 94°–107°W (Texas / ERCOT) |
| Period | 1950–2024 |
| Output format | GRIB (one file per year, saved to `era5_wind_data/`) |

### 2. `ercot_spatial_grid.py` — Assign ERA5 grid cells to ERCOT load zones

Generates the full ERA5 grid index for the Texas domain, validates cell alignment, and assigns each grid cell to an ERCOT load zone via a point-in-polygon spatial join against a GeoJSON boundary file.

| Field | Value |
|---|---|
| Input | `Texas_County_LoadZones.geojson` (see note below) |
| Output | `ercot_grid_with_loadzones.csv` — one row per ERA5 grid cell with load zone assignment |

> **Note on boundary file:** The `Texas_County_LoadZones.geojson` used in this study was produced by manually classifying Texas counties to ERCOT load zones in QGIS. This file is not included in the repository. To reproduce the analysis, create your own county-to-load-zone classification using publicly available county shapefiles from the [US Census Bureau](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) and ERCOT's published load zone definitions. Export the result as a GeoJSON and place it in the `data/` directory.

---
### 3. `wind_drought_identification.py` — Identify wind energy drought events

Applies a threshold-based event detection algorithm to ERA5-derived capacity factor time series, processing each ERA5 grid cell individually across the full 1950–2024 study period.

A wind energy drought is defined as any consecutive sequence of hours where the grid-cell CF falls below a user-specified threshold. The study uses **CF = 0.30** as the primary threshold. The threshold can be adjusted in the configuration block to support sensitivity analyses (e.g. CF = 0.10, 0.15).

For each identified event, the script computes:

| Output column | Description |
|---|---|
| `duration` | Number of consecutive below-threshold hours |
| `total_severity` | Sum of hourly shortfalls (threshold − CF) over the event |
| `avg_severity` | `total_severity` / `duration` |
| `pct_severity` | `avg_severity` expressed as a fraction of the threshold |
| `start_time` | Timestamp of the first hour of the event |

**Output:** one CSV per grid cell, named `wind_drought_{lat_idx}_{lon_idx}.csv`. The script skips cells whose output file already exists, so it is safe to interrupt and resume.

📁 Code: [`files/winddroughtid/wind_drought_identification.py`](files/winddroughtid/wind_drought_identification.py)

## Setup

**1. Install dependencies**
```bash
pip install cdsapi xarray geopandas netcdf4 shapely cfgrib pandas numpy
```

**2. Create a CDS account and configure your API key**

Register at https://cds.climate.copernicus.eu and accept the ERA5 terms of use. Then create a file at `~/.cdsapirc`:
```
url: https://cds.climate.copernicus.eu/api
key: <YOUR_API_KEY_HERE>
```
Your key is available at https://cds.climate.copernicus.eu/profile.  
**Do not commit this file to version control** — it is already listed in `.gitignore`.

---

## Usage

```bash
# Step 1: download ERA5 wind data (run first)
python era5_wind_download.py

# Step 2: assign grid cells to ERCOT load zones
python ercot_spatial_grid.py
```

Before running `ercot_spatial_grid.py`, update the `INPUT_DIR` path at the top of the script to point to your local `data/` folder containing `Texas_County_LoadZones.geojson`.

---
# Step 3: identify wind drought events for every grid cell
python files/winddroughtid/wind_drought_identification.py

## Notes

- Each annual ERA5 GRIB file is approximately 2–4 GB; the full 1950–2024 download requires significant storage.
- CDS queue times vary; each year typically takes 10–30 minutes. The download script skips years already completed and is safe to re-run after interruptions.
- ERCOT load zone boundaries were approximated by county classification. Minor boundary errors may exist near the service territory edge; see thesis methodology for discussion.
- `.csv`, `.geojson`, and `.grib` files are excluded from version control via `.gitignore`.

---

## References

Hersbach, H. et al. (2023). ERA5 hourly data on single levels from 1940 to present. *Copernicus Climate Change Service (C3S) Climate Data Store (CDS)*. https://doi.org/10.24381/cds.adbb2d47
