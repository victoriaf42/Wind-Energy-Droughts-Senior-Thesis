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

📁 Code: [`files/downloaddata/era5_wind_download.py`](files/downloaddata/era5_wind_download.py)

### 2. `ercot_spatial_grid.py` — Assign ERA5 grid cells to ERCOT load zones

Generates the full ERA5 grid index for the Texas domain, validates cell alignment, and assigns each grid cell to an ERCOT load zone via a point-in-polygon spatial join against a GeoJSON boundary file.

| Field | Value |
|---|---|
| Input | `Texas_County_LoadZones.geojson` (see note below) |
| Output | `ercot_grid_with_loadzones.csv` — one row per ERA5 grid cell with load zone assignment |

> **Note on boundary file:** The `Texas_County_LoadZones.geojson` used in this study was produced by manually classifying Texas counties to ERCOT load zones in QGIS. This file is not included in the repository. To reproduce the analysis, create your own county-to-load-zone classification using publicly available county shapefiles from the [US Census Bureau](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) and ERCOT's published load zone definitions. Export the result as a GeoJSON and place it in the `data/` directory.

📁 Code: [`files/ercotspatial/ercot_spatial_grid(1).py`](files/ercotspatial/ercot_spatial_grid(1).py)

### 3. `wind_cf_pipeline.py` — Wind speed, capacity factors, validation, and load zone aggregation

Converts ERA5 GRIB files into load-zone-level wind capacity factor (CF) time series. Runs in four sequential stages controlled by the `RUN_STAGES` list at the top of the script — you can run all four in one go or individual stages as needed.

| Stage | Description | Input | Output |
|---|---|---|---|
| 1 | Extract 100m u/v wind components and compute resultant wind speed | `{year}.grib` | `{year}_wind_speed.nc` |
| 2 | Apply Vestas V90-2MW power curve to produce hourly CF estimates | `{year}_wind_speed.nc` | `{year}_wind_cf.nc` |
| 3 | Compute summary statistics and CF distribution breakdown across all years | `{year}_wind_cf.nc` | `wind_cf_validation_summary.csv` |
| 4 | Map grid cells to ERCOT load zones and compute capacity-weighted CF time series (2020–2024) | `{year}_wind_cf.nc`, `{year}_plants.xlsx` | `grid_cell_weights_by_lz.csv`, `lz_cf_timeseries.csv` |

> **Note on the power curve:** Stage 2 uses the Vestas V90-2MW turbine curve (cut-in 3 m/s, rated speed 12.5 m/s, cut-out 25 m/s, rated capacity 2 MW). More recently deployed turbines achieve higher CFs; see thesis methodology for discussion.

> **Note on Stage 4 inputs:** The `{year}_plants.xlsx` files containing installed wind plant locations and nameplate capacities are not included in this repository. They were compiled from ERCOT's publicly available generation resource data.

📁 Code: [`files/windcfpipeline/wind_cf_pipeline.py`](files/windcfpipeline/wind_cf_pipeline.py)

### 4. `wind_drought_identification.py` — Identify wind energy drought events

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

### 5. `lz_drought_events_historical.py` — Load-zone drought event identification (1950–2024)

Identifies wind energy drought events at the ERCOT load zone level across the full 1950–2024 study period. Unlike `wind_drought_identification.py` (which operates at the individual ERA5 grid cell level), this script aggregates across all grid cells within a zone and applies a **capacity-fraction trigger**: a zone hour is classified as a drought hour only if at least 50% of the zone's 2024 installed wind capacity is simultaneously below the CF = 0.30 threshold.

| Field | Value |
|---|---|
| CF threshold | 0.30 |
| Capacity trigger | ≥ 50% of zone MW below threshold (CapThresh50pct) |
| Capacity weights | 2024 installed capacity — fixed throughout (see note) |
| Output | `ALL_ZONES_events_all_1950_2024_CF0.3_cap50pct.csv` |

**Output columns:** `load_zone`, `start_time`, `end_time`, `duration`, `avg_zone_cf` (capacity-weighted average CF during the event), `total_severity`, `avg_severity`, `pct_severity`.

> **Why 2024 capacity weights for the full historical period?** The goal is to characterise the meteorological hazard that the *current* (2024) wind fleet would face if historical weather conditions recurred — not to reconstruct what a historically changing fleet would have experienced. Using fixed 2024 weights makes the 1950–2024 hazard statistics directly comparable to the 2020–2024 financial risk analysis.

> **Note on capacity file:** `2024_onshore_wind_turbine.csv` was compiled from the EIA Form 860 dataset and is not included in the repository. See README data source notes.

> **Runtime:** Processing 75 years × 4 zones × ~123 grid cells takes approximately 20–60 minutes depending on hardware.

📁 Code: [`files/lzdroughthistorical/lz_drought_events_historical.py`](files/lzdroughthistorical/lz_drought_events_historical.py)

### 6. `exploratory_drought_hazard.py` — Historical drought hazard analysis (West & South, 1950–2024)

Characterises the historical wind energy drought hazard for LZ_WEST and LZ_SOUTH using the event file produced by `lz_drought_events_historical.py`. Produces the figures and summary statistics reported in the hazard section of the thesis.

| Analysis | Description |
|---|---|
| Duration histograms | Probability density of event duration with median marked |
| Exceedance probability surface | Annual probability of events exceeding joint (duration, CF) thresholds |
| Return period surface | Same threshold grid expressed as return period in years |
| Seasonal analysis | Event counts and severity scores by season |
| Monthly exceedance probability | Probability of at least one event exceeding duration thresholds by calendar month |

📁 Code: [`files/droughtanalysis/exploratory_drought_hazard.py`](files/droughtanalysis/exploratory_drought_hazard.py)

### 7. `ercot_price_aggregation.py` — Aggregate ERCOT settlement point prices to hourly

Reads raw ERCOT Settlement Point Price Excel files (one per year, one worksheet per month) and produces a single clean hourly CSV covering 2020–2024. This file is a required input for all downstream price analyses.

| Field | Value |
|---|---|
| Source | ERCOT Historical RTM Settlement Point Prices |
| Input | `{year}.xlsx` — annual Excel files, one sheet per month |
| Output | `ercot_hourly_aggregated_prices_2020-2024.csv` — columns: `hour`, `load_zone`, `price` |
| Zones retained | LZ_WEST, LZ_NORTH, LZ_SOUTH, LZ_HOUSTON |

> **Note on raw price files:** The annual ERCOT SPP Excel files are not included in the repository. They can be downloaded from the [ERCOT market data portal](https://www.ercot.com/mktinfo/prices) under "Historical RTM Settlement Point Prices". Each file contains 15-minute interval prices; this script averages across intervals to produce hourly values. ERCOT uses a 1–24 hour convention (Hour 1 = midnight to 1am); the script converts to standard 0-based datetime notation.

📁 Code: [`files/electricityprices/ercot_price_aggregation.py`](files/electricityprices/ercot_price_aggregation.py)

### 8. `drought_events_30cf.py` — Drought event summaries and hourly flags (CF = 0.30, 2020–2024)

Applies the CF = 0.30 drought threshold to the 2020–2024 period, joining year-specific installed wind capacity and load-zone wind share (`pct_wind`) to each event. Produces two complementary outputs per grid cell used in the price impact and PPA financial risk analysis.

| Output | File pattern | Description |
|---|---|---|
| Drought event summaries | `wind_results_{lat}_{lon}.csv` | One row per event with duration, severity metrics, load zone, installed capacity, `pct_wind`, and two capacity-weighted severity scores |
| Hourly drought flags | `grid_{lat}_{lon}_hourly.csv` | One row per hour flagging `is_drought`, CF shortfall, and raw CF — used for price exceedance analysis |

**`pct_wind`** is the share of total installed nameplate capacity in a load zone attributable to onshore wind turbines, calculated annually from EIA Form 860. It is used to produce two weighted severity scores per event: `weighted_severity_capacity` (severity × grid-cell wind capacity) and `weighted_severity_pct_wind` (severity × zone-level wind share), capturing different dimensions of financial exposure.

> **Why 2020–2024 only?** The full 1950–2024 hazard characterisation is handled by `wind_drought_identification.py`. This script is scoped to the period covered by ERCOT hourly price data required for the PPA financial risk analysis.

> **Note on capacity files:** `{year}_onshore_wind_turbine.csv` and `{year}_all_plants_with_loadzones.xlsx` are not included in the repository. They were compiled from the [EIA Form 860 dataset](https://www.eia.gov/electricity/data/eia860/) — plants filtered to ERCOT load zones, matched to ERA5 grid cells by nearest-neighbour lookup on latitude/longitude.

📁 Code: [`files/below30cf/drought_events_30cf.py`](files/below30cf/drought_events_30cf.py)

### 9. `price_capacity_prep.py` — Merge prices, compute pct_wind, validate capacity

Prepares the merged files used in the formal statistical analyses. Runs three tasks:

| Task | Description | Output |
|---|---|---|
| 1 | Joins ERCOT hourly settlement point prices to drought flag files; adds `log(price)` | `LZ_{ZONE}_CF0.3_..._hourly.csv` (updated) |
| 2 | Computes total, wind, and solar installed capacity by load zone and year from EIA Form 860 data; derives `pct_wind` | `loadzone_capacity_summary.csv` |
| 3 | Identifies grid cells where installed wind capacity changed year-over-year | `capacity_change_report.csv` |

> **Note on installed capacity data:** Annual plant files (`{year}_all_plants_with_loadzones.xlsx`, `{year}_onshore_wind_turbine.csv`) were compiled from the [EIA Form 860 dataset](https://www.eia.gov/electricity/data/eia860/). Plants were filtered to the ERCOT service territory and matched to ERA5 grid cells by nearest-neighbour lookup on latitude/longitude. Load zone assignments follow ERCOT's published county-to-load-zone mapping. These files are not included in the repository due to size.

> **Note on `pct_wind`:** `pct_wind` is the share of total installed nameplate capacity in a load zone attributable to onshore wind turbines, calculated annually. It is used to compute capacity-weighted drought severity scores in the event files.

📁 Code: [`files/priceprep/price_capacity_prep.py`](files/priceprep/price_capacity_prep.py)

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
# Step 1: download ERA5 wind data
python files/downloaddata/era5_wind_download.py

# Step 2: assign ERA5 grid cells to ERCOT load zones
python files/ercot_spatial/ercot_spatial_grid.py

# Step 3: wind speeds → capacity factors → validation → load zone aggregation
python files/windcfpipeline/wind_cf_pipeline.py

# Step 4: identify wind drought events for every grid cell
python files/winddroughtid/wind_drought_identification.py

# Step 5: identify load-zone drought events across full historical period (1950–2024)
python files/lzdroughthistorical/lz_drought_events_historical.py

# Step 6: historical drought hazard analysis — figures and summary statistics
python files/droughtanalysis/exploratory_drought_hazard.py

# Step 7: aggregate raw ERCOT settlement point prices to hourly
python files/electricityprices/ercot_price_aggregation.py

# Step 8: identify drought events at CF=0.30 and build hourly flags (2020–2024)
python files/drought30cf/drought_events_30cf.py

# Step 9: merge prices, compute pct_wind, validate capacity
python files/priceprep/price_capacity_prep.py

```

Before running `ercot_spatial_grid.py`, update the `INPUT_DIR` path at the top of the script to point to your local `data/` folder containing `Texas_County_LoadZones.geojson`.

---

## Notes

- Each annual ERA5 GRIB file is approximately 2–4 GB; the full 1950–2024 download requires significant storage.
- CDS queue times vary; each year typically takes 10–30 minutes. The download script skips years already completed and is safe to re-run after interruptions.
- ERCOT load zone boundaries were approximated by county classification. Minor boundary errors may exist near the service territory edge; see thesis methodology for discussion.
- `.csv`, `.geojson`, and `.grib` files are excluded from version control via `.gitignore`.

---

## References

Hersbach, H. et al. (2023). ERA5 hourly data on single levels from 1940 to present. *Copernicus Climate Change Service (C3S) Climate Data Store (CDS)*. https://doi.org/10.24381/cds.adbb2d47
