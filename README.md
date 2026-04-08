# Wind Energy Droughts — Data Preparation & Modeling Pipeline

Replication code for the data preparation pipeline used in:

> Victoria Farella (2025). Quantifying and Managing Wind Energy Drought Risk Under Physical Power Purchase Agreements in ERCOT. University of North Carolina at Chapel Hill.

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

### 8. `capacity_summary.py` — Compute pct_wind and installed capacity summary

Reads annual EIA Form 860 plant files and computes total installed capacity and wind share (`pct_wind`) per ERCOT load zone per year. Also validates year-over-year capacity changes at the grid cell level.

| Output | Description |
|---|---|
| `loadzone_capacity_summary.csv` | Total, wind, solar, and `pct_wind` by load zone and year |
| `capacity_change_report.csv` | Grid cells where installed wind capacity changed across years |

> **Must run before Step 9** — `pct_wind` from this file is required by `lz_drought_detection_2020_2024.py` to compute capacity-weighted severity scores.

📁 Code: [`files/capacitysummary/capacity_summary.py`](files/capacitysummary/capacity_summary.py)

### 9. `lz_drought_detection_2020_2024.py` — Load-zone drought event and hourly files (2020–2024)

Produces the load-zone-level drought event summaries and hourly flag files used throughout the price impact and PPA financial risk analysis. Applies the capacity-fraction trigger: a zone hour is classified as a drought hour when ≥ 50% of the zone's installed wind capacity is simultaneously below the CF threshold.

| Output | File pattern | Description |
|---|---|---|
| Hourly flags | `{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv` | One row per hour with `is_drought`, zone average CF, shortfall, capacity metrics, and `pct_wind`-weighted severity |
| Event summaries | `{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_events.csv` | One row per drought event with duration, severity, and weighted severity metrics |

Set `CF_THRESHOLDS` to a list (e.g. `[0.06, 0.10, 0.15, 0.30]`) to produce files for multiple thresholds in one run — useful for the sensitivity analyses reported in the thesis.

> **Dependency note:** This script requires `loadzone_capacity_summary.csv` produced by `capacity_summary.py' (Step 8) for `pct_wind` values. Run Step 8 before this step.

📁 Code: [`files/lzdrought2024/lz_drought_detection_2020_2024.py`](files/lzdrought2024/lz_drought_detection_2020_2024.py)

### 10. `drought_events_30cf.py` — Drought event summaries and hourly flags (CF = 0.30, 2020–2024)

Applies the CF = 0.30 drought threshold to the 2020–2024 period, joining year-specific installed wind capacity and load-zone wind share (`pct_wind`) to each event. Produces two complementary outputs per grid cell used in the price impact and PPA financial risk analysis.

| Output | File pattern | Description |
|---|---|---|
| Drought event summaries | `wind_results_{lat}_{lon}.csv` | One row per event with duration, severity metrics, load zone, installed capacity, `pct_wind`, and two capacity-weighted severity scores |
| Hourly drought flags | `grid_{lat}_{lon}_hourly.csv` | One row per hour flagging `is_drought`, CF shortfall, and raw CF — used for price exceedance analysis |

**`pct_wind`** is the share of total installed nameplate capacity in a load zone attributable to onshore wind turbines, calculated annually from EIA Form 860. It is used to produce two weighted severity scores per event: `weighted_severity_capacity` (severity × grid-cell wind capacity) and `weighted_severity_pct_wind` (severity × zone-level wind share), capturing different dimensions of financial exposure.

> **Why 2020–2024 only?** The full 1950–2024 hazard characterisation is handled by `wind_drought_identification.py`. This script is scoped to the period covered by ERCOT hourly price data required for the PPA financial risk analysis.

> **Note on capacity files:** `{year}_onshore_wind_turbine.csv` and `{year}_all_plants_with_loadzones.xlsx` are not included in the repository. They were compiled from the [EIA Form 860 dataset](https://www.eia.gov/electricity/data/eia860/) — plants filtered to ERCOT load zones, matched to ERA5 grid cells by nearest-neighbour lookup on latitude/longitude.

📁 Code: [`files/below30cf/drought_events_30cf.py`](files/below30cf/drought_events_30cf.py)

### 11. `grid_lz_drought_alignment.py` — Grid-cell to load-zone drought alignment analysis

Quantifies how well individual ERA5 grid cells represent their assigned ERCOT load zone in terms of wind energy drought co-occurrence, using the 2020–2024 hourly drought files. Produces an alignment score for each grid cell combining Spearman correlation and conditional probability statistics.

| Output | Description |
|---|---|
| `grid_loadzone_correlations_hourly30cf.csv` | Spearman correlation between grid and zone CF shortfall, plus drought overlap statistics |
| `grid_loadzone_conditional_probs_hourly30cf.csv` | P(LZ drought \| grid drought) with 95% Wilson confidence intervals |
| `grid_loadzone_correlations_scored_allcells_hourly30cf.csv` | Final scored file with grid coordinates — score = ci_low_95² × Spearman r |
| `alignment_score_map_LZ_WEST_LZ_SOUTH.png` | Spatial map of alignment scores for West and South zones |

The alignment score reflects how reliably a grid-cell drought signals a zone-wide generation shortfall. Grid cells with high scores are most relevant for the PPA financial risk analysis.

> **Note on spatial maps:** Requires `ercot.gpkg` (QGIS boundary file) and optionally `contextily` for basemap tiles (`pip install contextily`). Maps will be skipped gracefully if these are unavailable.

📁 Code: [`files/gridlzalignment/grid_lz_drought_alignment.py`](files/gridlzalignment/grid_lz_drought_alignment.py)

### 12. `price_merge.py` — Join ERCOT prices into hourly drought files

Merges ERCOT hourly settlement point prices into the load-zone hourly drought flag files and adds log-transformed prices. The merged files are the primary inputs for the Welch's ANOVA and proportions z-tests.

> Log prices are NaN for hours with zero or negative prices (ERCOT negative pricing events). Excluded hour counts are reported to the console.

📁 Code: [`files/pricemerge/price_merge.py`](files/pricemerge/price_merge.py)

### 13. `vulnerability_analysis.py` — Bartlett's test, Welch's ANOVA, and proportions z-tests

Implements the statistical tests reported in the vulnerability section. Tests whether wind energy drought conditions are associated with significantly different electricity prices, under progressively finer grouping schemes.

| Stage | Test | Scope | Output |
|---|---|---|---|
| 1 | Bartlett's test (variance homogeneity) | West + South | `bartletts_test_results.csv` |
| 2 | Welch's ANOVA (3 specifications) | West + South | `welch_anova_results.csv` |
| 3 | Proportions z-tests across 10 price thresholds | West + South | `ztest_aggregate_*.csv`, `ztest_duration_*.csv` |
| 4 | Price exceedance curves | West + South | `exceedance_curves_*.png` |

**Grouping schemes for ANOVA:**
- Case I: Drought vs No Drought (2 groups)
- Case II: Duration bins + No Drought (7 groups: <10h, 10–18h, 18–24h, 24–48h, 48–72h, ≥72h)
- Case III: Duration × CF severity bins + No Drought (25 groups)

> **Scope:** All stages use **West and South zones only** — these have the highest wind penetration in ERCOT and are the primary focus of the PPA financial risk analysis.

> **Dependency:** requires `pingouin` for Welch's ANOVA — `pip install pingouin`. Bartlett's tests and z-tests will still run without it.

📁 Code: [`files/vulnerability/vulnerability_analysis.py`](files/vulnerability/vulnerability_analysis.py)

### 14. `debt_financing_assumptions.py` — Derive debt service estimates from financing assumptions

Computes the annual and monthly debt service obligations used as the DSCR benchmark in `ppa_financial_simulations.py`, using the Capital Recovery Factor (CRF) method. All parameter values and sources are documented inline.

| Parameter | Value | Source |
|---|---|---|
| Capital cost | $1,024/kW | IRENA (2025) |
| Nominal WACC | 6.25% | NREL ATB (2024) |
| Project life | 25 years | NREL ATB (2024) |
| **Monthly debt service (nominal)** | **$683,474** | Derived |
| Annual debt service (nominal) | $8,201,688 | Derived |

> No input files required — run this to verify or update the financing assumptions before running `ppa_financial_simulations.py`. If assumptions change, update the `MONTHLY_DEBT_SERVICE` and `ANNUAL_DEBT_SERVICE` constants in `ppa_financial_simulations.py` to match.

📁 Code: [`files/ppasimulations/debt_financing_assumptions.py`](files/ppasimulations/debt_financing_assumptions.py)

### 15. `ppa_financial_simulations.py` — PPA financial risk simulations and DSCR analysis

Implements the physical PPA simulation framework from Section 3.4, quantifying combined price and volume risk across 48 sampled grid cells in the West and South load zones (2020–2024).

| Part | Description | Output |
|---|---|---|
| 1 | Hourly and monthly revenue simulation — all 48 cells × 4 contract levels | `all_cells_revenue_summary_by_cell.csv` |
| 2 | Consecutive shortfall event duration analysis by cell, zone, and contract | `all_cells_shortfall_duration_by_cell_and_contract.csv` |
| 3 | Monthly and annual DSCR against $683,474/month debt service benchmark | `all_cells_dscr_summary.csv`, `all_cells_annual_dscr.csv` |
| Figures | Monthly revenue box plots, shortfall duration by zone, DSCR time-series curves, DSCR ECDF | `output/ppa_financial_simulations/figures/` |

**Key assumptions:** Nameplate 100 MW · PPA price $50/MWh · Contract obligations 25/30/35/40 MWh/hour · Monthly debt service $683,474 (IRENA 2025 CapEx + NREL ATB 2024 WACC) · Uri excluded (Feb 10–20 2021) · Negative price hours excluded.

> **Input files:** The 48 `*_sample.csv` files in `SAMPLE_DIR` combine ERA5-derived hourly capacity factors with concurrent ERCOT real-time spot prices for each sampled grid cell. These files are not included in the repository due to size. See README data source notes for construction details.

> **Excluded from this script:** Single-cell exploratory analyses for grid cells 38_36, 14_26, and 6_23 from the original notebooks are not reproduced here as they are not part of the thesis Section 3.4 results.

📁 Code: [`files/ppasimulations/ppa_financial_simulations.py`](files/ppasimulations/ppa_financial_simulations.py)

### 16. `representative_cell_financial_risk.py` — Single-cell financial risk and DSCR (Cell 6_23, West)

Applies the physical PPA financial mechanics to a single representative grid cell in the West load zone (cell 6_23) under a 30 MWh contract at $50/MWh. Provides a detailed single-cell illustration of the risk framework before it is applied at scale in `ppa_financial_simulations.py`.

| Output | Description |
|---|---|
| `6_23_hourly_cash_flows.csv` | Hourly generation, shortfall, excess, PPA revenue, net gain/loss |
| `6_23_monthly_dscr.csv` | Monthly DSCR with 1.0/1.2/1.3 breach indicators |
| `6_23_annual_dscr.csv` | Annual DSCR per year with breach indicators |
| Figures | Hourly net gains/losses distribution, monthly DSCR time series, DSCR ECDF, annual DSCR bar chart |

> To analyse a different representative cell, update `SAMPLE_FILE` and `CELL_ID` in the configuration block.

📁 Code: [`files/ppasimulations/representative_cell_financial_risk.py`](files/ppasimulations/representative_cell_financial_risk.py)

### 17. `risk_management_storage.py` — Risk management: battery storage and storage + insurance (Cell 6_23)

Evaluates two layered risk management strategies against the unhedged baseline for the representative West load zone cell, measuring their effect on monthly and annual DSCR.

| Strategy | Description |
|---|---|
| Battery only | 5 MW / 4-hour co-located battery covers 0–5 MWh shortfall for the first 4 consecutive hours of each shortfall event. Cost annualised at $330/kWh CapEx, 4% discount rate, 10-year life → $67,810/month |
| Battery + Insurance | Battery layer as above, plus insurance covering the 20–30 MWh shortfall layer when spot price ≥ $100/MWh. Premium = 1.30× mean historical monthly payout |

| Output | Description |
|---|---|
| `6_23_monthly_battery_only.csv` | Monthly revenue and DSCR: baseline vs battery |
| `6_23_annual_dscr_battery_only.csv` | Annual DSCR: baseline vs battery |
| `6_23_monthly_battery_insurance.csv` | Monthly revenue and DSCR: baseline vs battery + insurance |
| `6_23_annual_dscr_battery_insurance.csv` | Annual DSCR: baseline vs battery + insurance |
| Figures | Monthly revenue comparison, monthly DSCR time series, annual DSCR bar charts — for both strategies |

> **Battery source:** NREL (2025). *Cost Projections for Utility-Scale Battery Storage: 2025 Update.* https://docs.nrel.gov/docs/fy25osti/93281.pdf

📁 Code: [`files/riskmanagement/risk_management_storage.py`](files/riskmanagement/risk_management_storage.py)

### 18. `risk_management_diversification.py` — Risk management: geographic diversification (Cell 6_23 baseline)

Evaluates three diversification strategies against the baseline (100 MW concentrated at West cell 6_23), measuring their effect on revenue volatility, VaR, and monthly/annual DSCR.

| Strategy | Configuration | Key result |
|---|---|---|
| 1 — Cross-zone | 50 MW West (6_23) + 50 MW South (38_37) | Reduces 99th percentile monthly VaR from -$446k to +$70k |
| 2 — Cross-zone + Insurance | Strategy 1 + insurance on 20–30 MWh shortfall when price ≥ $100/MWh | Further reduces tail risk; premium = 1.30× expected annual payout |
| 3 — Within-zone | 50 MW West site 1 (6_23) + 50 MW West site 2 (2_22) | Tests whether geographic spread within the West zone provides meaningful risk reduction — an interesting result given high within-zone CF correlation |

> **Note on price convention:** West spot price (cell 6_23) is used throughout all scenarios — including the South cell — to isolate the effect of generation geography on financial risk, holding price exposure constant.

| Output | Description |
|---|---|
| `diversification_cross_zone_monthly.csv` | Monthly revenue and DSCR: baseline vs 50/50 West+South |
| `diversification_cross_zone_annual_dscr.csv` | Annual DSCR: baseline vs 50/50 West+South |
| `diversification_cross_zone_insurance_monthly.csv` | Monthly revenue and DSCR: baseline vs diversified+insured |
| `diversification_cross_zone_insurance_annual_dscr.csv` | Annual DSCR: baseline vs diversified+insured |
| `diversification_within_zone_monthly.csv` | Monthly revenue and DSCR: baseline vs two West sites |
| Figures | Monthly revenue, monthly DSCR, and annual DSCR comparison charts for all three strategies |

📁 Code: [`files/riskmanagement/risk_management_diversification.py`](files/riskmanagement/risk_management_diversification.py)

### 19. `risk_management_insurance.py` — Risk management: insurance only (Cell 6_23)

Evaluates a standalone layer-based insurance contract as a risk management strategy for the representative West load zone cell, covering a defined MWh shortfall slice above a price trigger.

**Insurance structure:** Payout = insured layer MWh × max(spot − $50, 0), only when spot price ≥ $100/MWh. Primary specification: 15–30 MWh shortfall layer. The producer retains shortfall below the attachment point unhedged. Premium = 2.00× expected annual payout spread monthly.

| Output | Description |
|---|---|
| `6_23_monthly_insurance_only.csv` | Monthly revenue, insurance payout, and DSCR: baseline vs insured |
| `6_23_annual_dscr_insurance_only.csv` | Annual DSCR: baseline vs insured |
| `6_23_insurance_threshold_sensitivity.csv` | Grid search results across price triggers ($75–$500/MWh) and attachment points (10–30 MWh) — VaR improvement and net cost per combination |
| Figures | Monthly revenue comparison, monthly DSCR, annual DSCR bar chart, VaR99 improvement heatmap across threshold grid |

> **Premium loading:** A 2.00× loading (100% above expected payout) is applied to reflect the typical cost of catastrophe-style insurance with significant basis risk and limited historical data for pricing.

> **Sensitivity grid:** The threshold sensitivity analysis sweeps 72 combinations of price trigger and attachment point, producing a heatmap of VaR99 improvement to support parameter selection.

📁 Code: [`files/riskmanagement/risk_management_insurance.py`](files/riskmanagement/risk_management_insurance.py)

### 20. `risk_management_reserve_insurance.py` — Risk management: reserve fund + insurance (Cell 6_23)

Evaluates a layered reserve fund and insurance strategy, combining a self-funded lower layer with a premium-based upper layer, compared against the unhedged baseline.

**Layer structure:**

| Layer | Coverage | Trigger | Cost |
|---|---|---|---|
| Producer retained | 0–15 MWh shortfall | — | Unhedged |
| Reserve fund | 15–25 MWh shortfall | Spot ≥ $100/MWh | 15% of excess-energy spot sales; pays up to 50% of current balance per triggered hour |
| Insurance | 25–30 MWh shortfall | $75 ≤ spot ≤ $5,000/MWh | 1.30× expected monthly payout |

The reserve fund is simulated hourly as a running balance — it accumulates during good wind periods and depletes during sustained drought events, providing contingent protection at no fixed cost beyond the opportunity cost of withheld excess revenues. The insurance premium is fixed and deducted monthly regardless of trigger activity.

| Output | Description |
|---|---|
| `6_23_monthly_reserve_insurance.csv` | Monthly revenue, reserve activity, insurance payouts, and DSCR: baseline vs structured |
| `6_23_annual_dscr_reserve_insurance.csv` | Annual DSCR: baseline vs structured |
| Figures | Monthly revenue comparison, monthly DSCR time series, annual DSCR bar chart, reserve balance over time |

📁 Code: [`files/riskmanagement/risk_management_reserve_insurance.py`](files/riskmanagement/risk_management_reserve_insurance.py)

### 21. `risk_management_buyer_sharing.py` — Risk management: buyer risk sharing (Cell 6_23)

Evaluates a structured PPA in which the buyer absorbs the top shortfall layer contingently in exchange for a discounted fixed price, combined with a third-party insurance middle layer.

**Layer structure:**

| Layer | Coverage | Trigger | Cost to producer |
|---|---|---|---|
| Producer retained | 0–15 MWh shortfall | — | Unhedged |
| Insurance | 15–25 MWh shortfall | $75–$1,000/MWh | 1.30× expected monthly payout |
| Buyer risk-sharing | 25–30 MWh shortfall | Price ≥ $100/MWh | Discount: $50.00 → $48.97/MWh |

**Buyer discount pricing:** The required $/MWh discount is derived from the expected annual buyer payout loaded at 1.25×, divided by annual contracted MWh. The offered discount ($1.03/MWh) is compared against this required discount.The annual cost of the discount to the producer (approximately $246,829) is weighed against the expected annual buyer-share value received (approximately $197,222).

| Output | Description |
|---|---|
| `6_23_buyer_discount_pricing.csv` | Required vs offered discount, annual buyer payout by year, producer net tradeoff |
| `6_23_monthly_insurance_buyer_share.csv` | Monthly revenue, insurance payouts, buyer share, and DSCR: baseline vs structured |
| `6_23_annual_dscr_insurance_buyer_share.csv` | Annual DSCR: baseline vs structured |
| Figures | Buyer monthly payout distribution, monthly revenue comparison, monthly DSCR, annual DSCR bar chart |

📁 Code: [`files/riskmanagement/risk_management_buyer_sharing.py`](files/riskmanagement/risk_management_buyer_sharing.py)

## Data

The following input datasets are not included in this repository and must be obtained separately:

| Dataset | Source | Used by |
|---|---|---|
| ERA5 100m wind (GRIB) | Downloaded via `era5_wind_download.py` (CDS API) | Steps 1–6, 9–11 |
| ERCOT RTM Settlement Point Prices | https://www.ercot.com/mktinfo/prices | Steps 7, 12–13 |
| EIA Form 860 plant files | https://www.eia.gov/electricity/data/eia860/ | Steps 8–10 |
| Texas county load zone GeoJSON | QGIS — see Step 2 note | Step 2 |
| Grid-cell sample files (`*_sample.csv`) | Constructed from ERA5 CF + ERCOT prices | Steps 15–21 |

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

# Step 8: compute pct_wind and installed capacity summary
python files/capacitysummary/capacity_summary.py

# Step 9: produce load-zone drought event and hourly files (2020–2024)
python files/lzdrought2024/lz_drought_detection_2020_2024.py

# Step 10: identify drought events at CF=0.30 and build hourly flags (2020–2024)
python files/drought30cf/drought_events_30cf.py

# Step 11: grid-cell to load-zone drought alignment analysis
python files/gridlzalignment/grid_lz_drought_alignment.py

# Step 12: merge ERCOT prices into hourly drought files
python files/pricemerge/price_merge.py

# Step 13: Bartlett's test, Welch's ANOVA, and proportions z-tests
python files/vulnerability/vulnerability_analysis.py

# Step 14: derive debt service estimates from financing assumptions
python files/ppasimulations/debt_financing_assumptions.py

# Step 15: PPA financial risk simulations and DSCR analysis — all 48 cells
python files/ppasimulations/ppa_financial_simulations.py

# Step 16: single-cell financial risk and DSCR — representative West cell (6_23)
python files/ppasimulations/representative_cell_financial_risk.py

# Step 17: risk management — battery storage and storage + insurance (Cell 6_23)
python files/riskmanagement/risk_management_storage.py

# Step 18: risk management — geographic diversification
python files/riskmanagement/risk_management_diversification.py

# Step 19: risk management — insurance only (Cell 6_23)
python files/riskmanagement/risk_management_insurance.py

# Step 20: risk management — reserve fund + insurance (Cell 6_23)
python files/riskmanagement/risk_management_reserve_insurance.py

# Step 21: risk management — buyer risk sharing (Cell 6_23)
python files/riskmanagement/risk_management_buyer_sharing.py
```

Before running `ercot_spatial_grid.py`, update the `INPUT_DIR` path at the top of the script to point to your local `data/` folder containing `Texas_County_LoadZones.geojson`.

---

## Notes

- Each annual ERA5 GRIB file is approximately 2–4 GB; the full 1950–2024 download requires significant storage.
- CDS queue times vary; each year typically takes 10–30 minutes. The download script skips years already completed and is safe to re-run after interruptions.
- ERCOT load zone boundaries were approximated by county classification. Minor boundary errors may exist near the service territory edge; see thesis methodology for discussion.
- `.csv`, `.geojson`, and `.grib` files are excluded from version control via `.gitignore`.

---

## Data Sources

**ERA5 Reanalysis**
Hersbach, H. et al. (2023). ERA5 hourly data on single levels from 1940 to present. *Copernicus Climate Change Service (C3S) Climate Data Store (CDS)*. https://doi.org/10.24381/cds.adbb2d47

**ERCOT Settlement Point Prices**
ERCOT (2025). Historical RTM Settlement Point Prices. Electric Reliability Council of Texas. https://www.ercot.com/mktinfo/prices

**EIA Form 860 — Annual Electric Generator Report**
U.S. Energy Information Administration (2025). Form EIA-860 Annual Electric Generator Report. https://www.eia.gov/electricity/data/eia860/

**IRENA Capital Cost Benchmark**
IRENA (2025). Renewable Power Generation Costs in 2024. International Renewable Energy Agency. https://www.irena.org/-/media/Files/IRENA/Agency/Publication/2025/Jul/IRENA_TEC_RPGC_in_2024_2025.pdf

**NREL Annual Technology Baseline**
NREL (2024). Annual Technology Baseline. National Renewable Energy Laboratory. https://docs.nrel.gov/docs/fy25osti/91775.pdf

**NREL Battery Storage Cost Projections**
NREL (2025). Cost Projections for Utility-Scale Battery Storage: 2025 Update. National Renewable Energy Laboratory. https://docs.nrel.gov/docs/fy25osti/93281.pdf
