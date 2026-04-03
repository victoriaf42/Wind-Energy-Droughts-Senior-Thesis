# ERA5 Wind Data Retrieval

Downloads hourly 100m wind velocity components (u and v) from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu) for the Texas/ERCOT domain, as used in:

> [Your Name] (2025). *[Thesis Title]*. [Institution].

## Data

| Field | Value |
|---|---|
| Dataset | ERA5 hourly reanalysis, single levels |
| Variables | `100m_u_component_of_wind`, `100m_v_component_of_wind` |
| Domain | 26°–36°N, 94°–107°W (Texas / ERCOT) |
| Period | 1950–2024 |
| Output format | GRIB (one file per year) |

## Setup

**1. Install dependencies**
```bash
pip install cdsapi xarray geopandas netcdf4 shapely cfgrib pandas
```

**2. Create a CDS account and accept ERA5 terms of use**

Register at https://cds.climate.copernicus.eu and accept the dataset licence.

**3. Configure your API key**

Create a file at `~/.cdsapirc`:
```
url: https://cds.climate.copernicus.eu/api
key: <YOUR_API_KEY_HERE>
```
Your key is available at https://cds.climate.copernicus.eu/profile.  
**Do not commit this file to version control.**

## Usage

```bash
python era5_wind_download.py
```

Downloaded GRIB files are saved to `era5_wind_data/`. The script skips years that have already been downloaded, so it is safe to re-run after interruptions.

## Notes

- Each annual file is approximately 2–4 GB; the full 1950–2024 download requires significant storage.
- CDS queue times vary; each year typically takes 10–30 minutes.
- The script removes partially downloaded files on failure and will retry them on the next run.

## Reference

Hersbach, H. et al. (2023). ERA5 hourly data on single levels from 1940 to present. *Copernicus Climate Change Service (C3S) Climate Data Store (CDS)*. https://doi.org/10.24381/cds.adbb2d47
