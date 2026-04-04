"""
Grid-Cell to Load-Zone Drought Alignment Analysis (CF = 0.30)
==============================================================
Quantifies how well individual ERA5 grid cells represent their assigned
ERCOT load zone in terms of wind energy drought co-occurrence. For each
grid cell, this script computes:

  1. Spearman correlation between grid-cell CF shortfall and zone-level
     CF shortfall across all 2020–2024 hours.

  2. Conditional probability P(LZ drought | grid drought): the fraction of
     hours where the grid cell is in drought that also coincide with a
     load-zone-wide drought.

  3. Overlap statistics: grid_to_lz_overlap_pct (share of grid drought hours
     that are also LZ drought hours) and lz_to_grid_overlap_pct (share of
     LZ drought hours that coincide with a grid cell drought).

  4. A composite alignment score combining the conditional probability
     lower confidence interval and the Spearman correlation:
       score = ci_low_95² × spearman_shortfall_cf

     Grid cells with high scores are those where individual cell droughts
     reliably signal zone-wide generation shortfalls — the cells most
     relevant for PPA financial risk assessment.

These outputs inform which grid cells are most representative of load-zone
drought conditions and support the spatial interpretation of results in the
thesis vulnerability section.

Output files
------------
  grid_loadzone_correlations_hourly30cf.csv
      Spearman correlation and overlap statistics per grid cell.

  grid_loadzone_conditional_probs_hourly30cf.csv
      P(LZ drought | grid drought) with 95% confidence intervals.

  grid_loadzone_merged_for_scoring_hourly30cf.csv
      Merged correlation and probability metrics.

  grid_loadzone_correlations_scored_allcells_hourly30cf.csv
      Final scored file with grid lat/lon added, ready for mapping.

Requirements
------------
    pip install numpy pandas scipy scikit-learn geopandas matplotlib

    Optional: pip install contextily  (for basemap tiles in spatial maps)

Input files required
--------------------
  GRID_HOURLY_DIR : per-grid-cell hourly drought files from drought_events_30cf.py
                    grid_{lat}_{lon}_hourly.csv
                    columns: datetime, is_drought, shortfall_cf, load_zone

  LZ_HOURLY_DIR   : load-zone hourly drought files from lz_drought_detection_2020_2024.py
                    LZ_{ZONE}_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv
                    columns: datetime, is_drought, shortfall_cf

  GRID_MAPPING    : grid_to_loadzone_mapping.csv from ercot_spatial_grid.py
                    columns: lat_idx, lon_idx, lat, lon, load_zone

  CAPACITY_CSV    : 2024_onshore_wind_turbine.csv (EIA Form 860)
                    columns: lat_idx, lon_idx, Nameplate Capacity (MW)
                    used for marker sizing in spatial maps only

  ERCOT_GPKG      : ercot.gpkg — ERCOT boundary and county polygons
                    produced in QGIS; used for spatial maps only

Usage
-----
    python grid_lz_drought_alignment.py

    Update the path variables in the CONFIGURATION block before running.
    Set FOCUS_ZONES to restrict the spatial maps to specific load zones.
"""

import re
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from pathlib import Path
from scipy.stats import spearmanr

try:
    from sklearn.metrics import matthews_corrcoef
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import contextily as cx
    HAS_CTX = True
except ImportError:
    HAS_CTX = False


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Per-grid-cell hourly drought files from drought_events_30cf.py
GRID_HOURLY_DIR = Path("output/drought_hourly_30cf")

# Load-zone hourly files from lz_drought_detection_2020_2024.py
LZ_HOURLY_DIR = Path("output/lz_drought_2020_2024")

# Grid-to-load-zone mapping from ercot_spatial_grid.py
GRID_MAPPING = Path("output/load_zones/grid_to_loadzone_mapping.csv")

# 2024 installed capacity (EIA Form 860) — used for map marker sizing only
CAPACITY_CSV = Path("data/installed_capacities/2024_onshore_wind_turbine.csv")

# ERCOT boundary GeoPackage — used for spatial maps only
ERCOT_GPKG = Path("data/ercot.gpkg")

# Output directory
OUTPUT_DIR = Path("output/grid_lz_alignment")

# Study period
START = pd.Timestamp("2020-01-01 00:00:00")
END   = pd.Timestamp("2024-12-31 23:00:00")

# Load zones to include in spatial maps
FOCUS_ZONES = ["LZ_WEST", "LZ_SOUTH"]

# Scoring weight exponent for the conditional probability CI term
DURATION_WEIGHT_EXP = 2.0


# =============================================================================
# HELPERS
# =============================================================================

def to_hourly_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.floor("h")


def to_int01(s: pd.Series) -> pd.Series:
    return (pd.to_numeric(s, errors="coerce").fillna(0) > 0).astype("int8")


def norm_zone(z: str) -> str:
    z = str(z).strip().upper()
    return z if z.startswith("LZ_") else f"LZ_{z}"


def load_lz_hourly(lz_dir: Path) -> dict:
    """Load all load-zone hourly drought files into a dict keyed by zone name."""
    files = list(lz_dir.glob("LZ_*_CF0.3_CapThresh50pct_years_2020_2024_hourly.csv"))
    print(f"Found {len(files)} LZ hourly files")

    lz_data = {}
    zone_re = re.compile(r"(LZ_[A-Za-z0-9]+)")

    for f in files:
        m = zone_re.search(f.name)
        if not m:
            continue
        zone = m.group(1)

        df = pd.read_csv(f)
        if "datetime" not in df.columns:
            continue

        df["datetime"] = to_hourly_dt(df["datetime"])
        df = df.dropna(subset=["datetime"])

        # Drought flag
        if "is_drought" not in df.columns:
            for alt in ["drought", "lz_drought", "flag_drought"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "is_drought"})
                    break
        if "is_drought" not in df.columns:
            continue

        df["is_drought"]   = to_int01(df["is_drought"])
        df["shortfall_cf"] = pd.to_numeric(
            df.get("shortfall_cf", 0), errors="coerce"
        ).fillna(0.0)

        df = df[(df["datetime"] >= START) & (df["datetime"] <= END)]
        lz_data[zone] = df[["datetime", "is_drought", "shortfall_cf"]].copy()

    zones_loaded = sorted(lz_data.keys())
    print(f"Loaded {len(zones_loaded)} load zones: {zones_loaded}")
    return lz_data


# =============================================================================
# STEP 1: Spearman correlation and overlap statistics
# =============================================================================

def compute_correlations(lz_data: dict) -> pd.DataFrame:
    """
    For each grid-cell hourly file, compute Spearman correlation between
    grid CF shortfall and zone CF shortfall, plus drought overlap statistics.
    """
    grid_files = sorted(GRID_HOURLY_DIR.glob("grid_*_*_hourly.csv"))
    print(f"\nFound {len(grid_files)} grid hourly files")

    fname_re = re.compile(r"grid_(\d+)_(\d+)_hourly\.csv$")
    rows = []
    processed = skipped = 0

    for gf in grid_files:
        m = fname_re.match(gf.name)
        if not m:
            skipped += 1
            continue

        lat_idx = int(m.group(1))
        lon_idx = int(m.group(2))

        try:
            gdf = pd.read_csv(gf)
        except Exception:
            skipped += 1
            continue

        if "datetime" not in gdf.columns or "load_zone" not in gdf.columns:
            skipped += 1
            continue

        gdf["datetime"]    = to_hourly_dt(gdf["datetime"])
        gdf["is_drought"]  = to_int01(gdf.get("is_drought", 0))
        gdf["shortfall_cf"] = pd.to_numeric(
            gdf.get("shortfall_cf", 0), errors="coerce"
        ).fillna(0.0)
        gdf = gdf[(gdf["datetime"] >= START) & (gdf["datetime"] <= END)]

        zone = norm_zone(str(gdf["load_zone"].iloc[0]))
        if zone not in lz_data:
            skipped += 1
            continue

        lz = lz_data[zone]
        merged = gdf[["datetime", "is_drought", "shortfall_cf"]].merge(
            lz[["datetime", "is_drought", "shortfall_cf"]],
            on="datetime", how="inner",
            suffixes=("_grid", "_lz")
        )
        if len(merged) < 10:
            skipped += 1
            continue

        g_sev = merged["shortfall_cf_grid"].values
        z_sev = merged["shortfall_cf_lz"].values

        if g_sev.std() < 1e-9 or z_sev.std() < 1e-9:
            spearman_r = p_val = np.nan
        else:
            spearman_r, p_val = spearmanr(g_sev, z_sev)

        g_d = merged["is_drought_grid"].values
        z_d = merged["is_drought_lz"].values

        both       = int(np.sum((g_d == 1) & (z_d == 1)))
        grid_only  = int(np.sum((g_d == 1) & (z_d == 0)))
        lz_only    = int(np.sum((g_d == 0) & (z_d == 1)))
        n_grid     = int(g_d.sum())
        n_lz       = int(z_d.sum())

        rows.append({
            "lat_idx":              lat_idx,
            "lon_idx":              lon_idx,
            "load_zone":            zone,
            "spearman_shortfall_cf": spearman_r,
            "p_value_shortfall_cf":  p_val,
            "grid_to_lz_overlap_pct": both / n_grid if n_grid > 0 else np.nan,
            "lz_to_grid_overlap_pct": both / n_lz  if n_lz  > 0 else np.nan,
            "n_hours_merged":       len(merged),
        })

        processed += 1
        if processed % 50 == 0:
            print(f"  Processed {processed} grid cells...")

    print(f"\n  Processed: {processed} | Skipped: {skipped}")
    return pd.DataFrame(rows)


# =============================================================================
# STEP 2: Conditional probability P(LZ drought | grid drought)
# =============================================================================

def compute_conditional_probs(lz_data: dict) -> pd.DataFrame:
    """
    For each grid cell, compute P(LZ drought | grid drought) with a
    Wilson score 95% confidence interval.
    """
    grid_files = sorted(GRID_HOURLY_DIR.glob("grid_*_*_hourly.csv"))
    fname_re   = re.compile(r"grid_(\d+)_(\d+)_hourly\.csv$")
    rows = []
    processed = skipped = 0

    for gf in grid_files:
        m = fname_re.match(gf.name)
        if not m:
            skipped += 1
            continue

        lat_idx = int(m.group(1))
        lon_idx = int(m.group(2))

        try:
            gdf = pd.read_csv(gf)
        except Exception:
            skipped += 1
            continue

        if "datetime" not in gdf.columns or "load_zone" not in gdf.columns:
            skipped += 1
            continue

        gdf["datetime"]   = to_hourly_dt(gdf["datetime"])
        gdf["is_drought"] = to_int01(gdf.get("is_drought", 0))
        gdf = gdf[(gdf["datetime"] >= START) & (gdf["datetime"] <= END)]

        zone = norm_zone(str(gdf["load_zone"].iloc[0]))
        if zone not in lz_data:
            skipped += 1
            continue

        merged = gdf[["datetime", "is_drought"]].merge(
            lz_data[zone][["datetime", "is_drought"]],
            on="datetime", how="inner",
            suffixes=("_grid", "_lz")
        )
        if len(merged) < 10:
            skipped += 1
            continue

        g_d = merged["is_drought_grid"].values
        z_d = merged["is_drought_lz"].values

        n_grid_drought = int(g_d.sum())
        both           = int(np.sum((g_d == 1) & (z_d == 1)))
        grid_only      = int(np.sum((g_d == 1) & (z_d == 0)))
        lz_only        = int(np.sum((g_d == 0) & (z_d == 1)))

        # Wilson score confidence interval
        n   = n_grid_drought
        p   = both / n if n > 0 else np.nan
        z95 = 1.96
        if n > 0 and not np.isnan(p):
            denom = 1 + z95**2 / n
            centre = (p + z95**2 / (2 * n)) / denom
            margin = (z95 * np.sqrt(p * (1 - p) / n + z95**2 / (4 * n**2))) / denom
            ci_low  = max(0.0, centre - margin)
            ci_high = min(1.0, centre + margin)
        else:
            ci_low = ci_high = np.nan

        rows.append({
            "lat_idx":                       lat_idx,
            "lon_idx":                       lon_idx,
            "load_zone":                     zone,
            "p_lz_drought_given_grid_drought": p,
            "ci_low_95":                     ci_low,
            "ci_high_95":                    ci_high,
            "n_grid_drought_hours":          n_grid_drought,
            "both_drought_hours":            both,
            "grid_only_hours":               grid_only,
            "lz_only_hours":                 lz_only,
            "n_hours_merged":                len(merged),
        })

        processed += 1
        if processed % 50 == 0:
            print(f"  Processed {processed} grid cells...")

    print(f"\n  Processed: {processed} | Skipped: {skipped}")
    return pd.DataFrame(rows)


# =============================================================================
# STEP 3: Merge, score, add coordinates
# =============================================================================

def merge_and_score(corr_df: pd.DataFrame, prob_df: pd.DataFrame) -> pd.DataFrame:
    """Merge correlation and probability tables, compute alignment score."""
    merged = prob_df.merge(corr_df, on=["lat_idx", "lon_idx", "load_zone"],
                           how="inner")

    ci = pd.to_numeric(merged["ci_low_95"],           errors="coerce")
    sp = pd.to_numeric(merged["spearman_shortfall_cf"], errors="coerce")
    merged["score"] = (ci ** DURATION_WEIGHT_EXP) * sp

    # Add grid coordinates from mapping file
    mapping = pd.read_csv(GRID_MAPPING)
    mapping["lat_idx"] = pd.to_numeric(mapping["lat_idx"], errors="coerce").astype("Int64")
    mapping["lon_idx"] = pd.to_numeric(mapping["lon_idx"], errors="coerce").astype("Int64")
    mapping_sub = (
        mapping[["lat_idx", "lon_idx", "lat", "lon"]]
        .drop_duplicates(subset=["lat_idx", "lon_idx"])
        .rename(columns={"lat": "grid_latitude", "lon": "grid_longitude"})
    )
    merged = merged.merge(mapping_sub, on=["lat_idx", "lon_idx"], how="left")

    return merged


# =============================================================================
# STEP 4: Spatial maps (West and South)
# =============================================================================

def plot_spatial_scores(scored: pd.DataFrame):
    """
    Map alignment scores for FOCUS_ZONES, with marker size proportional to
    2024 installed capacity. Saves one map per zone pair.
    """
    if not ERCOT_GPKG.exists():
        print(f"  [SKIP] ERCOT GeoPackage not found: {ERCOT_GPKG}")
        return

    # Load capacity for marker sizing
    cap_cell = pd.DataFrame()
    if CAPACITY_CSV.exists():
        cap = pd.read_csv(CAPACITY_CSV)
        cap_cell = (
            cap.groupby(["lat_idx", "lon_idx"], as_index=False)
            ["Nameplate Capacity (MW)"].sum()
            .rename(columns={"Nameplate Capacity (MW)": "cap_mw"})
        )

    df = scored.copy()
    df["load_zone"] = df["load_zone"].apply(norm_zone)
    df = df[df["load_zone"].isin(FOCUS_ZONES)].copy()

    if not cap_cell.empty:
        df = df.merge(cap_cell, on=["lat_idx", "lon_idx"], how="left")
        df["cap_mw"] = df["cap_mw"].fillna(0)
    else:
        df["cap_mw"] = 50

    bins   = [-0.1, 0, 50, 100, 300, 500, np.inf]
    labels = ["0", "0–50", "50–100", "100–300", "300–500", "500+"]
    size_map = {"0": 5, "0–50": 20, "50–100": 30,
                "100–300": 60, "300–500": 90, "500+": 150}
    df["cap_bin"]    = pd.cut(df["cap_mw"], bins=bins, labels=labels)
    df["markersize"] = df["cap_bin"].astype(str).map(size_map).fillna(35)

    pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["grid_longitude"], df["grid_latitude"]),
        crs="EPSG:4326"
    )

    try:
        counties = gpd.read_file(ERCOT_GPKG)
        if "Load Zones" in counties.columns:
            counties["load_zone"] = counties["Load Zones"].apply(norm_zone)
            counties = counties[counties["load_zone"].isin(FOCUS_ZONES)]
    except Exception as e:
        print(f"  [WARN] Could not load ERCOT GeoPackage: {e}")
        counties = None

    norm = mcolors.Normalize(vmin=0, vmax=1)
    cmap = plt.cm.RdYlGn

    fig, ax = plt.subplots(figsize=(10, 10))

    if counties is not None:
        counties.to_crs(epsg=3857).boundary.plot(ax=ax, color="grey",
                                                  linewidth=0.5, alpha=0.5)
    if HAS_CTX:
        try:
            cx.add_basemap(ax, crs="EPSG:3857",
                           source=cx.providers.Esri.WorldGrayCanvas, alpha=0.5)
        except Exception:
            pass

    pts_plot = pts.to_crs(epsg=3857)
    sc = ax.scatter(
        pts_plot.geometry.x, pts_plot.geometry.y,
        c=pts_plot["score"],
        s=pts_plot["markersize"],
        cmap=cmap, norm=norm,
        edgecolors="black", linewidths=0.4, zorder=5
    )
    plt.colorbar(sc, ax=ax, label="Alignment score (ci_low² × Spearman r)")
    ax.set_title(
        f"Grid-Cell to Load-Zone Drought Alignment Score\n"
        f"{', '.join(FOCUS_ZONES)} — CF = 0.30, 2020–2024",
        fontsize=13
    )
    ax.set_axis_off()
    plt.tight_layout()

    out = OUTPUT_DIR / f"alignment_score_map_{'_'.join(FOCUS_ZONES)}.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Grid-Cell to Load-Zone Drought Alignment Analysis")
    print("=" * 55)
    print(f"Grid hourly dir : {GRID_HOURLY_DIR}")
    print(f"LZ hourly dir   : {LZ_HOURLY_DIR}")
    print(f"Period          : {START.date()} to {END.date()}")
    print(f"Output          : {OUTPUT_DIR}/")
    print("=" * 55)

    lz_data = load_lz_hourly(LZ_HOURLY_DIR)

    # Step 1: correlations
    print("\n[Step 1] Computing Spearman correlations and overlap statistics...")
    corr_df = compute_correlations(lz_data)
    corr_path = OUTPUT_DIR / "grid_loadzone_correlations_hourly30cf.csv"
    corr_df.to_csv(corr_path, index=False)
    print(f"  Saved: {corr_path.name} ({len(corr_df)} rows)")

    # Step 2: conditional probabilities
    print("\n[Step 2] Computing conditional probabilities P(LZ drought | grid drought)...")
    prob_df = compute_conditional_probs(lz_data)
    prob_path = OUTPUT_DIR / "grid_loadzone_conditional_probs_hourly30cf.csv"
    prob_df.to_csv(prob_path, index=False)
    print(f"  Saved: {prob_path.name} ({len(prob_df)} rows)")

    # Step 3: merge and score
    print("\n[Step 3] Merging and scoring...")
    scored = merge_and_score(corr_df, prob_df)
    scored_path = OUTPUT_DIR / "grid_loadzone_correlations_scored_allcells_hourly30cf.csv"
    scored.to_csv(scored_path, index=False)
    print(f"  Saved: {scored_path.name} ({len(scored)} rows)")
    print(f"  Score range: {scored['score'].min():.4f} to {scored['score'].max():.4f}")

    # Step 4: spatial maps
    print("\n[Step 4] Generating spatial maps...")
    plot_spatial_scores(scored)

    print("\nDone.")


if __name__ == "__main__":
    main()
