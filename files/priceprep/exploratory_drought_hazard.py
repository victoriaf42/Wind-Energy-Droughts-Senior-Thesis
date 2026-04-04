"""
Historical Drought Hazard — Exploratory Analysis (West & South, CF = 0.30)
============================================================================
Produces exploratory visualisations characterising the historical wind energy
drought hazard for the LZ_WEST and LZ_SOUTH load zones over 1950–2024, using
the CF = 0.30 / CapThresh50pct event file produced by
lz_drought_events_historical.py.

Analyses included
------------------
  1. Duration probability histogram — probability density of event duration
     by load zone, with median marked.

  2. Annual exceedance probability surface — for each (duration threshold,
     CF threshold) pair, the annual probability of observing at least one
     event with duration >= d AND avg_zone_cf <= c. Plotted as a 2D heatmap
     with duration on the x-axis and CF on the y-axis.

  3. Return period surface — same threshold grid as above, but expressed as
     return period in years (1 / annual rate).

  4. Seasonal event counts and severity — bar charts showing how many drought
     events occur in each season and the mean/95th percentile severity score
     (duration × CF shortfall below threshold).

  5. Monthly exceedance probability — probability that at least one drought
     event exceeding a given duration threshold occurs in each calendar month,
     averaged over 1950–2024.

Note on CF threshold consistency
----------------------------------
All analyses in this script use the CF = 0.30 event file and apply
EVENT_CF_THRESHOLD = 0.30 consistently for severity scoring. Earlier
exploratory work used CF = 0.15 for some cells — those are not reproduced
here. West and South zones are used throughout as they are the primary focus
of the financial risk analysis.

Requirements
------------
    pip install numpy pandas matplotlib

Input
-----
  EVENTS_FILE : ALL_ZONES_events_all_1950_2024_CF0.3_cap50pct.csv
                produced by lz_drought_events_historical.py

Usage
-----
    python exploratory_drought_hazard.py

    Update EVENTS_FILE and OUTPUT_DIR in the CONFIGURATION block below.
    All figures are saved as PNG files to OUTPUT_DIR.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

EVENTS_FILE = Path(
    "output/historical_drought_events/"
    "ALL_ZONES_events_all_1950_2024_CF0.3_cap50pct.csv"
)

OUTPUT_DIR = Path("output/exploratory_figures/drought_hazard")

# Zones of interest
ZONES = ["LZ_WEST", "LZ_SOUTH"]
ZONE_LABELS = {"LZ_WEST": "West", "LZ_SOUTH": "South"}

# CF threshold used for severity scoring (must match the event file)
EVENT_CF_THRESHOLD = 0.30

# Visual cap for return period plots (years)
DISPLAY_MAX_YEARS = 200


# =============================================================================
# LOAD AND CLEAN
# =============================================================================

def load_events(zones: list) -> tuple[pd.DataFrame, int, int, int]:
    """
    Load the CF = 0.30 event file, clean columns, and filter to target zones.

    Returns
    -------
    df      : cleaned DataFrame filtered to ZONES
    ymin    : first year in sample
    ymax    : last year in sample
    n_years : number of years in sample
    """
    df = pd.read_csv(EVENTS_FILE)

    df["duration"]    = pd.to_numeric(df["duration"],    errors="coerce")
    df["avg_zone_cf"] = pd.to_numeric(df["avg_zone_cf"], errors="coerce")
    df["start_time"]  = pd.to_datetime(df["start_time"], errors="coerce")
    df["load_zone"]   = df["load_zone"].astype(str).str.strip().str.upper()

    df = df[
        (df["duration"]    >  0) &
        (df["avg_zone_cf"].notna()) &
        (df["start_time"].notna()) &
        (df["load_zone"].isin(zones))
    ].copy()

    ymin    = int(df["start_time"].dt.year.min())
    ymax    = int(df["start_time"].dt.year.max())
    n_years = ymax - ymin + 1

    print(f"Loaded {len(df):,} events | {ymin}–{ymax} ({n_years} years) | zones: {zones}")
    return df, ymin, ymax, n_years


# =============================================================================
# ANALYSIS 1: Duration probability histogram
# =============================================================================

def analysis1_duration_histogram(df: pd.DataFrame):
    """
    Probability density histogram of drought event duration by load zone,
    with median marked.
    """
    print("\n[1] Duration probability histogram...")

    upper_cap = float(np.nanpercentile(df["duration"], 99.9))
    bins = np.linspace(0, upper_cap, 40)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)

    for ax, zone in zip(axes, ZONES):
        zone_data = df[df["load_zone"] == zone]["duration"].dropna()

        ax.hist(zone_data, bins=bins, density=True,
                edgecolor="black", alpha=0.75)
        ax.set_title(ZONE_LABELS.get(zone, zone), fontsize=13)
        ax.set_xlabel("Event Duration (hours)")
        ax.set_ylabel("Probability Density")
        ax.set_xlim(0, upper_cap)

        median_val = float(zone_data.median())
        ax.axvline(median_val, linestyle="--", linewidth=2, color="darkred",
                   label=f"Median = {median_val:.1f}h")
        ax.legend()

    plt.suptitle("Drought Event Duration Distribution (CF = 0.30, 1950–2024)",
                 fontsize=14, y=1.01)
    plt.tight_layout()
    out = OUTPUT_DIR / "duration_histogram_west_south.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# ANALYSIS 2: Annual exceedance probability surface
# =============================================================================

def analysis2_exceedance_surface(df: pd.DataFrame, n_years: int):
    """
    2D heatmap of annual exceedance probability P(duration >= d, CF <= c)
    for a grid of (duration threshold, CF threshold) pairs.
    """
    print("\n[2] Annual exceedance probability surface...")

    dur_cap  = float(np.nanpercentile(df["duration"],    99.5))
    cf_lo    = max(0.0, float(np.nanpercentile(df["avg_zone_cf"],  1.0)))
    cf_hi    = min(1.0, float(np.nanpercentile(df["avg_zone_cf"], 99.5)))

    dur_grid = np.linspace(0,    dur_cap, 80)
    cf_grid  = np.linspace(cf_lo, cf_hi,  70)

    D, C = np.meshgrid(dur_grid, cf_grid)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True, sharey=True)
    surf_for_cbar = None

    for ax, zone in zip(axes, ZONES):
        z_data = df[df["load_zone"] == zone][["duration", "avg_zone_cf"]].dropna().to_numpy()
        dur = z_data[:, 0]
        cf  = z_data[:, 1]

        # Lambda(d, c) = annual rate of events with duration >= d AND CF <= c
        lam = np.zeros_like(D, dtype=float)
        for i, c_thr in enumerate(cf_grid):
            mask_c = cf <= c_thr
            dur_c  = dur[mask_c]
            if dur_c.size == 0:
                continue
            counts = (dur_c[:, None] >= dur_grid[None, :]).sum(axis=0)
            lam[i, :] = counts / n_years

        surf = ax.imshow(
            lam,
            origin="lower",
            aspect="auto",
            extent=[dur_grid[0], dur_grid[-1], cf_grid[0], cf_grid[-1]],
            vmin=0, vmax=lam.max(),
            cmap="YlOrRd"
        )
        ax.set_title(ZONE_LABELS.get(zone, zone), fontsize=13)
        ax.set_xlabel("Duration threshold (hours)")
        ax.set_ylabel("Avg zone CF threshold")
        surf_for_cbar = surf

    if surf_for_cbar is not None:
        fig.colorbar(surf_for_cbar, ax=axes[-1],
                     label="Annual exceedance probability")

    plt.suptitle("Annual Exceedance Probability Surface (CF = 0.30, 1950–2024)",
                 fontsize=14, y=1.01)
    plt.tight_layout()
    out = OUTPUT_DIR / "exceedance_surface_west_south.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# ANALYSIS 3: Return period surface
# =============================================================================

def analysis3_return_period_surface(df: pd.DataFrame, n_years: int):
    """
    Same threshold grid as analysis2 but expressed as return period in years.
    """
    print("\n[3] Return period surface...")

    dur_cap  = float(np.nanpercentile(df["duration"],    99.5))
    cf_lo    = max(0.0, float(np.nanpercentile(df["avg_zone_cf"],  1.0)))
    cf_hi    = min(1.0, float(np.nanpercentile(df["avg_zone_cf"], 99.5)))

    dur_grid = np.linspace(0,    dur_cap, 80)
    cf_grid  = np.linspace(cf_lo, cf_hi,  70)

    D, C = np.meshgrid(dur_grid, cf_grid)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True, sharey=True)
    surf_for_cbar = None

    for ax, zone in zip(axes, ZONES):
        z_data = df[df["load_zone"] == zone][["duration", "avg_zone_cf"]].dropna().to_numpy()
        dur = z_data[:, 0]
        cf  = z_data[:, 1]

        lam = np.zeros_like(D, dtype=float)
        for i, c_thr in enumerate(cf_grid):
            mask_c = cf <= c_thr
            dur_c  = dur[mask_c]
            if dur_c.size == 0:
                continue
            counts = (dur_c[:, None] >= dur_grid[None, :]).sum(axis=0)
            lam[i, :] = counts / n_years

        rp = np.full_like(lam, np.nan)
        rp[lam > 0] = 1.0 / lam[lam > 0]
        rp_plot = np.clip(rp, 0, DISPLAY_MAX_YEARS)

        surf = ax.imshow(
            rp_plot,
            origin="lower",
            aspect="auto",
            extent=[dur_grid[0], dur_grid[-1], cf_grid[0], cf_grid[-1]],
            vmin=0, vmax=DISPLAY_MAX_YEARS,
            cmap="viridis_r"
        )
        ax.set_title(ZONE_LABELS.get(zone, zone), fontsize=13)
        ax.set_xlabel("Duration threshold (hours)")
        ax.set_ylabel("Avg zone CF threshold")
        surf_for_cbar = surf

    if surf_for_cbar is not None:
        fig.colorbar(surf_for_cbar, ax=axes[-1],
                     label=f"Return period (years, capped at {DISPLAY_MAX_YEARS})")

    plt.suptitle("Return Period Surface (CF = 0.30, 1950–2024)",
                 fontsize=14, y=1.01)
    plt.tight_layout()
    out = OUTPUT_DIR / "return_period_surface_west_south.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# ANALYSIS 4: Seasonal event counts and severity
# =============================================================================

def analysis4_seasonal_analysis(df: pd.DataFrame):
    """
    Bar charts of seasonal drought event counts and mean/95th percentile
    severity score (duration × max(0, threshold − avg_zone_cf)).
    """
    print("\n[4] Seasonal event counts and severity...")

    m = df["start_time"].dt.month
    df = df.copy()
    df["season"] = np.select(
        [m.isin([12, 1, 2]), m.isin([3, 4, 5]),
         m.isin([6, 7, 8]),  m.isin([9, 10, 11])],
        ["Winter", "Spring", "Summer", "Fall"],
        default=np.nan
    )
    df = df[df["season"].notna()].copy()
    df["severity_score"] = df["duration"] * np.maximum(
        EVENT_CF_THRESHOLD - df["avg_zone_cf"], 0
    )
    seasons_order = ["Winter", "Spring", "Summer", "Fall"]

    fig, axes = plt.subplots(2, 2, figsize=(13, 9))

    for col_idx, zone in enumerate(ZONES):
        z = df[df["load_zone"] == zone]
        counts   = z["season"].value_counts().reindex(seasons_order)
        mean_sev = z.groupby("season")["severity_score"].mean().reindex(seasons_order)
        p95_sev  = z.groupby("season")["severity_score"].quantile(0.95).reindex(seasons_order)

        ax_top = axes[0, col_idx]
        ax_bot = axes[1, col_idx]

        ax_top.bar(seasons_order, counts.values, edgecolor="black", alpha=0.8)
        ax_top.set_title(f"{ZONE_LABELS.get(zone, zone)} — Event counts", fontsize=12)
        ax_top.set_ylabel("Number of events")

        ax_bot.bar(seasons_order, mean_sev.values, label="Mean",
                   edgecolor="black", alpha=0.8)
        ax_bot.plot(seasons_order, p95_sev.values, "o--", color="darkred",
                    label="95th percentile")
        ax_bot.set_title(f"{ZONE_LABELS.get(zone, zone)} — Severity score", fontsize=12)
        ax_bot.set_ylabel("Severity score (hrs × CF shortfall)")
        ax_bot.legend()

    plt.suptitle("Seasonal Drought Characteristics (CF = 0.30, 1950–2024)",
                 fontsize=14, y=1.01)
    plt.tight_layout()
    out = OUTPUT_DIR / "seasonal_analysis_west_south.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# ANALYSIS 5: Monthly exceedance probability
# =============================================================================

def analysis5_monthly_exceedance(df: pd.DataFrame, n_years: int):
    """
    For each calendar month, compute the probability that at least one drought
    event exceeding each duration threshold occurred, averaged over 1950–2024.
    """
    print("\n[5] Monthly exceedance probability...")

    df = df.copy()
    df["month"] = df["start_time"].dt.month
    df["year"]  = df["start_time"].dt.year

    dur_thresholds = [10, 24, 48, 72]
    months = list(range(1, 13))
    month_labels = ["Jan","Feb","Mar","Apr","May","Jun",
                    "Jul","Aug","Sep","Oct","Nov","Dec"]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)

    for ax, zone in zip(axes, ZONES):
        z = df[df["load_zone"] == zone]

        for thr in dur_thresholds:
            probs = []
            for mo in months:
                mo_data = z[z["month"] == mo]
                # Years where at least one event >= thr occurred in this month
                years_with_event = mo_data[mo_data["duration"] >= thr]["year"].nunique()
                probs.append(years_with_event / n_years)

            ax.plot(months, probs, marker="o", linewidth=1.5,
                    label=f"≥ {thr}h")

        ax.set_title(ZONE_LABELS.get(zone, zone), fontsize=13)
        ax.set_xlabel("Month")
        ax.set_ylabel("Annual probability")
        ax.set_xticks(months)
        ax.set_xticklabels(month_labels, rotation=45)
        ax.legend(title="Duration threshold")
        ax.grid(alpha=0.3)

    plt.suptitle("Monthly Exceedance Probability (CF = 0.30, 1950–2024)",
                 fontsize=14, y=1.01)
    plt.tight_layout()
    out = OUTPUT_DIR / "monthly_exceedance_west_south.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out.name}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Historical Drought Hazard — Exploratory Analysis")
    print("=" * 55)
    print(f"Input  : {EVENTS_FILE}")
    print(f"Zones  : {ZONES}")
    print(f"Output : {OUTPUT_DIR}/")
    print("=" * 55)

    df, ymin, ymax, n_years = load_events(ZONES)

    analysis1_duration_histogram(df)
    analysis2_exceedance_surface(df, n_years)
    analysis3_return_period_surface(df, n_years)
    analysis4_seasonal_analysis(df)
    analysis5_monthly_exceedance(df, n_years)

    print(f"\nAll figures saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
