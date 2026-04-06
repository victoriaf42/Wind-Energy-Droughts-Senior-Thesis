"""
PPA Financial Risk Simulations — Revenue, Shortfall, and DSCR Analysis
========================================================================
Implements the physical PPA simulation framework described in Section 3.4
of the thesis, quantifying the combined price and volume risk faced by a
wind energy producer across 48 sampled grid cells in the West and South
load zones for 2020–2024.

The simulation is applied to four contract obligation levels (25, 30, 35,
and 40 MWh/hour) and produces three sets of outputs:

  Part 1 — Hourly and monthly revenue simulation
      For each grid cell and contract level, computes hourly producer
      revenues under the physical PPA financial mechanics described in
      Table 3 of the thesis. Aggregates to monthly summaries with VaR,
      CV, and net loss statistics. Saves a combined cross-cell summary.

  Part 2 — Shortfall event duration analysis
      Identifies consecutive hourly shortfall events (hours where
      generation < contract obligation) for each cell and contract
      level. Records mean and longest event duration. Saves a combined
      cross-cell summary.

  Part 3 — Monthly and annual DSCR
      Computes monthly DSCR = monthly revenue / monthly debt service
      for each cell and contract level. Summarises the distribution of
      DSCR across all cell-month observations, including probabilities
      of breaching the 1.0, 1.2, and 1.3 lender threshold benchmarks.
      Also computes annual DSCR for each cell and year.

Financial mechanics (Table 3)
------------------------------
  Condition                             Treatment
  ─────────────────────────────────────────────────────────────────────
  Actual generation < contract          Producer buys shortfall at spot
  Spot > fixed price                    Loss = (spot − fixed) × shortfall
  Spot ≤ fixed price                    No loss on shortfall purchases
  Actual generation > contract          Surplus sold at spot price
  No-arbitrage condition                No profit from buying below fixed
  Generation shortfall                  Floored at zero; no offset

Asset and contract parameters (Table 2)
-----------------------------------------
  Nameplate capacity   : 100 MW
  Fixed contract price : $50/MWh
  Contract obligations : 25, 30, 35, 40 MWh/hour
  Settlement           : Monthly, in the generator's load zone

Financing assumptions (Table 4)
---------------------------------
  Capital cost (CapEx) : $1,024/kW (IRENA 2025)
  Total CapEx          : $102,400,000
  Nominal WACC         : 6.25% (NREL ATB 2024)
  Project life         : 25 years
  CRF                  : 8.0095%
  Annual debt service  : $8,201,688
  Monthly debt service : $683,474

Exclusions
----------
  Uri exclusion: February 10–20, 2021 — hours in this window are
      excluded from all revenue and DSCR calculations to avoid
      distortion from the extreme price spike during Winter Storm Uri.
  Negative prices: hours with spot price < 0 are excluded, as these
      are primarily curtailment-driven and not representative of
      normal operating conditions.

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  SAMPLE_DIR : 48 pre-built grid-cell simulation files (*_sample.csv)
               columns: timestamp, capacity_factor, price, Load Zone
               These files combine ERA5-derived hourly CF estimates
               with concurrent ERCOT real-time spot prices for each
               sampled grid cell. See README for construction details.

Output files
------------
  all_cells_revenue_summary_by_cell.csv
      Mean revenue, VaR, CV, net loss statistics per cell × contract.

  all_cells_shortfall_duration_by_cell_and_contract.csv
      Mean and longest shortfall event duration per cell × contract.

  all_cells_dscr_summary.csv
      DSCR distribution statistics per contract level across all cells.

  all_cells_annual_dscr.csv
      Annual DSCR per cell × year × contract level.

  Figures saved to OUTPUT_DIR/figures/:
      monthly_revenue_boxplots.png
      monthly_revenue_by_zone.png
      shortfall_duration_boxplots.png
      dscr_curves_by_contract.png
      dscr_ecdf_by_contract.png

Usage
-----
    python ppa_financial_simulations.py

    Update SAMPLE_DIR and OUTPUT_DIR in the CONFIGURATION block before
    running. Ensure all 48 *_sample.csv files are present in SAMPLE_DIR.
"""

import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Directory containing the 48 sampled grid-cell simulation files
SAMPLE_DIR = Path("data/simulated_revs_south_west_gridcells")

# Output directory
OUTPUT_DIR = Path("output/ppa_financial_simulations")

# Asset parameters
NAMEPLATE_MW  = 100.0
PPA_PRICE     = 50.0
CONTRACT_LEVELS = [25.0, 30.0, 35.0, 40.0]

# Debt service (Table 4)
MONTHLY_DEBT_SERVICE = 683_474.0
ANNUAL_DEBT_SERVICE  = 8_201_688.0

# DSCR breach thresholds
DSCR_THRESHOLDS = [1.0, 1.2, 1.3]

# Uri exclusion window
URI_DATES = pd.to_datetime([
    "2021-02-10", "2021-02-11", "2021-02-12", "2021-02-13", "2021-02-14",
    "2021-02-15", "2021-02-16", "2021-02-17", "2021-02-18",
    "2021-02-19", "2021-02-20",
]).normalize()
URI_SET = set(URI_DATES)

# Column names in sample files
TS_COL       = "timestamp"
CF_COL       = "capacity_factor"
PRICE_COL    = "price"
LZ_COL       = "Load Zone"


# =============================================================================
# HELPERS
# =============================================================================

def load_sample(file_path: Path) -> pd.DataFrame | None:
    """Load and clean one grid-cell sample file."""
    df = pd.read_csv(file_path)
    df[TS_COL]    = pd.to_datetime(df[TS_COL],    errors="coerce")
    df[CF_COL]    = pd.to_numeric(df[CF_COL],     errors="coerce")
    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL],  errors="coerce")

    df = df.dropna(subset=[TS_COL, CF_COL, PRICE_COL]).copy()

    # Exclude Uri hours
    df = df[~df[TS_COL].dt.normalize().isin(URI_SET)].copy()

    # Exclude negative price hours
    df = df[df[PRICE_COL] >= 0].copy()

    if df.empty:
        return None

    df["gen_mwh"] = df[CF_COL].clip(lower=0) * NAMEPLATE_MW
    df["month"]   = df[TS_COL].dt.to_period("M")
    df["year"]    = df[TS_COL].dt.year

    return df


def compute_hourly_revenue(df: pd.DataFrame, contract_mw: float) -> pd.DataFrame:
    """
    Compute hourly PPA revenues using the Pair 2 (physical PPA) mechanics.

    For each hour:
      - Fixed PPA revenue on contracted volume
      - Surplus sold at spot price
      - Shortfall purchased at spot; loss = max(0, spot - fixed) × shortfall
    """
    out = df.copy()

    out["shortfall_mwh"] = (contract_mw - out["gen_mwh"]).clip(lower=0)
    out["excess_mwh"]    = (out["gen_mwh"] - contract_mw).clip(lower=0)

    out["ppa_revenue_usd"]     = contract_mw * PPA_PRICE
    out["excess_sales_usd"]    = out["excess_mwh"] * out[PRICE_COL]
    out["price_minus_fixed"]   = (out[PRICE_COL] - PPA_PRICE).clip(lower=0)
    out["shortfall_cost_usd"]  = out["shortfall_mwh"] * out["price_minus_fixed"]

    out["net_gain_loss_usd"]   = out["excess_sales_usd"] - out["shortfall_cost_usd"]
    out["total_revenue_usd"]   = out["ppa_revenue_usd"] + out["net_gain_loss_usd"]

    out["is_shortfall"] = (out["shortfall_mwh"] > 0).astype(int)
    out["is_surplus"]   = (out["excess_mwh"]    > 0).astype(int)
    out["is_net_loss"]  = (out["net_gain_loss_usd"] < 0).astype(int)

    return out


def dscr_stats(monthly_revenue: np.ndarray) -> dict:
    """Compute DSCR statistics across all cell-month observations."""
    dscr = monthly_revenue / MONTHLY_DEBT_SERVICE
    dscr = dscr[np.isfinite(dscr)]
    if len(dscr) == 0:
        return {}
    result = {
        "n_months":   int(len(dscr)),
        "mean":       float(np.mean(dscr)),
        "std":        float(np.std(dscr, ddof=1)) if len(dscr) > 1 else np.nan,
        "min":        float(np.min(dscr)),
        "p10":        float(np.percentile(dscr, 10)),
        "p50":        float(np.percentile(dscr, 50)),
        "p90":        float(np.percentile(dscr, 90)),
    }
    for thr in DSCR_THRESHOLDS:
        result[f"prob_below_{str(thr).replace('.', '_')}"] = float(np.mean(dscr < thr))
    return result


def ecdf(x: np.ndarray):
    x = np.sort(x[np.isfinite(x)])
    y = np.arange(1, len(x) + 1) / len(x)
    return x, y


# =============================================================================
# PART 1: Revenue simulation across all 48 cells
# =============================================================================

def part1_revenue_simulation(sample_files: list) -> pd.DataFrame:
    """
    Simulate hourly and monthly revenues for all cells and contract levels.
    Returns a DataFrame with one row per (cell, contract_level).
    """
    print("\n[Part 1] Revenue simulation across all cells...")

    all_rows = []
    all_monthly = []   # collect for DSCR (Part 3) and figures

    for fp in sample_files:
        stem = fp.stem.replace("_sample", "")
        lat_idx, lon_idx = map(int, stem.split("_"))

        df = load_sample(fp)
        if df is None:
            print(f"  [SKIP] {fp.name}")
            continue

        load_zone = str(df[LZ_COL].iloc[0]) if LZ_COL in df.columns else "unknown"

        for contract_mw in CONTRACT_LEVELS:
            rev = compute_hourly_revenue(df, contract_mw)

            # Monthly aggregation
            monthly = rev.groupby("month").agg(
                monthly_revenue_usd     = ("total_revenue_usd",   "sum"),
                monthly_net_loss_usd    = ("net_gain_loss_usd",   "sum"),
                n_hours                 = ("total_revenue_usd",   "count"),
                n_shortfall_hours       = ("is_shortfall",        "sum"),
                n_surplus_hours         = ("is_surplus",          "sum"),
                n_net_loss_hours        = ("is_net_loss",         "sum"),
            ).reset_index()
            monthly["lat_idx"]      = lat_idx
            monthly["lon_idx"]      = lon_idx
            monthly["load_zone"]    = load_zone
            monthly["contract_mwh"] = contract_mw
            all_monthly.append(monthly)

            rev_vals = monthly["monthly_revenue_usd"].dropna().values

            all_rows.append({
                "lat_idx":                     lat_idx,
                "lon_idx":                     lon_idx,
                "load_zone":                   load_zone,
                "contract_mwh":                contract_mw,
                "mean_monthly_revenue_usd":    float(np.mean(rev_vals)),
                "std_monthly_revenue_usd":     float(np.std(rev_vals, ddof=1)),
                "cv_monthly":                  float(np.std(rev_vals, ddof=1) / np.mean(rev_vals))
                                               if np.mean(rev_vals) != 0 else np.nan,
                "VaR_95_monthly_revenue_usd":  float(np.percentile(rev_vals, 5)),
                "VaR_99_monthly_revenue_usd":  float(np.percentile(rev_vals, 1)),
                "min_monthly_revenue_usd":     float(np.min(rev_vals)),
                "max_monthly_revenue_usd":     float(np.max(rev_vals)),
                "mean_monthly_net_loss_usd":   float(monthly["monthly_net_loss_usd"].mean()),
                "max_monthly_net_loss_usd":    float(monthly["monthly_net_loss_usd"].min()),
                "pct_shortfall_hours":         float(rev["is_shortfall"].mean()),
                "pct_surplus_hours":           float(rev["is_surplus"].mean()),
                "pct_net_loss_hours":          float(rev["is_net_loss"].mean()),
                "mean_hourly_revenue_usd":     float(rev["total_revenue_usd"].mean()),
                "std_hourly_revenue_usd":      float(rev["total_revenue_usd"].std(ddof=1)),
                "VaR_95_hourly_revenue_usd":   float(np.percentile(rev["total_revenue_usd"], 5)),
                "VaR_99_hourly_revenue_usd":   float(np.percentile(rev["total_revenue_usd"], 1)),
                "max_hourly_net_loss_usd":     float(rev["net_gain_loss_usd"].min()),
            })

    summary_df   = pd.DataFrame(all_rows)
    monthly_all  = pd.concat(all_monthly, ignore_index=True)

    out_path = OUTPUT_DIR / "all_cells_revenue_summary_by_cell.csv"
    summary_df.to_csv(out_path, index=False)
    print(f"  Saved: {out_path.name} ({len(summary_df)} rows)")

    # Cross-cell aggregate by contract level
    print("\n  Cross-cell summary by contract level:")
    for cmw in CONTRACT_LEVELS:
        sub = summary_df[summary_df["contract_mwh"] == cmw]
        print(f"  {int(cmw)} MWh | mean hourly rev=${sub['mean_hourly_revenue_usd'].mean():,.0f} | "
              f"99th VaR hourly=${sub['VaR_99_hourly_revenue_usd'].mean():,.0f} | "
              f"shortfall%={sub['pct_shortfall_hours'].mean()*100:.1f}%")

    return summary_df, monthly_all


# =============================================================================
# PART 2: Shortfall event duration analysis
# =============================================================================

def part2_shortfall_duration(sample_files: list) -> pd.DataFrame:
    """
    Identify consecutive shortfall events for each cell × contract level.
    Returns a DataFrame with mean and longest event duration.
    """
    print("\n[Part 2] Shortfall event duration analysis...")

    all_rows = []

    for fp in sample_files:
        stem = fp.stem.replace("_sample", "")
        lat_idx, lon_idx = map(int, stem.split("_"))

        df = load_sample(fp)
        if df is None:
            continue

        load_zone = str(df[LZ_COL].iloc[0]) if LZ_COL in df.columns else "unknown"
        df = df.sort_values(TS_COL).reset_index(drop=True)

        for contract_mw in CONTRACT_LEVELS:
            contract_cf = contract_mw / NAMEPLATE_MW
            df["is_shortfall"] = df[CF_COL] < contract_cf

            # Identify contiguous shortfall runs
            df["event_start"] = (
                df["is_shortfall"] & ~df["is_shortfall"].shift(fill_value=False)
            )
            df["event_id"] = df["event_start"].cumsum() * df["is_shortfall"]

            shortfall_events = df[df["event_id"] > 0]
            if shortfall_events.empty:
                continue

            durations = shortfall_events.groupby("event_id").size()

            all_rows.append({
                "lat_idx":                   lat_idx,
                "lon_idx":                   lon_idx,
                "load_zone":                 load_zone,
                "contract_mwh":              contract_mw,
                "n_events":                  int(len(durations)),
                "mean_event_duration_hours": float(durations.mean()),
                "longest_duration_hours":    float(durations.max()),
                "pct_shortfall_hours":       float(df["is_shortfall"].mean()),
            })

    dur_df = pd.DataFrame(all_rows)
    out_path = OUTPUT_DIR / "all_cells_shortfall_duration_by_cell_and_contract.csv"
    dur_df.to_csv(out_path, index=False)
    print(f"  Saved: {out_path.name} ({len(dur_df)} rows)")

    print("\n  Shortfall duration summary by zone and contract:")
    for cmw in CONTRACT_LEVELS:
        sub = dur_df[dur_df["contract_mwh"] == cmw]
        for zone in ["LZ_WEST", "LZ_SOUTH"]:
            z = sub[sub["load_zone"] == zone]
            if z.empty:
                continue
            print(f"  {zone} {int(cmw)} MWh | "
                  f"avg longest={z['longest_duration_hours'].mean():.0f}h | "
                  f"max longest={z['longest_duration_hours'].max():.0f}h")

    return dur_df


# =============================================================================
# PART 3: DSCR analysis
# =============================================================================

def part3_dscr(monthly_all: pd.DataFrame):
    """
    Compute monthly and annual DSCR across all cell-month observations.
    """
    print("\n[Part 3] DSCR analysis...")

    monthly_all = monthly_all.copy()
    monthly_all["dscr"] = monthly_all["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE

    # --- Monthly DSCR summary by contract level ---
    dscr_rows = []
    for cmw in CONTRACT_LEVELS:
        sub = monthly_all[monthly_all["contract_mwh"] == cmw]
        vals = sub["monthly_revenue_usd"].dropna().values
        stats = dscr_stats(vals)
        stats["contract_mwh"] = cmw
        dscr_rows.append(stats)

    dscr_summary = pd.DataFrame(dscr_rows)
    out1 = OUTPUT_DIR / "all_cells_dscr_summary.csv"
    dscr_summary.to_csv(out1, index=False)
    print(f"  Saved: {out1.name}")

    print("\n  Monthly DSCR summary:")
    for _, row in dscr_summary.iterrows():
        print(f"  {int(row['contract_mwh'])} MWh | mean={row['mean']:.2f} | "
              f"p10={row['p10']:.2f} | min={row['min']:.2f} | "
              f"P(DSCR<1.0)={row['prob_below_1_0']*100:.1f}% | "
              f"P(DSCR<1.2)={row['prob_below_1_2']*100:.1f}%")

    # --- Annual DSCR per cell × year × contract ---
    monthly_all["year"] = monthly_all["month"].apply(
        lambda p: p.year if hasattr(p, "year") else int(str(p)[:4])
    )
    annual = (
        monthly_all.groupby(["lat_idx", "lon_idx", "load_zone", "contract_mwh", "year"])
        ["monthly_revenue_usd"].sum()
        .reset_index()
        .rename(columns={"monthly_revenue_usd": "annual_revenue_usd"})
    )
    annual["annual_dscr"] = annual["annual_revenue_usd"] / ANNUAL_DEBT_SERVICE
    out2 = OUTPUT_DIR / "all_cells_annual_dscr.csv"
    annual.to_csv(out2, index=False)
    print(f"  Saved: {out2.name}")

    return monthly_all, dscr_summary


# =============================================================================
# FIGURES
# =============================================================================

def make_figures(summary_df: pd.DataFrame, dur_df: pd.DataFrame,
                 monthly_all: pd.DataFrame):
    """Produce all figures for Section 3.4."""
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")
    labels = [int(c) for c in CONTRACT_LEVELS]

    # --- Figure 1: Monthly revenue box plots — all cells (Figure 13) ---
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    metrics = [
        ("mean_monthly_revenue_usd",   "Mean Monthly Revenue"),
        ("VaR_95_monthly_revenue_usd", "95% VaR (Monthly Revenue)"),
        ("VaR_99_monthly_revenue_usd", "99% VaR (Monthly Revenue)"),
    ]
    for ax, (col, title) in zip(axes, metrics):
        data = [summary_df[summary_df["contract_mwh"] == c][col].dropna()
                for c in CONTRACT_LEVELS]
        ax.boxplot(data, tick_labels=labels)
        ax.set_title(title)
        ax.set_xlabel("Contract Obligation (MWh)")
        ax.yaxis.set_major_formatter(currency_fmt)
    axes[0].set_ylabel("USD")
    fig.suptitle(
        "Distribution of Monthly Revenue and Risk Metrics Across All Grid Cells",
        fontsize=14
    )
    plt.tight_layout()
    plt.savefig(fig_dir / "monthly_revenue_boxplots.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: monthly_revenue_boxplots.png")

    # --- Figure 2: Monthly revenue by load zone (Figure 14) ---
    zones = ["LZ_WEST", "LZ_SOUTH"]
    fig, axes = plt.subplots(3, 2, figsize=(14, 14), sharex=True)
    for row, (col, title) in enumerate(metrics):
        for col_idx, zone in enumerate(zones):
            ax = axes[row, col_idx]
            zone_df = summary_df[summary_df["load_zone"] == zone]
            data = [zone_df[zone_df["contract_mwh"] == c][col].dropna()
                    for c in CONTRACT_LEVELS]
            ax.boxplot(data, tick_labels=labels)
            ax.set_title(f"{zone} — {title}")
            ax.set_xlabel("Contract Obligation (MWh)")
            ax.yaxis.set_major_formatter(currency_fmt)
    plt.tight_layout()
    plt.savefig(fig_dir / "monthly_revenue_by_zone.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: monthly_revenue_by_zone.png")

    # --- Figure 3: Shortfall duration box plots (Figure 15) ---
    fig, axes = plt.subplots(2, 2, figsize=(14, 12), sharex=True, sharey="row")
    dur_metrics = [
        ("mean_event_duration_hours",  "Average Shortfall Duration"),
        ("longest_duration_hours",     "Longest Shortfall Duration"),
    ]
    for row, (col, title) in enumerate(dur_metrics):
        for col_idx, zone in enumerate(zones):
            ax = axes[row, col_idx]
            zone_df = dur_df[dur_df["load_zone"] == zone]
            data = [zone_df[zone_df["contract_mwh"] == c][col].dropna()
                    for c in CONTRACT_LEVELS]
            ax.boxplot(data, tick_labels=labels)
            ax.set_title(f"{zone} — {title}")
            ax.set_xlabel("Contract Obligation (MWh)")
    axes[0, 0].set_ylabel("Duration (Hours)")
    axes[1, 0].set_ylabel("Duration (Hours)")
    fig.suptitle("Shortfall Duration Risk by Load Zone and Contract Obligation",
                 fontsize=14)
    plt.tight_layout()
    plt.savefig(fig_dir / "shortfall_duration_boxplots.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: shortfall_duration_boxplots.png")

    # --- Figure 4: DSCR curves over time (Figure 16) ---
    monthly_all = monthly_all.copy()
    monthly_all["month_dt"] = monthly_all["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp() if not isinstance(p, pd.Timestamp) else p
    )

    dscr_thresh_colors = {1.0: "red", 1.2: "orange", 1.3: "gold"}

    fig, axes = plt.subplots(2, 2, figsize=(16, 10), sharey=True)
    axes = axes.flatten()

    for ax, cmw in zip(axes, CONTRACT_LEVELS):
        sub = monthly_all[monthly_all["contract_mwh"] == cmw].copy()
        sub["dscr"] = sub["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE

        # Median and percentile bands across cells, by month
        monthly_stats = sub.groupby("month_dt")["dscr"].agg(
            p10=lambda x: np.percentile(x, 10),
            p50=lambda x: np.percentile(x, 50),
            p90=lambda x: np.percentile(x, 90),
        ).reset_index()

        ax.fill_between(monthly_stats["month_dt"],
                        monthly_stats["p10"], monthly_stats["p90"],
                        alpha=0.2, label="P10–P90 band")
        ax.plot(monthly_stats["month_dt"], monthly_stats["p50"],
                linewidth=2, label="Median")

        for thr, col in dscr_thresh_colors.items():
            ax.axhline(thr, linestyle="--", linewidth=1.2, color=col,
                       label=f"DSCR = {thr:.1f}x")

        ax.set_title(f"{int(cmw)} MWh Contract")
        ax.set_ylabel("Monthly DSCR")
        ax.legend(fontsize=7, loc="upper right")
        ax.grid(alpha=0.3)

    fig.suptitle(
        "Monthly DSCR Across All Grid Cells by Contract Obligation Level (2020–2024)",
        fontsize=13
    )
    plt.tight_layout()
    plt.savefig(fig_dir / "dscr_curves_by_contract.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: dscr_curves_by_contract.png")

    # --- Figure 5: DSCR ECDF by contract level ---
    fig, ax = plt.subplots(figsize=(10, 6))
    for cmw in CONTRACT_LEVELS:
        sub = monthly_all[monthly_all["contract_mwh"] == cmw]
        dscr_vals = (sub["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE).dropna().values
        x, y = ecdf(dscr_vals)
        ax.plot(x, y, linewidth=2, label=f"{int(cmw)} MWh")
    for thr, col in dscr_thresh_colors.items():
        ax.axvline(thr, linestyle="--", linewidth=1.2, color=col,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xlim(-4, 6)
    ax.set_xlabel("Monthly DSCR")
    ax.set_ylabel("Cumulative probability")
    ax.set_title("ECDF of Monthly DSCR by Contract Obligation Level")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / "dscr_ecdf_by_contract.png", dpi=300, bbox_inches="tight")
    plt.close()
    print("  Saved: dscr_ecdf_by_contract.png")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sample_files = sorted(SAMPLE_DIR.glob("*_sample.csv"))
    if not sample_files:
        raise FileNotFoundError(
            f"No *_sample.csv files found in {SAMPLE_DIR}. "
            "Check SAMPLE_DIR in the configuration block."
        )

    print("PPA Financial Risk Simulations")
    print("=" * 55)
    print(f"Sample files  : {len(sample_files)}")
    print(f"Contracts     : {CONTRACT_LEVELS} MWh/hour")
    print(f"PPA price     : ${PPA_PRICE}/MWh")
    print(f"Debt service  : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output        : {OUTPUT_DIR}/")
    print("=" * 55)

    summary_df, monthly_all = part1_revenue_simulation(sample_files)
    dur_df                  = part2_shortfall_duration(sample_files)
    monthly_all, dscr_df    = part3_dscr(monthly_all)

    print("\nGenerating figures...")
    make_figures(summary_df, dur_df, monthly_all)

    print("\nAll outputs saved to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
