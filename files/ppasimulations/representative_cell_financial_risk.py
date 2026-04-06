"""
Representative Cell Financial Risk and DSCR — West Load Zone (Cell 6_23)
=========================================================================
Computes hourly cash flows, monthly DSCR, and annual DSCR for a single
representative grid cell in the West load zone (cell 6_23), under a
30 MWh physical PPA at $50/MWh fixed price.

This script supports Section 3.4 of the thesis by providing a single-cell
illustration of the financial risk mechanics described in Tables 2 and 3,
and the DSCR analysis described around Table 4. The representative cell
was selected from the West load zone sample of 35 cells.

The same financial mechanics applied here are used at scale across all
48 cells in ppa_financial_simulations.py.

Outputs
-------
  {CELL_ID}_hourly_cash_flows.csv
      Hourly cash flow breakdown: generation, shortfall, excess,
      PPA revenue, excess sales, shortfall cost, net gain/loss,
      total revenue.

  {CELL_ID}_monthly_dscr.csv
      Monthly DSCR = monthly revenue / $683,474 debt service.
      Includes breach indicators for 1.0, 1.2, and 1.3 thresholds.

  {CELL_ID}_annual_dscr.csv
      Annual DSCR = annual revenue / $8,201,688 debt service.

  Figures saved to OUTPUT_DIR/figures/:
      {CELL_ID}_hourly_net_gains_losses.png
      {CELL_ID}_monthly_dscr_timeseries.png
      {CELL_ID}_monthly_dscr_distribution.png
      {CELL_ID}_annual_dscr_timeseries.png

Financial mechanics (Table 3)
------------------------------
  Condition                             Treatment
  ─────────────────────────────────────────────────────────────────────
  Actual generation < contract (30 MWh) Producer buys shortfall at spot
  Spot > fixed price ($50/MWh)          Loss = (spot − $50) × shortfall
  Spot ≤ fixed price                    No loss on shortfall purchases
  Actual generation > contract          Surplus sold at spot price
  No-arbitrage condition                No profit from buying below fixed

Exclusions
----------
  Uri exclusion : February 10–20 2021
  Negative prices: hours with spot price < 0 excluded

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  SAMPLE_FILE : {CELL_ID}_sample.csv
                columns: timestamp, capacity_factor, price
                One of the 48 pre-built grid-cell simulation files.
                See ppa_financial_simulations.py for construction context.

Usage
-----
    python representative_cell_financial_risk.py

    Update SAMPLE_FILE and OUTPUT_DIR in the CONFIGURATION block below.
    To analyse a different representative cell, change SAMPLE_FILE and
    CELL_ID accordingly.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

# Representative West load zone cell
SAMPLE_FILE = Path("data/simulated_revs_south_west_gridcells/6_23_sample.csv")
CELL_ID     = "6_23"

# Output directory
OUTPUT_DIR = Path("output/representative_cell")

# Contract and asset parameters
NAMEPLATE_MW  = 100.0
CONTRACT_MW   = 30.0
PPA_PRICE     = 50.0

# Debt service benchmarks (from debt_financing_assumptions.py)
MONTHLY_DEBT_SERVICE = 683_474.0
ANNUAL_DEBT_SERVICE  = 8_201_688.0

# DSCR breach thresholds
DSCR_THRESHOLDS = [1.0, 1.2, 1.3]

# Uri exclusion
URI_DATES = pd.to_datetime([
    "2021-02-10", "2021-02-11", "2021-02-12", "2021-02-13", "2021-02-14",
    "2021-02-15", "2021-02-16", "2021-02-17", "2021-02-18",
    "2021-02-19", "2021-02-20",
]).normalize()
URI_SET = set(URI_DATES)

# Column names
TS_COL    = "timestamp"
PRICE_COL = "price"
CF_COL    = "capacity_factor"


# =============================================================================
# HELPERS
# =============================================================================

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")


def load_and_clean(file_path: Path) -> pd.DataFrame:
    """Load sample file, apply Uri and negative-price exclusions."""
    df = pd.read_csv(file_path)
    df[TS_COL]    = pd.to_datetime(df[TS_COL],    errors="coerce")
    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL],  errors="coerce")
    df[CF_COL]    = pd.to_numeric(df[CF_COL],      errors="coerce")

    df = df.dropna(subset=[TS_COL, PRICE_COL, CF_COL]).copy()
    df = df[~df[TS_COL].dt.normalize().isin(URI_SET)].copy()
    df = df[df[PRICE_COL] >= 0].copy()
    df = df.sort_values(TS_COL).reset_index(drop=True)

    df["gen_mwh"] = df[CF_COL].clip(lower=0) * NAMEPLATE_MW
    df["month"]   = df[TS_COL].dt.to_period("M")
    df["year"]    = df[TS_COL].dt.year

    return df


def compute_hourly_cashflows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply physical PPA financial mechanics (Table 3).

    Key columns added:
      shortfall_mwh              : max(0, contract − generation)
      excess_mwh                 : max(0, generation − contract)
      ppa_revenue_usd            : contract × fixed price
      excess_sales_usd           : excess × spot price
      shortfall_incremental_cost : shortfall × max(0, spot − fixed)
      producer_net_gain_loss_usd : excess sales − shortfall cost
      producer_total_revenue_usd : PPA revenue + net gain/loss
    """
    out = df.copy()

    out["shortfall_mwh"] = (CONTRACT_MW - out["gen_mwh"]).clip(lower=0)
    out["excess_mwh"]    = (out["gen_mwh"] - CONTRACT_MW).clip(lower=0)

    out["ppa_revenue_usd"]             = CONTRACT_MW * PPA_PRICE
    out["excess_sales_usd"]            = out["excess_mwh"] * out[PRICE_COL]
    out["price_minus_fixed"]           = (out[PRICE_COL] - PPA_PRICE).clip(lower=0)
    out["shortfall_incremental_cost"]  = out["shortfall_mwh"] * out["price_minus_fixed"]

    out["producer_net_gain_loss_usd"]  = (
        out["excess_sales_usd"] - out["shortfall_incremental_cost"]
    )
    out["producer_total_revenue_usd"]  = (
        out["ppa_revenue_usd"] + out["producer_net_gain_loss_usd"]
    )

    out["is_shortfall"] = (out["shortfall_mwh"] > 0).astype(int)
    out["is_net_loss"]  = (out["producer_net_gain_loss_usd"] < 0).astype(int)

    return out


def dscr_summary(dscr_vals: np.ndarray, label: str):
    """Print DSCR statistics to console."""
    x = dscr_vals[np.isfinite(dscr_vals)]
    print(f"\n--- {label} ---")
    print(f"Mean DSCR : {np.mean(x):.3f}")
    print(f"Min DSCR  : {np.min(x):.3f}")
    print(f"P10 DSCR  : {np.percentile(x, 10):.3f}")
    print(f"P50 DSCR  : {np.percentile(x, 50):.3f}")
    print(f"Std DSCR  : {np.std(x, ddof=1):.3f}")
    for thr in DSCR_THRESHOLDS:
        print(f"P(DSCR < {thr:.1f}): {np.mean(x < thr)*100:.1f}%")


# =============================================================================
# PART 1: Hourly cash flow simulation
# =============================================================================

def part1_hourly_cashflows(df: pd.DataFrame) -> pd.DataFrame:
    """Compute and save hourly cash flows, print extremes."""
    print("\n[Part 1] Hourly cash flow simulation...")

    rev = compute_hourly_cashflows(df)

    # Print extreme hours
    worst_idx = rev["producer_net_gain_loss_usd"].idxmin()
    best_idx  = rev["excess_sales_usd"].idxmax()

    print(f"\n  Best hour (max excess sales):")
    w = rev.loc[best_idx]
    print(f"    {w[TS_COL]}  price=${w[PRICE_COL]:,.2f}  "
          f"gen={w['gen_mwh']:.2f} MWh  excess={w['excess_mwh']:.2f} MWh  "
          f"sales=${w['excess_sales_usd']:,.2f}")

    print(f"\n  Worst hour (max net loss):")
    w = rev.loc[worst_idx]
    print(f"    {w[TS_COL]}  price=${w[PRICE_COL]:,.2f}  "
          f"gen={w['gen_mwh']:.2f} MWh  shortfall={w['shortfall_mwh']:.2f} MWh  "
          f"loss=${w['producer_net_gain_loss_usd']:,.2f}")

    # Save
    out_cols = [
        TS_COL, CF_COL, PRICE_COL,
        "gen_mwh", "shortfall_mwh", "excess_mwh",
        "ppa_revenue_usd", "excess_sales_usd",
        "shortfall_incremental_cost", "producer_net_gain_loss_usd",
        "producer_total_revenue_usd", "is_shortfall", "is_net_loss",
    ]
    out_path = OUTPUT_DIR / f"{CELL_ID}_hourly_cash_flows.csv"
    rev[out_cols].to_csv(out_path, index=False)
    print(f"\n  Saved: {out_path.name}")

    return rev


# =============================================================================
# PART 2: Monthly DSCR
# =============================================================================

def part2_monthly_dscr(rev: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to monthly revenue, compute DSCR, save results."""
    print("\n[Part 2] Monthly DSCR...")

    monthly = rev.groupby("month").agg(
        monthly_revenue_usd    = ("producer_total_revenue_usd", "sum"),
        monthly_net_gain_usd   = ("producer_net_gain_loss_usd", "sum"),
        n_hours                = ("producer_total_revenue_usd", "count"),
        n_shortfall_hours      = ("is_shortfall",               "sum"),
        n_net_loss_hours       = ("is_net_loss",                "sum"),
    ).reset_index()

    monthly["dscr"] = monthly["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE

    for thr in DSCR_THRESHOLDS:
        col = f"dscr_below_{str(thr).replace('.', '_')}"
        monthly[col] = (monthly["dscr"] < thr).astype(int)

    dscr_summary(monthly["dscr"].values, "Monthly DSCR")

    out_path = OUTPUT_DIR / f"{CELL_ID}_monthly_dscr.csv"
    monthly.to_csv(out_path, index=False)
    print(f"\n  Saved: {out_path.name}")

    return monthly


# =============================================================================
# PART 3: Annual DSCR
# =============================================================================

def part3_annual_dscr(rev: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to annual revenue, compute annual DSCR."""
    print("\n[Part 3] Annual DSCR...")

    annual = rev.groupby("year").agg(
        annual_revenue_usd = ("producer_total_revenue_usd", "sum"),
    ).reset_index()

    annual["annual_dscr"] = annual["annual_revenue_usd"] / ANNUAL_DEBT_SERVICE

    dscr_summary(annual["annual_dscr"].values, "Annual DSCR")

    for thr in DSCR_THRESHOLDS:
        annual[f"dscr_below_{str(thr).replace('.', '_')}"] = (
            annual["annual_dscr"] < thr
        ).astype(int)

    out_path = OUTPUT_DIR / f"{CELL_ID}_annual_dscr.csv"
    annual.to_csv(out_path, index=False)
    print(f"\n  Saved: {out_path.name}")

    return annual


# =============================================================================
# FIGURES
# =============================================================================

def make_figures(rev: pd.DataFrame, monthly: pd.DataFrame,
                 annual: pd.DataFrame):
    """Produce all figures for the representative cell analysis."""
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    # --- Figure 1: Hourly net gains/losses distribution ---
    x = rev["producer_net_gain_loss_usd"].values
    xmin = np.percentile(x[np.isfinite(x)], 0.5)
    xmax = np.percentile(x[np.isfinite(x)], 99.5)

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.hist(x, bins=300, density=True, alpha=0.75)
    for pct, col, ls, label in [
        (50, "black",  "--", "Median"),
        (5,  "orange", "-.", "95% VaR"),
        (1,  "red",    ":",  "99% VaR"),
    ]:
        val = np.percentile(x[np.isfinite(x)], pct)
        ax.axvline(val, color=col, linestyle=ls, linewidth=2,
                   label=f"{label}: ${val:,.0f}")
    ax.set_xlim(xmin, xmax)
    ax.set_title(
        f"Hourly Producer Incremental Gains & Losses — Cell {CELL_ID}\n"
        f"(30 MWh PPA at $50/MWh)"
    )
    ax.set_xlabel("Hourly Net Gain / Loss ($)")
    ax.set_ylabel("Probability Density")
    ax.xaxis.set_major_formatter(currency_fmt)
    ax.legend()
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_hourly_net_gains_losses.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_hourly_net_gains_losses.png")

    # --- Figure 2: Monthly DSCR time series ---
    monthly_dt = monthly["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp()
        if not isinstance(p, pd.Timestamp) else p
    )

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(monthly_dt, monthly["dscr"], linewidth=1.5, label="Monthly DSCR")
    ax.axhline(0, color="black", linewidth=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.2,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(
        f"Monthly DSCR — Cell {CELL_ID} (30 MWh PPA)\n"
        f"Monthly debt service: ${MONTHLY_DEBT_SERVICE:,.0f}"
    )
    ax.set_ylabel("Monthly DSCR")
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_monthly_dscr_timeseries.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_monthly_dscr_timeseries.png")

    # --- Figure 3: Monthly DSCR distribution (ECDF) ---
    dscr_vals = np.sort(monthly["dscr"].dropna().values)
    y = np.arange(1, len(dscr_vals) + 1) / len(dscr_vals)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(dscr_vals, y, linewidth=2)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axvline(thr, linestyle="--", color=col, linewidth=1.2,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xlabel("Monthly DSCR")
    ax.set_ylabel("Cumulative probability")
    ax.set_title(f"Monthly DSCR ECDF — Cell {CELL_ID}")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_monthly_dscr_distribution.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_monthly_dscr_distribution.png")

    # --- Figure 4: Annual DSCR bar chart ---
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(annual["year"].astype(str), annual["annual_dscr"], alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.2,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xlabel("Year")
    ax.set_ylabel("Annual DSCR")
    ax.set_title(
        f"Annual DSCR — Cell {CELL_ID} (30 MWh PPA)\n"
        f"Annual debt service: ${ANNUAL_DEBT_SERVICE:,.0f}"
    )
    ax.legend()
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_annual_dscr_timeseries.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_annual_dscr_timeseries.png")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not SAMPLE_FILE.exists():
        raise FileNotFoundError(
            f"Sample file not found: {SAMPLE_FILE}\n"
            "Check SAMPLE_FILE in the configuration block."
        )

    print("Representative Cell Financial Risk and DSCR")
    print("=" * 55)
    print(f"Cell          : {CELL_ID} (West load zone)")
    print(f"Contract      : {CONTRACT_MW} MWh/hour at ${PPA_PRICE}/MWh")
    print(f"Nameplate     : {NAMEPLATE_MW} MW")
    print(f"Debt service  : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output        : {OUTPUT_DIR}/")
    print("=" * 55)

    df  = load_and_clean(SAMPLE_FILE)
    rev = part1_hourly_cashflows(df)
    monthly = part2_monthly_dscr(rev)
    annual  = part3_annual_dscr(rev)

    print("\nGenerating figures...")
    make_figures(rev, monthly, annual)

    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
