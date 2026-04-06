"""
Risk Management Analysis — Battery Storage and Storage + Insurance
===================================================================
Evaluates two layered risk management strategies for a wind energy producer
under a physical PPA, applied to the representative West load zone cell (6_23).
This script supports the risk management section of the thesis that follows
the baseline financial risk characterisation in representative_cell_financial_risk.py.

Two strategies are assessed and compared against the unhedged baseline:

  Strategy 1 — Battery Storage Only
      A co-located battery (5 MW / 4-hour duration, 20 MWh) covers the
      first 0–5 MWh of hourly shortfall for the first 4 consecutive hours
      of each shortfall event. Battery cost is annualised from capex using
      the Capital Recovery Factor at a 4% discount rate over a 10-year
      asset life, and deducted monthly from producer net revenue.

  Strategy 2 — Battery Storage + Insurance
      The battery covers the first 0–5 MWh shortfall layer (same as above).
      An insurance contract then covers the 20–30 MWh shortfall layer when
      spot price ≥ $100/MWh. The premium is set at 1.30× the mean historical
      monthly payout (actuarially fair loading) and is deducted monthly.
      The producer retains the 5–20 MWh middle layer unhedged.

For each strategy, the script computes:
  - Hourly incremental net cash flows with and without the hedging layer
  - Monthly producer net revenue and DSCR vs the $683,474/month benchmark
  - Annual DSCR vs the $8,201,688/year benchmark
  - Comparison of DSCR breach probabilities across baseline and both strategies

Battery assumptions
--------------------
  Power            : 5 MW
  Duration         : 4 hours (20 MWh capacity)
  CapEx            : $330/kWh  (NREL 2025 utility-scale storage benchmark)
  Total battery CapEx: $6,600,000
  Asset life       : 10 years
  Discount rate    : 4%
  Annualised cost  : $813,720/year ($67,810/month)

Source: NREL (2025). Cost Projections for Utility-Scale Battery Storage:
2025 Update. https://docs.nrel.gov/docs/fy25osti/93281.pdf

Insurance layer assumptions (Strategy 2)
------------------------------------------
  Coverage layer   : 20–30 MWh shortfall after battery dispatch
  Price trigger    : spot price ≥ $100/MWh
  Premium loading  : 1.30× mean historical monthly payout
  Payout basis     : insured MWh × max(spot − fixed, 0)

Exclusions
----------
  Uri exclusion  : February 10–20 2021
  Negative prices: hours with spot price < 0 excluded

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  SAMPLE_FILE : 6_23_sample.csv
                columns: timestamp, capacity_factor, price
                Same file used in representative_cell_financial_risk.py.

Output files
------------
  Strategy 1 — Battery only:
    {CELL_ID}_monthly_battery_only.csv
    {CELL_ID}_annual_dscr_battery_only.csv

  Strategy 2 — Battery + Insurance:
    {CELL_ID}_monthly_battery_insurance.csv
    {CELL_ID}_annual_dscr_battery_insurance.csv

  Figures saved to OUTPUT_DIR/figures/:
    {CELL_ID}_battery_monthly_net_comparison.png
    {CELL_ID}_battery_monthly_dscr.png
    {CELL_ID}_battery_annual_dscr.png
    {CELL_ID}_battery_insurance_monthly_net_comparison.png
    {CELL_ID}_battery_insurance_monthly_dscr.png
    {CELL_ID}_battery_insurance_annual_dscr.png

Usage
-----
    python risk_management_storage.py

    Update SAMPLE_FILE and OUTPUT_DIR in the CONFIGURATION block before
    running. Battery and insurance parameters can be adjusted in the
    CONFIGURATION block.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.lines import Line2D
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths and parameters before running
# =============================================================================

SAMPLE_FILE = Path("data/simulated_revs_south_west_gridcells/6_23_sample.csv")
CELL_ID     = "6_23"
OUTPUT_DIR  = Path("output/risk_management")

# Asset and contract parameters
NAMEPLATE_MW   = 100.0
CONTRACT_MW    = 30.0
PPA_PRICE_BASE = 50.0
PPA_PRICE_NEW  = 50.0   # same fixed price for structured case

# Battery parameters (NREL 2025)
BATTERY_POWER_MW      = 5.0
BATTERY_DURATION_HRS  = 4
BATTERY_COST_PER_KWH  = 330.0    # $/kWh
BATTERY_LIFE_YRS      = 10
BATTERY_DISCOUNT_RATE = 0.04

# Insurance layer parameters (Strategy 2)
INS_LO_MWH    = 20.0    # lower bound of insured shortfall layer
INS_HI_MWH    = 30.0    # upper bound of insured shortfall layer
INS_PRICE_THR = 100.0   # spot price trigger ($/MWh)
LOADING       = 1.30    # premium loading factor

# Debt service benchmarks
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

TS_COL    = "timestamp"
PRICE_COL = "price"
CF_COL    = "capacity_factor"


# =============================================================================
# BATTERY COST
# =============================================================================

def annualised_battery_cost() -> tuple[float, float]:
    """
    Annualise battery CapEx via Capital Recovery Factor.

    Returns (annual_cost, monthly_cost) in USD.
    """
    energy_kwh = BATTERY_POWER_MW * 1_000 * BATTERY_DURATION_HRS
    capex      = energy_kwh * BATTERY_COST_PER_KWH
    r, n       = BATTERY_DISCOUNT_RATE, BATTERY_LIFE_YRS
    crf        = (r * (1 + r) ** n) / ((1 + r) ** n - 1)
    annual     = capex * crf
    return annual, annual / 12


BATTERY_ANNUAL_COST, BATTERY_MONTHLY_COST = annualised_battery_cost()
BATTERY_CAPEX = BATTERY_POWER_MW * 1_000 * BATTERY_DURATION_HRS * BATTERY_COST_PER_KWH


# =============================================================================
# DATA LOADING
# =============================================================================

def load_and_clean() -> pd.DataFrame:
    df = pd.read_csv(SAMPLE_FILE)
    df[TS_COL]    = pd.to_datetime(df[TS_COL],   errors="coerce")
    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")
    df[CF_COL]    = pd.to_numeric(df[CF_COL],    errors="coerce")

    df = df.dropna(subset=[TS_COL, PRICE_COL, CF_COL]).copy()
    df = df[~df[TS_COL].dt.normalize().isin(URI_SET)].copy()
    df = df[df[PRICE_COL] >= 0].copy()
    df = df.sort_values(TS_COL).reset_index(drop=True)

    df["gen_mwh"] = df[CF_COL].clip(lower=0) * NAMEPLATE_MW
    df["month"]   = df[TS_COL].dt.to_period("M")
    df["year"]    = df[TS_COL].dt.year
    return df


# =============================================================================
# BASELINE CASH FLOWS
# =============================================================================

def compute_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Physical PPA cash flows — no hedging."""
    out = df.copy()
    out["shortfall_mwh"]          = (CONTRACT_MW - out["gen_mwh"]).clip(lower=0)
    out["excess_mwh"]             = (out["gen_mwh"] - CONTRACT_MW).clip(lower=0)
    out["ppa_revenue_usd"]        = CONTRACT_MW * PPA_PRICE_BASE
    out["excess_sales_usd"]       = out["excess_mwh"] * out[PRICE_COL]
    out["price_minus_fixed"]      = (out[PRICE_COL] - PPA_PRICE_BASE).clip(lower=0)
    out["shortfall_cost_usd"]     = out["shortfall_mwh"] * out["price_minus_fixed"]
    out["net_gain_loss_usd"]      = out["excess_sales_usd"] - out["shortfall_cost_usd"]
    out["total_revenue_usd"]      = out["ppa_revenue_usd"] + out["net_gain_loss_usd"]
    return out


# =============================================================================
# BATTERY DISPATCH
# =============================================================================

def apply_battery_dispatch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dispatch battery to cover 0–BATTERY_POWER_MW of hourly shortfall
    for the first BATTERY_DURATION_HRS consecutive hours of each
    shortfall event.

    Returns df with battery_dispatch_mwh and residual_shortfall_mwh columns.
    """
    out = df.copy()

    shortfall = out["shortfall_mwh"].values.copy()
    battery_dispatch = np.zeros(len(out))
    consecutive_count = 0

    for i in range(len(out)):
        if shortfall[i] > 0:
            consecutive_count += 1
            if consecutive_count <= BATTERY_DURATION_HRS:
                dispatch = min(shortfall[i], BATTERY_POWER_MW)
                battery_dispatch[i] = dispatch
        else:
            consecutive_count = 0

    out["battery_dispatch_mwh"]  = battery_dispatch
    out["residual_shortfall_mwh"] = (shortfall - battery_dispatch).clip(lower=0)
    return out


# =============================================================================
# STRATEGY 1: BATTERY ONLY
# =============================================================================

def strategy1_battery_only(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply battery layer and compute net cash flows, monthly and annual DSCR.
    Battery cost deducted from monthly revenue.
    """
    out = apply_battery_dispatch(compute_baseline(df))

    price_minus_fixed = (out[PRICE_COL] - PPA_PRICE_NEW).clip(lower=0)

    out["battery_avoided_loss_usd"] = (
        out["battery_dispatch_mwh"] * price_minus_fixed
    )
    out["residual_shortfall_cost_usd"] = (
        out["residual_shortfall_mwh"] * price_minus_fixed
    )
    out["net_gain_loss_battery_usd"] = (
        out["excess_sales_usd"]
        + out["battery_avoided_loss_usd"]
        - out["shortfall_cost_usd"]
    )
    out["total_revenue_battery_usd"] = (
        CONTRACT_MW * PPA_PRICE_NEW + out["net_gain_loss_battery_usd"]
    )

    # Monthly aggregation
    monthly = out.groupby("month").agg(
        baseline_revenue_usd        = ("total_revenue_usd",        "sum"),
        battery_revenue_pre_cost_usd= ("total_revenue_battery_usd","sum"),
        battery_avoided_loss_usd    = ("battery_avoided_loss_usd", "sum"),
        battery_dispatch_mwh        = ("battery_dispatch_mwh",     "sum"),
        n_battery_hours             = ("battery_dispatch_mwh",     lambda x: (x > 0).sum()),
    ).reset_index()

    monthly["battery_revenue_net_usd"] = (
        monthly["battery_revenue_pre_cost_usd"] - BATTERY_MONTHLY_COST
    )
    monthly["baseline_dscr"] = monthly["baseline_revenue_usd"] / MONTHLY_DEBT_SERVICE
    monthly["battery_dscr"]  = monthly["battery_revenue_net_usd"] / MONTHLY_DEBT_SERVICE

    # Annual
    annual = out.groupby("year").agg(
        baseline_annual_revenue_usd  = ("total_revenue_usd",         "sum"),
        battery_annual_revenue_usd   = ("total_revenue_battery_usd", "sum"),
    ).reset_index()
    annual["battery_annual_net_usd"]  = annual["battery_annual_revenue_usd"] - BATTERY_ANNUAL_COST
    annual["baseline_annual_dscr"]    = annual["baseline_annual_revenue_usd"] / ANNUAL_DEBT_SERVICE
    annual["battery_annual_dscr"]     = annual["battery_annual_net_usd"]      / ANNUAL_DEBT_SERVICE

    return monthly, annual


# =============================================================================
# STRATEGY 2: BATTERY + INSURANCE
# =============================================================================

def strategy2_battery_insurance(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply battery + insurance layers.

    Layers:
      0 – BATTERY_POWER_MW MWh : battery (first 4 consecutive hrs)
      BATTERY_POWER_MW – INS_LO_MWH MWh: producer retained
      INS_LO_MWH – INS_HI_MWH MWh : insurance (when price ≥ threshold)

    Premium = 1.30 × mean historical monthly payout (loaded actuarially fair).
    """
    out = apply_battery_dispatch(compute_baseline(df))

    price_minus_fixed = (out[PRICE_COL] - PPA_PRICE_NEW).clip(lower=0)

    # Insurance layer
    insured_layer = (
        out["residual_shortfall_mwh"].clip(lower=INS_LO_MWH, upper=INS_HI_MWH)
        - INS_LO_MWH
    ).clip(lower=0)
    price_trigger = (out[PRICE_COL] >= INS_PRICE_THR).astype(float)

    out["insurance_payout_usd"] = insured_layer * price_minus_fixed * price_trigger
    out["battery_avoided_loss_usd"] = (
        out["battery_dispatch_mwh"] * price_minus_fixed
    )
    out["net_gain_loss_structured_usd"] = (
        out["excess_sales_usd"]
        + out["battery_avoided_loss_usd"]
        + out["insurance_payout_usd"]
        - out["shortfall_cost_usd"]
    )
    out["total_revenue_structured_usd"] = (
        CONTRACT_MW * PPA_PRICE_NEW + out["net_gain_loss_structured_usd"]
    )

    # Monthly aggregation — compute premium as 1.30× mean monthly payout
    monthly_payouts = out.groupby("month")["insurance_payout_usd"].sum()
    premium_per_month = float(monthly_payouts.mean()) * LOADING

    monthly = out.groupby("month").agg(
        baseline_revenue_usd           = ("total_revenue_usd",              "sum"),
        structured_revenue_pre_cost_usd= ("total_revenue_structured_usd",   "sum"),
        insurance_payout_usd           = ("insurance_payout_usd",           "sum"),
        battery_avoided_loss_usd       = ("battery_avoided_loss_usd",       "sum"),
        battery_dispatch_mwh           = ("battery_dispatch_mwh",           "sum"),
    ).reset_index()

    monthly["total_cost_usd"]              = BATTERY_MONTHLY_COST + premium_per_month
    monthly["structured_revenue_net_usd"]  = (
        monthly["structured_revenue_pre_cost_usd"] - monthly["total_cost_usd"]
    )
    monthly["baseline_dscr"]    = monthly["baseline_revenue_usd"]           / MONTHLY_DEBT_SERVICE
    monthly["structured_dscr"]  = monthly["structured_revenue_net_usd"]     / MONTHLY_DEBT_SERVICE

    # Annual
    annual = out.groupby("year").agg(
        baseline_annual_revenue_usd    = ("total_revenue_usd",             "sum"),
        structured_annual_revenue_usd  = ("total_revenue_structured_usd",  "sum"),
    ).reset_index()
    annual_premium = premium_per_month * 12
    annual["structured_annual_net_usd"] = (
        annual["structured_annual_revenue_usd"] - BATTERY_ANNUAL_COST - annual_premium
    )
    annual["baseline_annual_dscr"]   = annual["baseline_annual_revenue_usd"]  / ANNUAL_DEBT_SERVICE
    annual["structured_annual_dscr"] = annual["structured_annual_net_usd"]    / ANNUAL_DEBT_SERVICE

    return monthly, annual, premium_per_month


# =============================================================================
# HELPERS
# =============================================================================

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")


def print_dscr_comparison(label: str, baseline: pd.Series, structured: pd.Series):
    print(f"\n--- {label} ---")
    for thr in DSCR_THRESHOLDS:
        b = (baseline < thr).mean() * 100
        s = (structured < thr).mean() * 100
        print(f"  P(DSCR < {thr:.1f}): baseline={b:.1f}%  structured={s:.1f}%")
    print(f"  Mean DSCR: baseline={baseline.mean():.3f}  structured={structured.mean():.3f}")
    print(f"  Min  DSCR: baseline={baseline.min():.3f}  structured={structured.min():.3f}")


def month_dt(monthly: pd.DataFrame) -> pd.Series:
    return monthly["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp()
        if not isinstance(p, pd.Timestamp) else p
    )


# =============================================================================
# FIGURES
# =============================================================================

def make_battery_figures(monthly: pd.DataFrame, annual: pd.DataFrame):
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    dt = month_dt(monthly)

    # Monthly net comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dt, monthly["baseline_revenue_usd"],     linewidth=1.5, label="Baseline")
    ax.plot(dt, monthly["battery_revenue_net_usd"],  linewidth=1.5, label="Battery (net of cost)")
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs Battery — Cell {CELL_ID}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_monthly_net_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Monthly DSCR
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dt, monthly["baseline_dscr"], linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly["battery_dscr"],  linewidth=1.5, label="Battery DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs Battery — Cell {CELL_ID}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_monthly_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Annual DSCR
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(annual))
    w = 0.35
    ax.bar(x - w/2, annual["baseline_annual_dscr"], w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual["battery_annual_dscr"],  w, label="Battery", alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs Battery — Cell {CELL_ID}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_annual_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    print(f"  Battery figures saved to {fig_dir.name}/")


def make_battery_insurance_figures(monthly: pd.DataFrame, annual: pd.DataFrame):
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    dt = month_dt(monthly)

    # Monthly net comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dt, monthly["baseline_revenue_usd"],          linewidth=1.5, label="Baseline")
    ax.plot(dt, monthly["structured_revenue_net_usd"],    linewidth=1.5, label="Battery + Insurance (net)")
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs Battery + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_insurance_monthly_net_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Monthly DSCR
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dt, monthly["baseline_dscr"],   linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly["structured_dscr"], linewidth=1.5, label="Battery + Insurance DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs Battery + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_insurance_monthly_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Annual DSCR
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(annual))
    w = 0.35
    ax.bar(x - w/2, annual["baseline_annual_dscr"],   w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual["structured_annual_dscr"], w, label="Battery + Insurance", alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs Battery + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_battery_insurance_annual_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    print(f"  Battery + Insurance figures saved to {fig_dir.name}/")


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

    print("Risk Management Analysis — Battery Storage and Storage + Insurance")
    print("=" * 65)
    print(f"Cell            : {CELL_ID} (West load zone)")
    print(f"Contract        : {CONTRACT_MW} MWh/hour at ${PPA_PRICE_BASE}/MWh")
    print(f"Battery         : {BATTERY_POWER_MW} MW / {BATTERY_DURATION_HRS}h "
          f"@ ${BATTERY_COST_PER_KWH}/kWh")
    print(f"Battery CapEx   : ${BATTERY_CAPEX:,.0f}")
    print(f"Battery ann.    : ${BATTERY_ANNUAL_COST:,.0f}/yr  "
          f"(${BATTERY_MONTHLY_COST:,.0f}/month)")
    print(f"Insurance layer : {INS_LO_MWH}–{INS_HI_MWH} MWh when "
          f"price ≥ ${INS_PRICE_THR}/MWh  (loading {LOADING:.2f}×)")
    print(f"Output          : {OUTPUT_DIR}/")
    print("=" * 65)

    df = load_and_clean()
    baseline_rev = compute_baseline(df)

    # -------------------------------------------------------------------------
    # Strategy 1: Battery only
    # -------------------------------------------------------------------------
    print("\n[Strategy 1] Battery only...")
    monthly_bat, annual_bat = strategy1_battery_only(df)

    print_dscr_comparison(
        "Monthly DSCR — Battery vs Baseline",
        monthly_bat["baseline_dscr"],
        monthly_bat["battery_dscr"],
    )
    print_dscr_comparison(
        "Annual DSCR — Battery vs Baseline",
        annual_bat["baseline_annual_dscr"],
        annual_bat["battery_annual_dscr"],
    )

    monthly_bat.to_csv(OUTPUT_DIR / f"{CELL_ID}_monthly_battery_only.csv", index=False)
    annual_bat.to_csv(OUTPUT_DIR  / f"{CELL_ID}_annual_dscr_battery_only.csv", index=False)
    print(f"  Saved: {CELL_ID}_monthly_battery_only.csv")
    print(f"  Saved: {CELL_ID}_annual_dscr_battery_only.csv")

    make_battery_figures(monthly_bat, annual_bat)

    # -------------------------------------------------------------------------
    # Strategy 2: Battery + Insurance
    # -------------------------------------------------------------------------
    print("\n[Strategy 2] Battery + Insurance...")
    monthly_ins, annual_ins, premium = strategy2_battery_insurance(df)

    print(f"\n  Insurance parameters:")
    print(f"    Layer            : {INS_LO_MWH}–{INS_HI_MWH} MWh shortfall")
    print(f"    Price trigger    : ≥ ${INS_PRICE_THR}/MWh")
    print(f"    Monthly premium  : ${premium:,.0f}  (loading {LOADING:.2f}×)")
    print(f"    Annual premium   : ${premium*12:,.0f}")

    print_dscr_comparison(
        "Monthly DSCR — Battery + Insurance vs Baseline",
        monthly_ins["baseline_dscr"],
        monthly_ins["structured_dscr"],
    )
    print_dscr_comparison(
        "Annual DSCR — Battery + Insurance vs Baseline",
        annual_ins["baseline_annual_dscr"],
        annual_ins["structured_annual_dscr"],
    )

    monthly_ins.to_csv(OUTPUT_DIR / f"{CELL_ID}_monthly_battery_insurance.csv", index=False)
    annual_ins.to_csv(OUTPUT_DIR  / f"{CELL_ID}_annual_dscr_battery_insurance.csv", index=False)
    print(f"  Saved: {CELL_ID}_monthly_battery_insurance.csv")
    print(f"  Saved: {CELL_ID}_annual_dscr_battery_insurance.csv")

    make_battery_insurance_figures(monthly_ins, annual_ins)

    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
