"""
Risk Management Analysis — Reserve Fund + Insurance (Cell 6_23, West)
======================================================================
Evaluates a layered reserve fund and insurance strategy as a risk
management approach for a wind energy producer under a physical PPA,
applied to the representative West load zone cell (6_23).

This strategy combines two complementary protection layers:

  Reserve fund (lower layer — 15–25 MWh shortfall)
      The producer contributes 15% of hourly excess-energy spot sales
      to a self-funded reserve. When a qualifying shortfall occurs
      (shortfall ≥ 15 MWh and spot price ≥ $100/MWh), the reserve
      pays out up to 50% of its current balance to cover the
      incremental shortfall cost in that hour. The reserve grows
      during good wind periods and depletes during sustained droughts,
      providing contingent protection at no fixed cost beyond the
      opportunity cost of the withheld excess revenues.

  Insurance (upper layer — 25–30 MWh shortfall)
      A layer-based insurance contract covers the 25–30 MWh shortfall
      slice when spot price is between $75/MWh and $5,000/MWh.
      Payout = insured layer MWh × max(spot − fixed, 0).
      Premium = 1.30× mean monthly payout, deducted each month.

Layer structure
----------------
  0–15 MWh shortfall   : producer retained (unhedged)
  15–25 MWh shortfall  : reserve fund (self-funded, contingent payout)
  25–30 MWh shortfall  : insurance contract (premium-based)

The reserve fund is simulated hourly as a running balance — it
accumulates when excess generation is sold at spot and depletes when
qualifying shortfall hours trigger payouts. The insurance premium is
fixed and deducted regardless of trigger activity.

Three analyses are produced:
  1. Hourly and monthly incremental net cash flows — comparing
     baseline unhedged vs reserve + insurance structured case.
  2. Monthly DSCR — vs $683,474/month debt service benchmark.
  3. Annual DSCR — vs $8,201,688/year benchmark.

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  SAMPLE_FILE : 6_23_sample.csv
                columns: timestamp, capacity_factor, price
                Same file used in all prior risk management scripts.

Output files
------------
  {CELL_ID}_monthly_reserve_insurance.csv
  {CELL_ID}_annual_dscr_reserve_insurance.csv

  Figures saved to OUTPUT_DIR/figures/:
    {CELL_ID}_reserve_insurance_monthly_net_comparison.png
    {CELL_ID}_reserve_insurance_monthly_dscr.png
    {CELL_ID}_reserve_insurance_annual_dscr.png
    {CELL_ID}_reserve_balance_timeseries.png

Usage
-----
    python risk_management_reserve_insurance.py

    Update SAMPLE_FILE and OUTPUT_DIR in the CONFIGURATION block before
    running. Reserve and insurance parameters are set in the
    CONFIGURATION block below.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths and parameters before running
# =============================================================================

SAMPLE_FILE = Path("data/simulated_revs_south_west_gridcells/6_23_sample.csv")
CELL_ID     = "6_23"
OUTPUT_DIR  = Path("output/risk_management")

# Contract and asset parameters
NAMEPLATE_MW   = 100.0
CONTRACT_MW    = 30.0
PPA_PRICE_BASE = 50.0
PPA_PRICE_NEW  = 50.0

# Reserve fund parameters
RESERVE_CONTRIB_SHARE = 0.15   # fraction of excess-energy spot sales contributed
RESERVE_LO_MWH        = 15.0  # reserve covers shortfall above this MWh
RESERVE_HI_MWH        = 25.0  # reserve covers shortfall up to this MWh
RESERVE_PRICE_THR     = 100.0  # spot price trigger for reserve payout
RESERVE_PAYOUT_SHARE  = 0.50  # maximum fraction of current balance per triggered hour
INITIAL_RESERVE_BAL   = 0.0   # starting reserve balance

# Insurance layer parameters
INS_LO_MWH      = 25.0     # insurance attaches above this MWh shortfall
INS_HI_MWH      = 30.0     # insurance exhaustion point
INS_PRICE_FLOOR = 75.0     # minimum spot price for insurance trigger
INS_PRICE_CAP   = 5_000.0  # maximum spot price for insurance trigger
LOADING         = 1.30     # premium loading factor

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

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")


# =============================================================================
# DATA LOADING
# =============================================================================

def load_and_clean() -> pd.DataFrame:
    df = pd.read_csv(SAMPLE_FILE)
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


# =============================================================================
# CORE FINANCIAL MECHANICS
# =============================================================================

def layer_mwh(shortfall: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Amount of shortfall within the [lo, hi] MWh layer."""
    return np.clip(shortfall - lo, 0.0, hi - lo)


def simulate_reserve_and_insurance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate hourly reserve fund + insurance cash flows.

    Reserve logic (hourly simulation):
      - Contribution: RESERVE_CONTRIB_SHARE × excess_sales each hour
      - Payout condition: shortfall in [RESERVE_LO, RESERVE_HI] MWh
        AND spot price >= RESERVE_PRICE_THR
      - Payout amount: min(reserve_layer_cost, RESERVE_PAYOUT_SHARE × balance)
      - Balance updated each hour after contribution and payout

    Insurance logic:
      - Covers shortfall in [INS_LO, INS_HI] MWh
      - Triggers when INS_PRICE_FLOOR <= spot <= INS_PRICE_CAP
      - Payout = insured_mwh × max(spot − fixed, 0)
      - Premium = LOADING × mean monthly payout, deducted each month

    Returns DataFrame with all hourly and structured revenue columns.
    """
    out = df.copy()

    shortfall = (CONTRACT_MW - out["gen_mwh"]).clip(lower=0).values
    excess    = (out["gen_mwh"] - CONTRACT_MW).clip(lower=0).values
    price     = out[PRICE_COL].values
    pmf       = np.maximum(price - PPA_PRICE_NEW, 0.0)  # max(spot − fixed, 0)

    # Baseline cash flows
    ppa_rev    = CONTRACT_MW * PPA_PRICE_NEW
    excess_sal = excess * price
    sf_cost    = shortfall * pmf
    baseline_net = excess_sal - sf_cost

    # ---- Reserve fund simulation (hourly loop) ----
    n = len(out)
    reserve_contrib  = np.zeros(n)
    reserve_payout   = np.zeros(n)
    reserve_balance  = np.zeros(n)
    reserve_layer    = layer_mwh(shortfall, RESERVE_LO_MWH, RESERVE_HI_MWH)
    reserve_eligible = (
        (shortfall >= RESERVE_LO_MWH) & (price >= RESERVE_PRICE_THR)
    ).astype(float)

    balance = INITIAL_RESERVE_BAL

    for i in range(n):
        contrib = RESERVE_CONTRIB_SHARE * excess_sal[i]
        balance += contrib
        reserve_contrib[i] = contrib

        if reserve_eligible[i] > 0:
            layer_cost = reserve_layer[i] * pmf[i]
            payout = min(layer_cost, RESERVE_PAYOUT_SHARE * balance)
            balance -= payout
            reserve_payout[i] = payout

        reserve_balance[i] = balance

    # ---- Insurance layer ----
    ins_layer   = layer_mwh(shortfall, INS_LO_MWH, INS_HI_MWH)
    ins_trigger = (
        (price >= INS_PRICE_FLOOR) & (price <= INS_PRICE_CAP)
    ).astype(float)
    ins_payout  = ins_layer * pmf * ins_trigger

    # Assign to output
    out["shortfall_mwh"]         = shortfall
    out["excess_mwh"]            = excess
    out["excess_sales_usd"]      = excess_sal
    out["shortfall_cost_usd"]    = sf_cost
    out["baseline_net_usd"]      = baseline_net
    out["baseline_total_rev_usd"]= ppa_rev + baseline_net

    out["reserve_contrib_usd"]   = reserve_contrib
    out["reserve_payout_usd"]    = reserve_payout
    out["reserve_balance_usd"]   = reserve_balance
    out["insurance_payout_usd"]  = ins_payout

    # Premium computed from monthly mean payout
    monthly_ins = out.groupby("month")["insurance_payout_usd"].sum()
    monthly_premium = float(monthly_ins.mean()) * LOADING
    annual_premium  = monthly_premium * 12

    out["monthly_premium_usd"]   = monthly_premium
    out["structured_net_usd"]    = (
        baseline_net
        - reserve_contrib
        + reserve_payout
        + ins_payout
        - monthly_premium   # deducted evenly (will aggregate to monthly)
    )
    out["structured_total_rev_usd"] = ppa_rev + out["structured_net_usd"]

    out.attrs["monthly_premium"] = monthly_premium
    out.attrs["annual_premium"]  = annual_premium

    return out


# =============================================================================
# HELPERS
# =============================================================================

def print_dscr_comparison(label: str, base: pd.Series, structured: pd.Series):
    print(f"\n  {label}")
    for thr in DSCR_THRESHOLDS:
        b = (base < thr).mean() * 100
        s = (structured < thr).mean() * 100
        print(f"    P(DSCR<{thr:.1f}): baseline={b:.1f}%  structured={s:.1f}%")
    print(f"    Mean : baseline={base.mean():.3f}  structured={structured.mean():.3f}")
    print(f"    Min  : baseline={base.min():.3f}  structured={structured.min():.3f}")


def month_dt(monthly: pd.DataFrame) -> pd.Series:
    return monthly["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp()
        if not isinstance(p, pd.Timestamp) else p
    )


# =============================================================================
# ANALYSIS 1: Incremental net cash flows
# =============================================================================

def analysis1_incremental_net(out: pd.DataFrame, monthly_premium: float) -> pd.DataFrame:
    print("\n[Analysis 1] Incremental net cash flows...")

    monthly = out.groupby("month").agg(
        baseline_rev_usd       = ("baseline_total_rev_usd",   "sum"),
        baseline_net_usd       = ("baseline_net_usd",         "sum"),
        structured_rev_usd     = ("structured_total_rev_usd", "sum"),
        reserve_contrib_usd    = ("reserve_contrib_usd",      "sum"),
        reserve_payout_usd     = ("reserve_payout_usd",       "sum"),
        insurance_payout_usd   = ("insurance_payout_usd",     "sum"),
        reserve_balance_eom_usd= ("reserve_balance_usd",      "last"),
    ).reset_index()

    # Deduct monthly premium from structured revenue (net basis)
    monthly["structured_net_rev_usd"] = monthly["structured_rev_usd"] - monthly_premium

    print(f"\n  Baseline  VaR95: ${np.percentile(monthly['baseline_rev_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['baseline_rev_usd'], 1):,.0f}")
    print(f"  Structured VaR95: ${np.percentile(monthly['structured_net_rev_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['structured_net_rev_usd'], 1):,.0f}")
    print(f"\n  Reserve — total contributed: ${monthly['reserve_contrib_usd'].sum():,.0f}  "
          f"total paid out: ${monthly['reserve_payout_usd'].sum():,.0f}  "
          f"final balance: ${monthly['reserve_balance_eom_usd'].iloc[-1]:,.0f}")
    print(f"  Insurance — total payout: ${monthly['insurance_payout_usd'].sum():,.0f}  "
          f"total premium: ${monthly_premium * len(monthly):,.0f}")

    return monthly


# =============================================================================
# ANALYSIS 2: Monthly DSCR
# =============================================================================

def analysis2_monthly_dscr(monthly: pd.DataFrame, monthly_premium: float) -> pd.DataFrame:
    print("\n[Analysis 2] Monthly DSCR...")

    monthly = monthly.copy()
    monthly["baseline_dscr"]    = monthly["baseline_rev_usd"]        / MONTHLY_DEBT_SERVICE
    monthly["structured_dscr"]  = monthly["structured_net_rev_usd"]  / MONTHLY_DEBT_SERVICE

    print_dscr_comparison("Monthly DSCR",
                          monthly["baseline_dscr"],
                          monthly["structured_dscr"])
    return monthly


# =============================================================================
# ANALYSIS 3: Annual DSCR
# =============================================================================

def analysis3_annual_dscr(out: pd.DataFrame, annual_premium: float) -> pd.DataFrame:
    print("\n[Analysis 3] Annual DSCR...")

    annual = out.groupby("year").agg(
        baseline_annual_rev_usd    = ("baseline_total_rev_usd",   "sum"),
        structured_annual_rev_usd  = ("structured_total_rev_usd", "sum"),
    ).reset_index()

    annual["structured_net_annual_usd"] = (
        annual["structured_annual_rev_usd"] - annual_premium
    )
    annual["baseline_dscr"]   = annual["baseline_annual_rev_usd"]    / ANNUAL_DEBT_SERVICE
    annual["structured_dscr"] = annual["structured_net_annual_usd"]  / ANNUAL_DEBT_SERVICE

    print_dscr_comparison("Annual DSCR",
                          annual["baseline_dscr"],
                          annual["structured_dscr"])
    for _, row in annual.iterrows():
        print(f"    {int(row['year'])}: baseline={row['baseline_dscr']:.3f}  "
              f"structured={row['structured_dscr']:.3f}")

    return annual


# =============================================================================
# FIGURES
# =============================================================================

def make_figures(monthly: pd.DataFrame, annual: pd.DataFrame, out: pd.DataFrame):
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    dt = month_dt(monthly)

    # Figure 1: Monthly revenue comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dt, monthly["baseline_rev_usd"],       linewidth=1.5, label="Baseline")
    ax.plot(dt, monthly["structured_net_rev_usd"], linewidth=1.5, label="Reserve + Insurance (net)")
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs Reserve + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_reserve_insurance_monthly_net_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_reserve_insurance_monthly_net_comparison.png")

    # Figure 2: Monthly DSCR
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dt, monthly["baseline_dscr"],   linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly["structured_dscr"], linewidth=1.5, label="Reserve + Insurance DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs Reserve + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_reserve_insurance_monthly_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_reserve_insurance_monthly_dscr.png")

    # Figure 3: Annual DSCR bar chart
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(annual))
    w = 0.35
    ax.bar(x - w/2, annual["baseline_dscr"],   w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual["structured_dscr"], w, label="Reserve + Insurance", alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs Reserve + Insurance — Cell {CELL_ID}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_reserve_insurance_annual_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_reserve_insurance_annual_dscr.png")

    # Figure 4: Reserve balance over time
    reserve_monthly = out.groupby("month").agg(
        reserve_balance_eom = ("reserve_balance_usd", "last")
    ).reset_index()
    dt_res = month_dt(reserve_monthly)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(dt_res, reserve_monthly["reserve_balance_eom"],
                    alpha=0.5, label="Reserve balance (EOM)")
    ax.plot(dt_res, reserve_monthly["reserve_balance_eom"], linewidth=1.2)
    ax.set_title(f"Reserve Fund Balance Over Time — Cell {CELL_ID}")
    ax.set_ylabel("Reserve Balance ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_reserve_balance_timeseries.png",
                dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {CELL_ID}_reserve_balance_timeseries.png")


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

    print("Risk Management Analysis — Reserve Fund + Insurance")
    print("=" * 60)
    print(f"Cell             : {CELL_ID} (West load zone)")
    print(f"Contract         : {CONTRACT_MW} MWh/hour at ${PPA_PRICE_BASE}/MWh")
    print(f"Reserve layer    : {RESERVE_LO_MWH}–{RESERVE_HI_MWH} MWh shortfall "
          f"when price ≥ ${RESERVE_PRICE_THR}/MWh")
    print(f"Reserve contrib  : {RESERVE_CONTRIB_SHARE:.0%} of excess-energy spot sales")
    print(f"Reserve payout   : up to {RESERVE_PAYOUT_SHARE:.0%} of balance per triggered hour")
    print(f"Insurance layer  : {INS_LO_MWH}–{INS_HI_MWH} MWh shortfall "
          f"when ${INS_PRICE_FLOOR}–${INS_PRICE_CAP:,.0f}/MWh")
    print(f"Premium loading  : {LOADING:.2f}×")
    print(f"Debt service     : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output           : {OUTPUT_DIR}/")
    print("=" * 60)

    df  = load_and_clean()
    out = simulate_reserve_and_insurance(df)

    monthly_premium = out.attrs["monthly_premium"]
    annual_premium  = out.attrs["annual_premium"]

    print(f"\n  Expected monthly insurance payout : "
          f"${monthly_premium / LOADING:,.0f}")
    print(f"  Monthly premium (loading {LOADING:.2f}×) : ${monthly_premium:,.0f}")
    print(f"  Annual premium                    : ${annual_premium:,.0f}")

    monthly = analysis1_incremental_net(out, monthly_premium)
    monthly = analysis2_monthly_dscr(monthly, monthly_premium)
    annual  = analysis3_annual_dscr(out, annual_premium)

    # Save outputs
    monthly.to_csv(OUTPUT_DIR / f"{CELL_ID}_monthly_reserve_insurance.csv",   index=False)
    annual.to_csv(OUTPUT_DIR  / f"{CELL_ID}_annual_dscr_reserve_insurance.csv", index=False)
    print(f"\n  Saved: {CELL_ID}_monthly_reserve_insurance.csv")
    print(f"  Saved: {CELL_ID}_annual_dscr_reserve_insurance.csv")

    make_figures(monthly, annual, out)
    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
