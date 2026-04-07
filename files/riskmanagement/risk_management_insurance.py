"""
Risk Management Analysis — Insurance Only (Cell 6_23, West)
=============================================================
Evaluates a standalone insurance contract as a risk management strategy
for a wind energy producer under a physical PPA, applied to the
representative West load zone cell (6_23).

Insurance is structured as a layer-based contract covering a defined
slice of the hourly generation shortfall above a price trigger:

  Payout (per hour) = insured_layer_mwh × max(spot − fixed, 0)

  where insured_layer_mwh = min(shortfall, INS_HI_MWH) − INS_LO_MWH,
  clipped to [0, INS_HI_MWH − INS_LO_MWH],
  and the contract only pays when spot price ≥ INS_PRICE_THR.

The producer retains all shortfall below the attachment point
(INS_LO_MWH) and above the exhaustion point (INS_HI_MWH), and bears
the full incremental cost of those retained layers.

Premium methodology
--------------------
Annual payouts are summed for each year of the study period. The
expected annual payout is the mean across years, and the annual premium
is computed as:

  annual_premium = expected_annual_payout × LOADING

Monthly premium = annual_premium / 12, deducted from monthly revenue
each month regardless of whether the contract triggered that month.
A loading of 2.00 (100% above expected payout) is applied to reflect
the typical cost of catastrophe-style insurance with significant basis
risk and limited historical data for pricing.

Four analyses are produced:

  1. Incremental net cash flows — monthly net revenue and VaR,
     comparing baseline and insured scenarios.

  2. Monthly DSCR — monthly DSCR vs $683,474/month debt service
     benchmark, with breach probabilities at 1.0, 1.2, and 1.3.

  3. Annual DSCR — annual DSCR vs $8,201,688/year benchmark.

  4. Threshold sensitivity grid search — sweeps price triggers
     ($75–$500/MWh in $25 steps) and attachment points (10–30 MWh
     in 5 MWh steps) to identify which combinations produce the
     greatest VaR reduction net of premium cost. Results saved as
     a CSV for inspection.

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  SAMPLE_FILE : 6_23_sample.csv
                columns: timestamp, capacity_factor, price
                Same file used in representative_cell_financial_risk.py
                and risk_management_storage.py.

Output files
------------
  {CELL_ID}_monthly_insurance_only.csv
  {CELL_ID}_annual_dscr_insurance_only.csv
  {CELL_ID}_insurance_threshold_sensitivity.csv

  Figures saved to OUTPUT_DIR/figures/:
    {CELL_ID}_insurance_monthly_net_comparison.png
    {CELL_ID}_insurance_monthly_dscr.png
    {CELL_ID}_insurance_annual_dscr.png
    {CELL_ID}_insurance_sensitivity_heatmap.png

Usage
-----
    python risk_management_insurance.py

    Update SAMPLE_FILE and OUTPUT_DIR in the CONFIGURATION block before
    running. Insurance layer and premium parameters are set in the
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
NAMEPLATE_MW = 100.0
CONTRACT_MW  = 30.0
PPA_PRICE    = 50.0

# Insurance layer (primary specification)
INS_LO_MWH    = 15.0    # attachment point (MWh shortfall)
INS_HI_MWH    = 30.0    # exhaustion point (MWh shortfall)
INS_PRICE_THR = 100.0   # spot price trigger ($/MWh)
LOADING       = 2.00    # premium loading (2.00 = 100% above expected payout)

# Debt service benchmarks
MONTHLY_DEBT_SERVICE = 683_474.0
ANNUAL_DEBT_SERVICE  = 8_201_688.0

# DSCR breach thresholds
DSCR_THRESHOLDS = [1.0, 1.2, 1.3]

# Sensitivity grid
PRICE_THRESHOLDS_GRID  = np.arange(75.0, 500.0 + 1, 25.0)
ATTACH_POINTS_GRID_MWH = np.arange(10.0, 30.0 + 1, 5.0)

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

def baseline_cashflows(df: pd.DataFrame) -> pd.DataFrame:
    """Unhedged physical PPA cash flows."""
    out = df.copy()
    out["shortfall_mwh"]      = (CONTRACT_MW - out["gen_mwh"]).clip(lower=0)
    out["excess_mwh"]         = (out["gen_mwh"] - CONTRACT_MW).clip(lower=0)
    out["ppa_revenue_usd"]    = CONTRACT_MW * PPA_PRICE
    out["excess_sales_usd"]   = out["excess_mwh"] * out[PRICE_COL]
    out["price_minus_fixed"]  = (out[PRICE_COL] - PPA_PRICE).clip(lower=0)
    out["shortfall_cost_usd"] = out["shortfall_mwh"] * out["price_minus_fixed"]
    out["net_gain_loss_usd"]  = out["excess_sales_usd"] - out["shortfall_cost_usd"]
    out["total_revenue_usd"]  = out["ppa_revenue_usd"] + out["net_gain_loss_usd"]
    return out


def layer_mwh(shortfall: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Amount of shortfall covered within the [lo, hi] MWh layer."""
    return np.clip(shortfall - lo, 0.0, hi - lo)


def apply_insurance(df: pd.DataFrame,
                    lo: float = INS_LO_MWH,
                    hi: float = INS_HI_MWH,
                    price_thr: float = INS_PRICE_THR,
                    loading: float = LOADING) -> tuple[pd.DataFrame, float, float]:
    """
    Apply layer-based insurance to unhedged cash flows.

    Returns:
      insured_df   : DataFrame with insurance payout and insured revenue columns
      monthly_prem : monthly premium (USD)
      annual_prem  : annual premium (USD)
    """
    shortfall = df["shortfall_mwh"].values
    price     = df[PRICE_COL].values

    insured   = layer_mwh(shortfall, lo, hi)
    trigger   = (price >= price_thr).astype(float)
    pmf       = np.maximum(price - PPA_PRICE, 0.0)

    out = df.copy()
    out["insured_mwh"]              = insured
    out["insurance_payout_usd"]     = insured * pmf * trigger
    out["insured_total_revenue_usd"]= (
        out["total_revenue_usd"] + out["insurance_payout_usd"]
    )

    # Premium: mean annual payout × loading, spread monthly
    annual_payouts = out.groupby("year")["insurance_payout_usd"].sum()
    annual_prem    = float(annual_payouts.mean()) * loading
    monthly_prem   = annual_prem / 12

    out["monthly_premium_usd"] = monthly_prem
    out["insured_net_revenue_usd"] = (
        out["insured_total_revenue_usd"] - monthly_prem
    )

    return out, monthly_prem, annual_prem


# =============================================================================
# HELPERS
# =============================================================================

def print_dscr_comparison(label: str, base: pd.Series, insured: pd.Series):
    print(f"\n  {label}")
    for thr in DSCR_THRESHOLDS:
        b = (base < thr).mean() * 100
        s = (insured < thr).mean() * 100
        print(f"    P(DSCR<{thr:.1f}): baseline={b:.1f}%  insured={s:.1f}%")
    print(f"    Mean : baseline={base.mean():.3f}  insured={insured.mean():.3f}")
    print(f"    Min  : baseline={base.min():.3f}  insured={insured.min():.3f}")


def month_dt(monthly: pd.DataFrame) -> pd.Series:
    return monthly["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp()
        if not isinstance(p, pd.Timestamp) else p
    )


# =============================================================================
# ANALYSIS 1: Incremental net cash flows
# =============================================================================

def analysis1_incremental_net(df: pd.DataFrame,
                               insured_df: pd.DataFrame,
                               monthly_prem: float,
                               annual_prem: float):
    """Monthly net revenue and VaR comparison."""
    print("\n[Analysis 1] Incremental net cash flows...")

    monthly_base = df.groupby("month").agg(
        monthly_net_usd  = ("net_gain_loss_usd",  "sum"),
        monthly_rev_usd  = ("total_revenue_usd",  "sum"),
    ).reset_index()

    monthly_ins = insured_df.groupby("month").agg(
        monthly_ins_payout_usd = ("insurance_payout_usd", "sum"),
        monthly_ins_rev_usd    = ("insured_total_revenue_usd", "sum"),
    ).reset_index()
    monthly_ins["monthly_ins_net_usd"] = (
        monthly_ins["monthly_ins_rev_usd"] - monthly_prem
    )

    monthly = monthly_base.merge(monthly_ins, on="month")

    print(f"\n  Annual premium: ${annual_prem:,.0f}  "
          f"(monthly: ${monthly_prem:,.0f})")
    print(f"  Baseline  VaR95: ${np.percentile(monthly['monthly_rev_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['monthly_rev_usd'], 1):,.0f}")
    print(f"  Insured   VaR95: ${np.percentile(monthly['monthly_ins_net_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['monthly_ins_net_usd'], 1):,.0f}")

    return monthly


# =============================================================================
# ANALYSIS 2: Monthly DSCR
# =============================================================================

def analysis2_monthly_dscr(monthly: pd.DataFrame) -> pd.DataFrame:
    """Monthly DSCR with and without insurance."""
    print("\n[Analysis 2] Monthly DSCR...")

    monthly = monthly.copy()
    monthly["baseline_dscr"] = monthly["monthly_rev_usd"]     / MONTHLY_DEBT_SERVICE
    monthly["insured_dscr"]  = monthly["monthly_ins_net_usd"] / MONTHLY_DEBT_SERVICE

    print_dscr_comparison("Monthly DSCR",
                          monthly["baseline_dscr"],
                          monthly["insured_dscr"])
    return monthly


# =============================================================================
# ANALYSIS 3: Annual DSCR
# =============================================================================

def analysis3_annual_dscr(df: pd.DataFrame,
                           insured_df: pd.DataFrame,
                           annual_prem: float) -> pd.DataFrame:
    """Annual DSCR with and without insurance."""
    print("\n[Analysis 3] Annual DSCR...")

    annual_base = df.groupby("year")["total_revenue_usd"].sum().reset_index()
    annual_base.columns = ["year", "baseline_annual_revenue_usd"]

    annual_ins = insured_df.groupby("year")["insured_total_revenue_usd"].sum().reset_index()
    annual_ins.columns = ["year", "insured_annual_revenue_usd"]
    annual_ins["insured_net_annual_usd"] = (
        annual_ins["insured_annual_revenue_usd"] - annual_prem
    )

    annual = annual_base.merge(annual_ins, on="year")
    annual["baseline_dscr"] = annual["baseline_annual_revenue_usd"] / ANNUAL_DEBT_SERVICE
    annual["insured_dscr"]  = annual["insured_net_annual_usd"]      / ANNUAL_DEBT_SERVICE

    print_dscr_comparison("Annual DSCR",
                          annual["baseline_dscr"],
                          annual["insured_dscr"])

    for _, row in annual.iterrows():
        print(f"    {int(row['year'])}: baseline={row['baseline_dscr']:.3f}  "
              f"insured={row['insured_dscr']:.3f}")

    return annual


# =============================================================================
# ANALYSIS 4: Threshold sensitivity grid search
# =============================================================================

def analysis4_sensitivity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sweep across price triggers and attachment points, recording
    VaR improvement and net premium cost for each combination.
    """
    print("\n[Analysis 4] Threshold sensitivity grid search...")
    print(f"  Price triggers: ${PRICE_THRESHOLDS_GRID[0]:.0f}–"
          f"${PRICE_THRESHOLDS_GRID[-1]:.0f}/MWh  "
          f"| Attachment points: {ATTACH_POINTS_GRID_MWH[0]:.0f}–"
          f"{ATTACH_POINTS_GRID_MWH[-1]:.0f} MWh")

    base_monthly = df.groupby("month")["total_revenue_usd"].sum()
    base_var95   = float(np.percentile(base_monthly, 5))
    base_var99   = float(np.percentile(base_monthly, 1))

    rows = []
    n_total = len(PRICE_THRESHOLDS_GRID) * len(ATTACH_POINTS_GRID_MWH)

    for attach in ATTACH_POINTS_GRID_MWH:
        for price_thr in PRICE_THRESHOLDS_GRID:
            ins_df, m_prem, a_prem = apply_insurance(
                df, lo=attach, hi=CONTRACT_MW,
                price_thr=price_thr, loading=LOADING
            )

            monthly_ins = ins_df.groupby("month")["insured_total_revenue_usd"].sum()
            monthly_net = monthly_ins - m_prem

            var95_ins = float(np.percentile(monthly_net, 5))
            var99_ins = float(np.percentile(monthly_net, 1))

            annual_payouts = ins_df.groupby("year")["insurance_payout_usd"].sum()
            payout_ratio   = float(annual_payouts.mean()) / a_prem if a_prem > 0 else np.nan

            rows.append({
                "attach_mwh":       attach,
                "price_thr":        price_thr,
                "ins_lo_mwh":       attach,
                "ins_hi_mwh":       CONTRACT_MW,
                "monthly_premium":  m_prem,
                "annual_premium":   a_prem,
                "var95_improvement": var95_ins - base_var95,
                "var99_improvement": var99_ins - base_var99,
                "net_annual_cost":  a_prem - float(annual_payouts.mean()),
                "payout_premium_ratio": payout_ratio,
            })

    results = pd.DataFrame(rows)
    print(f"  Scenarios run: {len(results)}")

    # Best by VaR99 improvement
    best = results.loc[results["var99_improvement"].idxmax()]
    print(f"\n  Best by VaR99 improvement:")
    print(f"    Attach: {best['attach_mwh']:.0f} MWh  "
          f"Price trigger: ${best['price_thr']:.0f}/MWh")
    print(f"    VaR99 improvement: ${best['var99_improvement']:,.0f}")
    print(f"    Monthly premium  : ${best['monthly_premium']:,.0f}")
    print(f"    Annual premium   : ${best['annual_premium']:,.0f}")

    return results


# =============================================================================
# FIGURES
# =============================================================================

def make_figures(monthly: pd.DataFrame, annual: pd.DataFrame,
                 sensitivity: pd.DataFrame, fig_dir: Path):
    dt = month_dt(monthly)

    # Figure 1: Monthly net revenue comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dt, monthly["monthly_rev_usd"],     linewidth=1.5, label="Baseline")
    ax.plot(dt, monthly["monthly_ins_net_usd"], linewidth=1.5, label="Insurance only (net of premium)")
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs Insurance Only — Cell {CELL_ID}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_insurance_monthly_net_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 2: Monthly DSCR
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dt, monthly["baseline_dscr"], linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly["insured_dscr"],  linewidth=1.5, label="Insurance Only DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs Insurance Only — Cell {CELL_ID}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_insurance_monthly_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 3: Annual DSCR bar chart
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(annual))
    w = 0.35
    ax.bar(x - w/2, annual["baseline_dscr"], w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual["insured_dscr"],  w, label="Insurance Only", alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs Insurance Only — Cell {CELL_ID}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_insurance_annual_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 4: Sensitivity heatmap — VaR99 improvement
    pivot = sensitivity.pivot_table(
        index="attach_mwh",
        columns="price_thr",
        values="var99_improvement"
    )
    fig, ax = plt.subplots(figsize=(14, 5))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([f"${int(p)}" for p in pivot.columns], rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([f"{int(a)} MWh" for a in pivot.index])
    ax.set_xlabel("Price trigger ($/MWh)")
    ax.set_ylabel("Attachment point (MWh shortfall)")
    ax.set_title(f"VaR99 Improvement vs Baseline — Insurance Threshold Sensitivity\n"
                 f"Cell {CELL_ID}, loading {LOADING:.2f}×")
    plt.colorbar(im, ax=ax, label="VaR99 improvement ($)")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_insurance_sensitivity_heatmap.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    print(f"  Figures saved to {fig_dir.name}/")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    if not SAMPLE_FILE.exists():
        raise FileNotFoundError(
            f"Sample file not found: {SAMPLE_FILE}\n"
            "Check SAMPLE_FILE in the configuration block."
        )

    print("Risk Management Analysis — Insurance Only")
    print("=" * 55)
    print(f"Cell           : {CELL_ID} (West load zone)")
    print(f"Contract       : {CONTRACT_MW} MWh/hour at ${PPA_PRICE}/MWh")
    print(f"Insurance layer: {INS_LO_MWH}–{INS_HI_MWH} MWh shortfall "
          f"when price ≥ ${INS_PRICE_THR}/MWh")
    print(f"Loading        : {LOADING:.2f}×")
    print(f"Debt service   : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output         : {OUTPUT_DIR}/")
    print("=" * 55)

    df         = load_and_clean()
    base_df    = baseline_cashflows(df)
    insured_df, monthly_prem, annual_prem = apply_insurance(base_df)

    print(f"\n  Expected annual payout : ${annual_prem / LOADING:,.0f}")
    print(f"  Annual premium         : ${annual_prem:,.0f}")
    print(f"  Monthly premium        : ${monthly_prem:,.0f}")

    monthly      = analysis1_incremental_net(base_df, insured_df, monthly_prem, annual_prem)
    monthly      = analysis2_monthly_dscr(monthly)
    annual       = analysis3_annual_dscr(base_df, insured_df, annual_prem)
    sensitivity  = analysis4_sensitivity(base_df)

    # Save outputs
    monthly.to_csv(OUTPUT_DIR / f"{CELL_ID}_monthly_insurance_only.csv", index=False)
    annual.to_csv(OUTPUT_DIR  / f"{CELL_ID}_annual_dscr_insurance_only.csv", index=False)
    sensitivity.to_csv(
        OUTPUT_DIR / f"{CELL_ID}_insurance_threshold_sensitivity.csv", index=False
    )
    print(f"\n  Saved: {CELL_ID}_monthly_insurance_only.csv")
    print(f"  Saved: {CELL_ID}_annual_dscr_insurance_only.csv")
    print(f"  Saved: {CELL_ID}_insurance_threshold_sensitivity.csv")

    make_figures(monthly, annual, sensitivity, fig_dir)

    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
