"""
Risk Management Analysis — Buyer Risk Sharing (Cell 6_23, West)
================================================================
Evaluates a structured PPA contract in which the buyer absorbs a defined
layer of generation shortfall risk in exchange for a discounted fixed price,
applied to the representative West load zone cell (6_23).

Structure
---------
The contract embeds three layers of risk allocation:

  Producer retained (0–15 MWh shortfall)
      The producer bears the full incremental cost of any shortfall below
      15 MWh unhedged.

  Insurance (15–25 MWh shortfall, $75–$1,000/MWh trigger)
      A third-party insurance contract covers this middle layer. Premium =
      1.30× expected monthly payout, deducted each month.

  Buyer risk-sharing (25–30 MWh shortfall, price ≥ $100/MWh)
      The buyer absorbs the top shortfall layer contingently. In exchange,
      the fixed PPA price is discounted from $50.00/MWh to $48.97/MWh.
      Payout = covered MWh × max(spot − structured fixed price, 0).

Pricing the buyer discount
---------------------------
Cell 0 (buyer_discount_pricing) derives the $/MWh PPA discount that makes
the buyer indifferent between the standard and structured contracts. The
required discount is the expected annual buyer payout divided by annual
contracted MWh (30 MWh/hour × 8,760 hours), loaded at 1.25×:

  required_discount = (expected_annual_payout × 1.25) / annual_contracted_mwh

The offered discount ($50.00 − $48.97 = $1.03/MWh) is compared against
this required discount to assess whether the structured price is sufficient
to compensate the buyer for the risk transferred.

Producer net-of-discount tradeoff
-----------------------------------
Because the producer gives up $1.03/MWh on the full 30 MWh fixed delivery,
the annual cost of the discount to the producer is approximately $246,829.
The offsetting benefit is the buyer-share value received (avoided shortfall
cost) averaged at $197,222/year. Cell 2 computes this tradeoff explicitly.

Three analyses are produced:
  1. Buyer discount pricing — required vs offered discount, expected
     annual buyer payout by year.
  2. Incremental net cash flows — monthly revenue and VaR: baseline vs
     insurance + buyer structure.
  3. Monthly DSCR — vs $683,474/month debt service benchmark.
  4. Annual DSCR — vs $8,201,688/year benchmark.

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
  {CELL_ID}_buyer_discount_pricing.csv
  {CELL_ID}_monthly_insurance_buyer_share.csv
  {CELL_ID}_annual_dscr_insurance_buyer_share.csv

  Figures saved to OUTPUT_DIR/figures/:
    {CELL_ID}_buyer_monthly_payout_distribution.png
    {CELL_ID}_buyer_insurance_monthly_net_comparison.png
    {CELL_ID}_buyer_insurance_monthly_dscr.png
    {CELL_ID}_buyer_insurance_annual_dscr.png

Usage
-----
    python risk_management_buyer_sharing.py

    Update SAMPLE_FILE and OUTPUT_DIR in the CONFIGURATION block before
    running. Layer and pricing parameters can be adjusted below.
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

# Contract parameters
NAMEPLATE_MW   = 100.0
CONTRACT_MW    = 30.0
PPA_PRICE_BASE = 50.0     # baseline PPA fixed price
PPA_PRICE_NEW  = 48.97    # structured PPA fixed price (discounted)

# Insurance layer
INS_LO_MWH      = 15.0
INS_HI_MWH      = 25.0
INS_PRICE_FLOOR = 75.0
INS_PRICE_CAP   = 1_000.0
INS_LOADING     = 1.30    # insurance premium loading

# Buyer risk-sharing layer
BUY_LO_MWH    = 25.0
BUY_HI_MWH    = 30.0
BUY_PRICE_THR = 100.0    # spot price trigger for buyer layer

# Buyer discount pricing loading
BUY_LOADING = 1.25

# Debt service benchmarks
MONTHLY_DEBT_SERVICE = 683_474.0
ANNUAL_DEBT_SERVICE  = 8_201_688.0

# DSCR thresholds
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
# LAYER HELPER
# =============================================================================

def layer_mwh(shortfall: np.ndarray, lo: float, hi: float) -> np.ndarray:
    """Amount of shortfall within the [lo, hi] MWh layer."""
    return np.clip(shortfall - lo, 0.0, hi - lo)


# =============================================================================
# ANALYSIS 1: Buyer discount pricing
# =============================================================================

def analysis1_buyer_discount_pricing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute expected annual buyer payout and required $/MWh discount.

    Payout basis: covered_mwh × max(spot − structured_fixed, 0)
    Required discount = (expected annual payout × BUY_LOADING) / annual_contracted_mwh
    """
    print("\n[Analysis 1] Buyer discount pricing...")

    shortfall = (CONTRACT_MW - df["gen_mwh"]).clip(lower=0).values
    price     = df[PRICE_COL].values

    buy_layer  = layer_mwh(shortfall, BUY_LO_MWH, BUY_HI_MWH)
    pmf_new    = np.maximum(price - PPA_PRICE_NEW, 0.0)
    trigger    = (price >= BUY_PRICE_THR).astype(float)

    df = df.copy()
    df["buyer_payout_usd"] = buy_layer * pmf_new * trigger

    # Annual payouts
    annual_payouts = df.groupby("year")["buyer_payout_usd"].sum()
    expected_annual = float(annual_payouts.mean())
    annual_contracted_mwh = CONTRACT_MW * 8_760

    required_discount = (expected_annual * BUY_LOADING) / annual_contracted_mwh
    offered_discount  = PPA_PRICE_BASE - PPA_PRICE_NEW

    print(f"\n  Baseline PPA price      : ${PPA_PRICE_BASE:.2f}/MWh")
    print(f"  Structured PPA price    : ${PPA_PRICE_NEW:.2f}/MWh")
    print(f"  Offered discount        : ${offered_discount:.2f}/MWh")
    print(f"  Required discount       : ${required_discount:.2f}/MWh "
          f"(loading {BUY_LOADING:.2f}×)")
    print(f"\n  Expected annual buyer payout : ${expected_annual:,.0f}")
    print(f"  Probability buyer pays (monthly): "
          f"{(df.groupby('month')['buyer_payout_usd'].sum() > 0).mean()*100:.1f}%")

    print("\n  Annual buyer payout by year:")
    for yr, val in annual_payouts.items():
        print(f"    {yr}: ${val:,.0f}")

    # Producer cost of discount
    annual_discount_cost = offered_discount * CONTRACT_MW * 8_760
    print(f"\n  Annual producer cost of discount : ${annual_discount_cost:,.0f}")
    print(f"  Expected annual buyer-share value: ${expected_annual:,.0f}")
    print(f"  Net expected tradeoff            : "
          f"${expected_annual - annual_discount_cost:,.0f}")

    # Save
    pricing_df = annual_payouts.reset_index()
    pricing_df.columns = ["year", "annual_buyer_payout_usd"]
    pricing_df["offered_discount_per_mwh"]  = offered_discount
    pricing_df["required_discount_per_mwh"] = required_discount
    pricing_df["annual_discount_cost_usd"]  = annual_discount_cost
    pricing_df["net_tradeoff_usd"] = (
        pricing_df["annual_buyer_payout_usd"] - annual_discount_cost
    )

    return df, pricing_df


# =============================================================================
# ANALYSIS 2: Insurance + buyer — incremental net cash flows
# =============================================================================

def analysis2_incremental_net(df: pd.DataFrame) -> tuple[pd.DataFrame, float, float]:
    """
    Compute hourly cash flows for baseline and insurance + buyer structure.
    Returns the df with all columns, plus monthly_premium and annual_premium.
    """
    print("\n[Analysis 2] Insurance + buyer risk sharing — incremental net cash flows...")

    shortfall = (CONTRACT_MW - df["gen_mwh"]).clip(lower=0).values
    excess    = (df["gen_mwh"] - CONTRACT_MW).clip(lower=0).values
    price     = df[PRICE_COL].values

    # Baseline at new PPA price
    pmf_new    = np.maximum(price - PPA_PRICE_NEW, 0.0)
    excess_sal = excess * price
    sf_cost    = shortfall * pmf_new

    # Insurance layer
    ins_layer  = layer_mwh(shortfall, INS_LO_MWH, INS_HI_MWH)
    ins_trig   = (
        (price >= INS_PRICE_FLOOR) & (price <= INS_PRICE_CAP)
    ).astype(float)
    ins_payout = ins_layer * pmf_new * ins_trig

    # Buyer layer
    buy_layer  = layer_mwh(shortfall, BUY_LO_MWH, BUY_HI_MWH)
    buy_trig   = (price >= BUY_PRICE_THR).astype(float)
    buy_share  = buy_layer * pmf_new * buy_trig

    df = df.copy()

    # Baseline at original price
    pmf_base        = np.maximum(price - PPA_PRICE_BASE, 0.0)
    df["baseline_net_usd"]  = excess_sal - shortfall * pmf_base
    df["baseline_total_rev_usd"] = CONTRACT_MW * PPA_PRICE_BASE + df["baseline_net_usd"]

    # Structured: insurance payout + buyer share reduce net loss
    df["insurance_payout_usd"] = ins_payout
    df["buyer_share_usd"]      = buy_share
    df["structured_net_pre_prem_usd"] = (
        excess_sal - sf_cost + ins_payout + buy_share
    )
    df["structured_total_rev_pre_prem_usd"] = (
        CONTRACT_MW * PPA_PRICE_NEW + df["structured_net_pre_prem_usd"]
    )

    # Premium = 1.30 × mean monthly insurance payout
    monthly_ins = df.groupby("month")["insurance_payout_usd"].sum()
    monthly_premium = float(monthly_ins.mean()) * INS_LOADING
    annual_premium  = monthly_premium * 12

    df["monthly_premium_usd"] = monthly_premium

    # Monthly aggregation
    monthly = df.groupby("month").agg(
        baseline_rev_usd        = ("baseline_total_rev_usd",          "sum"),
        structured_rev_pre_prem = ("structured_total_rev_pre_prem_usd","sum"),
        insurance_payout_usd    = ("insurance_payout_usd",            "sum"),
        buyer_share_usd         = ("buyer_share_usd",                 "sum"),
    ).reset_index()

    monthly["structured_net_rev_usd"] = (
        monthly["structured_rev_pre_prem"] - monthly_premium
    )

    print(f"\n  Expected monthly insurance payout : ${float(monthly_ins.mean()):,.0f}")
    print(f"  Monthly premium (loading {INS_LOADING:.2f}×) : ${monthly_premium:,.0f}")
    print(f"  Average monthly buyer share value : "
          f"${monthly['buyer_share_usd'].mean():,.0f}")
    print(f"\n  Baseline  VaR95: ${np.percentile(monthly['baseline_rev_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['baseline_rev_usd'], 1):,.0f}")
    print(f"  Structured VaR95: "
          f"${np.percentile(monthly['structured_net_rev_usd'], 5):,.0f}  "
          f"VaR99: ${np.percentile(monthly['structured_net_rev_usd'], 1):,.0f}")

    return monthly, monthly_premium, annual_premium


# =============================================================================
# ANALYSIS 3: Monthly DSCR
# =============================================================================

def analysis3_monthly_dscr(monthly: pd.DataFrame) -> pd.DataFrame:
    print("\n[Analysis 3] Monthly DSCR...")

    monthly = monthly.copy()
    monthly["baseline_dscr"]    = monthly["baseline_rev_usd"]        / MONTHLY_DEBT_SERVICE
    monthly["structured_dscr"]  = monthly["structured_net_rev_usd"]  / MONTHLY_DEBT_SERVICE

    for thr in DSCR_THRESHOLDS:
        b = (monthly["baseline_dscr"]   < thr).mean() * 100
        s = (monthly["structured_dscr"] < thr).mean() * 100
        print(f"  P(DSCR<{thr:.1f}): baseline={b:.1f}%  structured={s:.1f}%")

    print(f"  Mean : baseline={monthly['baseline_dscr'].mean():.3f}  "
          f"structured={monthly['structured_dscr'].mean():.3f}")
    print(f"  Min  : baseline={monthly['baseline_dscr'].min():.3f}  "
          f"structured={monthly['structured_dscr'].min():.3f}")

    return monthly


# =============================================================================
# ANALYSIS 4: Annual DSCR
# =============================================================================

def analysis4_annual_dscr(df: pd.DataFrame, annual_premium: float) -> pd.DataFrame:
    print("\n[Analysis 4] Annual DSCR...")

    annual = df.groupby("year").agg(
        baseline_annual_rev_usd        = ("baseline_total_rev_usd",           "sum"),
        structured_annual_rev_pre_prem = ("structured_total_rev_pre_prem_usd","sum"),
    ).reset_index()

    annual["structured_net_annual_usd"] = (
        annual["structured_annual_rev_pre_prem"] - annual_premium
    )
    annual["baseline_dscr"]   = annual["baseline_annual_rev_usd"]    / ANNUAL_DEBT_SERVICE
    annual["structured_dscr"] = annual["structured_net_annual_usd"]  / ANNUAL_DEBT_SERVICE

    for thr in DSCR_THRESHOLDS:
        b = (annual["baseline_dscr"]   < thr).mean() * 100
        s = (annual["structured_dscr"] < thr).mean() * 100
        print(f"  P(annual DSCR<{thr:.1f}): baseline={b:.1f}%  structured={s:.1f}%")

    print(f"  Mean : baseline={annual['baseline_dscr'].mean():.3f}  "
          f"structured={annual['structured_dscr'].mean():.3f}")
    print(f"  Min  : baseline={annual['baseline_dscr'].min():.3f}  "
          f"structured={annual['structured_dscr'].min():.3f}")

    for _, row in annual.iterrows():
        print(f"    {int(row['year'])}: baseline={row['baseline_dscr']:.3f}  "
              f"structured={row['structured_dscr']:.3f}")

    return annual


# =============================================================================
# FIGURES
# =============================================================================

def make_figures(df_with_buyer: pd.DataFrame,
                 monthly: pd.DataFrame,
                 annual: pd.DataFrame,
                 fig_dir: Path):

    def month_dt(m):
        return m["month"].apply(
            lambda p: pd.Period(p, "M").to_timestamp()
            if not isinstance(p, pd.Timestamp) else p
        )

    dt = month_dt(monthly)

    # Figure 1: Buyer monthly payout distribution
    monthly_buyer = df_with_buyer.groupby("month")["buyer_payout_usd"].sum()
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(monthly_buyer.values, bins=30, alpha=0.8, edgecolor="black")
    ax.axvline(monthly_buyer.mean(), linestyle="--", color="black", linewidth=2,
               label=f"Mean ${monthly_buyer.mean():,.0f}")
    ax.axvline(np.percentile(monthly_buyer, 95), linestyle=":", color="red",
               linewidth=2, label=f"95th pct ${np.percentile(monthly_buyer, 95):,.0f}")
    ax.set_title(f"Monthly Buyer Payout Distribution — Cell {CELL_ID}\n"
                 f"(Buyer layer: {BUY_LO_MWH:.0f}–{BUY_HI_MWH:.0f} MWh, "
                 f"price ≥ ${BUY_PRICE_THR:.0f}/MWh)")
    ax.set_xlabel("Monthly Buyer Payout ($)")
    ax.set_ylabel("Frequency")
    ax.xaxis.set_major_formatter(currency_fmt)
    ax.legend()
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_buyer_monthly_payout_distribution.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 2: Monthly revenue comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dt, monthly["baseline_rev_usd"],       linewidth=1.5, label="Baseline ($50/MWh)")
    ax.plot(dt, monthly["structured_net_rev_usd"], linewidth=1.5,
            label=f"Insurance + Buyer Share (net, ${PPA_PRICE_NEW}/MWh)")
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs Insurance + Buyer Risk Sharing — Cell {CELL_ID}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_buyer_insurance_monthly_net_comparison.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 3: Monthly DSCR
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dt, monthly["baseline_dscr"],   linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly["structured_dscr"], linewidth=1.5,
            label="Insurance + Buyer Share DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs Insurance + Buyer Risk Sharing — Cell {CELL_ID}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_buyer_insurance_monthly_dscr.png",
                dpi=300, bbox_inches="tight")
    plt.close()

    # Figure 4: Annual DSCR bar chart
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(annual))
    w = 0.35
    ax.bar(x - w/2, annual["baseline_dscr"],   w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual["structured_dscr"], w, label="Insurance + Buyer Share", alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs Insurance + Buyer Risk Sharing — Cell {CELL_ID}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{CELL_ID}_buyer_insurance_annual_dscr.png",
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

    print("Risk Management Analysis — Buyer Risk Sharing")
    print("=" * 60)
    print(f"Cell             : {CELL_ID} (West load zone)")
    print(f"Contract         : {CONTRACT_MW} MWh/hour")
    print(f"Baseline price   : ${PPA_PRICE_BASE}/MWh")
    print(f"Structured price : ${PPA_PRICE_NEW}/MWh "
          f"(discount = ${PPA_PRICE_BASE - PPA_PRICE_NEW:.2f}/MWh)")
    print(f"Insurance layer  : {INS_LO_MWH}–{INS_HI_MWH} MWh shortfall "
          f"when ${INS_PRICE_FLOOR}–${INS_PRICE_CAP:,.0f}/MWh")
    print(f"Buyer layer      : {BUY_LO_MWH}–{BUY_HI_MWH} MWh shortfall "
          f"when price ≥ ${BUY_PRICE_THR}/MWh")
    print(f"Debt service     : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output           : {OUTPUT_DIR}/")
    print("=" * 60)

    df = load_and_clean()

    # Analysis 1: buyer discount pricing
    df_with_buyer, pricing_df = analysis1_buyer_discount_pricing(df)
    pricing_df.to_csv(
        OUTPUT_DIR / f"{CELL_ID}_buyer_discount_pricing.csv", index=False
    )
    print(f"\n  Saved: {CELL_ID}_buyer_discount_pricing.csv")

    # Analyses 2–4: structured cash flows, DSCR
    # Add baseline total revenue column to df_with_buyer for figure 4
    pmf_base = np.maximum(df_with_buyer[PRICE_COL].values - PPA_PRICE_BASE, 0.0)
    shortfall = (CONTRACT_MW - df_with_buyer["gen_mwh"]).clip(lower=0).values
    excess    = (df_with_buyer["gen_mwh"] - CONTRACT_MW).clip(lower=0).values
    df_with_buyer["baseline_total_rev_usd"] = (
        CONTRACT_MW * PPA_PRICE_BASE
        + excess * df_with_buyer[PRICE_COL].values
        - shortfall * pmf_base
    )

    pmf_new = np.maximum(df_with_buyer[PRICE_COL].values - PPA_PRICE_NEW, 0.0)
    ins_layer = layer_mwh(shortfall, INS_LO_MWH, INS_HI_MWH)
    ins_trig  = (
        (df_with_buyer[PRICE_COL].values >= INS_PRICE_FLOOR) &
        (df_with_buyer[PRICE_COL].values <= INS_PRICE_CAP)
    ).astype(float)
    buy_layer = layer_mwh(shortfall, BUY_LO_MWH, BUY_HI_MWH)
    buy_trig  = (df_with_buyer[PRICE_COL].values >= BUY_PRICE_THR).astype(float)

    df_with_buyer["structured_total_rev_pre_prem_usd"] = (
        CONTRACT_MW * PPA_PRICE_NEW
        + excess * df_with_buyer[PRICE_COL].values
        - shortfall * pmf_new
        + ins_layer * pmf_new * ins_trig
        + buy_layer * pmf_new * buy_trig
    )

    monthly, monthly_premium, annual_premium = analysis2_incremental_net(df_with_buyer)
    monthly = analysis3_monthly_dscr(monthly)
    annual  = analysis4_annual_dscr(df_with_buyer, annual_premium)

    monthly.to_csv(
        OUTPUT_DIR / f"{CELL_ID}_monthly_insurance_buyer_share.csv", index=False
    )
    annual.to_csv(
        OUTPUT_DIR / f"{CELL_ID}_annual_dscr_insurance_buyer_share.csv", index=False
    )
    print(f"\n  Saved: {CELL_ID}_monthly_insurance_buyer_share.csv")
    print(f"  Saved: {CELL_ID}_annual_dscr_insurance_buyer_share.csv")

    make_figures(df_with_buyer, monthly, annual, fig_dir)
    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
