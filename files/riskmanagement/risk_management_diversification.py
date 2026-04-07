"""
Risk Management Analysis — Geographic Diversification
=======================================================
Evaluates geographic diversification as a risk management strategy for
a wind energy producer under a physical PPA, comparing the baseline
(100 MW concentrated at a single West load zone site) against two
diversification approaches.

This script supports the risk management section of the thesis, applied
to the representative West load zone cell (6_23) as baseline.

Three scenarios are analysed:

  Baseline
      100 MW at West site (cell 6_23). Producer bears full shortfall
      exposure under a 30 MWh PPA at $50/MWh fixed price.

  Strategy 1 — Cross-zone diversification (West + South)
      50 MW at West site (cell 6_23) + 50 MW at South site (cell 38_37).
      Combined hourly generation counts toward the 30 MWh obligation.
      West spot price used for shortfall costs and excess sales in both
      cases, enabling a clean like-for-like comparison of generation
      geography holding price exposure constant.

  Strategy 2 — Cross-zone diversification + insurance
      Same 50/50 West + South split, with insurance covering the
      20–30 MWh shortfall layer when West spot price ≥ $100/MWh.
      Premium = 1.30× expected annual payout, spread monthly.

  Strategy 3 — Within-zone diversification (two West sites)
      50 MW at West site 1 (cell 6_23) + 50 MW at West site 2 (cell 2_22).
      Tests whether geographic spread within the same load zone provides
      meaningful risk reduction — an interesting result given that
      within-zone cells are subject to the same meteorological conditions.

For each strategy the script computes:
  - Monthly revenue, VaR, and CV vs baseline
  - Monthly DSCR vs $683,474/month debt service benchmark
  - Annual DSCR vs $8,201,688/year benchmark
  - DSCR breach probabilities at 1.0, 1.2, and 1.3 thresholds

Note on price convention
--------------------------
West spot price (from cell 6_23) is used throughout for both the baseline
and all diversified cases. This isolates the effect of generation geography
on financial risk, holding price exposure constant and avoiding confounding
from South zone price differences.

Requirements
------------
    pip install numpy pandas matplotlib

Input files required
--------------------
  WEST_FILE_1 : 6_23_sample.csv  — representative West cell (baseline + Strategy 1–3)
  SOUTH_FILE  : 38_37_sample.csv — South cell (Strategies 1 and 2)
  WEST_FILE_2 : 2_22_sample.csv  — second West cell (Strategy 3)

  All files have columns: timestamp, capacity_factor, price, Load Zone.
  These are the pre-built grid-cell simulation files from SAMPLE_DIR.

Output files
------------
  diversification_cross_zone_monthly.csv
  diversification_cross_zone_annual_dscr.csv
  diversification_cross_zone_insurance_monthly.csv
  diversification_cross_zone_insurance_annual_dscr.csv
  diversification_within_zone_monthly.csv

  Figures saved to OUTPUT_DIR/figures/:
    diversification_cross_zone_monthly_revenue.png
    diversification_cross_zone_monthly_dscr.png
    diversification_cross_zone_annual_dscr.png
    diversification_cross_zone_insurance_monthly_revenue.png
    diversification_cross_zone_insurance_annual_dscr.png
    diversification_within_zone_monthly_revenue.png
    diversification_within_zone_monthly_dscr.png

Usage
-----
    python risk_management_diversification.py

    Update the file paths in the CONFIGURATION block before running.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from pathlib import Path


# =============================================================================
# CONFIGURATION — update paths before running
# =============================================================================

DATA_DIR    = Path("data/simulated_revs_south_west_gridcells")
WEST_FILE_1 = DATA_DIR / "6_23_sample.csv"    # baseline + all strategies
SOUTH_FILE  = DATA_DIR / "38_37_sample.csv"   # cross-zone South cell
WEST_FILE_2 = DATA_DIR / "2_22_sample.csv"    # within-zone West cell 2

OUTPUT_DIR  = Path("output/risk_management")

# Contract and asset parameters
PPA_PRICE        = 50.0
CONTRACT_MWH     = 30.0
BASELINE_MW      = 100.0
WEST_DIV_MW      = 50.0
SOUTH_DIV_MW     = 50.0
WEST2_DIV_MW     = 50.0

# Debt service benchmarks
MONTHLY_DEBT_SERVICE = 683_474.0
ANNUAL_DEBT_SERVICE  = 8_201_688.0

# DSCR thresholds
DSCR_THRESHOLDS = [1.0, 1.2, 1.3]

# Insurance parameters (Strategy 2)
INS_LO_MWH    = 20.0
INS_HI_MWH    = 30.0
INS_PRICE_THR = 100.0
LOADING       = 1.30

# Uri exclusion
URI_DATES = pd.to_datetime([
    "2021-02-10", "2021-02-11", "2021-02-12", "2021-02-13", "2021-02-14",
    "2021-02-15", "2021-02-16", "2021-02-17", "2021-02-18",
    "2021-02-19", "2021-02-20",
]).normalize()
URI_SET = set(URI_DATES)

TS_COL    = "timestamp"
CF_COL    = "capacity_factor"
PRICE_COL = "price"

currency_fmt = FuncFormatter(lambda x, _: f"${x:,.0f}")


# =============================================================================
# DATA LOADING
# =============================================================================

def load_and_clean(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df[TS_COL]    = pd.to_datetime(df[TS_COL],    errors="coerce")
    df[CF_COL]    = pd.to_numeric(df[CF_COL],     errors="coerce")
    df[PRICE_COL] = pd.to_numeric(df[PRICE_COL],  errors="coerce")
    df = df.dropna(subset=[TS_COL, CF_COL, PRICE_COL]).copy()
    df = df[~df[TS_COL].dt.normalize().isin(URI_SET)].copy()
    df = df[df[PRICE_COL] >= 0].copy()
    df = df.sort_values(TS_COL).reset_index(drop=True)
    df["month"] = df[TS_COL].dt.to_period("M")
    df["year"]  = df[TS_COL].dt.year
    return df


# =============================================================================
# REVENUE CALCULATION — shared financial mechanics
# =============================================================================

def ppa_revenue(gen_mwh: pd.Series, price: pd.Series,
                nameplate_share: float = 1.0) -> pd.DataFrame:
    """
    Compute hourly PPA cash flows for a given generation profile.

    nameplate_share: fraction of 100 MW nameplate (e.g. 0.5 for 50 MW site).
    Uses West spot price throughout for shortfall costs and excess sales.
    """
    contract = CONTRACT_MWH * nameplate_share
    ppa_pr   = PPA_PRICE

    shortfall = (contract - gen_mwh).clip(lower=0)
    excess    = (gen_mwh - contract).clip(lower=0)

    ppa_rev    = contract * ppa_pr
    excess_sal = excess * price
    pmf        = (price - ppa_pr).clip(lower=0)
    sf_cost    = shortfall * pmf
    net        = excess_sal - sf_cost
    total      = ppa_rev + net

    return pd.DataFrame({
        "gen_mwh":        gen_mwh.values,
        "shortfall_mwh":  shortfall.values,
        "excess_mwh":     excess.values,
        "ppa_revenue_usd":ppa_rev.values,
        "excess_sales_usd": excess_sal.values,
        "shortfall_cost_usd": sf_cost.values,
        "net_gain_loss_usd": net.values,
        "total_revenue_usd": total.values,
    })


def add_insurance(df: pd.DataFrame, price: pd.Series) -> pd.DataFrame:
    """Add insurance payout column for the 20–30 MWh shortfall layer."""
    out = df.copy()
    insured = (
        out["shortfall_mwh"].clip(lower=INS_LO_MWH, upper=INS_HI_MWH) - INS_LO_MWH
    ).clip(lower=0)
    trigger = (price.values >= INS_PRICE_THR).astype(float)
    pmf     = (price.values - PPA_PRICE).clip(0)
    out["insurance_payout_usd"] = insured * pmf * trigger
    out["insured_total_revenue_usd"] = (
        out["total_revenue_usd"] + out["insurance_payout_usd"]
    )
    return out


# =============================================================================
# MONTHLY / ANNUAL AGGREGATION
# =============================================================================

def monthly_agg(df: pd.DataFrame, rev_col: str, month_col: pd.Series) -> pd.DataFrame:
    tmp = df.copy()
    tmp["month"] = month_col.values
    out = tmp.groupby("month")[rev_col].sum().reset_index()
    out.columns = ["month", "monthly_revenue_usd"]
    out["dscr"] = out["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE
    return out


def annual_agg(df: pd.DataFrame, rev_col: str, year_col: pd.Series) -> pd.DataFrame:
    tmp = df.copy()
    tmp["year"] = year_col.values
    out = tmp.groupby("year")[rev_col].sum().reset_index()
    out.columns = ["year", "annual_revenue_usd"]
    out["dscr"] = out["annual_revenue_usd"] / ANNUAL_DEBT_SERVICE
    return out


def print_dscr_comparison(label: str, base_dscr: pd.Series, strat_dscr: pd.Series):
    print(f"\n  {label}")
    for thr in DSCR_THRESHOLDS:
        b = (base_dscr < thr).mean() * 100
        s = (strat_dscr < thr).mean() * 100
        print(f"    P(DSCR<{thr:.1f}): baseline={b:.1f}%  strategy={s:.1f}%")
    print(f"    Mean: baseline={base_dscr.mean():.3f}  strategy={strat_dscr.mean():.3f}")
    print(f"    Min : baseline={base_dscr.min():.3f}  strategy={strat_dscr.min():.3f}")


def revenue_summary(label: str, monthly_rev: pd.Series):
    print(f"\n  {label}")
    print(f"    Mean monthly : ${monthly_rev.mean():,.0f}")
    print(f"    Std monthly  : ${monthly_rev.std(ddof=1):,.0f}")
    print(f"    CV monthly   : {monthly_rev.std(ddof=1)/monthly_rev.mean():.4f}")
    print(f"    95% VaR      : ${np.percentile(monthly_rev, 5):,.0f}")
    print(f"    99% VaR      : ${np.percentile(monthly_rev, 1):,.0f}")


# =============================================================================
# FIGURES
# =============================================================================

def month_dt(monthly: pd.DataFrame) -> pd.Series:
    return monthly["month"].apply(
        lambda p: pd.Period(p, "M").to_timestamp()
        if not isinstance(p, pd.Timestamp) else p
    )


def save_comparison_figures(
    fig_dir: Path,
    monthly_base: pd.DataFrame,
    monthly_strat: pd.DataFrame,
    annual_base: pd.DataFrame,
    annual_strat: pd.DataFrame,
    strategy_label: str,
    prefix: str,
):
    dt = month_dt(monthly_base)

    # Monthly revenue
    fig, ax = plt.subplots(figsize=(13, 6))
    ax.plot(dt, monthly_base["monthly_revenue_usd"],  linewidth=1.5, label="Baseline (100 MW West)")
    ax.plot(dt, monthly_strat["monthly_revenue_usd"], linewidth=1.5, label=strategy_label)
    ax.axhline(MONTHLY_DEBT_SERVICE, linestyle="--", color="red", linewidth=1.2,
               label=f"Debt service ${MONTHLY_DEBT_SERVICE:,.0f}")
    ax.set_title(f"Monthly Revenue: Baseline vs {strategy_label}")
    ax.set_ylabel("Monthly Revenue ($)")
    ax.yaxis.set_major_formatter(currency_fmt)
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{prefix}_monthly_revenue.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Monthly DSCR
    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(dt, monthly_base["dscr"],  linewidth=1.5, label="Baseline DSCR")
    ax.plot(dt, monthly_strat["dscr"], linewidth=1.5, label=f"{strategy_label} DSCR")
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_title(f"Monthly DSCR: Baseline vs {strategy_label}")
    ax.set_ylabel("DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(fig_dir / f"{prefix}_monthly_dscr.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Annual DSCR
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(annual_base))
    w = 0.35
    ax.bar(x - w/2, annual_base["dscr"],  w, label="Baseline", alpha=0.8)
    ax.bar(x + w/2, annual_strat["dscr"], w, label=strategy_label, alpha=0.8)
    for thr, col in zip([1.0, 1.2, 1.3], ["red", "orange", "gold"]):
        ax.axhline(thr, linestyle="--", color=col, linewidth=1.1,
                   label=f"DSCR = {thr:.1f}x")
    ax.set_xticks(x)
    ax.set_xticklabels(annual_base["year"].astype(str))
    ax.set_title(f"Annual DSCR: Baseline vs {strategy_label}")
    ax.set_ylabel("Annual DSCR")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(fig_dir / f"{prefix}_annual_dscr.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"  Figures saved: {prefix}_*.png")


# =============================================================================
# STRATEGY 1: Cross-zone diversification (West + South)
# =============================================================================

def strategy1_cross_zone(west: pd.DataFrame, south: pd.DataFrame,
                          fig_dir: Path) -> tuple:
    print("\n[Strategy 1] Cross-zone diversification (50 MW West + 50 MW South)...")

    merged = west.merge(
        south[[TS_COL, CF_COL]].rename(columns={CF_COL: "cf_south"}),
        on=TS_COL, how="inner"
    )
    merged["month"] = west.set_index(TS_COL).loc[merged[TS_COL], "month"].values
    merged["year"]  = west.set_index(TS_COL).loc[merged[TS_COL], "year"].values

    price = merged[PRICE_COL]

    # Baseline
    base_gen = merged[CF_COL].clip(lower=0) * BASELINE_MW
    base_rev = ppa_revenue(base_gen, price, nameplate_share=1.0)

    # Diversified: combined generation (West 50MW + South 50MW), valued at West price
    div_gen = (
        merged[CF_COL].clip(lower=0)     * WEST_DIV_MW
        + merged["cf_south"].clip(lower=0) * SOUTH_DIV_MW
    )
    div_rev = ppa_revenue(div_gen, price, nameplate_share=1.0)

    # Monthly
    m_base = monthly_agg(base_rev, "total_revenue_usd", merged["month"])
    m_div  = monthly_agg(div_rev,  "total_revenue_usd", merged["month"])

    revenue_summary("Baseline",      m_base["monthly_revenue_usd"])
    revenue_summary("Cross-zone div", m_div["monthly_revenue_usd"])
    print_dscr_comparison("Monthly DSCR", m_base["dscr"], m_div["dscr"])

    # Annual
    a_base = annual_agg(base_rev, "total_revenue_usd", merged["year"])
    a_div  = annual_agg(div_rev,  "total_revenue_usd", merged["year"])
    print_dscr_comparison("Annual DSCR", a_base["dscr"], a_div["dscr"])

    # Save
    m_combined = m_base.rename(columns={"monthly_revenue_usd": "baseline_revenue_usd",
                                        "dscr": "baseline_dscr"})
    m_combined["diversified_revenue_usd"] = m_div["monthly_revenue_usd"].values
    m_combined["diversified_dscr"]        = m_div["dscr"].values
    m_combined.to_csv(OUTPUT_DIR / "diversification_cross_zone_monthly.csv", index=False)

    a_combined = a_base.rename(columns={"annual_revenue_usd": "baseline_revenue_usd",
                                        "dscr": "baseline_dscr"})
    a_combined["diversified_revenue_usd"] = a_div["annual_revenue_usd"].values
    a_combined["diversified_dscr"]        = a_div["dscr"].values
    a_combined.to_csv(OUTPUT_DIR / "diversification_cross_zone_annual_dscr.csv", index=False)

    save_comparison_figures(fig_dir, m_base, m_div, a_base, a_div,
                            "50 MW West + 50 MW South",
                            "diversification_cross_zone")

    return merged, base_rev, div_rev


# =============================================================================
# STRATEGY 2: Cross-zone diversification + insurance
# =============================================================================

def strategy2_cross_zone_insurance(merged: pd.DataFrame,
                                   base_rev: pd.DataFrame,
                                   div_rev: pd.DataFrame,
                                   fig_dir: Path):
    print("\n[Strategy 2] Cross-zone diversification + insurance...")

    price = merged[PRICE_COL]

    # Add insurance to diversified case
    div_ins = add_insurance(div_rev, price)

    # Premium = 1.30 × mean monthly payout
    tmp = div_ins.copy()
    tmp["month"] = merged["month"].values
    monthly_payouts = tmp.groupby("month")["insurance_payout_usd"].sum()
    monthly_premium = float(monthly_payouts.mean()) * LOADING
    annual_premium  = monthly_premium * 12

    print(f"  Expected annual payout : ${monthly_payouts.sum() / len(monthly_payouts) * 12:,.0f}")
    print(f"  Annual premium         : ${annual_premium:,.0f}")
    print(f"  Monthly premium        : ${monthly_premium:,.0f}")

    # Insured net of premium
    div_ins["insured_net_revenue_usd"] = (
        div_ins["insured_total_revenue_usd"] - monthly_premium / (
            merged.groupby("month")[TS_COL].transform("count")
        )
    )

    # Monthly
    m_base = monthly_agg(base_rev, "total_revenue_usd", merged["month"])
    m_ins  = monthly_agg(div_ins,  "insured_total_revenue_usd", merged["month"])
    m_ins["monthly_revenue_usd"] = (
        m_ins["monthly_revenue_usd"] - monthly_premium
    )
    m_ins["dscr"] = m_ins["monthly_revenue_usd"] / MONTHLY_DEBT_SERVICE

    print_dscr_comparison("Monthly DSCR", m_base["dscr"], m_ins["dscr"])

    # Annual
    a_base = annual_agg(base_rev, "total_revenue_usd", merged["year"])

    tmp2 = div_ins.copy()
    tmp2["year"] = merged["year"].values
    a_ins = tmp2.groupby("year")["insured_total_revenue_usd"].sum().reset_index()
    a_ins.columns = ["year", "annual_revenue_usd"]
    a_ins["annual_revenue_usd"] -= annual_premium
    a_ins["dscr"] = a_ins["annual_revenue_usd"] / ANNUAL_DEBT_SERVICE

    print_dscr_comparison("Annual DSCR", a_base["dscr"], a_ins["dscr"])

    # Save
    m_comb = m_base.rename(columns={"monthly_revenue_usd": "baseline_revenue_usd",
                                    "dscr": "baseline_dscr"})
    m_comb["diversified_insured_revenue_usd"] = m_ins["monthly_revenue_usd"].values
    m_comb["diversified_insured_dscr"]        = m_ins["dscr"].values
    m_comb.to_csv(OUTPUT_DIR / "diversification_cross_zone_insurance_monthly.csv",
                  index=False)

    a_comb = a_base.rename(columns={"annual_revenue_usd": "baseline_revenue_usd",
                                    "dscr": "baseline_dscr"})
    a_comb["diversified_insured_revenue_usd"] = a_ins["annual_revenue_usd"].values
    a_comb["diversified_insured_dscr"]        = a_ins["dscr"].values
    a_comb.to_csv(OUTPUT_DIR / "diversification_cross_zone_insurance_annual_dscr.csv",
                  index=False)

    save_comparison_figures(fig_dir, m_base, m_ins, a_base, a_ins,
                            "50 MW West + 50 MW South + Insurance",
                            "diversification_cross_zone_insurance")


# =============================================================================
# STRATEGY 3: Within-zone diversification (two West sites)
# =============================================================================

def strategy3_within_zone(west1: pd.DataFrame, west2: pd.DataFrame,
                           fig_dir: Path):
    print("\n[Strategy 3] Within-zone diversification (50 MW West site 1 + 50 MW West site 2)...")
    print("  Note: same load zone — cells experience correlated meteorology.")

    merged = west1.merge(
        west2[[TS_COL, CF_COL]].rename(columns={CF_COL: "cf_west2"}),
        on=TS_COL, how="inner"
    )
    merged["month"] = west1.set_index(TS_COL).loc[merged[TS_COL], "month"].values
    merged["year"]  = west1.set_index(TS_COL).loc[merged[TS_COL], "year"].values

    price = merged[PRICE_COL]

    # Baseline
    base_gen = merged[CF_COL].clip(lower=0) * BASELINE_MW
    base_rev = ppa_revenue(base_gen, price, nameplate_share=1.0)

    # Within-zone diversified
    div_gen = (
        merged[CF_COL].clip(lower=0)      * WEST_DIV_MW
        + merged["cf_west2"].clip(lower=0) * WEST2_DIV_MW
    )
    div_rev = ppa_revenue(div_gen, price, nameplate_share=1.0)

    # CF correlation between the two West sites
    corr = np.corrcoef(
        merged[CF_COL].clip(lower=0).values,
        merged["cf_west2"].clip(lower=0).values
    )[0, 1]
    print(f"  CF correlation between West sites: {corr:.4f}")

    # Monthly
    m_base = monthly_agg(base_rev, "total_revenue_usd", merged["month"])
    m_div  = monthly_agg(div_rev,  "total_revenue_usd", merged["month"])

    revenue_summary("Baseline",          m_base["monthly_revenue_usd"])
    revenue_summary("Within-zone div",   m_div["monthly_revenue_usd"])
    print_dscr_comparison("Monthly DSCR", m_base["dscr"], m_div["dscr"])

    # Annual
    a_base = annual_agg(base_rev, "total_revenue_usd", merged["year"])
    a_div  = annual_agg(div_rev,  "total_revenue_usd", merged["year"])
    print_dscr_comparison("Annual DSCR", a_base["dscr"], a_div["dscr"])

    # Save
    m_comb = m_base.rename(columns={"monthly_revenue_usd": "baseline_revenue_usd",
                                    "dscr": "baseline_dscr"})
    m_comb["within_zone_revenue_usd"] = m_div["monthly_revenue_usd"].values
    m_comb["within_zone_dscr"]        = m_div["dscr"].values
    m_comb.to_csv(OUTPUT_DIR / "diversification_within_zone_monthly.csv", index=False)

    save_comparison_figures(fig_dir, m_base, m_div, a_base, a_div,
                            "50 MW West Site 1 + 50 MW West Site 2",
                            "diversification_within_zone")


# =============================================================================
# MAIN
# =============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig_dir = OUTPUT_DIR / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    for path in [WEST_FILE_1, SOUTH_FILE, WEST_FILE_2]:
        if not path.exists():
            raise FileNotFoundError(
                f"Sample file not found: {path}\n"
                "Check DATA_DIR in the configuration block."
            )

    print("Risk Management Analysis — Geographic Diversification")
    print("=" * 60)
    print(f"Baseline     : 100 MW at West cell 6_23")
    print(f"Strategy 1   : 50 MW West (6_23) + 50 MW South (38_37)")
    print(f"Strategy 2   : Strategy 1 + insurance (20–30 MWh layer, price ≥ ${INS_PRICE_THR})")
    print(f"Strategy 3   : 50 MW West site 1 (6_23) + 50 MW West site 2 (2_22)")
    print(f"PPA          : {CONTRACT_MWH} MWh/hour at ${PPA_PRICE}/MWh")
    print(f"Debt service : ${MONTHLY_DEBT_SERVICE:,.0f}/month")
    print(f"Output       : {OUTPUT_DIR}/")
    print("=" * 60)

    west1 = load_and_clean(WEST_FILE_1)
    south = load_and_clean(SOUTH_FILE)
    west2 = load_and_clean(WEST_FILE_2)

    merged, base_rev, div_rev = strategy1_cross_zone(west1, south, fig_dir)
    strategy2_cross_zone_insurance(merged, base_rev, div_rev, fig_dir)
    strategy3_within_zone(west1, west2, fig_dir)

    print(f"\nAll outputs saved to: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
