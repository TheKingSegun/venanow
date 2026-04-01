"""
analytics/health_score.py

Financial Health Score engine (0–100).
Composite score weighted across 5 dimensions.
"""

from __future__ import annotations

from typing import Optional
import pandas as pd
import numpy as np

from utils.logger import logger


# ── Scoring weights ───────────────────────────────────────────────────────────
WEIGHTS = {
    "savings_rate":        0.30,
    "expense_stability":   0.20,
    "debt_ratio":          0.20,
    "emergency_coverage":  0.20,
    "income_stability":    0.10,
}


def compute_health_score(
    df: pd.DataFrame,
    emergency_fund: float = 0.0,
    total_debt: float = 0.0,
) -> dict:
    """
    Compute the composite financial health score.

    Args:
        df             : Classified transactions DataFrame.
        emergency_fund : Total emergency fund balance.
        total_debt     : Total outstanding debt (loans, credit cards).

    Returns:
        {
            score: int (0-100),
            grade: str ('Excellent'|'Good'|'Fair'|'At Risk'),
            breakdown: {dimension: {score, weight, value, label}},
            insights: [str],
        }
    """
    income   = df[df["tx_type"] == "credit"]["amount"].sum()
    expenses = df[df["tx_type"] == "debit"]["amount"].sum()
    net      = income - expenses

    breakdown = {}
    insights  = []

    # ── 1. Savings Rate ───────────────────────────────────────────────────────
    sav_rate = net / income if income > 0 else 0
    sav_score = _score_savings_rate(sav_rate)
    breakdown["savings_rate"] = {
        "score": sav_score, "weight": WEIGHTS["savings_rate"],
        "value": round(sav_rate * 100, 1), "label": f"{sav_rate*100:.1f}% savings rate",
    }
    if sav_rate < 0.10:
        insights.append("Critical: savings rate below 10%. Immediate budget review needed.")
    elif sav_rate < 0.20:
        insights.append("Savings rate under 20% — increase by reducing discretionary spending.")

    # ── 2. Expense Stability ─────────────────────────────────────────────────
    stab_score, cv = _score_expense_stability(df)
    breakdown["expense_stability"] = {
        "score": stab_score, "weight": WEIGHTS["expense_stability"],
        "value": round(cv * 100, 1), "label": f"CV={cv*100:.0f}% expense variability",
    }
    if cv > 0.4:
        insights.append("High expense variability — spending spikes are hurting your stability score.")

    # ── 3. Debt Ratio ─────────────────────────────────────────────────────────
    debt_ratio = total_debt / income if income > 0 and total_debt > 0 else 0
    debt_score = _score_debt_ratio(debt_ratio)
    breakdown["debt_ratio"] = {
        "score": debt_score, "weight": WEIGHTS["debt_ratio"],
        "value": round(debt_ratio * 100, 1), "label": f"{debt_ratio*100:.0f}% debt-to-income",
    }
    if debt_ratio > 0.40:
        insights.append("High debt ratio. Prioritize debt repayment before new goals.")

    # ── 4. Emergency Fund Coverage ────────────────────────────────────────────
    monthly_essentials = _estimate_essential_expenses(df)
    ef_months = emergency_fund / monthly_essentials if monthly_essentials > 0 else 0
    ef_score = _score_emergency_fund(ef_months)
    breakdown["emergency_coverage"] = {
        "score": ef_score, "weight": WEIGHTS["emergency_coverage"],
        "value": round(ef_months, 1), "label": f"{ef_months:.1f} months covered",
    }
    if ef_months < 3:
        insights.append(f"Emergency fund covers only {ef_months:.1f} months. Target is 6.")

    # ── 5. Income Stability ───────────────────────────────────────────────────
    inc_score, inc_cv = _score_income_stability(df)
    breakdown["income_stability"] = {
        "score": inc_score, "weight": WEIGHTS["income_stability"],
        "value": round(inc_cv * 100, 1), "label": f"CV={inc_cv*100:.0f}% income variability",
    }
    if inc_cv > 0.3:
        insights.append("Variable income detected. Build a larger buffer for low-income months.")

    # ── Composite Score ───────────────────────────────────────────────────────
    composite = sum(
        breakdown[dim]["score"] * weight
        for dim, weight in WEIGHTS.items()
    )
    composite = max(0, min(100, round(composite)))

    if composite >= 80:
        grade = "Excellent"
    elif composite >= 65:
        grade = "Good"
    elif composite >= 45:
        grade = "Fair"
    else:
        grade = "At Risk"

    if not insights:
        insights.append("Your finances are on a healthy trajectory. Keep it up!")

    logger.info(f"Health score computed: {composite}/100 ({grade})")

    return {
        "score":     composite,
        "grade":     grade,
        "breakdown": breakdown,
        "insights":  insights,
    }


# ── Individual Scorers ────────────────────────────────────────────────────────

def _score_savings_rate(rate: float) -> float:
    """0-100 score for savings rate. 30%+ = 100."""
    if rate >= 0.30:  return 100
    if rate >= 0.20:  return 80 + (rate - 0.20) / 0.10 * 20
    if rate >= 0.10:  return 50 + (rate - 0.10) / 0.10 * 30
    if rate >= 0:     return rate / 0.10 * 50
    return 0  # Negative savings


def _score_expense_stability(df: pd.DataFrame) -> tuple[float, float]:
    """Score based on coefficient of variation of weekly expenses."""
    debits = df[df["tx_type"] == "debit"].copy()
    if len(debits) < 4:
        return 70, 0.0  # Not enough data — neutral score

    debits["_week"] = pd.to_datetime(debits["tx_date"]).dt.to_period("W")
    weekly = debits.groupby("_week")["amount"].sum()
    cv = weekly.std() / weekly.mean() if weekly.mean() > 0 else 0

    if cv <= 0.15:   score = 100
    elif cv <= 0.30: score = 80 - (cv - 0.15) / 0.15 * 20
    elif cv <= 0.50: score = 60 - (cv - 0.30) / 0.20 * 30
    else:            score = max(0, 30 - (cv - 0.50) * 60)

    return round(score, 1), round(cv, 4)


def _score_debt_ratio(ratio: float) -> float:
    """Score for debt-to-income ratio. 0 debt = 100."""
    if ratio == 0:    return 100
    if ratio <= 0.15: return 90
    if ratio <= 0.30: return 70 - (ratio - 0.15) / 0.15 * 20
    if ratio <= 0.50: return 50 - (ratio - 0.30) / 0.20 * 30
    return max(0, 20 - (ratio - 0.50) * 40)


def _score_emergency_fund(months_covered: float) -> float:
    """Score for emergency fund coverage. 6+ months = 100."""
    if months_covered >= 6:  return 100
    if months_covered >= 3:  return 60 + (months_covered - 3) / 3 * 40
    if months_covered >= 1:  return 25 + (months_covered - 1) / 2 * 35
    return max(0, months_covered * 25)


def _score_income_stability(df: pd.DataFrame) -> tuple[float, float]:
    """Score based on monthly income variability."""
    credits = df[df["tx_type"] == "credit"].copy()
    if len(credits) < 2:
        return 70, 0.0

    credits["_month"] = pd.to_datetime(credits["tx_date"]).dt.to_period("M")
    monthly = credits.groupby("_month")["amount"].sum()
    if len(monthly) < 2:
        return 70, 0.0

    cv = monthly.std() / monthly.mean() if monthly.mean() > 0 else 0

    if cv <= 0.10:   score = 100
    elif cv <= 0.25: score = 85
    elif cv <= 0.40: score = 65
    else:            score = max(30, 65 - (cv - 0.40) * 100)

    return round(score, 1), round(cv, 4)


def _estimate_essential_expenses(df: pd.DataFrame) -> float:
    """
    Estimate monthly essential expenses (rent, utilities, food, transport).
    Used for emergency fund coverage calculation.
    """
    essential_cats = {"rent", "utilities", "food", "transport"}
    debits = df[df["tx_type"] == "debit"]

    if "category" in debits.columns:
        essential = debits[debits["category"].isin(essential_cats)]["amount"].sum()
    else:
        essential = debits["amount"].sum() * 0.60  # Assume 60% are essentials

    # Normalize to one month
    if "tx_date" in df.columns:
        dates = pd.to_datetime(df["tx_date"])
        n_months = max(1, (dates.max() - dates.min()).days / 30)
        essential = essential / n_months

    return round(essential, 2)
