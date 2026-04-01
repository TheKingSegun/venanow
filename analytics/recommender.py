"""
analytics/recommender.py

Rule-based recommendation engine.
Generates specific, quantifiable, Naira-denominated recommendations
based on transaction data, budgets, and behavioral patterns.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
import numpy as np

from utils.currency import fmt_naira
from utils.logger import logger


# ── Recommendation dataclass ──────────────────────────────────────────────────

class Recommendation:
    def __init__(
        self,
        rec_type: str,      # 'warning' | 'opportunity' | 'alert' | 'tip'
        title: str,
        body: str,
        category: Optional[str] = None,
        impact_amount: Optional[float] = None,
        priority: int = 5,  # 1=highest
    ):
        self.type          = rec_type
        self.title         = title
        self.body          = body
        self.category      = category
        self.impact_amount = impact_amount
        self.priority      = priority

    def to_dict(self) -> dict:
        return {
            "type":          self.type,
            "title":         self.title,
            "body":          self.body,
            "category":      self.category,
            "impact_amount": self.impact_amount,
            "priority":      self.priority,
        }


# ── Target benchmarks ─────────────────────────────────────────────────────────

CATEGORY_TARGETS: dict[str, float] = {
    # Maximum recommended % of total income per category
    "food":          0.15,
    "transport":     0.10,
    "subscriptions": 0.05,
    "utilities":     0.08,
    "rent":          0.30,
    "transfers":     0.15,
    "miscellaneous": 0.05,
}

MIN_SAVINGS_RATE    = 0.20   # 20% of income
GOOD_SAVINGS_RATE   = 0.30   # 30% is "good"
EMERGENCY_MONTHS    = 6      # target months of expenses in emergency fund
HIGH_SUB_THRESHOLD  = 0.05   # subscriptions > 5% income = flag


# ── Entry Point ───────────────────────────────────────────────────────────────

def generate_recommendations(
    df: pd.DataFrame,
    budgets: Optional[dict] = None,         # {category_slug: budget_amount}
    emergency_fund: Optional[float] = None, # Current emergency fund balance
    goals: Optional[list[dict]] = None,
) -> list[dict]:
    """
    Generate all recommendations for a user's transaction data.

    Args:
        df             : Classified transactions DataFrame.
        budgets        : Optional budget targets per category.
        emergency_fund : Current emergency fund balance.
        goals          : List of user goal dicts.

    Returns:
        Sorted list of recommendation dicts (priority ascending).
    """
    if df.empty:
        return []

    recs: list[Recommendation] = []

    # Compute key financials
    income     = df[df["tx_type"] == "credit"]["amount"].sum()
    expenses   = df[df["tx_type"] == "debit"]["amount"].sum()
    net        = income - expenses
    sav_rate   = net / income if income > 0 else 0

    # Category spend
    cat_spend: dict[str, float] = {}
    if "category" in df.columns:
        cat_spend = (
            df[df["tx_type"] == "debit"]
            .groupby("category")["amount"]
            .sum()
            .to_dict()
        )

    logger.debug(f"Generating recommendations. Income={income:.0f}, Expenses={expenses:.0f}")

    # ── Run all checks ──────────────────────────────────────────────────────
    recs += _check_savings_rate(sav_rate, net, income)
    recs += _check_negative_cashflow(income, expenses)
    recs += _check_category_overspend(cat_spend, income, budgets)
    recs += _check_subscriptions(df, cat_spend.get("subscriptions", 0), income)
    recs += _check_emergency_fund(emergency_fund, expenses)
    recs += _check_food_habits(df, cat_spend.get("food", 0), income)
    recs += _check_weekend_spending(df, expenses)
    recs += _check_income_stability(df)
    if goals:
        recs += _check_goals(goals, net)

    # Sort by priority, deduplicate on title
    seen = set()
    unique_recs = []
    for r in sorted(recs, key=lambda x: x.priority):
        if r.title not in seen:
            seen.add(r.title)
            unique_recs.append(r.to_dict())

    logger.info(f"Generated {len(unique_recs)} recommendations.")
    return unique_recs


# ── Recommendation Checks ─────────────────────────────────────────────────────

def _check_savings_rate(
    sav_rate: float, net: float, income: float
) -> list[Recommendation]:
    recs = []
    if sav_rate < 0:
        recs.append(Recommendation(
            "alert",
            "⚠ Negative Cash Flow This Month",
            f"You spent {fmt_naira(abs(net))} more than you earned. "
            f"Immediate action needed: identify and cut non-essential spending.",
            priority=1,
            impact_amount=abs(net),
        ))
    elif sav_rate < MIN_SAVINGS_RATE:
        gap_amount = (MIN_SAVINGS_RATE - sav_rate) * income
        recs.append(Recommendation(
            "warning",
            f"↓ Savings Rate Too Low ({sav_rate*100:.1f}%)",
            f"Your savings rate is below the 20% minimum. "
            f"To reach 20%, save an additional {fmt_naira(gap_amount)} this month. "
            f"Start by cutting discretionary spending.",
            priority=2,
            impact_amount=gap_amount,
        ))
    elif sav_rate < GOOD_SAVINGS_RATE:
        gap_amount = (GOOD_SAVINGS_RATE - sav_rate) * income
        recs.append(Recommendation(
            "tip",
            f"↑ Boost Savings to 30% (currently {sav_rate*100:.1f}%)",
            f"You're close to the 30% target. Saving {fmt_naira(gap_amount)} more per month "
            f"would put you in the 'healthy finances' zone.",
            priority=5,
            impact_amount=gap_amount,
        ))
    else:
        recs.append(Recommendation(
            "opportunity",
            f"✓ Strong Savings Rate ({sav_rate*100:.1f}%)",
            f"You're saving above 30% — excellent discipline. "
            f"Consider putting the surplus into a goal fund or investment.",
            priority=8,
        ))
    return recs


def _check_negative_cashflow(income: float, expenses: float) -> list[Recommendation]:
    recs = []
    burn_rate = expenses / income if income > 0 else 1
    if burn_rate >= 0.95 and burn_rate < 1.0:
        recs.append(Recommendation(
            "warning",
            "🔥 High Burn Rate",
            f"You're spending {burn_rate*100:.1f}% of your income. "
            f"At this rate, one unexpected expense could push you negative. "
            f"Build a {fmt_naira(expenses * 0.1)} buffer.",
            priority=2,
            impact_amount=expenses * 0.1,
        ))
    return recs


def _check_category_overspend(
    cat_spend: dict,
    income: float,
    budgets: Optional[dict],
) -> list[Recommendation]:
    recs = []
    if income <= 0:
        return recs

    for cat, target_pct in CATEGORY_TARGETS.items():
        actual = cat_spend.get(cat, 0)
        if actual == 0:
            continue

        actual_pct = actual / income
        target_amt = target_pct * income

        # Check against benchmark
        if actual_pct > target_pct * 1.2:  # 20% over target
            overspend = actual - target_amt
            saving_15pct = actual * 0.15
            recs.append(Recommendation(
                "warning",
                f"↑ Overspending on {cat.title().replace('_', ' ')}",
                f"You spent {fmt_naira(actual)} on {cat} ({actual_pct*100:.1f}% of income). "
                f"Target is {target_pct*100:.0f}%. "
                f"Reducing by 15% saves {fmt_naira(saving_15pct)}/month.",
                category=cat,
                impact_amount=saving_15pct,
                priority=3,
            ))

        # Check against budget if provided
        if budgets and cat in budgets:
            budget = budgets[cat]
            if actual > budget:
                over = actual - budget
                recs.append(Recommendation(
                    "warning",
                    f"Over Budget: {cat.title()}",
                    f"You've exceeded your {cat} budget by {fmt_naira(over)} "
                    f"({fmt_naira(actual)} spent vs {fmt_naira(budget)} budgeted).",
                    category=cat,
                    impact_amount=over,
                    priority=3,
                ))
    return recs


def _check_subscriptions(
    df: pd.DataFrame,
    sub_total: float,
    income: float,
) -> list[Recommendation]:
    recs = []
    if sub_total == 0:
        return recs

    n_subs = 0
    if "is_recurring" in df.columns and "category" in df.columns:
        sub_df = df[(df["category"] == "subscriptions") & (df["is_recurring"])]
        n_subs = sub_df["recurring_group"].nunique() if "recurring_group" in sub_df.columns else 0

    if sub_total / income > HIGH_SUB_THRESHOLD if income > 0 else False:
        recs.append(Recommendation(
            "warning",
            f"↺ Subscription Overload ({n_subs} services, {fmt_naira(sub_total)}/mo)",
            f"Your {n_subs} subscriptions cost {fmt_naira(sub_total)}/month "
            f"({sub_total/income*100:.1f}% of income). "
            f"Cancelling 2 lowest-used could save ~{fmt_naira(sub_total * 0.35)}/month.",
            category="subscriptions",
            impact_amount=sub_total * 0.35,
            priority=3,
        ))
    elif n_subs >= 4:
        recs.append(Recommendation(
            "tip",
            f"📱 {n_subs} Active Subscriptions Detected",
            f"You're paying {fmt_naira(sub_total)}/month across {n_subs} services. "
            f"Review which you use less than once a week.",
            category="subscriptions",
            priority=6,
        ))
    return recs


def _check_emergency_fund(
    emergency_fund: Optional[float],
    monthly_expenses: float,
) -> list[Recommendation]:
    recs = []
    if emergency_fund is None or monthly_expenses <= 0:
        return recs

    target = monthly_expenses * EMERGENCY_MONTHS
    coverage = emergency_fund / monthly_expenses

    if coverage < 1:
        recs.append(Recommendation(
            "alert",
            "🆘 Emergency Fund Critical (< 1 Month Coverage)",
            f"Your emergency fund covers only {coverage:.1f} months of expenses. "
            f"You need {fmt_naira(target - emergency_fund)} more to reach the 6-month target. "
            f"Prioritize this above all non-essential spending.",
            impact_amount=target - emergency_fund,
            priority=2,
        ))
    elif coverage < 3:
        monthly_needed = (target - emergency_fund) / 6
        recs.append(Recommendation(
            "warning",
            f"🛡 Emergency Fund Below Target ({coverage:.1f} of 6 months)",
            f"Target: {fmt_naira(target)}. Current: {fmt_naira(emergency_fund)}. "
            f"Save {fmt_naira(monthly_needed)}/month to close the gap in 6 months.",
            impact_amount=monthly_needed,
            priority=3,
        ))
    elif coverage < EMERGENCY_MONTHS:
        recs.append(Recommendation(
            "tip",
            f"🛡 Keep Building Emergency Fund ({coverage:.1f}/{EMERGENCY_MONTHS} months)",
            f"You're making progress. {fmt_naira(target - emergency_fund)} more to fully fund.",
            priority=7,
        ))
    return recs


def _check_food_habits(
    df: pd.DataFrame,
    food_total: float,
    income: float,
) -> list[Recommendation]:
    recs = []
    if food_total == 0 or "description" not in df.columns:
        return recs

    food_df = df[(df.get("category", "") == "food") & (df["tx_type"] == "debit")]

    # Eating out vs groceries
    eat_out_keywords = ["restaurant", "dominos", "kfc", "chicken republic", "mr biggs",
                         "cafe", "glovo", "chowdeck", "jumia food", "pizza", "fast food"]
    grocery_keywords = ["shoprite", "spar", "market", "grocery", "supermarket", "park n shop"]

    eat_out_amt = food_df[
        food_df["description"].str.lower().str.contains("|".join(eat_out_keywords), na=False)
    ]["amount"].sum()

    grocery_amt = food_df[
        food_df["description"].str.lower().str.contains("|".join(grocery_keywords), na=False)
    ]["amount"].sum()

    if eat_out_amt > 0 and grocery_amt > 0:
        ratio = eat_out_amt / grocery_amt
        if ratio > 2:
            potential_save = eat_out_amt * 0.40
            recs.append(Recommendation(
                "tip",
                f"🍔 Eating Out {ratio:.1f}x More Than Groceries",
                f"You spent {fmt_naira(eat_out_amt)} on restaurants vs {fmt_naira(grocery_amt)} on groceries. "
                f"Cooking 3 extra meals per week could save ~{fmt_naira(potential_save)}/month.",
                category="food",
                impact_amount=potential_save,
                priority=5,
            ))
    return recs


def _check_weekend_spending(df: pd.DataFrame, total_expenses: float) -> list[Recommendation]:
    recs = []
    if "tx_date" not in df.columns:
        return recs

    df = df.copy()
    df["_dow"] = pd.to_datetime(df["tx_date"]).dt.dayofweek
    weekend = df[(df["_dow"] >= 5) & (df["tx_type"] == "debit")]
    weekday = df[(df["_dow"] < 5)  & (df["tx_type"] == "debit")]

    w_spend = weekend["amount"].sum()
    wd_spend = weekday["amount"].sum()

    if total_expenses > 0:
        w_pct = w_spend / total_expenses * 100
        if w_pct > 40:
            recs.append(Recommendation(
                "tip",
                f"📅 Weekend Spending is {w_pct:.0f}% of Total Expenses",
                f"You spend {fmt_naira(w_spend)} on weekends vs {fmt_naira(wd_spend)} on weekdays. "
                f"Plan weekend activities with a spending cap to reduce impulse purchases.",
                priority=6,
                impact_amount=w_spend * 0.15,
            ))
    return recs


def _check_income_stability(df: pd.DataFrame) -> list[Recommendation]:
    recs = []
    credits = df[df["tx_type"] == "credit"].copy()
    if len(credits) < 2:
        return recs

    monthly_income = credits.groupby(
        pd.to_datetime(credits["tx_date"]).dt.to_period("M")
    )["amount"].sum()

    if len(monthly_income) < 2:
        return recs

    cv = monthly_income.std() / monthly_income.mean() if monthly_income.mean() > 0 else 0

    if cv > 0.3:
        recs.append(Recommendation(
            "tip",
            "📊 Variable Income Detected",
            f"Your monthly income varies significantly (CV={cv:.0%}). "
            f"Budget based on your lowest income month ({fmt_naira(monthly_income.min())}) "
            f"to avoid shortfalls.",
            priority=5,
        ))
    return recs


def _check_goals(goals: list[dict], monthly_savings: float) -> list[Recommendation]:
    recs = []
    if monthly_savings <= 0:
        return recs

    total_needed = sum(
        g.get("monthly_contribution", 0) or 0
        for g in goals if not g.get("is_completed")
    )

    if total_needed > monthly_savings:
        gap = total_needed - monthly_savings
        recs.append(Recommendation(
            "warning",
            "◈ Goal Contributions Exceed Monthly Savings",
            f"Your active goals require {fmt_naira(total_needed)}/month, "
            f"but you're only saving {fmt_naira(monthly_savings)}. "
            f"Consider extending goal timelines or reducing non-essential spending by {fmt_naira(gap)}.",
            impact_amount=gap,
            priority=4,
        ))
    return recs
