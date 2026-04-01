"""
pipeline/recurring.py

Subscription and recurring payment detection engine.
Uses frequency analysis and fuzzy merchant grouping to identify:
  - Monthly subscriptions (Netflix, DStv, etc.)
  - Recurring transfers (rent, savings)
  - Regular utility payments
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import numpy as np

from utils.logger import logger


# ── Known Subscription Services ───────────────────────────────────────────────

KNOWN_SUBSCRIPTIONS: dict[str, dict] = {
    "netflix":          {"name": "Netflix",          "type": "entertainment", "action": "review"},
    "spotify":          {"name": "Spotify",          "type": "entertainment", "action": "review"},
    "apple music":      {"name": "Apple Music",      "type": "entertainment", "action": "review"},
    "apple tv":         {"name": "Apple TV+",        "type": "entertainment", "action": "review"},
    "dstv":             {"name": "DStv",             "type": "entertainment", "action": "review"},
    "gotv":             {"name": "GOtv",             "type": "entertainment", "action": "review"},
    "showmax":          {"name": "Showmax",          "type": "entertainment", "action": "review"},
    "starplus":         {"name": "Star+",            "type": "entertainment", "action": "review"},
    "chatgpt":          {"name": "ChatGPT Plus",     "type": "productivity", "action": "keep"},
    "openai":           {"name": "OpenAI",           "type": "productivity", "action": "keep"},
    "adobe":            {"name": "Adobe CC",         "type": "creative",     "action": "review"},
    "canva":            {"name": "Canva Pro",        "type": "creative",     "action": "review"},
    "microsoft 365":    {"name": "Microsoft 365",   "type": "productivity", "action": "keep"},
    "google one":       {"name": "Google One",      "type": "storage",      "action": "keep"},
    "google workspace": {"name": "Google Workspace","type": "productivity", "action": "keep"},
    "amazon prime":     {"name": "Amazon Prime",    "type": "entertainment", "action": "review"},
    "dropbox":          {"name": "Dropbox",         "type": "storage",      "action": "review"},
    "notion":           {"name": "Notion",          "type": "productivity", "action": "keep"},
    "slack":            {"name": "Slack",           "type": "productivity", "action": "keep"},
    "zoom":             {"name": "Zoom",            "type": "productivity", "action": "keep"},
    "linkedin":         {"name": "LinkedIn Premium","type": "professional", "action": "review"},
    "vpn":              {"name": "VPN Service",     "type": "security",     "action": "keep"},
    "antivirus":        {"name": "Antivirus",       "type": "security",     "action": "keep"},
}


# ── Entry Point ───────────────────────────────────────────────────────────────

def detect_recurring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect recurring transactions and annotate the DataFrame.

    Adds columns:
        is_recurring    : bool
        recurring_group : str | None — normalized merchant/group name

    Returns:
        Annotated DataFrame.
    """
    logger.info("Detecting recurring transactions...")

    df = df.copy()
    df["is_recurring"]    = False
    df["recurring_group"] = None

    # Strategy 1: Known subscription name matching
    df = _match_known_subscriptions(df)

    # Strategy 2: Frequency-based detection (same merchant, same amount, regular interval)
    df = _detect_by_frequency(df)

    n_recurring = df["is_recurring"].sum()
    logger.info(f"Recurring detection complete: {n_recurring} recurring transactions found.")
    return df


def get_recurring_summary(df: pd.DataFrame) -> list[dict]:
    """
    Summarize detected recurring payments for display.

    Returns a list of dicts, one per recurring merchant/group:
    {
        name, frequency, estimated_monthly_cost,
        last_charged, next_estimated, action, category
    }
    """
    if not df["is_recurring"].any():
        return []

    recurring = df[df["is_recurring"]].copy()
    groups = recurring.groupby("recurring_group")

    summaries = []
    for group_name, group_df in groups:
        group_df = group_df.sort_values("tx_date")
        amounts   = group_df["amount"].tolist()
        dates     = pd.to_datetime(group_df["tx_date"]).tolist()

        monthly_cost = _estimate_monthly_cost(amounts, dates)
        frequency    = _estimate_frequency(dates)
        last_date    = dates[-1].date() if hasattr(dates[-1], "date") else dates[-1]
        next_date    = _estimate_next_date(dates)

        # Pull action hint from known subscriptions
        action = "review"
        for key, meta in KNOWN_SUBSCRIPTIONS.items():
            if key in group_name.lower():
                action = meta["action"]
                break

        summaries.append({
            "name":                 group_name,
            "frequency":            frequency,
            "estimated_monthly":    round(monthly_cost, 2),
            "last_charged":         str(last_date),
            "next_estimated":       str(next_date) if next_date else "Unknown",
            "action":               action,
            "occurrence_count":     len(group_df),
            "amount_variation":     round(float(np.std(amounts)), 2),
        })

    # Sort by monthly cost descending
    summaries.sort(key=lambda x: x["estimated_monthly"], reverse=True)
    return summaries


# ── Strategy 1: Known Subscription Match ─────────────────────────────────────

def _match_known_subscriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Flag transactions matching known subscription service names."""
    for keyword, meta in KNOWN_SUBSCRIPTIONS.items():
        mask = df["description"].str.lower().str.contains(keyword, regex=False, na=False)
        if mask.any():
            df.loc[mask, "is_recurring"]    = True
            df.loc[mask, "recurring_group"] = meta["name"]
    return df


# ── Strategy 2: Frequency-based Detection ────────────────────────────────────

def _detect_by_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group transactions by normalized merchant name + amount bucket.
    Flag as recurring if: appears ≥2 times with ≥20 day intervals.
    """
    debits = df[df["tx_type"] == "debit"].copy()
    if len(debits) == 0:
        return df

    # Normalize merchant key for grouping
    debits["_merchant_key"] = debits["description"].apply(_normalize_merchant_key)

    # Group by merchant key + rounded amount (within 5%)
    debits["_amt_bucket"] = debits["amount"].apply(lambda a: round(a / 100) * 100)

    groups = debits.groupby(["_merchant_key", "_amt_bucket"])

    for (merchant, _), group in groups:
        if len(group) < 2:
            continue

        dates = pd.to_datetime(group["tx_date"]).sort_values()
        gaps  = dates.diff().dropna().dt.days.tolist()

        # Check for regular intervals: weekly (7±3), biweekly (14±5), monthly (30±10)
        if _is_regular_interval(gaps):
            idx = group.index
            df.loc[idx, "is_recurring"]    = True
            df.loc[idx, "recurring_group"] = _title_case_merchant(merchant)

    return df


def _normalize_merchant_key(desc: str) -> str:
    """Reduce a description to its core merchant identifier."""
    desc = desc.lower()
    # Remove common noise words
    noise = r"\b(pos|atm|trf|transfer|payment|purchase|via|from|to|online|card|web|mobile)\b"
    desc = re.sub(noise, "", desc)
    # Remove numbers and special chars
    desc = re.sub(r"[^a-z\s]", "", desc)
    # Collapse whitespace
    return " ".join(desc.split())[:40]


def _title_case_merchant(key: str) -> str:
    return key.title().strip()


def _is_regular_interval(gaps: list[float]) -> bool:
    """
    Return True if the gap pattern matches weekly, biweekly, or monthly cycles.
    Allows ±30% tolerance to handle billing date drift.
    """
    if not gaps:
        return False
    avg_gap = np.mean(gaps)

    INTERVALS = [7, 14, 28, 30, 31, 90, 365]
    for interval in INTERVALS:
        if abs(avg_gap - interval) / interval <= 0.35:
            # Also check that std dev is not too high
            if np.std(gaps) <= interval * 0.4:
                return True
    return False


# ── Date & Cost Estimators ────────────────────────────────────────────────────

def _days_mean_gap(dates: list) -> float:
    s = pd.Series(pd.to_datetime(dates)).sort_values().reset_index(drop=True)
    diffs = s.diff().dropna()
    days = diffs.dt.total_seconds() / 86400
    return float(days.mean())


def _estimate_monthly_cost(amounts: list[float], dates: list) -> float:
    """
    Convert recurring cost to a monthly equivalent.
    If weekly → multiply by 4.33. If biweekly × 2. Monthly → as-is.
    """
    avg_amount = np.mean(amounts)
    if len(dates) < 2:
        return avg_amount

    avg_gap = _days_mean_gap(dates)

    if avg_gap <= 10:      # Weekly
        return avg_amount * 4.33
    elif avg_gap <= 20:    # Biweekly
        return avg_amount * 2.0
    elif avg_gap <= 40:    # Monthly
        return avg_amount
    elif avg_gap <= 100:   # Quarterly
        return avg_amount / 3.0
    else:                  # Annual
        return avg_amount / 12.0


def _estimate_frequency(dates: list) -> str:
    """Return human-readable frequency string."""
    if len(dates) < 2:
        return "Unknown"
    avg_gap = _days_mean_gap(dates)

    if avg_gap <= 9:    return "Weekly"
    if avg_gap <= 19:   return "Bi-weekly"
    if avg_gap <= 40:   return "Monthly"
    if avg_gap <= 100:  return "Quarterly"
    return "Annual"


def _estimate_next_date(dates: list) -> Optional[date]:
    """Estimate the next charge date based on the average interval."""
    if len(dates) < 2:
        return None
    s = pd.Series(pd.to_datetime(dates)).sort_values().reset_index(drop=True)
    avg_gap = _days_mean_gap(dates)
    last = s.iloc[-1].date()
    return last + timedelta(days=int(avg_gap))
