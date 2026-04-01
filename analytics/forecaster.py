"""
analytics/forecaster.py

Cash flow and balance forecaster.
Uses linear trend + seasonality (day-of-week) for short-term predictions.
Estimates "days until low balance" threshold.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd
import numpy as np

from utils.logger import logger


LOW_BALANCE_THRESHOLD = 50_000   # ₦50,000 — flag if forecast dips below this


def forecast_cashflow(
    df: pd.DataFrame,
    current_balance: Optional[float] = None,
    horizon_days: int = 30,
) -> dict:
    """
    Forecast daily cash flow for the next `horizon_days`.

    Args:
        df              : Cleaned transactions DataFrame.
        current_balance : Current account balance (latest balance in statement if None).
        horizon_days    : Number of days to forecast.

    Returns:
        {
            daily_forecast: [{date, net_flow, projected_balance}],
            days_until_low: int | None,
            expected_month_end_balance: float,
            estimated_monthly_income: float,
            estimated_monthly_expenses: float,
        }
    """
    if df.empty:
        return _empty_forecast(horizon_days)

    df = df.copy()
    df["tx_date"] = pd.to_datetime(df["tx_date"])

    # Get starting balance
    if current_balance is None:
        bal_col = df.get("balance")
        if bal_col is not None and bal_col.notna().any():
            current_balance = float(df.sort_values("tx_date")["balance"].dropna().iloc[-1])
        else:
            current_balance = 0.0

    logger.debug(f"Forecasting from balance={current_balance:.0f} for {horizon_days} days.")

    # Build daily history
    daily = _build_daily_history(df)

    if len(daily) < 7:
        # Not enough history — use simple average
        return _simple_forecast(df, current_balance, horizon_days)

    # Estimate daily net flow using rolling average (bias toward recent)
    daily["net_flow"] = daily["credits"] - daily["debits"]
    avg_daily_net = float(daily["net_flow"].ewm(span=14).mean().iloc[-1])

    # Day-of-week seasonal adjustment
    dow_adj = _compute_dow_adjustment(daily)

    # Generate forecast
    today = date.today()
    forecast_rows = []
    balance = current_balance

    for i in range(horizon_days):
        future_date = today + timedelta(days=i + 1)
        dow = future_date.weekday()

        # Apply seasonality
        adj_factor = dow_adj.get(dow, 1.0)
        predicted_net = avg_daily_net * adj_factor

        # Add expected income on payday (assume monthly salary around 25th-31st)
        if future_date.day in range(25, 32) and i < 10:
            predicted_net += _estimate_monthly_income(df) / 30  # Spread over period

        balance += predicted_net
        forecast_rows.append({
            "date":                str(future_date),
            "net_flow":            round(predicted_net, 2),
            "projected_balance":   round(balance, 2),
        })

    # Days until low balance
    days_until_low = None
    for i, row in enumerate(forecast_rows):
        if row["projected_balance"] < LOW_BALANCE_THRESHOLD:
            days_until_low = i + 1
            break

    monthly_income   = float(daily["credits"].mean() * 30)
    monthly_expenses = float(daily["debits"].mean() * 30)

    return {
        "daily_forecast":              forecast_rows,
        "days_until_low_balance":      days_until_low,
        "low_balance_threshold":       LOW_BALANCE_THRESHOLD,
        "current_balance":             round(current_balance, 2),
        "expected_month_end_balance":  round(forecast_rows[-1]["projected_balance"], 2),
        "estimated_monthly_income":    round(monthly_income, 2),
        "estimated_monthly_expenses":  round(monthly_expenses, 2),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_daily_history(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate transactions into a daily credits/debits time series."""
    credits = df[df["tx_type"] == "credit"].groupby("tx_date")["amount"].sum().rename("credits")
    debits  = df[df["tx_type"] == "debit" ].groupby("tx_date")["amount"].sum().rename("debits")

    daily = pd.concat([credits, debits], axis=1).fillna(0)
    daily.index = pd.to_datetime(daily.index)

    # Fill missing days with 0
    full_range = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    daily = daily.reindex(full_range, fill_value=0)

    return daily


def _compute_dow_adjustment(daily: pd.DataFrame) -> dict[int, float]:
    """
    Compute day-of-week multipliers relative to the mean daily net flow.
    Returns {weekday_int: multiplier} where 0=Monday, 6=Sunday.
    """
    daily = daily.copy()
    daily["dow"] = daily.index.dayofweek
    daily["net"] = daily["credits"] - daily["debits"]

    overall_mean = daily["net"].mean()
    if overall_mean == 0:
        return {d: 1.0 for d in range(7)}

    dow_means = daily.groupby("dow")["net"].mean()
    adj = {}
    for dow in range(7):
        adj[dow] = float(dow_means.get(dow, overall_mean) / overall_mean) if overall_mean != 0 else 1.0
        # Clamp to reasonable range
        adj[dow] = max(0.2, min(3.0, adj[dow]))
    return adj


def _estimate_monthly_income(df: pd.DataFrame) -> float:
    """Estimate average monthly income from the transaction history."""
    credits = df[df["tx_type"] == "credit"].copy()
    credits["_month"] = pd.to_datetime(credits["tx_date"]).dt.to_period("M")
    monthly = credits.groupby("_month")["amount"].sum()
    return float(monthly.mean()) if len(monthly) > 0 else 0.0


def _simple_forecast(df: pd.DataFrame, balance: float, days: int) -> dict:
    """Fallback forecast using simple income/expense averages when history is thin."""
    income   = df[df["tx_type"] == "credit"]["amount"].sum()
    expenses = df[df["tx_type"] == "debit"]["amount"].sum()
    net_daily = (income - expenses) / max(len(df["tx_date"].unique()), 1)

    rows = []
    for i in range(days):
        balance += net_daily
        rows.append({
            "date": str(date.today() + timedelta(days=i+1)),
            "net_flow": round(net_daily, 2),
            "projected_balance": round(balance, 2),
        })
    return {
        "daily_forecast": rows,
        "days_until_low_balance": None,
        "low_balance_threshold": LOW_BALANCE_THRESHOLD,
        "current_balance": round(balance - net_daily * days, 2),
        "expected_month_end_balance": round(rows[-1]["projected_balance"], 2),
        "estimated_monthly_income": round(income, 2),
        "estimated_monthly_expenses": round(expenses, 2),
    }


def _empty_forecast(days: int) -> dict:
    return {
        "daily_forecast": [],
        "days_until_low_balance": None,
        "low_balance_threshold": LOW_BALANCE_THRESHOLD,
        "current_balance": 0,
        "expected_month_end_balance": 0,
        "estimated_monthly_income": 0,
        "estimated_monthly_expenses": 0,
    }
