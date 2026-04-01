"""
tests/test_classifier.py + test_recommender.py

Tests for the classification and recommendation engines.
"""

import pytest
import pandas as pd
from datetime import date, timedelta

from pipeline.classifier import classify_transaction, classify_dataframe
from pipeline.recurring import detect_recurring, get_recurring_summary
from analytics.recommender import generate_recommendations
from analytics.health_score import compute_health_score
from analytics.forecaster import forecast_cashflow


# ── Classifier Tests ──────────────────────────────────────────────────────────

class TestClassifyTransaction:
    # Income
    def test_salary_credit(self):
        cat, conf = classify_transaction("Salary Payment Glovo Nigeria", "credit")
        assert cat == "income"
        assert conf >= 0.70

    def test_unmatched_credit_is_income(self):
        cat, conf = classify_transaction("Random incoming payment", "credit")
        assert cat == "income"

    # Food
    def test_shoprite(self):
        cat, _ = classify_transaction("POS PURCHASE AT SHOPRITE LEKKI", "debit")
        assert cat == "food"

    def test_dominos(self):
        cat, _ = classify_transaction("DOMINOS PIZZA VICTORIA ISLAND", "debit")
        assert cat == "food"

    def test_glovo_food(self):
        cat, _ = classify_transaction("GLOVO FOOD DELIVERY ORDER", "debit")
        assert cat == "food"

    # Transport
    def test_uber(self):
        cat, _ = classify_transaction("UBER TRIP - VICTORIA ISLAND LAGOS", "debit")
        assert cat == "transport"

    def test_bolt(self):
        cat, _ = classify_transaction("BOLT RIDE IKEJA TO LEKKI", "debit")
        assert cat == "transport"

    def test_fuel_station(self):
        cat, _ = classify_transaction("POS TOTAL ENERGIES FILLING STATION", "debit")
        assert cat == "transport"

    # Utilities
    def test_ekedc(self):
        cat, _ = classify_transaction("EKEDC PREPAID TOKEN ELECTRICITY", "debit")
        assert cat == "utilities"

    def test_spectranet(self):
        cat, _ = classify_transaction("SPECTRANET INTERNET SUBSCRIPTION", "debit")
        assert cat == "utilities"

    # Subscriptions
    def test_netflix(self):
        cat, _ = classify_transaction("NETFLIX SUBSCRIPTION MONTHLY", "debit")
        assert cat == "subscriptions"

    def test_spotify(self):
        cat, _ = classify_transaction("SPOTIFY PREMIUM MONTHLY", "debit")
        assert cat == "subscriptions"

    def test_chatgpt(self):
        cat, _ = classify_transaction("CHATGPT PLUS SUBSCRIPTION", "debit")
        assert cat == "subscriptions"

    def test_dstv(self):
        cat, _ = classify_transaction("DSTV COMPACT PLUS SUBSCRIPTION", "debit")
        assert cat == "subscriptions"

    # Rent
    def test_rent(self):
        cat, _ = classify_transaction("TRF TO LANDLORD RENT PAYMENT LEKKI", "debit")
        assert cat == "rent"

    # Transfers
    def test_nip_transfer(self):
        cat, _ = classify_transaction("NIP TRF TO CHIMA OBI PERSONAL", "debit")
        assert cat == "transfers"

    def test_opay_transfer(self):
        cat, _ = classify_transaction("OPAY TRANSFER TO JOHN", "debit")
        assert cat == "transfers"

    # Miscellaneous
    def test_unknown_debit_is_misc(self):
        cat, conf = classify_transaction("UNKNOWN RANDOM PAYMENT XYZ123", "debit")
        assert cat == "miscellaneous"
        assert conf == 0.50  # Low confidence


class TestClassifyDataframe:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame([
            {"description": "Salary Payment Glovo",      "tx_type": "credit", "amount": 850000},
            {"description": "POS SHOPRITE LEKKI",         "tx_type": "debit",  "amount": 25000},
            {"description": "NETFLIX SUBSCRIPTION",       "tx_type": "debit",  "amount": 5900},
            {"description": "UBER TRIP VICTORIA ISLAND",  "tx_type": "debit",  "amount": 4200},
            {"description": "NIP TRF TO MAMA ACCOUNT",    "tx_type": "debit",  "amount": 30000},
        ])

    def test_adds_category_column(self, sample_df):
        result = classify_dataframe(sample_df)
        assert "category" in result.columns

    def test_adds_confidence_column(self, sample_df):
        result = classify_dataframe(sample_df)
        assert "confidence" in result.columns

    def test_all_rows_classified(self, sample_df):
        result = classify_dataframe(sample_df)
        assert result["category"].notna().all()

    def test_confidence_in_range(self, sample_df):
        result = classify_dataframe(sample_df)
        assert (result["confidence"] >= 0).all()
        assert (result["confidence"] <= 1).all()


# ── Recurring Detection Tests ─────────────────────────────────────────────────

class TestRecurringDetection:
    @pytest.fixture
    def recurring_df(self):
        """DataFrame with obvious recurring subscriptions."""
        rows = []
        base = date(2026, 1, 1)
        for i in range(3):
            rows.append({
                "tx_date": base + timedelta(days=30 * i),
                "description": "NETFLIX SUBSCRIPTION MONTHLY",
                "amount": 5900,
                "tx_type": "debit",
                "balance": 900000,
            })
        for i in range(3):
            rows.append({
                "tx_date": base + timedelta(days=30 * i + 2),
                "description": "SPOTIFY PREMIUM MONTHLY",
                "amount": 3200,
                "tx_type": "debit",
                "balance": 850000,
            })
        return pd.DataFrame(rows)

    def test_detects_netflix(self, recurring_df):
        result = detect_recurring(recurring_df)
        netflix = result[result["description"].str.contains("NETFLIX", case=False, na=False)]
        assert netflix["is_recurring"].all()

    def test_detects_spotify(self, recurring_df):
        result = detect_recurring(recurring_df)
        spotify = result[result["description"].str.contains("SPOTIFY", case=False, na=False)]
        assert spotify["is_recurring"].all()

    def test_recurring_group_set(self, recurring_df):
        result = detect_recurring(recurring_df)
        recurring = result[result["is_recurring"]]
        assert recurring["recurring_group"].notna().all()

    def test_summary_structure(self, recurring_df):
        result = detect_recurring(recurring_df)
        summary = get_recurring_summary(result)
        assert isinstance(summary, list)
        if summary:
            item = summary[0]
            assert "name" in item
            assert "estimated_monthly" in item
            assert "frequency" in item
            assert "action" in item


# ── Recommender Tests ─────────────────────────────────────────────────────────

class TestRecommender:
    @pytest.fixture
    def healthy_df(self):
        """User with good savings rate."""
        return pd.DataFrame([
            {"tx_type": "credit", "amount": 850000, "category": "income",
             "tx_date": date(2026, 3, 25), "description": "Salary"},
            {"tx_type": "debit",  "amount": 100000, "category": "rent",
             "tx_date": date(2026, 3, 1), "description": "Rent"},
            {"tx_type": "debit",  "amount": 80000,  "category": "food",
             "tx_date": date(2026, 3, 10), "description": "Food"},
            {"tx_type": "debit",  "amount": 50000,  "category": "transport",
             "tx_date": date(2026, 3, 15), "description": "Transport"},
        ])

    @pytest.fixture
    def struggling_df(self):
        """User spending more than they earn."""
        return pd.DataFrame([
            {"tx_type": "credit", "amount": 300000, "category": "income",
             "tx_date": date(2026, 3, 25), "description": "Salary"},
            {"tx_type": "debit",  "amount": 150000, "category": "food",
             "tx_date": date(2026, 3, 10), "description": "Food"},
            {"tx_type": "debit",  "amount": 100000, "category": "rent",
             "tx_date": date(2026, 3, 1), "description": "Rent"},
            {"tx_type": "debit",  "amount": 80000,  "category": "subscriptions",
             "tx_date": date(2026, 3, 5), "description": "Subs"},
            {"tx_type": "debit",  "amount": 50000,  "category": "transport",
             "tx_date": date(2026, 3, 12), "description": "Transport"},
        ])

    def test_returns_list(self, healthy_df):
        recs = generate_recommendations(healthy_df)
        assert isinstance(recs, list)

    def test_each_rec_has_required_fields(self, healthy_df):
        recs = generate_recommendations(healthy_df)
        for rec in recs:
            assert "type" in rec
            assert "title" in rec
            assert "body" in rec
            assert "priority" in rec

    def test_negative_cashflow_alert(self, struggling_df):
        recs = generate_recommendations(struggling_df)
        types = [r["type"] for r in recs]
        # Should have at least a warning
        assert "alert" in types or "warning" in types

    def test_high_subscription_warning(self, struggling_df):
        recs = generate_recommendations(struggling_df)
        titles = " ".join(r["title"].lower() for r in recs)
        assert "subscription" in titles

    def test_empty_df_returns_empty(self):
        recs = generate_recommendations(pd.DataFrame())
        assert recs == []

    def test_priority_sorted(self, struggling_df):
        recs = generate_recommendations(struggling_df)
        priorities = [r["priority"] for r in recs]
        assert priorities == sorted(priorities)


# ── Health Score Tests ────────────────────────────────────────────────────────

class TestHealthScore:
    @pytest.fixture
    def base_df(self):
        return pd.DataFrame([
            {"tx_type": "credit", "amount": 850000, "tx_date": date(2026, 3, 25),
             "category": "income", "description": "Salary"},
            {"tx_type": "debit", "amount": 612000, "tx_date": date(2026, 3, 15),
             "category": "food", "description": "Various expenses"},
        ])

    def test_score_in_range(self, base_df):
        result = compute_health_score(base_df)
        assert 0 <= result["score"] <= 100

    def test_grade_exists(self, base_df):
        result = compute_health_score(base_df)
        assert result["grade"] in ("Excellent", "Good", "Fair", "At Risk")

    def test_breakdown_has_all_dimensions(self, base_df):
        result = compute_health_score(base_df)
        breakdown = result["breakdown"]
        assert "savings_rate" in breakdown
        assert "expense_stability" in breakdown
        assert "debt_ratio" in breakdown
        assert "emergency_coverage" in breakdown
        assert "income_stability" in breakdown

    def test_high_savings_rate_gives_high_score(self):
        df = pd.DataFrame([
            {"tx_type": "credit", "amount": 1_000_000, "tx_date": date(2026, 3, 25),
             "category": "income", "description": "Salary"},
            {"tx_type": "debit",  "amount": 200_000,   "tx_date": date(2026, 3, 15),
             "category": "food", "description": "Expenses"},
        ])
        result = compute_health_score(df, emergency_fund=1_200_000)
        assert result["score"] >= 70

    def test_negative_savings_gives_low_score(self):
        df = pd.DataFrame([
            {"tx_type": "credit", "amount": 300_000, "tx_date": date(2026, 3, 25),
             "category": "income", "description": "Salary"},
            {"tx_type": "debit",  "amount": 500_000, "tx_date": date(2026, 3, 15),
             "category": "food", "description": "Expenses"},
        ])
        result = compute_health_score(df)
        assert result["score"] < 50


# ── Forecaster Tests ──────────────────────────────────────────────────────────

class TestForecaster:
    @pytest.fixture
    def history_df(self):
        """30 days of transaction history."""
        rows = []
        base = date(2026, 3, 1)
        for i in range(30):
            d = base + timedelta(days=i)
            rows.append({"tx_type": "debit",  "amount": 20000, "tx_date": d, "balance": 500000 - i * 5000})
            if i % 30 == 24:  # Salary on 25th
                rows.append({"tx_type": "credit", "amount": 850000, "tx_date": d, "balance": 1000000})
        return pd.DataFrame(rows)

    def test_returns_correct_keys(self, history_df):
        result = forecast_cashflow(history_df)
        assert "daily_forecast" in result
        assert "days_until_low_balance" in result
        assert "expected_month_end_balance" in result

    def test_forecast_length(self, history_df):
        result = forecast_cashflow(history_df, horizon_days=30)
        assert len(result["daily_forecast"]) == 30

    def test_each_row_has_date_and_balance(self, history_df):
        result = forecast_cashflow(history_df)
        for row in result["daily_forecast"]:
            assert "date" in row
            assert "projected_balance" in row
            assert "net_flow" in row

    def test_empty_df(self):
        result = forecast_cashflow(pd.DataFrame())
        assert result["daily_forecast"] == []
