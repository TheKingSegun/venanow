"""
tests/test_ingestion.py

Unit tests for the ingestion and cleaning pipeline.
Run with: pytest tests/ -v
"""

import pytest
import io
import csv
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.ingestion import ingest_statement, _parse_amount_with_type, _clean_description
from pipeline.cleaner import clean_transactions, validate_dataframe
from utils.currency import (
    parse_naira, fmt_naira, detect_bank, detect_channel,
    extract_merchant, sniff_bank_profile
)


# ── Currency utils ────────────────────────────────────────────────────────────

class TestParseNaira:
    def test_plain_number(self):
        assert parse_naira("18600") == 18600.0

    def test_comma_formatted(self):
        assert parse_naira("1,234,567.50") == 1234567.50

    def test_naira_symbol(self):
        assert parse_naira("₦18,600.00") == 18600.0

    def test_dr_suffix(self):
        assert parse_naira("18,600.00 DR") == 18600.0

    def test_cr_suffix(self):
        assert parse_naira("18,600.00CR") == 18600.0

    def test_negative(self):
        assert parse_naira("-18600") == -18600.0

    def test_empty(self):
        assert parse_naira("") == 0.0

    def test_invalid(self):
        assert parse_naira("N/A") == 0.0


class TestFmtNaira:
    def test_basic(self):
        assert fmt_naira(1234567.5) == "₦1,234,567.50"

    def test_zero(self):
        assert fmt_naira(0) == "₦0.00"

    def test_with_sign_positive(self):
        assert fmt_naira(5000, show_sign=True) == "+₦5,000.00"

    def test_with_sign_negative(self):
        assert fmt_naira(-5000, show_sign=True) == "-₦5,000.00"


class TestDetectBank:
    def test_gtbank(self):
        assert detect_bank("TRF FROM GTBank - JOHN DOE") == "GTBank"

    def test_zenith(self):
        assert detect_bank("ZENITH BANK TRANSFER") == "Zenith"

    def test_opay(self):
        assert detect_bank("OPAY WALLET TRANSFER") == "OPay"

    def test_palmpay(self):
        assert detect_bank("PALMPAY PAYMENT") == "PalmPay"

    def test_kuda(self):
        assert detect_bank("KUDA BANK TRANSFER") == "Kuda"

    def test_unknown(self):
        assert detect_bank("RANDOM PAYMENT DESCRIPTION") is None


class TestDetectChannel:
    def test_pos(self):
        assert detect_channel("POS PURCHASE AT SHOPRITE") == "POS"

    def test_atm(self):
        assert detect_channel("ATM WITHDRAWAL AT GTB BRANCH") == "ATM"

    def test_transfer(self):
        assert detect_channel("NIP TRF TO JOHN DOE") == "Transfer"

    def test_online(self):
        assert detect_channel("NETFLIX SUBSCRIPTION PAYMENT") == "Card Online"

    def test_ussd(self):
        assert detect_channel("USSD AIRTIME PURCHASE") == "USSD"

    def test_default(self):
        assert detect_channel("SOME RANDOM DESCRIPTION") == "Bank Transfer"


class TestExtractMerchant:
    def test_pos_prefix(self):
        result = extract_merchant("POS PURCHASE AT SHOPRITE LEKKI/REF12345678")
        assert "Shoprite" in result or "shoprite" in result.lower()

    def test_transfer(self):
        result = extract_merchant("TRF TO JOHN DOE SAVINGS")
        assert "John" in result or "john" in result.lower()


# ── Amount Parsing ────────────────────────────────────────────────────────────

class TestParseAmountWithType:
    def test_negative_is_debit(self):
        amt, tx_type = _parse_amount_with_type("-18600.00")
        assert amt == 18600.0
        assert tx_type == "debit"

    def test_cr_suffix(self):
        amt, tx_type = _parse_amount_with_type("18600.00 CR")
        assert amt == 18600.0
        assert tx_type == "credit"

    def test_dr_suffix(self):
        amt, tx_type = _parse_amount_with_type("18600.00 DR")
        assert tx_type == "debit"

    def test_plain_positive_defaults_debit(self):
        amt, tx_type = _parse_amount_with_type("5000")
        assert tx_type == "debit"


# ── Description Cleaning ──────────────────────────────────────────────────────

class TestCleanDescription:
    def test_removes_long_numbers(self):
        result = _clean_description("SHOPRITE LEKKI 123456789012")
        assert "123456789012" not in result
        assert "SHOPRITE" in result

    def test_removes_pipe_suffix(self):
        result = _clean_description("UBER TRIP|SESSION12345")
        assert "SESSION12345" not in result

    def test_empty(self):
        assert _clean_description("") == ""

    def test_truncates_long(self):
        long_desc = "A" * 300
        assert len(_clean_description(long_desc)) <= 255


# ── Pipeline Integration ──────────────────────────────────────────────────────

@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a minimal GTBank-style CSV for testing."""
    csv_data = [
        ["Date", "Details", "Debit", "Credit", "Balance"],
        ["01/01/2026", "Salary Payment Glovo",        "",        "850000", "1000000"],
        ["03/01/2026", "POS SHOPRITE LEKKI",           "25000",  "",       "975000"],
        ["05/01/2026", "NETFLIX SUBSCRIPTION",          "5900",  "",       "969100"],
        ["07/01/2026", "UBER TRIP VICTORIA ISLAND",     "4200",  "",       "964900"],
        ["10/01/2026", "EKEDC PREPAID TOKEN",           "15000", "",       "949900"],
        ["15/01/2026", "NIP TRF TO MAMA ACCOUNT",      "30000", "",       "919900"],
        ["25/01/2026", "POS DOMINOS PIZZA",             "12500", "",       "907400"],
        # Duplicate row (should be removed)
        ["25/01/2026", "POS DOMINOS PIZZA",             "12500", "",       "907400"],
        # Noise row
        ["", "Opening Balance", "", "", ""],
    ]
    path = tmp_path / "test_statement.csv"
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(csv_data)
    return path


class TestIngestionPipeline:
    def test_ingest_csv(self, sample_csv_file):
        df = ingest_statement(sample_csv_file, file_type="csv")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "tx_date" in df.columns
        assert "amount" in df.columns
        assert "tx_type" in df.columns
        assert "description" in df.columns

    def test_amounts_positive(self, sample_csv_file):
        df = ingest_statement(sample_csv_file, file_type="csv")
        assert (df["amount"] > 0).all()

    def test_tx_type_valid(self, sample_csv_file):
        df = ingest_statement(sample_csv_file, file_type="csv")
        assert df["tx_type"].isin(["debit", "credit"]).all()

    def test_fingerprint_exists(self, sample_csv_file):
        df = ingest_statement(sample_csv_file, file_type="csv")
        assert "fingerprint" in df.columns
        assert df["fingerprint"].notna().all()


class TestCleaner:
    def test_deduplication(self, sample_csv_file):
        raw_df = ingest_statement(sample_csv_file, file_type="csv")
        clean_df = clean_transactions(raw_df)
        # Duplicate "DOMINOS PIZZA" row should be removed
        dominos = clean_df[clean_df["description"].str.contains("DOMINOS", case=False, na=False)]
        assert len(dominos) <= 1

    def test_no_zero_amounts(self, sample_csv_file):
        raw_df = ingest_statement(sample_csv_file, file_type="csv")
        clean_df = clean_transactions(raw_df)
        assert (clean_df["amount"] > 0).all()

    def test_sorted_by_date_desc(self, sample_csv_file):
        raw_df = ingest_statement(sample_csv_file, file_type="csv")
        clean_df = clean_transactions(raw_df)
        dates = pd.to_datetime(clean_df["tx_date"])
        assert dates.is_monotonic_decreasing

    def test_validate_report(self, sample_csv_file):
        raw_df = ingest_statement(sample_csv_file, file_type="csv")
        clean_df = clean_transactions(raw_df)
        report = validate_dataframe(clean_df)
        assert "total_rows" in report
        assert report["total_rows"] > 0
        assert "total_debits" in report
        assert "total_credits" in report
