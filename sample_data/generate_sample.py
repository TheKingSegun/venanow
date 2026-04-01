"""
sample_data/generate_sample.py

Generates a realistic Nigerian bank statement CSV for testing.
Covers 3 months of transactions across all categories.

Usage:
    python sample_data/generate_sample.py
    # Outputs: sample_data/sample_statement.csv
               sample_data/sample_statement_gtbank.csv
               sample_data/sample_statement_opay.csv
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path


random.seed(42)

OUTPUT_DIR = Path(__file__).parent


# ── Transaction Templates ─────────────────────────────────────────────────────

INCOME_SOURCES = [
    ("Salary Payment - Glovo Nigeria Ltd",    850_000,  "credit", "Bank Transfer"),
    ("Performance Bonus - Glovo Nigeria",      45_000,  "credit", "Bank Transfer"),
    ("Freelance Payment - Vantage Analytics",  80_000,  "credit", "Transfer"),
    ("OPay Transfer In - Chidi Eze",           20_000,  "credit", "OPay"),
    ("Interest Credit",                         1_200,  "credit", "Bank Transfer"),
]

FOOD_TRANSACTIONS = [
    ("POS PURCHASE - SHOPRITE LEKKI MALL",   31_000, "debit", "POS"),
    ("POS PURCHASE - SHOPRITE LEKKI MALL",   18_600, "debit", "POS"),
    ("POS - DOMINOS PIZZA VICTORIA ISLAND",  22_500, "debit", "POS"),
    ("POS - CHICKEN REPUBLIC VI LAGOS",       8_200, "debit", "POS"),
    ("GLOVO FOOD DELIVERY ORDER",             6_800, "debit", "App (Debit)"),
    ("CHOWDECK ORDER - RESTAURANT",           5_400, "debit", "Card Online"),
    ("POS - COLD STONE CREAMERY IKEJA",       4_200, "debit", "POS"),
    ("JUMIA FOOD DELIVERY",                   7_100, "debit", "Card Online"),
    ("POS - BARCELOS LEKKI PHASE 1",         12_500, "debit", "POS"),
    ("POS - PARK N SHOP SUPERMARKET",        24_300, "debit", "POS"),
    ("POS - SPAR SUPERMARKET ONIRU",         19_800, "debit", "POS"),
    ("GLOVO GROCERY DELIVERY",                9_200, "debit", "App (Debit)"),
    ("POS - KFC AJAH LEKKI",                  6_700, "debit", "POS"),
    ("POS - TASTEE FRIED CHICKEN",            4_100, "debit", "POS"),
    ("POS - CHICKEN REPUBLIC IKEJA",          5_300, "debit", "POS"),
]

TRANSPORT_TRANSACTIONS = [
    ("UBER TRIP - VICTORIA ISLAND LAGOS",     4_200, "debit", "App (Debit)"),
    ("BOLT RIDE - IKEJA TO LEKKI",            3_100, "debit", "App (Debit)"),
    ("UBER TRIP - LEKKI PHASE 1",             2_800, "debit", "App (Debit)"),
    ("POS - TOTAL ENERGIES FILLING STATION", 18_000, "debit", "POS"),
    ("POS - CONOIL FILLING STATION LEKKI",   15_500, "debit", "POS"),
    ("NIP TRF - BRT BUS TICKET",              1_200, "debit", "USSD"),
    ("BOLT RIDE - VI TO IKEJA",               4_500, "debit", "App (Debit)"),
    ("POS - OANDO FILLING STATION",          16_200, "debit", "POS"),
    ("UBER TRIP - AIRPORT LAGOS",             8_500, "debit", "App (Debit)"),
    ("POS - MOBIL FILLING STATION",          14_000, "debit", "POS"),
]

UTILITY_TRANSACTIONS = [
    ("EKEDC PREPAID TOKEN - ELECTRICITY",    15_000, "debit", "Transfer"),
    ("EKEDC PREPAID TOKEN - ELECTRICITY",    10_000, "debit", "Transfer"),
    ("LAWMA WASTE MANAGEMENT BILL",           8_000, "debit", "Transfer"),
    ("SPECTRANET INTERNET SUBSCRIPTION",     15_000, "debit", "Card Online"),
    ("MTN AIRTIME RECHARGE VIA USSD",         5_000, "debit", "USSD"),
    ("AIRTEL DATA BUNDLE PURCHASE",           3_000, "debit", "USSD"),
    ("DSTV SELF SERVICE PAYMENT",            10_500, "debit", "Card Online"),
    ("LWSC WATER BILL PAYMENT",               4_500, "debit", "Transfer"),
    ("PHCN BILL PAYMENT",                    12_000, "debit", "Transfer"),
]

SUBSCRIPTION_TRANSACTIONS = [
    ("NETFLIX SUBSCRIPTION MONTHLY",         5_900, "debit", "Card Online"),
    ("SPOTIFY PREMIUM MONTHLY",              3_200, "debit", "Card Online"),
    ("CHATGPT PLUS SUBSCRIPTION",           12_800, "debit", "Card Online"),
    ("ADOBE CREATIVE CLOUD ANNUAL/12",       6_000, "debit", "Card Online"),
    ("APPLE ICLOUD+ STORAGE PLAN",           1_100, "debit", "Card Online"),
    ("DSTV COMPACT+ SUBSCRIPTION",          10_500, "debit", "Card Online"),
]

RENT_TRANSACTIONS = [
    ("TRF TO LEKKI PHASE 1 LANDLORD - RENT PAYMENT", 100_000, "debit", "Bank Transfer"),
]

TRANSFER_TRANSACTIONS = [
    ("NIP TRF TO CHIMA OBI - PERSONAL",     50_000, "debit", "Bank Transfer"),
    ("NIP TRF TO MAMA ACCOUNT - FAMILY",    30_000, "debit", "Bank Transfer"),
    ("NIP TRF TO FRIEND ADE",               20_000, "debit", "Bank Transfer"),
    ("TRF TO PIGGYVEST SAVINGS",            50_000, "debit", "Bank Transfer"),
    ("TRF TO COWRYWISE INVESTMENT",         25_000, "debit", "Bank Transfer"),
    ("NIP TRF TO SISTER NGOZI",             15_000, "debit", "Bank Transfer"),
]

MISC_TRANSACTIONS = [
    ("JUMIA ONLINE PURCHASE - ELECTRONICS", 34_000, "debit", "Card Online"),
    ("ATM CASH WITHDRAWAL - GTB ATM",       20_000, "debit", "ATM"),
    ("POS - PHARMACIES HEALTH SHOP",         8_500, "debit", "POS"),
    ("KONGA ONLINE SHOPPING",               12_200, "debit", "Card Online"),
    ("POS - BOOK STORE IKEJA",               6_400, "debit", "POS"),
    ("BARBER SHOP - CASH PAYMENT",           3_500, "debit", "POS"),
    ("GYM MEMBERSHIP - RCCG SPORTS CENTRE", 15_000, "debit", "Bank Transfer"),
    ("POS - PHARMACY DRUGSTORE",             4_300, "debit", "POS"),
]


# ── Generator ─────────────────────────────────────────────────────────────────

def generate_statement(
    months: int = 3,
    start_date: date = date(2026, 1, 1),
    opening_balance: float = 250_000,
) -> list[dict]:
    """Generate a realistic 3-month transaction list."""

    all_transactions = []
    balance = opening_balance
    current_date = start_date

    end_date = start_date + timedelta(days=months * 30)

    # Monthly recurring events
    def add_monthly_events(month_start: date):
        events = []
        # Salary — 25th of month
        payday = month_start.replace(day=25)
        events.append((_jitter_date(payday, 1), INCOME_SOURCES[0]))
        # Rent — 1st-5th
        events.append((_jitter_date(month_start.replace(day=1), 4), RENT_TRANSACTIONS[0]))
        # Subscriptions — spread across month
        for i, sub in enumerate(SUBSCRIPTION_TRANSACTIONS):
            sub_date = month_start + timedelta(days=3 + i * 4)
            events.append((sub_date, sub))
        # Utilities
        events.append((_jitter_date(month_start.replace(day=10), 3), UTILITY_TRANSACTIONS[0]))  # EKEDC
        events.append((_jitter_date(month_start.replace(day=15), 2), UTILITY_TRANSACTIONS[2]))  # LAWMA
        events.append((_jitter_date(month_start.replace(day=20), 3), UTILITY_TRANSACTIONS[3]))  # Internet
        # Savings transfer
        events.append((_jitter_date(month_start.replace(day=27), 2), TRANSFER_TRANSACTIONS[3]))
        return events

    # Track months processed
    processed_months = set()

    d = start_date
    while d < end_date:
        month_key = (d.year, d.month)
        month_start = d.replace(day=1)

        # Add monthly recurring events for this month
        if month_key not in processed_months:
            processed_months.add(month_key)
            for event_date, tx_template in add_monthly_events(month_start):
                if start_date <= event_date < end_date:
                    all_transactions.append(_make_tx(event_date, tx_template))

        # Add random daily transactions (2-5 per day)
        n_daily = random.randint(0, 4)
        for _ in range(n_daily):
            pool = (
                FOOD_TRANSACTIONS * 4 +     # Higher weight
                TRANSPORT_TRANSACTIONS * 2 +
                MISC_TRANSACTIONS * 1 +
                TRANSFER_TRANSACTIONS * 1
            )
            tx_template = random.choice(pool)
            # Add some variance to amount
            base_amount = tx_template[1]
            varied = base_amount * random.uniform(0.85, 1.15)
            tx_varied = (tx_template[0], round(varied, -2), tx_template[2], tx_template[3])
            all_transactions.append(_make_tx(d, tx_varied))

        # Occasional income (freelance, transfers in)
        if random.random() < 0.06:
            inc = random.choice(INCOME_SOURCES[1:])
            all_transactions.append(_make_tx(d, inc))

        d += timedelta(days=1)

    # Sort by date
    all_transactions.sort(key=lambda r: r["Date"])

    # Compute running balance
    balance = opening_balance
    result = []
    for tx in all_transactions:
        if tx["Type"] == "Credit":
            balance += tx["_raw_amount"]
        else:
            balance -= tx["_raw_amount"]
        tx["Balance"] = round(balance, 2)
        del tx["_raw_amount"]
        result.append(tx)

    return result


def _make_tx(tx_date: date, template: tuple) -> dict:
    desc, amount, tx_type, channel = template
    debit  = round(amount, 2) if tx_type == "debit"  else ""
    credit = round(amount, 2) if tx_type == "credit" else ""
    return {
        "Date":        tx_date.strftime("%d/%m/%Y"),
        "Description": desc,
        "Debit":       debit,
        "Credit":      credit,
        "Type":        "Debit" if tx_type == "debit" else "Credit",
        "Balance":     0,  # Filled in later
        "_raw_amount": amount,
    }


def _jitter_date(d: date, max_days: int = 2) -> date:
    return d + timedelta(days=random.randint(0, max_days))


# ── GTBank-style CSV ──────────────────────────────────────────────────────────

def write_gtbank_csv(transactions: list[dict], path: Path):
    """Write in GTBank statement format."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # GTBank header metadata rows
        writer.writerow(["Account Statement"])
        writer.writerow(["Account Name:", "David Adeyemi Okafor"])
        writer.writerow(["Account Number:", "0123456789"])
        writer.writerow(["Currency:", "NGN"])
        writer.writerow([])
        writer.writerow(["Date", "Details", "Debit", "Credit", "Balance"])
        for tx in transactions:
            writer.writerow([
                tx["Date"],
                tx["Description"],
                tx["Debit"],
                tx["Credit"],
                tx["Balance"],
            ])


# ── OPay-style CSV ────────────────────────────────────────────────────────────

def write_opay_csv(transactions: list[dict], path: Path):
    """Write in OPay wallet statement format."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Remark", "Amount", "Type", "Balance"])
        for tx in transactions:
            amount = tx["Debit"] if tx["Type"] == "Debit" else tx["Credit"]
            writer.writerow([
                tx["Date"].replace("/", "-"),  # OPay uses dashes
                tx["Description"],
                amount,
                tx["Type"],
                tx["Balance"],
            ])


# ── Generic CSV ───────────────────────────────────────────────────────────────

def write_generic_csv(transactions: list[dict], path: Path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Date", "Description", "Debit", "Credit", "Balance"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(transactions)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating sample bank statements...")

    transactions = generate_statement(months=3, start_date=date(2026, 1, 1))
    print(f"  Generated {len(transactions)} transactions over 3 months.")

    # Generic CSV
    generic_path = OUTPUT_DIR / "sample_statement.csv"
    write_generic_csv(transactions, generic_path)
    print(f"  ✓ Generic CSV: {generic_path}")

    # GTBank format
    gtbank_path = OUTPUT_DIR / "sample_statement_gtbank.csv"
    write_gtbank_csv(transactions, gtbank_path)
    print(f"  ✓ GTBank CSV:  {gtbank_path}")

    # OPay format (subset — wallet transactions)
    opay_txs = [t for t in transactions if "opay" in t["Description"].lower() or random.random() < 0.15]
    opay_path = OUTPUT_DIR / "sample_statement_opay.csv"
    write_opay_csv(opay_txs[:50], opay_path)
    print(f"  ✓ OPay CSV:    {opay_path}")

    # Print summary
    total_credits = sum(float(t["Credit"]) for t in transactions if t["Credit"])
    total_debits  = sum(float(t["Debit"])  for t in transactions if t["Debit"])
    print(f"\n  Summary:")
    print(f"    Total Income:   ₦{total_credits:>14,.2f}")
    print(f"    Total Expenses: ₦{total_debits:>14,.2f}")
    print(f"    Net Savings:    ₦{total_credits - total_debits:>14,.2f}")
    print(f"\nDone. Use sample_statement_gtbank.csv to test the upload endpoint.")
