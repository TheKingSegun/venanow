"""
utils/currency.py
Nigerian financial context utilities — bank detection, channel parsing,
Naira formatting, and fintech recognition.
"""

from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional


# ── Naira Formatting ──────────────────────────────────────────────────────────

def fmt_naira(amount: float, show_sign: bool = False) -> str:
    """Format a number as Nigerian Naira. e.g. 1234567.5 → '₦1,234,567.50'"""
    sign = ""
    if show_sign:
        sign = "+" if amount >= 0 else "-"
        amount = abs(amount)
    return f"{sign}₦{amount:,.2f}"


def parse_naira(value: str) -> float:
    """Parse a Naira string to float. Handles '₦1,234.56', '1234.56 DR', etc."""
    cleaned = re.sub(r"[₦,\s]", "", str(value))
    cleaned = re.sub(r"(DR|CR)$", "", cleaned, flags=re.IGNORECASE).strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


# ── Bank Detection ────────────────────────────────────────────────────────────

NIGERIAN_BANKS: dict[str, list[str]] = {
    "GTBank":     ["gtbank", "guaranty trust", "gtb", "0013", "058"],
    "Zenith":     ["zenith", "zenith bank", "057"],
    "UBA":        ["uba", "united bank for africa", "033"],
    "Access":     ["access bank", "access diamond", "044"],
    "First Bank": ["first bank", "firstbank", "fbn", "011"],
    "Sterling":   ["sterling bank", "232"],
    "FCMB":       ["fcmb", "first city monument", "214"],
    "Stanbic":    ["stanbic", "stanbic ibtc", "221"],
    "Fidelity":   ["fidelity", "070"],
    "Union":      ["union bank", "032"],
    "Ecobank":    ["ecobank", "050"],
    "WEMA":       ["wema", "035"],
    # Fintechs / Mobile money
    "OPay":       ["opay", "o-pay", "paycom", "100004"],
    "PalmPay":    ["palmpay", "palm pay", "100033"],
    "Kuda":       ["kuda", "kuda bank", "090267"],
    "Moniepoint": ["moniepoint", "teamapt", "50515"],
    "Carbon":     ["carbon", "one finance", "100026"],
    "Chipper":    ["chipper", "chipper cash"],
    "Flutterwave": ["flutterwave", "rave"],
    "Paystack":   ["paystack"],
    "VBank":      ["vbank", "v bank", "090110"],
    "Brass":      ["brass"],
}

def detect_bank(text: str) -> Optional[str]:
    """Detect Nigerian bank name from description/narration text."""
    lower = text.lower()
    for bank, keywords in NIGERIAN_BANKS.items():
        if any(kw in lower for kw in keywords):
            return bank
    return None


# ── Channel Detection ─────────────────────────────────────────────────────────

CHANNEL_PATTERNS: list[tuple[str, list[str]]] = [
    ("POS",          ["pos ", "/pos/", "point of sale", "pos purchase", "debit purchase"]),
    ("ATM",          ["atm ", "atm/", "cash withdrawal", "atm withdrawal"]),
    ("Transfer",     ["trf", "transfer", "nip", "neft", "rtgs", "interbank", "ussd trf"]),
    ("USSD",         ["ussd", "*737*", "*770*", "*919*", "*966*"]),
    ("Card Online",  ["web", "online", "e-commerce", "card payment", "purchase online",
                      "netflix", "spotify", "google", "apple", "amazon", "paypal"]),
    ("Mobile App",   ["mobile", "app", "kuda app", "opay app", "palmpay app"]),
    ("Direct Debit", ["direct debit", "standing order", "mandate"]),
    ("Cheque",       ["cheque", "check", "chq"]),
    ("Cash",         ["cash deposit", "cash lodgement", "over the counter"]),
]

def detect_channel(description: str) -> str:
    """Infer transaction channel from description."""
    lower = description.lower()
    for channel, patterns in CHANNEL_PATTERNS:
        if any(p in lower for p in patterns):
            return channel
    return "Bank Transfer"  # Default


# ── Merchant Extraction ───────────────────────────────────────────────────────

# Strip prefixes common in Nigerian bank narrations
NARRATION_NOISE = re.compile(
    r"^(trf from|trf to|transfer from|transfer to|payment to|payment from|"
    r"pos purchase at|atm withdrawal at|via|nip|neft|ussd|web purchase|"
    r"internet banking|mobile banking|debit card purchase)\s*[:\-]?\s*",
    re.IGNORECASE
)
# Strip trailing reference codes like /REF:1234567 or |0000123
TRAILING_REF = re.compile(r"[\|/\\]\s*(ref|ref#|trans|trn|rrn|session)?[\s#:]?\w{5,}.*$", re.IGNORECASE)


def extract_merchant(description: str) -> str:
    """
    Clean a bank narration to produce a readable merchant/counterparty name.
    e.g. 'POS PURCHASE AT SHOPRITE LEKKI/POS/REF1234' → 'Shoprite Lekki'
    """
    cleaned = NARRATION_NOISE.sub("", description)
    cleaned = TRAILING_REF.sub("", cleaned)
    # Title-case and collapse whitespace
    cleaned = " ".join(cleaned.split()).title()
    return cleaned[:100] if cleaned else description[:100]


# ── Statement Format Sniffing ─────────────────────────────────────────────────

@dataclass
class BankProfile:
    """Maps a bank's statement column names to standard schema fields."""
    bank_name:    str
    date_col:     str
    desc_col:     str
    debit_col:    Optional[str]   # Some banks split debit/credit
    credit_col:   Optional[str]
    amount_col:   Optional[str]   # Others use single amount col + type
    type_col:     Optional[str]
    balance_col:  Optional[str]
    date_format:  str
    skip_rows:    int = 0
    encoding:     str = "utf-8"


BANK_PROFILES: dict[str, BankProfile] = {
    "gtbank": BankProfile(
        bank_name="GTBank", date_col="Date", desc_col="Details",
        debit_col="Debit", credit_col="Credit", amount_col=None,
        type_col=None, balance_col="Balance",
        date_format="%d/%m/%Y", skip_rows=0
    ),
    "zenith": BankProfile(
        bank_name="Zenith", date_col="Trans. Date", desc_col="Remarks",
        debit_col="Debit", credit_col="Credit", amount_col=None,
        type_col=None, balance_col="Balance",
        date_format="%d-%b-%Y", skip_rows=0
    ),
    "uba": BankProfile(
        bank_name="UBA", date_col="Transaction Date", desc_col="Transaction Details",
        debit_col=None, credit_col=None, amount_col="Amount",
        type_col="Transaction Type", balance_col="Ledger Balance",
        date_format="%Y-%m-%d", skip_rows=0
    ),
    "access": BankProfile(
        bank_name="Access", date_col="Trans Date", desc_col="Narration",
        debit_col="Withdrawals", credit_col="Deposits", amount_col=None,
        type_col=None, balance_col="Balance",
        date_format="%d/%m/%Y", skip_rows=0
    ),
    "opay": BankProfile(
        bank_name="OPay", date_col="Date", desc_col="Remark",
        debit_col=None, credit_col=None, amount_col="Amount",
        type_col="Type", balance_col="Balance",
        date_format="%Y-%m-%d %H:%M:%S", skip_rows=0
    ),
    "kuda": BankProfile(
        bank_name="Kuda", date_col="Date", desc_col="Description",
        debit_col="Debit", credit_col="Credit", amount_col=None,
        type_col=None, balance_col="Balance",
        date_format="%d %b %Y", skip_rows=0
    ),
}


def sniff_bank_profile(columns: list[str]) -> Optional[BankProfile]:
    """
    Heuristically match CSV column names to a known bank profile.
    Returns None if no match — caller should use generic parsing.
    """
    col_set = {c.lower().strip() for c in columns}

    scores: dict[str, int] = {}
    for key, profile in BANK_PROFILES.items():
        score = 0
        probe_cols = [
            profile.date_col, profile.desc_col,
            profile.debit_col, profile.credit_col,
            profile.amount_col, profile.balance_col
        ]
        for pc in probe_cols:
            if pc and pc.lower() in col_set:
                score += 1
        scores[key] = score

    best = max(scores, key=scores.get)
    return BANK_PROFILES[best] if scores[best] >= 2 else None
