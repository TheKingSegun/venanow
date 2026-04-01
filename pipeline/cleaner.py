"""
pipeline/cleaner.py

Data cleaning, deduplication, and normalization.
Operates on the standardized DataFrame from ingestion.py.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import numpy as np

from utils.logger import logger


# ── Entry point ───────────────────────────────────────────────────────────────

def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full cleaning pipeline:
    1. Remove duplicates (exact + near-duplicates)
    2. Handle missing values
    3. Normalize amounts and descriptions
    4. Remove noise rows (bank fees metadata, opening/closing balance lines)
    5. Sort chronologically

    Args:
        df: Raw standardized DataFrame from ingestion.py

    Returns:
        Cleaned DataFrame.
    """
    original_count = len(df)
    logger.info(f"Cleaning {original_count} raw transactions...")

    df = _remove_noise_rows(df)
    df = _normalize_amounts(df)
    df = _normalize_descriptions(df)
    df = _deduplicate(df)
    df = _fill_missing(df)
    df = _sort(df)

    removed = original_count - len(df)
    logger.info(f"Cleaning complete: {len(df)} kept, {removed} removed.")
    return df


# ── Noise Row Removal ─────────────────────────────────────────────────────────

# Descriptions that are statement metadata, not real transactions
NOISE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"^opening balance$",
        r"^closing balance$",
        r"^brought forward$",
        r"^carried forward$",
        r"^balance b/f",
        r"^balance c/f",
        r"^total (debit|credit|transactions)",
        r"^statement of account",
        r"^account summary",
        r"^\s*$",
    ]
]

def _remove_noise_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that are statement metadata rather than real transactions."""
    def is_noise(desc: str) -> bool:
        return any(p.match(str(desc)) for p in NOISE_PATTERNS)

    mask = ~df["description"].apply(is_noise)
    removed = (~mask).sum()
    if removed:
        logger.debug(f"Removed {removed} noise/metadata rows.")
    return df[mask].reset_index(drop=True)


# ── Amount Normalization ──────────────────────────────────────────────────────

def _normalize_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure amounts are:
    - Always positive floats
    - Rounded to 2 decimal places
    - tx_type is strictly 'debit' or 'credit'
    """
    df["amount"] = df["amount"].abs().round(2)

    # Remove rows with zero or nonsensical amounts
    df = df[df["amount"] > 0].copy()

    # Normalize tx_type
    df["tx_type"] = df["tx_type"].str.lower().str.strip()
    df["tx_type"] = df["tx_type"].apply(
        lambda t: "credit" if t in ("cr", "credit", "deposit", "in") else "debit"
    )

    # Round balance too
    if "balance" in df.columns:
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce").round(2)

    return df


# ── Description Normalization ─────────────────────────────────────────────────

# Common Nigerian bank narration patterns to normalize
NORMALIZATION_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bVIA\s+PAYSTACK\b", re.I),         "via Paystack"),
    (re.compile(r"\bVIA\s+FLUTTERWAVE\b", re.I),      "via Flutterwave"),
    (re.compile(r"\bPOS\s+PURCHASE\s+AT\b", re.I),    "POS —"),
    (re.compile(r"\bATM\s+WITHDRAWAL\s+AT\b", re.I),  "ATM —"),
    (re.compile(r"\bTRF\s+FROM\b", re.I),             "Transfer from"),
    (re.compile(r"\bTRF\s+TO\b", re.I),               "Transfer to"),
    (re.compile(r"\bNIP\s+\w+\s+TRF", re.I),         "NIP Transfer"),
    (re.compile(r"\bUSSD\b", re.I),                   "USSD"),
    (re.compile(r"\s{2,}", re.I),                     " "),
]

def _normalize_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """Apply normalization rules to description field."""
    def normalize(desc: str) -> str:
        for pattern, replacement in NORMALIZATION_RULES:
            desc = pattern.sub(replacement, desc)
        return desc.strip()

    df["description"] = df["description"].apply(normalize)
    return df


# ── Deduplication ─────────────────────────────────────────────────────────────

def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate transactions using two strategies:

    1. Exact dedup: identical fingerprint (date + amount + description hash)
    2. Near-dedup: same date + amount + first 40 chars of description
       (catches formatting differences across statement periods)
    """
    before = len(df)

    # Strategy 1: Exact fingerprint
    if "fingerprint" in df.columns:
        df = df.drop_duplicates(subset=["fingerprint"], keep="first")

    # Strategy 2: Near-duplicate (same day, same amount, similar description)
    df["_near_key"] = (
        df["tx_date"].astype(str) + "|" +
        df["amount"].astype(str) + "|" +
        df["description"].str[:40].str.lower().str.strip()
    )
    df = df.drop_duplicates(subset=["_near_key"], keep="first")
    df = df.drop(columns=["_near_key"])

    removed = before - len(df)
    if removed:
        logger.info(f"Deduplication removed {removed} duplicate transactions.")

    return df.reset_index(drop=True)


# ── Missing Values ────────────────────────────────────────────────────────────

def _fill_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values sensibly:
    - balance: forward-fill (reasonable for statements), then leave NaN
    - description: fill with 'Unknown Transaction'
    - channel: fill with 'Unknown'
    """
    if "balance" in df.columns:
        df["balance"] = df["balance"].ffill()

    df["description"] = df["description"].fillna("Unknown Transaction")
    df["channel"]     = df.get("channel", pd.Series(dtype=str)).fillna("Unknown")
    df["merchant"]    = df.get("merchant", pd.Series(dtype=str)).fillna("")

    return df


# ── Sorting ───────────────────────────────────────────────────────────────────

def _sort(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by date descending (most recent first), then by amount descending."""
    return df.sort_values(["tx_date", "amount"], ascending=[False, False]).reset_index(drop=True)


# ── Validation Report ─────────────────────────────────────────────────────────

def validate_dataframe(df: pd.DataFrame) -> dict:
    """
    Run quality checks on the cleaned DataFrame.
    Returns a report dict with stats and any warnings.
    """
    report = {
        "total_rows": len(df),
        "date_range": {
            "start": str(df["tx_date"].min()) if len(df) else None,
            "end":   str(df["tx_date"].max()) if len(df) else None,
        },
        "total_debits":  float(df[df["tx_type"] == "debit"]["amount"].sum()),
        "total_credits": float(df[df["tx_type"] == "credit"]["amount"].sum()),
        "null_counts": df.isnull().sum().to_dict(),
        "warnings": [],
    }

    if report["total_rows"] == 0:
        report["warnings"].append("No transactions found after cleaning.")

    if df["tx_date"].isnull().any():
        n = df["tx_date"].isnull().sum()
        report["warnings"].append(f"{n} rows have unparseable dates — dropped.")

    if (df["amount"] <= 0).any():
        n = (df["amount"] <= 0).sum()
        report["warnings"].append(f"{n} rows have zero or negative amounts.")

    # Sanity check: credits shouldn't vastly exceed debits (possible parsing error)
    if report["total_credits"] > 0 and report["total_debits"] == 0:
        report["warnings"].append("All transactions parsed as credits — check tx_type column mapping.")

    return report
