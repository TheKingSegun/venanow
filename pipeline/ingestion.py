"""
pipeline/ingestion.py

Multi-format bank statement ingestion.
Handles PDF (pdfplumber), CSV, and Excel (openpyxl/xlrd).
Outputs a standardized pandas DataFrame.

Standard schema:
    tx_date      : datetime.date
    description  : str
    amount       : float  (always positive)
    tx_type      : 'debit' | 'credit'
    balance      : float | None
    raw_desc     : str    (original, unprocessed)
"""

from __future__ import annotations

import io
import re
import hashlib
from pathlib import Path
from datetime import date
from typing import Optional

import pandas as pd
import pdfplumber

from utils.currency import (
    parse_naira, sniff_bank_profile, detect_channel,
    extract_merchant, detect_bank, BankProfile
)
from utils.logger import logger


# ── Standard output columns ───────────────────────────────────────────────────

STANDARD_COLS = ["tx_date", "description", "amount", "tx_type", "balance", "raw_desc"]


# ── Entry point ───────────────────────────────────────────────────────────────

def ingest_statement(
    file_path: str | Path,
    file_type: Optional[str] = None,
) -> pd.DataFrame:
    """
    Parse a bank statement file into the standard transaction DataFrame.

    Args:
        file_path : Path to the statement file.
        file_type : 'pdf', 'csv', or 'excel'. Auto-detected if None.

    Returns:
        DataFrame with columns: tx_date, description, amount, tx_type,
                                balance, raw_desc
    Raises:
        ValueError if the file cannot be parsed.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Statement file not found: {path}")

    ext = file_type or path.suffix.lower().lstrip(".")
    logger.info(f"Ingesting statement: {path.name} (type={ext})")

    if ext == "pdf":
        df = _parse_pdf(path)
    elif ext == "csv":
        df = _parse_csv(path)
    elif ext in ("xls", "xlsx", "excel"):
        df = _parse_excel(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    df = _standardize(df)
    logger.info(f"Ingestion complete: {len(df)} transactions extracted.")
    return df


# ── PDF Parser ────────────────────────────────────────────────────────────────

KNOWN_HEADER_KEYWORDS = {
    "date", "trans date", "transaction date", "value date", "posting date",
    "narration", "description", "details", "remarks", "particulars", "reference",
    "debit", "credit", "withdrawal", "deposit", "withdrawals", "deposits",
    "amount", "dr", "cr", "balance", "ledger balance", "closing balance",
    "type", "transaction type", "channel",
}


def _is_header_row(row: list) -> bool:
    """Return True if this table row looks like a column header row."""
    if not row:
        return False
    cells = [str(c or "").strip().lower() for c in row]
    non_empty = [c for c in cells if c]
    if len(non_empty) < 2:
        return False
    matches = sum(
        1 for c in non_empty
        if c in KNOWN_HEADER_KEYWORDS
        or any(k in c for k in KNOWN_HEADER_KEYWORDS)
    )
    return matches >= 2


def _normalize_headers(row: list) -> list[str]:
    """Clean and normalize a header row into usable keys."""
    return [str(h or "").strip().lower().replace("\n", " ") for h in row]


def _parse_pdf(path: Path) -> pd.DataFrame:
    """
    Extract transactions from a bank statement PDF using pdfplumber.
    Strategy:
      1. Try table extraction on each page — find the real header row
         by scanning (not assuming row 0 is always the header).
      2. Fall back to line-by-line regex if no tables found on a page.
    Aggregates all pages before deciding if extraction failed.
    """
    all_rows: list[dict] = []
    last_headers: list[str] | None = None

    with pdfplumber.open(path) as pdf:
        logger.debug(f"PDF has {len(pdf.pages)} page(s).")

        for page_num, page in enumerate(pdf.pages, 1):
            # Try structured table extraction with explicit line strategy first
            tables = page.extract_tables({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
            }) or page.extract_tables()

            page_rows: list[dict] = []

            if tables:
                for table in tables:
                    extracted = _parse_pdf_table(table, last_headers)
                    if extracted["rows"]:
                        page_rows.extend(extracted["rows"])
                        last_headers = extracted["headers"]

            if not page_rows:
                # Fallback: raw text regex
                text = page.extract_text() or ""
                page_rows = _parse_pdf_text(text)
                logger.debug(f"Page {page_num}: text fallback → {len(page_rows)} rows.")
            else:
                logger.debug(f"Page {page_num}: {len(page_rows)} rows from table.")

            all_rows.extend(page_rows)

    if not all_rows:
        raise ValueError(
            "Could not read statement. The PDF format may not be supported yet. "
            "Please try exporting your statement as CSV from your bank's app, "
            "or contact support with your bank name."
        )

    return pd.DataFrame(all_rows)


def _parse_pdf_table(
    table: list[list],
    fallback_headers: list[str] | None = None,
) -> dict:
    """
    Convert a pdfplumber table into row dicts.

    Scans ALL rows to find the real header — Nigerian bank statement PDFs
    commonly have 1-4 metadata rows before column headers (account name,
    statement period, branch info, etc).

    Returns: {"headers": [...], "rows": [...]}
    """
    if not table or len(table) < 2:
        return {"headers": fallback_headers or [], "rows": []}

    # Scan every row to find the first header row
    header_idx = None
    for i, row in enumerate(table):
        if _is_header_row(row):
            header_idx = i
            break

    if header_idx is None:
        if fallback_headers:
            # Continuation page — no header, use previous page's headers
            headers = fallback_headers
            data_rows = table
        else:
            # Absolute last resort: assume row 0 is header
            headers = _normalize_headers(table[0])
            data_rows = table[1:]
    else:
        headers = _normalize_headers(table[header_idx])
        data_rows = table[header_idx + 1:]

    # Remove empty header slots (merged cells produce empty strings)
    # by giving them a placeholder so column count stays aligned
    headers = [h if h else f"_col{i}" for i, h in enumerate(headers)]

    rows = []
    for raw_row in data_rows:
        if not raw_row:
            continue
        cells = [str(c or "").strip() for c in raw_row]
        if all(c == "" for c in cells):
            continue

        # Skip summary / sub-header rows
        joined = " ".join(cells).lower()
        if any(skip in joined for skip in [
            "opening balance", "closing balance", "brought forward",
            "carried forward", "total", "sub total", "page total",
            "transaction count", "statement of account",
        ]):
            continue

        row = {}
        for i, h in enumerate(headers):
            row[h] = cells[i] if i < len(cells) else ""
        rows.append(row)

    return {"headers": headers, "rows": rows}


# Regex handles dates like: 01/03/2026, 01-03-2026, 2026-03-01
PDF_LINE_RE = re.compile(
    r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{2}[\/\-]\d{2})"
    r"[ \t]+(.+?)[ \t]+"
    r"([\d,]+\.?\d*(?:\s*(?:DR|CR))?)\s*"
    r"([\d,]+\.?\d*)?"
    , re.IGNORECASE
)


def _parse_pdf_text(text: str) -> list[dict]:
    """
    Last-resort: extract transactions from raw PDF text via regex.
    Handles lines like:
      15/03/2026  POS SHOPRITE LEKKI   18,600.00DR   1,042,000.00
      2026-03-15  Transfer to Segun    5,000.00       999,000.00
    """
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or len(line) < 15:
            continue
        m = PDF_LINE_RE.search(line)
        if m:
            rows.append({
                "date": m.group(1),
                "description": m.group(2).strip(),
                "amount": m.group(3).strip(),
                "balance": (m.group(4) or "").strip(),
            })
    return rows


# ── CSV Parser ────────────────────────────────────────────────────────────────

def _parse_csv(path: Path) -> pd.DataFrame:
    """
    Parse a CSV bank statement. Detects encoding, skips metadata rows,
    and sniffs the bank profile for correct column mapping.
    """
    header_row = _find_header_row(path)

    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(
                path, encoding=enc, dtype=str,
                skip_blank_lines=True, skiprows=header_row
            )
            break
        except UnicodeDecodeError:
            continue
        except Exception:
            try:
                df = pd.read_csv(path, encoding=enc, dtype=str, skip_blank_lines=True)
                break
            except UnicodeDecodeError:
                continue
    else:
        raise ValueError("Could not decode CSV — unsupported encoding.")

    df = _drop_metadata_rows(df)
    logger.debug(f"CSV columns detected: {list(df.columns)}")

    profile = sniff_bank_profile(list(df.columns))
    if profile:
        logger.info(f"Matched bank profile: {profile.bank_name}")
        return _apply_profile(df, profile)
    else:
        logger.warning("No bank profile matched — using generic column detection.")
        return _generic_column_map(df)


def _parse_excel(path: Path) -> pd.DataFrame:
    """Parse XLS/XLSX bank statement."""
    try:
        df = pd.read_excel(path, dtype=str, skip_blank_lines=True)
    except Exception as e:
        raise ValueError(f"Could not parse Excel file: {e}")

    df = _drop_metadata_rows(df)
    logger.debug(f"Excel columns detected: {list(df.columns)}")

    profile = sniff_bank_profile(list(df.columns))
    if profile:
        logger.info(f"Matched bank profile: {profile.bank_name}")
        return _apply_profile(df, profile)
    return _generic_column_map(df)


# ── Column Mapping ────────────────────────────────────────────────────────────

def _apply_profile(df: pd.DataFrame, profile: BankProfile) -> pd.DataFrame:
    """
    Apply a known BankProfile to map columns → standard names.
    Handles both split (debit/credit columns) and unified (amount + type) schemas.
    """
    result = pd.DataFrame()

    result["tx_date"] = pd.to_datetime(
        df[profile.date_col], format=profile.date_format, errors="coerce"
    ).dt.date
    if result["tx_date"].isna().all():
        result["tx_date"] = pd.to_datetime(
            df[profile.date_col], format="mixed", errors="coerce"
        ).dt.date

    result["raw_desc"] = df[profile.desc_col].fillna("").str.strip()

    if profile.balance_col and profile.balance_col in df.columns:
        result["balance"] = df[profile.balance_col].apply(parse_naira)
    else:
        result["balance"] = None

    if profile.debit_col and profile.credit_col:
        debit_amt  = df[profile.debit_col].apply(parse_naira)
        credit_amt = df[profile.credit_col].apply(parse_naira)
        result["amount"]  = debit_amt.where(debit_amt > 0, credit_amt)
        result["tx_type"] = debit_amt.apply(lambda x: "debit" if x > 0 else "credit")
    elif profile.amount_col and profile.type_col:
        result["amount"] = df[profile.amount_col].apply(parse_naira)
        type_raw = df[profile.type_col].str.lower().str.strip()
        result["tx_type"] = type_raw.apply(
            lambda t: "credit" if any(w in t for w in ["cr", "credit", "deposit"]) else "debit"
        )
    else:
        raw_amount = df.get(profile.amount_col or "amount", pd.Series(dtype=str))
        result["amount"], result["tx_type"] = zip(*raw_amount.apply(_parse_amount_with_type))
        result["amount"] = list(result["amount"])
        result["tx_type"] = list(result["tx_type"])

    return result


def _generic_column_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Best-effort mapping for unknown bank formats.
    Tries common column name variations.
    """
    col = {c.lower().strip(): c for c in df.columns}

    date_candidates  = ["date", "trans date", "transaction date", "value date", "posting date"]
    desc_candidates  = ["description", "narration", "details", "remarks", "particulars"]
    bal_candidates   = ["balance", "ledger balance", "available balance", "closing balance"]

    date_col = next((col[c] for c in date_candidates if c in col), None)
    desc_col = next((col[c] for c in desc_candidates if c in col), None)
    bal_col  = next((col[c] for c in bal_candidates if c in col), None)

    if not date_col or not desc_col:
        raise ValueError(
            f"Cannot determine date/description columns. Found: {list(df.columns)}. "
            "Please ensure your statement has recognizable column headers."
        )

    result = pd.DataFrame()
    result["tx_date"]  = pd.to_datetime(df[date_col], errors="coerce").dt.date
    result["raw_desc"] = df[desc_col].fillna("").str.strip()
    result["balance"]  = df[bal_col].apply(parse_naira) if bal_col else None

    debit_col  = col.get("debit") or col.get("withdrawals") or col.get("dr")
    credit_col = col.get("credit") or col.get("deposits") or col.get("cr")
    amt_col    = col.get("amount") or col.get("transaction amount")

    if debit_col and credit_col:
        debit_amt  = df[debit_col].apply(parse_naira)
        credit_amt = df[credit_col].apply(parse_naira)
        result["amount"]  = debit_amt.where(debit_amt > 0, credit_amt)
        result["tx_type"] = debit_amt.apply(lambda x: "debit" if x > 0 else "credit")
    elif amt_col:
        result["amount"], result["tx_type"] = zip(*df[amt_col].apply(_parse_amount_with_type))
        result["amount"] = list(result["amount"])
        result["tx_type"] = list(result["tx_type"])
    else:
        raise ValueError("Cannot find amount column in statement.")

    return result


def _parse_amount_with_type(value: str) -> tuple[float, str]:
    """
    Handle amounts like '-18600', '18600 DR', '18,600.00CR'.
    Returns (abs_amount, 'debit' | 'credit').
    """
    s = str(value or "").strip()
    is_credit = bool(re.search(r"cr\b", s, re.IGNORECASE))
    is_debit  = bool(re.search(r"dr\b", s, re.IGNORECASE))
    amt = parse_naira(s)
    if amt < 0:
        return abs(amt), "debit"
    if is_credit:
        return amt, "credit"
    if is_debit:
        return amt, "debit"
    return amt, "debit"


# ── Standardize ───────────────────────────────────────────────────────────────

def _standardize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final standardization pass:
    - Handles both profile-mapped columns (tx_date, raw_desc)
      AND text-fallback columns (date, description)
    - Clean descriptions
    - Add channel, merchant, bank detection
    - Drop rows with no date or zero amount
    """
    # Handle raw text fallback column names from _parse_pdf_text()
    if "date" in df.columns and "tx_date" not in df.columns:
        df["tx_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "description" in df.columns and "raw_desc" not in df.columns:
        df["raw_desc"] = df["description"].fillna("").str.strip()

    # Handle PDF table rows that came through with header-named columns
    # Try to map common header variations to standard names
    col_lower = {c.lower().strip(): c for c in df.columns}

    if "tx_date" not in df.columns:
        for candidate in ["date", "trans date", "transaction date", "value date", "posting date"]:
            if candidate in col_lower:
                df["tx_date"] = pd.to_datetime(df[col_lower[candidate]], errors="coerce").dt.date
                break

    if "raw_desc" not in df.columns:
        for candidate in ["narration", "description", "details", "remarks", "particulars"]:
            if candidate in col_lower:
                df["raw_desc"] = df[col_lower[candidate]].fillna("").str.strip()
                break

    if "raw_desc" not in df.columns:
        df["raw_desc"] = ""

    # Amount & tx_type from PDF table rows
    if "amount" not in df.columns or df["amount"].isna().all() or (df["amount"] == "").all():
        # Try split debit/credit columns
        debit_col  = next((col_lower[c] for c in ["debit", "withdrawals", "dr"] if c in col_lower), None)
        credit_col = next((col_lower[c] for c in ["credit", "deposits", "cr"] if c in col_lower), None)
        amt_col    = next((col_lower[c] for c in ["amount", "transaction amount"] if c in col_lower), None)

        if debit_col and credit_col:
            debit_amt  = df[debit_col].apply(parse_naira)
            credit_amt = df[credit_col].apply(parse_naira)
            df["amount"]  = debit_amt.where(debit_amt > 0, credit_amt)
            df["tx_type"] = debit_amt.apply(lambda x: "debit" if x > 0 else "credit")
        elif amt_col:
            parsed = df[amt_col].apply(_parse_amount_with_type)
            df["amount"]  = [p[0] for p in parsed]
            df["tx_type"] = [p[1] for p in parsed]

    if "tx_type" not in df.columns:
        df["tx_type"] = "debit"

    if "balance" not in df.columns:
        bal_col = next((col_lower[c] for c in ["balance", "ledger balance", "closing balance"] if c in col_lower), None)
        df["balance"] = df[bal_col].apply(parse_naira) if bal_col else None

    # Ensure raw_desc exists
    if "raw_desc" not in df.columns:
        df["raw_desc"] = df.get("description", "").fillna("")

    # Clean description
    df["description"] = df["raw_desc"].apply(_clean_description)

    # Enrich
    df["channel"]  = df["raw_desc"].apply(detect_channel)
    df["merchant"] = df["raw_desc"].apply(extract_merchant)
    df["bank"]     = df["raw_desc"].apply(detect_bank)

    # Type coercions
    df["amount"]  = pd.to_numeric(df["amount"], errors="coerce").fillna(0).abs()
    df["balance"] = pd.to_numeric(df.get("balance"), errors="coerce")

    # Drop unusable rows
    df = df[df["tx_date"].notna()]
    df = df[df["amount"] > 0]
    df = df[df["description"].str.len() > 0]

    # Add dedup fingerprint
    df["fingerprint"] = [
        hashlib.sha256(
            f"{row['tx_date']}|{row['amount']}|{str(row['raw_desc'])[:60]}".encode()
        ).hexdigest()
        for _, row in df.iterrows()
    ]

    return df[["tx_date", "description", "raw_desc", "amount", "tx_type",
               "balance", "channel", "merchant", "bank", "fingerprint"]]


def _clean_description(raw: str) -> str:
    """Remove noise, references, and normalize whitespace."""
    if not raw:
        return ""
    cleaned = re.sub(r"\b\d{8,}\b", "", raw)
    cleaned = re.sub(r"\|.*$", "", cleaned)
    cleaned = " ".join(cleaned.split()).strip()
    return cleaned[:255] if cleaned else raw[:255]


def _find_header_row(path: Path, max_scan: int = 15) -> int:
    """
    Scan the first `max_scan` lines to find the row index that contains
    recognizable column headers.
    Returns 0 if not found.
    """
    header_keywords = {
        "date", "trans date", "transaction date", "value date",
        "details", "description", "narration", "remarks", "particulars",
        "debit", "credit", "amount", "balance", "withdrawals", "deposits",
    }
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_scan:
                    break
                cols = {c.strip().lower().strip('"') for c in line.split(",")}
                if len(cols & header_keywords) >= 2:
                    return i
    except Exception:
        pass
    return 0


def _drop_metadata_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop leading rows that are bank header/metadata, not transactions.
    """
    date_pattern   = re.compile(r"\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}|\d{4}-\d{2}-\d{2}")
    amount_pattern = re.compile(r"[\d,]+\.\d{2}")

    def _is_data_row(row):
        vals = " ".join(str(v) for v in row.values)
        return bool(date_pattern.search(vals) or amount_pattern.search(vals))

    mask = df.apply(_is_data_row, axis=1)
    return df[mask].reset_index(drop=True)
