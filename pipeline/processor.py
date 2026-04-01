"""
pipeline/processor.py

Pipeline orchestrator — runs all stages in sequence:
  1. Ingest (PDF/CSV/Excel → raw DataFrame)
  2. Clean (deduplicate, normalize)
  3. Classify (category assignment)
  4. Detect recurring (subscriptions)
  5. Validate & return result

This is the single entry point for the API to call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import time

import pandas as pd

from pipeline.ingestion  import ingest_statement
from pipeline.cleaner    import clean_transactions, validate_dataframe
from pipeline.classifier import classify_dataframe
from pipeline.recurring  import detect_recurring, get_recurring_summary
from utils.logger        import logger


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    """Container for the full pipeline output."""
    transactions:       pd.DataFrame       # Final annotated transactions
    validation_report:  dict               # Quality stats from cleaner
    recurring_summary:  list[dict]         # Detected subscriptions/recurring
    bank_detected:      Optional[str]      # e.g. 'GTBank'
    period_start:       Optional[str]      # ISO date string
    period_end:         Optional[str]      # ISO date string
    tx_count:           int
    processing_time_s:  float
    warnings:           list[str] = field(default_factory=list)
    errors:             list[str] = field(default_factory=list)


# ── Entry Point ───────────────────────────────────────────────────────────────

def run_pipeline(
    file_path: str | Path,
    file_type: Optional[str] = None,
    user_id: Optional[str] = None,
) -> PipelineResult:
    """
    Run the full VenaNow pipeline on a bank statement file.

    Args:
        file_path : Path to the uploaded statement.
        file_type : 'pdf', 'csv', or 'excel'. Auto-detected if None.
        user_id   : For logging context.

    Returns:
        PipelineResult with all processed data.

    Raises:
        Exception on unrecoverable errors (file not found, parse failure).
    """
    t_start = time.perf_counter()
    path = Path(file_path)
    warnings: list[str] = []
    errors:   list[str] = []

    logger.info(f"[Pipeline] Starting for user={user_id}, file={path.name}")

    # ── Stage 1: Ingest ───────────────────────────────────────────────────────
    logger.info("[Stage 1/4] Ingestion")
    try:
        raw_df = ingest_statement(path, file_type)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise RuntimeError(f"Could not read statement: {e}") from e

    if raw_df.empty:
        raise RuntimeError("No transactions found in the uploaded file.")

    # ── Stage 2: Clean ────────────────────────────────────────────────────────
    logger.info("[Stage 2/4] Cleaning")
    clean_df = clean_transactions(raw_df)
    validation = validate_dataframe(clean_df)
    warnings.extend(validation.get("warnings", []))

    if clean_df.empty:
        raise RuntimeError(
            "All transactions were removed during cleaning. "
            "Check that the file is a standard bank statement."
        )

    # ── Stage 3: Classify ─────────────────────────────────────────────────────
    logger.info("[Stage 3/4] Classification")
    classified_df = classify_dataframe(clean_df)

    # ── Stage 4: Recurring Detection ──────────────────────────────────────────
    logger.info("[Stage 4/4] Recurring detection")
    final_df = detect_recurring(classified_df)
    recurring_summary = get_recurring_summary(final_df)

    # ── Metadata ──────────────────────────────────────────────────────────────
    dates = pd.to_datetime(final_df["tx_date"])
    period_start = str(dates.min().date()) if not dates.empty else None
    period_end   = str(dates.max().date()) if not dates.empty else None

    # Detect dominant bank from annotations
    if "bank" in final_df.columns:
        bank_counts = final_df["bank"].value_counts()
        bank_detected = bank_counts.index[0] if not bank_counts.empty and bank_counts.iloc[0] > 0 else None
    else:
        bank_detected = None

    elapsed = time.perf_counter() - t_start
    logger.info(
        f"[Pipeline] Complete. {len(final_df)} transactions, "
        f"{len(recurring_summary)} recurring groups, {elapsed:.2f}s"
    )

    return PipelineResult(
        transactions=final_df,
        validation_report=validation,
        recurring_summary=recurring_summary,
        bank_detected=bank_detected,
        period_start=period_start,
        period_end=period_end,
        tx_count=len(final_df),
        processing_time_s=round(elapsed, 3),
        warnings=warnings,
        errors=errors,
    )


# ── Convenience: DataFrame → API-ready dict ───────────────────────────────────

def result_to_dict(result: PipelineResult) -> dict:
    """
    Convert PipelineResult to a JSON-serializable dict for the API response.
    """
    df = result.transactions

    # Category spending summary
    if "category" in df.columns:
        cat_spend = (
            df[df["tx_type"] == "debit"]
            .groupby("category")["amount"]
            .sum()
            .round(2)
            .to_dict()
        )
    else:
        cat_spend = {}

    # Month summary
    total_income   = float(df[df["tx_type"] == "credit"]["amount"].sum())
    total_expenses = float(df[df["tx_type"] == "debit"]["amount"].sum())
    net_savings    = round(total_income - total_expenses, 2)
    savings_rate   = round(net_savings / total_income * 100, 2) if total_income > 0 else 0.0

    return {
        "status":          "success",
        "tx_count":        result.tx_count,
        "bank_detected":   result.bank_detected,
        "period_start":    result.period_start,
        "period_end":      result.period_end,
        "processing_time": result.processing_time_s,
        "summary": {
            "total_income":    total_income,
            "total_expenses":  total_expenses,
            "net_savings":     net_savings,
            "savings_rate_pct": savings_rate,
        },
        "category_spend":       cat_spend,
        "recurring_summary":    result.recurring_summary,
        "validation_report":    result.validation_report,
        "warnings":             result.warnings,
        "transactions": df.to_dict(orient="records"),
    }
