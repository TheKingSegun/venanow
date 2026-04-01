"""
api/routes/statements.py

Statement upload endpoint — accepts file, runs pipeline, returns analysis.
"""

import os
import uuid
import shutil
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks

from pipeline.processor import run_pipeline, result_to_dict
from analytics.recommender import generate_recommendations
from analytics.health_score import compute_health_score
from analytics.forecaster import forecast_cashflow
from utils.logger import logger

router = APIRouter()

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "./uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_TYPES = {
    "application/pdf":                                          "pdf",
    "text/csv":                                                 "csv",
    "application/vnd.ms-excel":                                 "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "application/octet-stream":                                 None,  # Sniff from extension
}

MAX_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_SIZE_MB", 10)) * 1024 * 1024


@router.post("/upload")
async def upload_statement(
    file: UploadFile = File(...),
    user_id: str = Form(...),
    emergency_fund: Optional[float] = Form(0.0),
):
    """
    Upload a bank statement and run the full processing pipeline.

    Returns a comprehensive financial analysis including:
    - Categorized transactions
    - Income/expense summary
    - Category breakdown
    - Recommendations
    - Health score
    - Cash flow forecast
    - Recurring payment detection
    """
    # ── Validate file ─────────────────────────────────────────────────────────
    file_ext = Path(file.filename or "").suffix.lower().lstrip(".")
    if file_ext not in ("pdf", "csv", "xls", "xlsx"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: .{file_ext}. Accepted: PDF, CSV, XLS, XLSX."
        )

    # Check file size (read into memory briefly)
    content = await file.read()
    if len(content) > MAX_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {MAX_SIZE_BYTES // 1024 // 1024}MB."
        )

    # ── Save to disk ──────────────────────────────────────────────────────────
    upload_id = str(uuid.uuid4())
    save_path  = UPLOAD_DIR / f"{upload_id}_{file.filename}"

    with open(save_path, "wb") as f:
        f.write(content)

    logger.info(f"Statement saved: {save_path} (user={user_id})")

    # ── Run pipeline ──────────────────────────────────────────────────────────
    try:
        result = run_pipeline(save_path, file_type=file_ext, user_id=user_id)
    except RuntimeError as e:
        save_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        save_path.unlink(missing_ok=True)
        logger.error(f"Pipeline error for user={user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")

    df = result.transactions

    # ── Generate analytics ────────────────────────────────────────────────────
    recommendations = generate_recommendations(
        df,
        emergency_fund=emergency_fund,
    )

    health = compute_health_score(
        df,
        emergency_fund=emergency_fund or 0.0,
    )

    forecast = forecast_cashflow(df)

    # ── Build response ────────────────────────────────────────────────────────
    pipeline_dict = result_to_dict(result)

    response = {
        **pipeline_dict,
        "upload_id":         upload_id,
        "user_id":           user_id,
        "recommendations":   recommendations,
        "health_score":      health,
        "cash_flow_forecast": forecast,
    }

    # Clean up temp file (can be moved to background task in production)
    # save_path.unlink(missing_ok=True)

    return response


@router.get("/history/{user_id}")
async def get_upload_history(user_id: str):
    """
    Return list of previous statement uploads for a user.
    In production: queries statement_uploads table.
    """
    # Stub — wire to DB in production
    return {"user_id": user_id, "uploads": []}
