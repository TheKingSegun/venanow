"""
api/routes/statements.py

Statement upload endpoint.
Now uses background jobs — returns a job_id immediately,
pipeline runs in the background, frontend polls /api/jobs/{job_id}.
"""

import os
import uuid
import math
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks

from pipeline.processor import run_pipeline, result_to_dict
from analytics.recommender import generate_recommendations
from analytics.health_score import compute_health_score
from analytics.forecaster import forecast_cashflow
from api.routes.jobs import JOB_STORE, JobStatus
from utils.logger import logger

router = APIRouter()

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "./uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_SIZE_MB", 10)) * 1024 * 1024


@router.post("/upload")
async def upload_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    user_id: str = Form(...),
    emergency_fund: Optional[float] = Form(0.0),
):
    """
    Upload a bank statement. Returns a job_id immediately.
    Pipeline runs in the background — poll GET /api/jobs/{job_id} for results.
    """
    # Validate file type
    file_ext = Path(file.filename or "").suffix.lower().lstrip(".")
    if file_ext not in ("pdf", "csv", "xls", "xlsx"):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: .{file_ext}. Accepted: PDF, CSV, XLS, XLSX."
        )

    # Check file size
    content = await file.read()
    if len(content) > MAX_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {MAX_SIZE_BYTES // 1024 // 1024}MB."
        )

    # Save to disk
    upload_id = str(uuid.uuid4())
    save_path = UPLOAD_DIR / f"{upload_id}_{file.filename}"
    with open(save_path, "wb") as f:
        f.write(content)

    logger.info(f"Statement saved: {save_path} (user={user_id})")

    # Register job as pending
    job_id = str(uuid.uuid4())
    JOB_STORE[job_id] = {
        "status": JobStatus.PENDING,
        "user_id": user_id,
        "upload_id": upload_id,
        "result": None,
        "error": None,
    }

    # Kick off background processing — returns immediately
    background_tasks.add_task(
        _run_pipeline_job,
        job_id=job_id,
        save_path=save_path,
        file_ext=file_ext,
        user_id=user_id,
        upload_id=upload_id,
        emergency_fund=emergency_fund or 0.0,
    )

    return {
        "job_id": job_id,
        "status": JobStatus.PENDING,
        "message": "Statement received. Poll /api/jobs/{job_id} for results.",
    }


@router.get("/history/{user_id}")
async def get_upload_history(user_id: str):
    """Return list of previous uploads. Stub — wire to DB in production."""
    return {"user_id": user_id, "uploads": []}


# ── Background task ───────────────────────────────────────────────────────────

def _run_pipeline_job(
    job_id: str,
    save_path: Path,
    file_ext: str,
    user_id: str,
    upload_id: str,
    emergency_fund: float,
):
    """Runs in background. Updates JOB_STORE when done."""
    JOB_STORE[job_id]["status"] = JobStatus.PROCESSING

    try:
        result = run_pipeline(save_path, file_type=file_ext, user_id=user_id)
        df = result.transactions

        recommendations = generate_recommendations(df, emergency_fund=emergency_fund)
        health = compute_health_score(df, emergency_fund=emergency_fund)
        forecast = forecast_cashflow(df)
        pipeline_dict = result_to_dict(result)

        response = {
            **pipeline_dict,
            "upload_id":          upload_id,
            "user_id":            user_id,
            "recommendations":    recommendations,
            "health_score":       health,
            "cash_flow_forecast": forecast,
        }

        JOB_STORE[job_id]["status"] = JobStatus.COMPLETE
        JOB_STORE[job_id]["result"] = _sanitize(response)
        logger.info(f"Job {job_id} complete for user={user_id}")

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        JOB_STORE[job_id]["status"] = JobStatus.FAILED
        JOB_STORE[job_id]["error"] = str(e)

    finally:
        # Clean up uploaded file
        try:
            save_path.unlink(missing_ok=True)
        except Exception:
            pass


def _sanitize(obj):
    """Recursively replace NaN/Infinity floats with None for JSON compliance."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj
