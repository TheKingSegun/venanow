"""
api/routes/statements.py

Statement upload endpoint with:
- Supabase JWT auth (user_id comes from token, not form field)
- Background job processing
- Full DB persistence to Supabase after each pipeline run
"""

import os
import uuid
import math
import json
from pathlib import Path
from typing import Optional
from datetime import date

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks, Depends
import httpx

from pipeline.processor import run_pipeline, result_to_dict
from analytics.recommender import generate_recommendations
from analytics.health_score import compute_health_score
from analytics.forecaster import forecast_cashflow
from api.routes.jobs import JOB_STORE, JobStatus
from api.auth import get_current_user
from utils.logger import logger

router = APIRouter()

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "./uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_SIZE_BYTES = int(os.getenv("MAX_UPLOAD_SIZE_MB", 10)) * 1024 * 1024

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


@router.post("/upload")
async def upload_statement(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    emergency_fund: Optional[float] = Form(0.0),
    user=Depends(get_current_user),
):
    user_id = user["sub"]

    file_ext = Path(file.filename or "").suffix.lower().lstrip(".")
    if file_ext not in ("pdf", "csv", "xls", "xlsx"):
        raise HTTPException(status_code=400, detail=f"Unsupported file type: .{file_ext}.")

    content = await file.read()
    if len(content) > MAX_SIZE_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large. Max {MAX_SIZE_BYTES // 1024 // 1024}MB.")

    upload_id = str(uuid.uuid4())
    save_path = UPLOAD_DIR / f"{upload_id}_{file.filename}"
    with open(save_path, "wb") as f:
        f.write(content)

    logger.info(f"Statement saved: {save_path} (user={user_id})")

    job_id = str(uuid.uuid4())
    JOB_STORE[job_id] = {"status": JobStatus.PENDING, "user_id": user_id, "upload_id": upload_id, "result": None, "error": None}

    background_tasks.add_task(
        _run_pipeline_job,
        job_id=job_id, save_path=save_path, file_ext=file_ext,
        file_name=file.filename, file_size_kb=len(content) // 1024,
        user_id=user_id, upload_id=upload_id, emergency_fund=emergency_fund or 0.0,
    )

    return {"job_id": job_id, "status": JobStatus.PENDING, "message": "Poll /api/jobs/{job_id} for results."}


@router.get("/history/{user_id}")
async def get_upload_history(user_id: str, user=Depends(get_current_user)):
    if user["sub"] != user_id:
        raise HTTPException(status_code=403, detail="Access denied.")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return {"user_id": user_id, "uploads": []}
    async with httpx.AsyncClient() as client:
        res = await client.get(
            f"{SUPABASE_URL}/rest/v1/statement_uploads",
            headers=_supabase_headers(),
            params={"user_id": f"eq.{user_id}", "order": "created_at.desc", "limit": "20"},
        )
    return {"user_id": user_id, "uploads": res.json() if res.is_success else []}


# ── Background job ────────────────────────────────────────────────────────────

def _run_pipeline_job(job_id, save_path, file_ext, file_name, file_size_kb, user_id, upload_id, emergency_fund):
    import asyncio
    JOB_STORE[job_id]["status"] = JobStatus.PROCESSING
    try:
        result = run_pipeline(save_path, file_type=file_ext, user_id=user_id)
        df = result.transactions
        recommendations = generate_recommendations(df, emergency_fund=emergency_fund)
        health = compute_health_score(df, emergency_fund=emergency_fund)
        forecast = forecast_cashflow(df)
        pipeline_dict = result_to_dict(result)
        response = _sanitize({**pipeline_dict, "upload_id": upload_id, "user_id": user_id,
                               "recommendations": recommendations, "health_score": health, "cash_flow_forecast": forecast})

        if SUPABASE_URL and SUPABASE_SERVICE_KEY:
            try:
                asyncio.run(_persist_to_db(user_id, upload_id, file_name, file_ext, file_size_kb, result, pipeline_dict, health, recommendations))
            except Exception as db_err:
                logger.warning(f"DB persistence failed (non-fatal): {db_err}")

        JOB_STORE[job_id]["status"] = JobStatus.COMPLETE
        JOB_STORE[job_id]["result"] = response
        logger.info(f"Job {job_id} complete for user={user_id}")

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        JOB_STORE[job_id]["status"] = JobStatus.FAILED
        JOB_STORE[job_id]["error"] = str(e)
    finally:
        try:
            save_path.unlink(missing_ok=True)
        except Exception:
            pass


async def _persist_to_db(user_id, upload_id, file_name, file_ext, file_size_kb, result, pipeline_dict, health, recommendations):
    df = result.transactions
    summary = pipeline_dict.get("summary", {})
    headers = _supabase_headers()

    async with httpx.AsyncClient(timeout=30) as client:
        # 1. Upsert statement upload record
        await client.post(f"{SUPABASE_URL}/rest/v1/statement_uploads",
            headers={**headers, "Prefer": "resolution=merge-duplicates"},
            json={"id": upload_id, "user_id": user_id, "filename": file_name, "file_type": file_ext,
                  "file_size_kb": file_size_kb, "bank_detected": pipeline_dict.get("bank_detected", ""),
                  "period_start": pipeline_dict.get("period_start"), "period_end": pipeline_dict.get("period_end"),
                  "tx_count": len(df), "status": "complete", "processed_at": date.today().isoformat()})

        # 2. Insert transactions in batches of 100
        tx_rows = []
        for _, row in df.iterrows():
            bal = row.get("balance")
            try:
                bal_float = float(bal) if bal is not None else None
                if bal_float is not None and math.isnan(bal_float): bal_float = None
            except: bal_float = None
            tx_rows.append({
                "user_id": user_id, "upload_id": upload_id,
                "tx_date": str(row.get("tx_date", "")),
                "description": str(row.get("description", ""))[:500],
                "raw_description": str(row.get("raw_desc", ""))[:500],
                "amount": float(row.get("amount", 0)),
                "tx_type": str(row.get("tx_type", "debit")),
                "balance": bal_float,
                "channel": str(row.get("channel", ""))[:100],
                "merchant": str(row.get("merchant", ""))[:200],
                "bank_detected": str(row.get("bank", ""))[:100],
                "is_recurring": bool(row.get("is_recurring", False)),
                "fingerprint": str(row.get("fingerprint", ""))[:64],
                "classified_by": "rule",
            })

        for i in range(0, len(tx_rows), 100):
            await client.post(f"{SUPABASE_URL}/rest/v1/transactions",
                headers={**headers, "Prefer": "resolution=ignore-duplicates"},
                json=tx_rows[i:i+100])

        # 3. Upsert health snapshot
        await client.post(f"{SUPABASE_URL}/rest/v1/financial_health_snapshots",
            headers={**headers, "Prefer": "resolution=merge-duplicates"},
            json={"user_id": user_id, "month": date.today().replace(day=1).isoformat(),
                  "total_income": float(summary.get("total_income", 0)),
                  "total_expenses": float(summary.get("total_expenses", 0)),
                  "net_savings": float(summary.get("net_savings", 0)),
                  "savings_rate": float(summary.get("savings_rate_pct", 0)),
                  "health_score": int(health.get("score", 0)),
                  "score_breakdown": health.get("breakdown", {}),
                  "expense_by_category": pipeline_dict.get("category_spend", {})})

        # 4. Replace recommendations
        await client.delete(f"{SUPABASE_URL}/rest/v1/recommendations",
            headers=headers, params={"user_id": f"eq.{user_id}"})
        if recommendations:
            await client.post(f"{SUPABASE_URL}/rest/v1/recommendations",
                headers=headers,
                json=[{"user_id": user_id, "type": r.get("type", "tip"),
                       "title": r.get("title", "")[:200], "body": r.get("body", ""),
                       "impact_amount": float(r["impact_amount"]) if r.get("impact_amount") else None,
                       "priority": int(r.get("priority", 5))} for r in recommendations])

    logger.info(f"DB persistence complete for upload={upload_id}")


def _supabase_headers():
    return {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}", "Content-Type": "application/json"}


def _sanitize(obj):
    if isinstance(obj, float): return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict): return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_sanitize(i) for i in obj]
    return obj
