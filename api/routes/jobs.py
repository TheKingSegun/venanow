"""
api/routes/jobs.py

Job status polling endpoint.
Frontend polls GET /api/jobs/{job_id} until status is "complete" or "failed".

JOB_STORE is an in-memory dict — jobs are lost on Render restart.
In production this will be replaced with a Supabase jobs table.
"""

from fastapi import APIRouter, HTTPException

router = APIRouter()

class JobStatus:
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETE   = "complete"
    FAILED     = "failed"

JOB_STORE: dict[str, dict] = {}
router = APIRouter()


class JobStatus(str):
    PENDING    = "pending"
    PROCESSING = "processing"
    COMPLETE   = "complete"
    FAILED     = "failed"


# In-memory job store — keyed by job_id
# Structure: { job_id: { status, user_id, upload_id, result, error } }
JOB_STORE: dict[str, dict] = {}


@router.get("/{job_id}")
async def get_job_status(job_id: str):
    """
    Poll for job status.

    Returns:
        - status: "pending" | "processing" | "complete" | "failed"
        - result: full analysis dict (only when status == "complete")
        - error:  error message (only when status == "failed")
    """
    job = JOB_STORE.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    return {
        "job_id":    job_id,
        "status":    job["status"],
        "user_id":   job.get("user_id"),
        "upload_id": job.get("upload_id"),
        "result":    job.get("result"),   # None until complete
        "error":     job.get("error"),    # None unless failed
    }


@router.get("/user/{user_id}")
async def get_user_jobs(user_id: str):
    """List all jobs for a user (most recent first)."""
    user_jobs = [
        {"job_id": jid, "status": j["status"], "upload_id": j.get("upload_id")}
        for jid, j in JOB_STORE.items()
        if j.get("user_id") == user_id
    ]
    return {"user_id": user_id, "jobs": list(reversed(user_jobs))}
