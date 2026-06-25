from fastapi import FastAPI, HTTPException
from uuid import uuid4
from datetime import datetime
import asyncio

from app.models import JobRequest, JobResponse, JobStatus
from app.workers import FAKE_DB, execute_job

app = FastAPI(
    title="AI-Native Asynchronous Job Queue",
    description="Week 1 Foundations: Core API & Async Execution Loop",
    version="1.0.0"
)

@app.post("/v1/jobs", response_model=JobResponse, status_code=202)
async def create_job(request: JobRequest):
    """Submits a new background job and returns its tracking ID immediately."""
    job_id = str(uuid4())
    now = datetime.utcnow()
    
    # Instantiate the internal tracking representation
    new_job = JobResponse(
        id=job_id,
        job_type=request.job_type,
        status=JobStatus.PENDING,
        created_at=now,
        updated_at=now
    )
    
    # Store it in memory
    FAKE_DB[job_id] = new_job
    
    # Hand off execution to the background event loop instantly without blocking
    asyncio.create_task(execute_job(job_id))
    
    return new_job

@app.get("/v1/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str):
    """Fetches the latest execution status or result of a given job."""
    if job_id not in FAKE_DB:
        raise HTTPException(status_code=404, detail="Job not found")
    return FAKE_DB[job_id]

@app.get("/v1/jobs")
async def list_all_jobs():
    """Lists every job currently resident in the volatile memory database."""
    return list(FAKE_DB.values())

@app.delete("/v1/jobs/{job_id}", status_code=204)
async def cancel_or_delete_job(job_id: str):
    """Purges a job tracking index from the memory store."""
    if job_id not in FAKE_DB:
        raise HTTPException(status_code=404, detail="Job not found")
    del FAKE_DB[job_id]
    return