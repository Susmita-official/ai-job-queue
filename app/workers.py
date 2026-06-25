import asyncio
from datetime import datetime
from app.models import JobStatus

# This temporary local dict behaves like our database for Week 1
FAKE_DB = {}

async def execute_job(job_id: str):
    """Simulates background job execution with state transitions."""
    if job_id not in FAKE_DB:
        return

    job = FAKE_DB[job_id]
    job.status = JobStatus.RUNNING
    job.updated_at = datetime.utcnow()
    
    try:
        # Simulate processing time (e.g., waiting on an LLM or script)
        await asyncio.sleep(5)
        
        # Simulate a successful finish
        job.status = JobStatus.SUCCESS
        job.result = {"message": f"Successfully completed {job.job_type} job", "processed_at": str(datetime.utcnow())}
    except Exception as e:
        # If anything breaks, make sure the user sees FAILED, not a server crash
        job.status = JobStatus.FAILED
        job.error = str(e)
    finally:
        job.updated_at = datetime.utcnow()