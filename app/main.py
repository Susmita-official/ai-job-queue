from fastapi import FastAPI, HTTPException
from uuid import uuid4
from datetime import datetime
import asyncio
import redis.asyncio as aioredis

from app.models import JobRequest, JobResponse, JobStatus
from app.workers import execute_job
from contextlib import asynccontextmanager
from app.database import engine, Base, AsyncSessionLocal
from app.models import JobRecord

from typing import List
from sqlalchemy import text, func
from sqlalchemy.future import select
from app.workers import celery_execute_job
from fastapi import WebSocket, WebSocketDisconnect
from app.websocket_manager import manager

import time
import logging
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse

# Set up a basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP CODE ---
    print("Server is starting up... Building database tables!")
    async with engine.begin() as conn:
        # This translates our Python blueprint into a real PostgreSQL table
        await conn.run_sync(Base.metadata.create_all)
    
    yield # This pauses the function while the server is running
    
    # --- SHUTDOWN CODE ---
    print("Server is shutting down...")

app = FastAPI(
    title="AI-Native Asynchronous Job Queue",
    description="Week 1 Foundations: Core API & Async Execution Loop",
    version="1.0.0",
    lifespan = lifespan
)

# --- MIDDLEWARE ---

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Pass the request down the chain to the actual endpoint
    response = await call_next(request)
    
    # Calculate how long it took to get the response back
    process_time = time.time() - start_time
    
    # Log the exact method, path, status, and latency
    logger.info(f"{request.method} {request.url.path} - Status: {response.status_code} - Latency: {process_time:.4f}s")
    
    return response

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Grab the user's IP address
    client_ip = request.client.host
    redis_client = aioredis.from_url("redis://localhost:6379/0")
    
    # 60-second sliding window rules
    current_time = int(time.time() * 1000) # Current time in milliseconds
    window_size_ms = 60000 
    request_limit = 100
    
    redis_key = f"rate_limit:{client_ip}"
    
    try:
        # 1. Delete any request timestamps older than 60 seconds
        await redis_client.zremrangebyscore(redis_key, 0, current_time - window_size_ms)
        
        # 2. Count how many requests this IP has made in the last 60 seconds
        request_count = await redis_client.zcard(redis_key)
        
        # 3. If they hit the limit, block them immediately (Status 429: Too Many Requests)
        if request_count >= request_limit:
            return JSONResponse(
                status_code=429, 
                content={"detail": "Rate limit exceeded. Try again in a minute."}
            )
        
        # 4. Otherwise, log this new request's timestamp in Redis
        await redis_client.zadd(redis_key, {str(current_time): current_time})
        # Set an expiration on the key so Redis doesn't fill up with old data
        await redis_client.expire(redis_key, 60)
        
    finally:
        await redis_client.aclose()
        
    # Pass the request through to the actual endpoint
    response = await call_next(request)
    return response

@app.post("/v1/jobs", response_model=JobResponse, status_code=202)
async def create_job(request: JobRequest):
    """Submits a new background job and returns its tracking ID immediately."""
    job_id = str(uuid4())
    now = datetime.utcnow()
    
    async with AsyncSessionLocal() as session:
        # Instantiate the internal tracking representation
        new_job = JobRecord(
            id=job_id,
            job_type=request.job_type,
            status=JobStatus.PENDING,
            payload=request.payload,
            created_at=now,
            updated_at=now
        )
        #Store it in PostgreSQL (Goodbye FAKE_DB!)
        session.add(new_job)
        await session.commit()
        await session.refresh(new_job) # Grabs the officially saved version from the DB
    
    # Hand off execution to Celery instantly without blocking!
    celery_execute_job.delay(new_job.id)
    
    return new_job

@app.get("/v1/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str):
    """Fetches the latest execution status or result of a given job."""
    async with AsyncSessionLocal() as session:
        job = await session.get(JobRecord,job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

@app.get("/v1/jobs", response_model=List[JobResponse])
async def list_all_jobs():
    """Fetches every single job currently in the database."""
    async with AsyncSessionLocal() as session:
        # 1. Build a SQL query that says "SELECT * FROM jobs"
        query = select(JobRecord)
        
        # 2. Execute the query against PostgreSQL
        result = await session.execute(query)
        
        # 3. Extract the actual Python objects from the result
        jobs = result.scalars().all()
        
        return jobs

@app.delete("/v1/jobs/{job_id}")
async def delete_job(job_id: str):
    """Deletes a job from the database."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch the job
        job = await session.get(JobRecord, job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # 2 & 3. Delete it and save the changes
        await session.delete(job)
        await session.commit()
        
        return {"detail": f"Job {job_id} successfully deleted from database"}

@app.post("/test-webhook")
async def receive_webhook(payload: dict):
    """A dummy endpoint to act as an external service receiving our webhook."""
    print("\n" + "="*50)
    print("🎉 WEBHOOK RECEIVED SUCCESSFULLY!")
    print(f"Job ID: {payload.get('job_id')}")
    print(f"Status: {payload.get('status')}")
    print("Result Data:")
    print(payload.get('result'))
    print("="*50 + "\n")
    return {"message": "Webhook processed!"}

# --- WEBSOCKET REDIS LISTENER ---
async def listen_to_redis_for_job(job_id: str, websocket: WebSocket):
    # Connect to the Redis Radio Tower
    redis_client = aioredis.from_url("redis://localhost:6379/0")
    pubsub = redis_client.pubsub()
    
    # Tune into the exact frequency for this specific job
    channel_name = f"job:{job_id}:status"
    await pubsub.subscribe(channel_name)
    
    try:
        # Listen forever while the connection is open
        async for message in pubsub.listen():
            if message["type"] == "message":
                # When a message comes through, push it immediately to the browser!
                raw_data = message["data"].decode("utf-8")
                await websocket.send_text(raw_data)
    finally:
        await pubsub.unsubscribe(channel_name)
        await redis_client.aclose()

# --- SYSTEM ENDPOINTS ---

@app.get("/health")
async def health_check():
    health_status = {"status": "ok", "database": "unhealthy", "redis": "unhealthy"}
    
    # 1. Check PostgreSQL Database
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            health_status["database"] = "healthy"
    except Exception as e:
        print(f"DB Health Check Failed: {e}")

    # 2. Check Redis
    try:
        redis_client = aioredis.from_url("redis://localhost:6379/0")
        await redis_client.ping()
        health_status["redis"] = "healthy"
        await redis_client.aclose()
    except Exception as e:
        print(f"Redis Health Check Failed: {e}")

    # If anything is broken, throw a 503 error
    if health_status["database"] != "healthy" or health_status["redis"] != "healthy":
        raise HTTPException(status_code=503, detail=health_status)

    return health_status

@app.get("/metrics")
async def get_metrics():
    metrics = {
        "jobs": {
            "PENDING": 0,
            "RUNNING": 0,
            "SUCCESS": 0,
            "FAILED": 0
        }
    }
    
    try:
        async with AsyncSessionLocal() as session:
            # Query the database to group and count jobs by their status
            query = select(JobRecord.status, func.count(JobRecord.id)).group_by(JobRecord.status)
            result = await session.execute(query)
            
            for status, count in result.all():
                # Extract the string value from the Enum to use as a dictionary key
                status_key = status.value if hasattr(status, 'value') else str(status)
                metrics["jobs"][status_key] = count
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {str(e)}")
        
    return metrics

# --- WEBSOCKET ROUTE ---
@app.websocket("/v1/jobs/{job_id}/stream")
async def job_status_stream(websocket: WebSocket, job_id: str):
    await manager.connect(job_id, websocket)
    
    # Fire up the Redis listener in the background
    redis_task = asyncio.create_task(listen_to_redis_for_job(job_id, websocket))
    
    try:
        while True:
            # Keep the web connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        # If the user closes their tab, clean everything up
        manager.disconnect(job_id, websocket)
        redis_task.cancel()


# --- SERVER-SENT EVENTS (SSE) ROUTE ---

async def sse_job_listener(job_id: str):
    """Generator that listens to Redis and yields SSE formatted strings."""
    redis_client = aioredis.from_url("redis://localhost:6379/0")
    pubsub = redis_client.pubsub()
    channel_name = f"job:{job_id}:status"
    
    await pubsub.subscribe(channel_name)
    
    try:
        # Yield an initial connection message
        yield f"data: {{\"status\": \"CONNECTED\", \"message\": \"Listening to job {job_id}\"}}\n\n"
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                raw_data = message["data"].decode("utf-8")
                
                # SSE strictly requires the "data: " prefix and double newline suffix
                yield f"data: {raw_data}\n\n"
                
                # Close the stream automatically if the job finishes
                if "SUCCESS" in raw_data or "FAILED" in raw_data:
                    break
    finally:
        await pubsub.unsubscribe(channel_name)
        await redis_client.aclose()


@app.get("/v1/jobs/{job_id}/events")
async def job_status_events(job_id: str):
    """The actual FastAPI endpoint returning the SSE stream."""
    return StreamingResponse(
        sse_job_listener(job_id), 
        media_type="text/event-stream"
    )

