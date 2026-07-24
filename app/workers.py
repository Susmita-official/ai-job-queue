import asyncio
from datetime import datetime
import httpx
from functools import wraps
from openai import AsyncOpenAI
import json
import redis.asyncio as aioredis

from app.database import AsyncSessionLocal
from app.models import JobRecord, JobStatus
from app.config import OPENAI_API_KEY

# NEW: Import your Celery app
from app.celery_app import celery_app

# Initialize Groq client
client = AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    base_url="https://api.groq.com/openai/v1"
)

# --- WEEK 3 ROADMAP: Custom Decorators ---

def with_timeout(seconds: int):
    """Cancels a hung job if it takes longer than the specified seconds."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
        return wrapper
    return decorator

def with_retry(max_attempts: int = 3, backoff_factor: float = 2.0):
    """Retries a failing function using exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise e # If we've hit max retries, fail completely
                    
                    sleep_time = backoff_factor ** (attempt + 1)
                    print(f"⚠️ Execution hiccup ({e}). Retrying in {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
            raise last_exception
        return wrapper
    return decorator


# --- AI JOB PROCESSORS ---

@with_retry(max_attempts=3, backoff_factor=2.0)
@with_timeout(seconds=30)
async def process_llm_job(prompt: str) -> dict:
    response = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "You are a helpful AI assistant."},
            {"role": "user", "content": prompt}
        ]
    )
    return {"answer": response.choices[0].message.content}

@with_retry(max_attempts=3, backoff_factor=2.0)
@with_timeout(seconds=45) # Agents take a bit longer!
async def process_agent_job(prompt: str) -> dict:
    # STEP 1: The AI writes the story in English
    response_1 = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "You are a creative writer. Write a very short, 2-sentence story."},
            {"role": "user", "content": prompt}
        ]
    )
    english_story = response_1.choices[0].message.content
    
    # STEP 2: The AI translates its own story into French
    response_2 = await client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "You are a translator. Translate the user's text into French."},
            {"role": "user", "content": english_story}
        ]
    )
    french_story = response_2.choices[0].message.content
    
    return {
        "original_story": english_story,
        "french_translation": french_story
    }

async def publish_job_update(job_id: str, status: str, result: dict = None):
    redis_client = aioredis.from_url("redis://localhost:6379/0")
    
    # Package the update into a clean JSON payload
    message = {"job_id": job_id, "status": status}
    if result:
        message["result"] = result
        
    # Broadcast it to the exact frequency FastAPI is listening to
    await redis_client.publish(f"job:{job_id}:status", json.dumps(message))
    await redis_client.aclose()

# --- CORE WORKER LOGIC ---

async def execute_job(job_id: str):
    # 1. Open the workspace
    async with AsyncSessionLocal() as session:
        # 2. Fetch the job from PostgreSQL
        job = await session.get(JobRecord, job_id)
        if not job:
            print(f"Worker Error: Job {job_id} not found!")
            return

        # 3. Update to RUNNING
        job.status = JobStatus.RUNNING
        job.updated_at = datetime.utcnow()
        await session.commit()
        await publish_job_update(job_id, "RUNNING")
        
        try:
            # Safely get the job type as a lowercase string
            job_type_str = str(job.job_type).lower()

            if "llm" in job_type_str:
                prompt = job.payload.get("prompt", "say hello!")
                job.result = await process_llm_job(prompt)
                job.status = JobStatus.SUCCESS

            elif "agent" in job_type_str:
                prompt = job.payload.get("prompt", "Write a story about a brave knight.")
                job.result = await process_agent_job(prompt)
                job.status = JobStatus.SUCCESS

            else:
                await asyncio.sleep(2)
                job.result = {"answer": f"Processed data for {job.job_type}"}
                job.status = JobStatus.SUCCESS

            # Webhook Delivery Logic
            webhook_url = job.payload.get("webhook_url")
            if webhook_url:
                try:
                    async with httpx.AsyncClient() as http_client:
                        status_val = job.status.value if hasattr(job.status, 'value') else str(job.status)
                        await http_client.post(
                            webhook_url, 
                            json={
                                "job_id": job.id,
                                "status": status_val,
                                "result": job.result
                            }
                        )
                except Exception as webhook_error:
                    print(f"Webhook failed to send: {webhook_error}")    
        
        except asyncio.TimeoutError:
            # Caught by our @with_timeout decorator!
            job.status = JobStatus.FAILED
            job.error = "Job exceeded maximum execution time."
        except Exception as e:
            # Caught by our @with_retry decorator after failing 3 times!
            job.status = JobStatus.FAILED
            job.error = str(e)
            
        finally:
            # Save the final status to DB
            job.updated_at = datetime.utcnow()
            await session.commit()
            
            # Broadcast the FINAL status and the result to the WebSocket!
            final_status = job.status.value if hasattr(job.status, 'value') else str(job.status)
            await publish_job_update(job_id, final_status, job.result)


@celery_app.task(name="tasks.execute_ai_job")
def celery_execute_job(job_id: str):
    """
    This is the official Celery entry point! 
    Celery runs synchronously, so we use asyncio.run to trigger our async logic.
    """
    asyncio.run(execute_job(job_id))

@celery_app.task(name="tasks.scheduled_maintenance")
def scheduled_maintenance(message: str):
    """
    A simple scheduled task that runs automatically via Celery Beat.
    """
    # In a real app, you might query the DB for stale jobs here.
    # For now, we'll just print a message to prove it works!
    print("=" * 40)
    print(f"⏰ CELERY BEAT AUTOMATIC TASK TRIGGERED!")
    print(f"Message: {message}")
    print(f"Time: {datetime.utcnow()}")
    print("=" * 40)
    
    return {"status": "Maintenance completed"}