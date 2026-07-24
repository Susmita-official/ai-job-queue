from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from sqlalchemy import Column, String, DateTime, JSON
from app.database import Base

# 1. Define the states a job can be in
class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"

# 2. Define the types of jobs our system handles
class JobType(str, Enum):
    TASK = "task"
    LLM = "llm"
    AGENT = "agent"

# 3. Define what the user MUST send us
class JobRequest(BaseModel):
    job_type: JobType
    payload: dict = Field(..., description="The input data required for the job execution")

    # This validator acts as a bouncer, rejecting empty payloads
    @field_validator("payload")
    @classmethod
    def payload_must_not_be_empty(cls, v: dict) -> dict:
        if not v:
            raise ValueError("Payload dictionary cannot be empty")
        return v

# 4. Define what we send back to the user
class JobResponse(BaseModel):
    id: str
    job_type: JobType
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    result: Optional[Any] = None
    error: Optional[str] = None
    model_config = {"from_attributes": True}

class JobRecord(Base):
    # WHY: This tells PostgreSQL exactly what to name the table in the database
    __tablename__ = "jobs"
    id = Column(String, primary_key=True)
    job_type = Column(String)
    status = Column(String)
    created_at = Column(DateTime)
    payload = Column(JSON)
    result = Column(JSON, nullable=True)
    error = Column(String,nullable=True)
    updated_at = Column(DateTime)

