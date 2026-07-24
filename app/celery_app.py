from celery import Celery
from celery.schedules import crontab

# Initialize the Celery application
celery_app = Celery(
    "ai_job_queue",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
    include=["app.workers"]  # <--- THIS IS THE MISSING LINK!
)

# Optional configuration settings
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

# Tell Celery which queue to send specific tasks to
celery_app.conf.task_routes = {
    'tasks.execute_ai_job': {'queue': 'ai_tasks'},
    'tasks.scheduled_maintenance': {'queue': 'maintenance_tasks'},
}

# --- CELERY BEAT SCHEDULE ---
celery_app.conf.beat_schedule = {
    "run-maintenance-every-minute": {
        "task": "tasks.scheduled_maintenance",
        "schedule": 60.0, 
        "args": ("System cleanup check",)
    },
}