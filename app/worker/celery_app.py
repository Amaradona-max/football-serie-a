from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

celery_app = Celery(
    "football_predictions",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.worker.tasks"]
)

# Configuration
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Europe/Rome",
    enable_utc=True,
)

# Adaptive scheduling based on match days
celery_app.conf.beat_schedule = {
    # During match days (Saturday-Sunday, 12:00-23:00) - sync every 2 minutes
    "sync-live-data-matchdays": {
        "task": "app.worker.tasks.sync_live_data",
        "schedule": crontab(
            day_of_week="6,7",  # Saturday, Sunday
            hour="12-23",      # 12:00 - 23:00
            minute="*/2"       # Every 2 minutes
        ),
    },
    # Weekdays and off-hours - sync every 30 minutes
    "sync-live-data-weekdays": {
        "task": "app.worker.tasks.sync_live_data",
        "schedule": crontab(
            minute="*/30"      # Every 30 minutes
        ),
    },
    # Standings sync - once per day at 3:00 AM
    "sync-standings-daily": {
        "task": "app.worker.tasks.sync_standings",
        "schedule": crontab(hour=3, minute=0),  # 3:00 AM daily
    },
    # Historical data sync - once per week on Monday at 4:00 AM
    "sync-historical-data-weekly": {
        "task": "app.worker.tasks.sync_historical_data",
        "schedule": crontab(
            day_of_week=1,      # Monday
            hour=4,            # 4:00 AM
            minute=0
        ),
    },
    # Cache cleanup - once per day at 2:00 AM
    "cleanup-cache-daily": {
        "task": "app.worker.tasks.cleanup_cache",
        "schedule": crontab(hour=2, minute=0),  # 2:00 AM daily
    },
}

# Task routes
celery_app.conf.task_routes = {
    "app.worker.tasks.sync_live_data": {"queue": "live_data"},
    "app.worker.tasks.sync_standings": {"queue": "static_data"},
    "app.worker.tasks.sync_historical_data": {"queue": "historical_data"},
    "app.worker.tasks.cleanup_cache": {"queue": "maintenance"},
}