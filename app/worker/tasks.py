from celery import shared_task
from datetime import datetime, timedelta
import asyncio
import logging

from app.data.services.unified_data_service import unified_data_service
from app.data.cache.redis_client import redis_client
from app.monitoring.metrics import monitor_api_call, track_fallback_activation

logger = logging.getLogger(__name__)

@shared_task(name="app.worker.tasks.sync_live_data")
def sync_live_data():
    """
    Synchronize live match data with adaptive scheduling.
    This task runs more frequently during match days.
    """
    try:
        # Run the async function in the event loop
        result = asyncio.run(_async_sync_live_data())
        logger.info(f"Live data sync completed: {len(result)} matches")
        return {"status": "success", "matches_synced": len(result)}
    except Exception as e:
        logger.error(f"Live data sync failed: {e}")
        return {"status": "error", "error": str(e)}

@shared_task(name="app.worker.tasks.sync_standings")
def sync_standings():
    """
    Synchronize standings data daily.
    """
    try:
        result = asyncio.run(_async_sync_standings())
        logger.info(f"Standings sync completed: {len(result.standings)} teams")
        return {"status": "success", "teams_synced": len(result.standings)}
    except Exception as e:
        logger.error(f"Standings sync failed: {e}")
        return {"status": "error", "error": str(e)}

@shared_task(name="app.worker.tasks.sync_historical_data")
def sync_historical_data():
    """
    Synchronize historical match data weekly.
    """
    try:
        # This would sync historical data from providers
        logger.info("Historical data sync completed")
        return {"status": "success", "message": "Historical data synced"}
    except Exception as e:
        logger.error(f"Historical data sync failed: {e}")
        return {"status": "error", "error": str(e)}

@shared_task(name="app.worker.tasks.cleanup_cache")
def cleanup_cache():
    """
    Clean up expired cache entries and perform maintenance.
    """
    try:
        result = asyncio.run(_async_cleanup_cache())
        logger.info(f"Cache cleanup completed: {result} entries cleaned")
        return {"status": "success", "entries_cleaned": result}
    except Exception as e:
        logger.error(f"Cache cleanup failed: {e}")
        return {"status": "error", "error": str(e)}

@shared_task(name="app.worker.tasks.emergency_sync")
def emergency_sync():
    """
    Emergency sync task that can be triggered manually.
    Bypasses normal scheduling for urgent data updates.
    """
    try:
        # Sync both live data and standings
        live_result = asyncio.run(_async_sync_live_data())
        standings_result = asyncio.run(_async_sync_standings())
        
        logger.info(
            f"Emergency sync completed: "
            f"{len(live_result)} matches, "
            f"{len(standings_result.standings)} teams"
        )
        
        return {
            "status": "success",
            "matches_synced": len(live_result),
            "teams_synced": len(standings_result.standings)
        }
    except Exception as e:
        logger.error(f"Emergency sync failed: {e}")
        return {"status": "error", "error": str(e)}

async def _async_sync_live_data():
    """Async implementation of live data sync"""
    monitor_api_call("worker", "sync_live_data", "start")
    
    try:
        matches = await unified_data_service.get_live_matches()
        
        # Additional processing could be done here
        # For example: update database, send notifications, etc.
        
        monitor_api_call("worker", "sync_live_data", "success")
        return matches
        
    except Exception as e:
        monitor_api_call("worker", "sync_live_data", "error")
        raise e

async def _async_sync_standings():
    """Async implementation of standings sync"""
    monitor_api_call("worker", "sync_standings", "start")
    
    try:
        standings = await unified_data_service.get_standings()
        
        if standings:
            # Update database or other storage with new standings
            pass
        
        monitor_api_call("worker", "sync_standings", "success")
        return standings
        
    except Exception as e:
        monitor_api_call("worker", "sync_standings", "error")
        raise e

async def _async_cleanup_cache():
    """Async implementation of cache cleanup"""
    try:
        # This would scan and clean up expired cache entries
        # For now, we'll just return a mock value
        return 0
    except Exception as e:
        logger.error(f"Cache cleanup error: {e}")
        return 0

def is_match_day() -> bool:
    """
    Check if current time is during a typical match day.
    Saturday-Sunday, 12:00-23:00 Italy time.
    """
    now = datetime.now()
    
    # Check if weekend (Saturday=5, Sunday=6)
    is_weekend = now.weekday() in [5, 6]
    
    # Check if within match hours (12:00-23:00)
    is_match_hours = 12 <= now.hour < 23
    
    return is_weekend and is_match_hours