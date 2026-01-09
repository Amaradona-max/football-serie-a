from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional

from app.data.services.unified_data_service import unified_data_service
from app.data.models.common import MatchLive, MatchHistorical, Standings
from app.core.dependencies import verify_api_key
from app.monitoring.metrics import monitor_api_call

router = APIRouter()

@router.get("/fixtures/live", response_model=List[MatchLive])
async def get_live_fixtures(api_key: str = Depends(verify_api_key)):
    """
    Get live matches from Serie A.
    Uses hybrid data providers with fallback mechanism.
    """
    try:
        monitor_api_call("api", "fixtures_live", "request")
        matches = await unified_data_service.get_live_matches()
        monitor_api_call("api", "fixtures_live", "success")
        return matches
    except Exception as e:
        monitor_api_call("api", "fixtures_live", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/fixtures/{match_id}", response_model=MatchHistorical)
async def get_fixture_by_id(
    match_id: int, 
    api_key: str = Depends(verify_api_key)
):
    """
    Get detailed information about a specific match.
    Includes historical data and statistics.
    """
    try:
        monitor_api_call("api", "fixture_by_id", "request")
        match = await unified_data_service.get_match_by_id(match_id)
        if not match:
            raise HTTPException(status_code=404, detail="Match not found")
        monitor_api_call("api", "fixture_by_id", "success")
        return match
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "fixture_by_id", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/standings", response_model=Standings)
async def get_standings(api_key: str = Depends(verify_api_key)):
    """
    Get current Serie A standings.
    Updated regularly with cache fallback.
    """
    try:
        monitor_api_call("api", "standings", "request")
        standings = await unified_data_service.get_standings()
        if not standings:
            raise HTTPException(status_code=404, detail="Standings not available")
        monitor_api_call("api", "standings", "success")
        return standings
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "standings", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def health_check():
    """
    Health check endpoint with system status.
    Returns status of all providers and connections.
    """
    from app.data.cache.redis_client import redis_client
    from app.core.config import settings
    
    health_status = {
        "status": "healthy",
        "environment": settings.ENVIRONMENT,
        "components": {
            "redis": await _check_redis_health(),
            "providers": {
                "football_data": unified_data_service.circuit_breaker_state["football_data"],
                "api_football": unified_data_service.circuit_breaker_state["api_football"],
                "statsbomb": unified_data_service.circuit_breaker_state["statsbomb"]
            }
        },
        "cache_stats": {
            "hits": "N/A",  # Would be implemented with actual cache statistics
            "misses": "N/A"
        }
    }
    
    return health_status

@router.get("/predictions/next-matchday")
async def get_next_matchday_predictions(api_key: str = Depends(verify_api_key)):
    """
    Get predictions for the next matchday.
    Uses machine learning models for predictions.
    """
    try:
        monitor_api_call("api", "predictions", "request")
        
        # Import prediction service
        from app.ml.service import prediction_service
        
        # Generate predictions using ML models
        predictions = await prediction_service.predict_next_matchday()
        
        monitor_api_call("api", "predictions", "success")
        return predictions
    except Exception as e:
        monitor_api_call("api", "predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

async def _check_redis_health() -> str:
    """Check Redis connection health"""
    try:
        from app.data.cache.redis_client import redis_client
        if await redis_client.exists("health_check"):
            return "connected"
        
        # Test write
        await redis_client.setex("health_check", 10, "test")
        return "connected"
    except Exception:
        return "disconnected"

async def _generate_predictions():
    """Generate predictions using ML models"""
    # This function is kept for backward compatibility
    from app.ml.service import prediction_service
    return await prediction_service.predict_next_matchday()

# Include all endpoints
api_router = APIRouter()
api_router.include_router(router, prefix="", tags=["v1"])

# Include monitoring endpoints
from app.monitoring.endpoints import router as monitoring_router
api_router.include_router(monitoring_router, tags=["monitoring"])

# Include detailed stats endpoints
from app.api.v1.detailed import router as detailed_router
api_router.include_router(detailed_router, prefix="/detailed", tags=["detailed"])
