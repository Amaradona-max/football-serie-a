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

@router.get("/norway/fixtures/live", response_model=List[MatchLive])
async def get_live_fixtures_norway(api_key: str = Depends(verify_api_key)):
    """
    Get live matches from Norway Eliteserien.
    """
    try:
        monitor_api_call("api", "fixtures_live_norway", "request")
        matches = await unified_data_service.get_live_matches_norway()
        monitor_api_call("api", "fixtures_live_norway", "success")
        return matches
    except Exception as e:
        monitor_api_call("api", "fixtures_live_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/premier/fixtures/live", response_model=List[MatchLive])
async def get_live_fixtures_premier(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "fixtures_live_premier", "request")
        matches = await unified_data_service.get_live_matches_premier()
        monitor_api_call("api", "fixtures_live_premier", "success")
        return matches
    except Exception as e:
        monitor_api_call("api", "fixtures_live_premier", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bundesliga/fixtures/live", response_model=List[MatchLive])
async def get_live_fixtures_bundesliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "fixtures_live_bundesliga", "request")
        matches = await unified_data_service.get_live_matches_bundesliga()
        monitor_api_call("api", "fixtures_live_bundesliga", "success")
        return matches
    except Exception as e:
        monitor_api_call("api", "fixtures_live_bundesliga", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/laliga/fixtures/live", response_model=List[MatchLive])
async def get_live_fixtures_laliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "fixtures_live_laliga", "request")
        matches = await unified_data_service.get_live_matches_laliga()
        monitor_api_call("api", "fixtures_live_laliga", "success")
        return matches
    except Exception as e:
        monitor_api_call("api", "fixtures_live_laliga", "error")
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

@router.get("/norway/standings", response_model=Standings)
async def get_standings_norway(api_key: str = Depends(verify_api_key)):
    """
    Get current Norway Eliteserien standings.
    """
    try:
        monitor_api_call("api", "standings_norway", "request")
        standings = await unified_data_service.get_standings_norway()
        if not standings:
            raise HTTPException(status_code=404, detail="Standings not available")
        monitor_api_call("api", "standings_norway", "success")
        return standings
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "standings_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/premier/standings", response_model=Standings)
async def get_standings_premier(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "standings_premier", "request")
        standings = await unified_data_service.get_standings_premier()
        if not standings:
            raise HTTPException(status_code=404, detail="Standings not available")
        monitor_api_call("api", "standings_premier", "success")
        return standings
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "standings_premier", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bundesliga/standings", response_model=Standings)
async def get_standings_bundesliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "standings_bundesliga", "request")
        standings = await unified_data_service.get_standings_bundesliga()
        if not standings:
            raise HTTPException(status_code=404, detail="Standings not available")
        monitor_api_call("api", "standings_bundesliga", "success")
        return standings
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "standings_bundesliga", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/laliga/standings", response_model=Standings)
async def get_standings_laliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "standings_laliga", "request")
        standings = await unified_data_service.get_standings_laliga()
        if not standings:
            raise HTTPException(status_code=404, detail="Standings not available")
        monitor_api_call("api", "standings_laliga", "success")
        return standings
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "standings_laliga", "error")
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

@router.get("/norway/predictions/next-matchday")
async def get_next_matchday_predictions_norway(api_key: str = Depends(verify_api_key)):
    """
    Get predictions for the next matchday of Norway Eliteserien.
    """
    try:
        monitor_api_call("api", "norway_predictions", "request")
        from app.ml.service import prediction_service
        predictions = await prediction_service.predict_next_matchday_norway()
        monitor_api_call("api", "norway_predictions", "success")
        return predictions
    except Exception as e:
        monitor_api_call("api", "norway_predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/premier/predictions/next-matchday")
async def get_next_matchday_predictions_premier(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "premier_predictions", "request")
        from app.ml.service import prediction_service
        predictions = await prediction_service.predict_next_matchday_premier()
        monitor_api_call("api", "premier_predictions", "success")
        return predictions
    except Exception as e:
        monitor_api_call("api", "premier_predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bundesliga/predictions/next-matchday")
async def get_next_matchday_predictions_bundesliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "bundesliga_predictions", "request")
        from app.ml.service import prediction_service
        predictions = await prediction_service.predict_next_matchday_bundesliga()
        monitor_api_call("api", "bundesliga_predictions", "success")
        return predictions
    except Exception as e:
        monitor_api_call("api", "bundesliga_predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/laliga/predictions/next-matchday")
async def get_next_matchday_predictions_laliga(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "laliga_predictions", "request")
        from app.ml.service import prediction_service
        predictions = await prediction_service.predict_next_matchday_laliga()
        monitor_api_call("api", "laliga_predictions", "success")
        return predictions
    except Exception as e:
        monitor_api_call("api", "laliga_predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/evaluate/seriea")
async def evaluate_predictions_seriea(
    matchday: Optional[int] = Query(None, description="Giornata specifica da valutare (default: ultima disponibile)"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_evaluate_seriea", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_predictions_serie_a(matchday)
        monitor_api_call("api", "predictions_evaluate_seriea", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_evaluate_seriea", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/norway/predictions/evaluate")
async def evaluate_predictions_norway(
    matchday: Optional[int] = Query(None, description="Giornata specifica da valutare (default: ultima disponibile)"),
    api_key: str = Depends(verify_api_key),
    ):
    try:
        monitor_api_call("api", "norway_predictions_evaluate", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_predictions_norway(matchday)
        monitor_api_call("api", "norway_predictions_evaluate", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "norway_predictions_evaluate", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/evaluate/premier")
async def evaluate_predictions_premier(
    matchday: Optional[int] = Query(None, description="Giornata specifica da valutare (default: ultima disponibile)"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "premier_predictions_evaluate", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_predictions_premier(matchday)
        monitor_api_call("api", "premier_predictions_evaluate", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "premier_predictions_evaluate", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/evaluate/bundesliga")
async def evaluate_predictions_bundesliga(
    matchday: Optional[int] = Query(None, description="Giornata specifica da valutare (default: ultima disponibile)"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "bundesliga_predictions_evaluate", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_predictions_bundesliga(matchday)
        monitor_api_call("api", "bundesliga_predictions_evaluate", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "bundesliga_predictions_evaluate", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/evaluate/laliga")
async def evaluate_predictions_laliga(
    matchday: Optional[int] = Query(None, description="Giornata specifica da valutare (default: ultima disponibile)"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "laliga_predictions_evaluate", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_predictions_laliga(matchday)
        monitor_api_call("api", "laliga_predictions_evaluate", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "laliga_predictions_evaluate", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/reliability/seriea")
async def get_reliability_seriea(
    last_n_matchdays: int = Query(18, ge=1, le=38, description="Numero di giornate recenti da considerare"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_reliability_seriea", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_recent_matchdays_serie_a(last_n_matchdays)
        monitor_api_call("api", "predictions_reliability_seriea", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_reliability_seriea", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/norway/predictions/reliability")
async def get_reliability_norway(
    last_n_matchdays: int = Query(18, ge=1, le=30, description="Numero di giornate recenti da considerare"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "norway_predictions_reliability", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_recent_matchdays_norway(last_n_matchdays)
        monitor_api_call("api", "norway_predictions_reliability", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "norway_predictions_reliability", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/reliability/premier")
async def get_reliability_premier(
    last_n_matchdays: int = Query(18, ge=1, le=38, description="Numero di giornate recenti da considerare"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_reliability_premier", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_recent_matchdays_premier(last_n_matchdays)
        monitor_api_call("api", "predictions_reliability_premier", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_reliability_premier", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/reliability/bundesliga")
async def get_reliability_bundesliga(
    last_n_matchdays: int = Query(18, ge=1, le=34, description="Numero di giornate recenti da considerare"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_reliability_bundesliga", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_recent_matchdays_bundesliga(last_n_matchdays)
        monitor_api_call("api", "predictions_reliability_bundesliga", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_reliability_bundesliga", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/reliability/laliga")
async def get_reliability_laliga(
    last_n_matchdays: int = Query(18, ge=1, le=38, description="Numero di giornate recenti da considerare"),
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_reliability_laliga", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.evaluate_recent_matchdays_laliga(last_n_matchdays)
        monitor_api_call("api", "predictions_reliability_laliga", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_reliability_laliga", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/history/2025")
async def get_history_2025_stats(
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_history_2025", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.get_2025_stats()
        monitor_api_call("api", "predictions_history_2025", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_history_2025", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/history/2026")
async def get_history_2026_stats(
    api_key: str = Depends(verify_api_key),
):
    try:
        monitor_api_call("api", "predictions_history_2026", "request")
        from app.ml.service import prediction_service
        result = await prediction_service.get_2026_stats()
        monitor_api_call("api", "predictions_history_2026", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "predictions_history_2026", "error")
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
