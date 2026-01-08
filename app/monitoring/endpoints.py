from fastapi import APIRouter
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

router = APIRouter()

@router.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint.
    Exposes all application metrics for monitoring.
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

@router.get("/health/metrics")
async def health_metrics():
    """
    Health metrics endpoint with detailed system metrics.
    """
    from app.data.cache.redis_client import redis_client
    from app.data.services.unified_data_service import unified_data_service
    
    metrics_data = {
        "circuit_breakers": {
            provider: state
            for provider, state in unified_data_service.circuit_breaker_state.items()
        },
        "failure_counts": {
            provider: count
            for provider, count in unified_data_service.circuit_breaker_failures.items()
        },
        "redis_connected": await _check_redis_connection()
    }
    
    return metrics_data

async def _check_redis_connection() -> bool:
    """Check if Redis is connected"""
    try:
        from app.data.cache.redis_client import redis_client
        return await redis_client.exists("health_check") or True
    except Exception:
        return False