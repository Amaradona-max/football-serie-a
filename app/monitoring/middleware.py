from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import time
from prometheus_client import Histogram

# HTTP request metrics
http_request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint', 'status_code']
)

http_requests_total = Histogram(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            status_code = 500
            response = Response("Internal Server Error", status_code=500)
        
        duration = time.time() - start_time
        
        # Extract endpoint path (simplified)
        endpoint = request.url.path
        if endpoint.startswith("/api"):
            # For API endpoints, use the path as is
            pass
        else:
            # For other endpoints, use a generic label
            endpoint = "other"
        
        # Record metrics
        http_request_duration.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=status_code
        ).observe(duration)
        
        http_requests_total.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=status_code
        ).observe(1)
        
        return response