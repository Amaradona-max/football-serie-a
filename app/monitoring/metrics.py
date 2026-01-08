from prometheus_client import Counter, Histogram, Gauge
from typing import Optional

# API call metrics
api_calls_total = Counter(
    'api_calls_total', 
    'Total API calls', 
    ['provider', 'endpoint', 'status']
)

api_errors_total = Counter(
    'api_errors_total', 
    'Total API errors', 
    ['provider', 'endpoint']
)

# Cache metrics
cache_hits_total = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['cache_type']
)

cache_misses_total = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['cache_type']
)

# Fallback metrics
fallback_activations_total = Counter(
    'fallback_activations_total',
    'Total fallback activations',
    ['fallback_type']
)

# Response time metrics
api_response_time = Histogram(
    'api_response_time_seconds',
    'API response time in seconds',
    ['provider', 'endpoint']
)

# Circuit breaker metrics
circuit_breaker_state = Gauge(
    'circuit_breaker_state',
    'Circuit breaker state (0=CLOSED, 1=OPEN)',
    ['provider']
)

circuit_breaker_failures = Gauge(
    'circuit_breaker_failures',
    'Circuit breaker failure count',
    ['provider']
)

def monitor_api_call(provider: str, endpoint: str, status: str):
    """Track API calls with status"""
    api_calls_total.labels(provider=provider, endpoint=endpoint, status=status).inc()
    
    if status == "error":
        api_errors_total.labels(provider=provider, endpoint=endpoint).inc()

def track_cache_hit(cache_type: str):
    """Track cache hits"""
    cache_hits_total.labels(cache_type=cache_type).inc()

def track_cache_miss(cache_type: str):
    """Track cache misses"""
    cache_misses_total.labels(cache_type=cache_type).inc()

def track_fallback_activation(fallback_type: str):
    """Track fallback activations"""
    fallback_activations_total.labels(fallback_type=fallback_type).inc()

def update_circuit_breaker_state(provider: str, state: str):
    """Update circuit breaker state metric"""
    state_value = 1 if state == "OPEN" else 0
    circuit_breaker_state.labels(provider=provider).set(state_value)

def update_circuit_breaker_failures(provider: str, failures: int):
    """Update circuit breaker failure count metric"""
    circuit_breaker_failures.labels(provider=provider).set(failures)

def measure_response_time(provider: str, endpoint: str):
    """Context manager to measure API response time"""
    return api_response_time.labels(provider=provider, endpoint=endpoint).time()