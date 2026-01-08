from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import asyncio
import json
from functools import wraps

from app.data.providers.football_data import FootballDataProvider
from app.data.providers.api_football import ApiFootballProvider
from app.data.providers.statsbomb import StatsBombProvider
from app.data.models.common import MatchLive, MatchHistorical, Standings, Team, Provider
from app.data.cache.redis_client import redis_client
from app.core.config import settings
from app.monitoring.metrics import monitor_api_call, track_fallback_activation

class UnifiedDataService:
    def __init__(self):
        self.providers = [
            ApiFootballProvider(),
            FootballDataProvider(),
            StatsBombProvider()
        ]
        self.circuit_breaker_state = {provider.provider_name: "CLOSED" for provider in self.providers}
        self.circuit_breaker_failures = {provider.provider_name: 0 for provider in self.providers}
        self.max_failures = 3
        self.reset_timeout = 300  # 5 minutes
    
    async def get_live_matches(self) -> List[MatchLive]:
        cache_key = f"live_matches:{datetime.now().strftime('%Y-%m-%d')}"
        
        # Try to get from cache first
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "live_matches", "hit")
            return cached_data
        
        monitor_api_call("cache", "live_matches", "miss")
        
        # Try providers in order with fallback
        result = None
        last_error = None
        
        for provider in self.providers:
            if self.circuit_breaker_state[provider.provider_name] == "OPEN":
                continue
                
            try:
                result = await self._execute_with_retry(provider.get_live_matches, provider)
                if result is not None:
                    # Cache the successful result
                    await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_LIVE)
                    monitor_api_call(provider.provider_name, "live_matches", "success")
                    return result
                    
            except Exception as e:
                last_error = e
                monitor_api_call(provider.provider_name, "live_matches", "error")
                await self._handle_provider_failure(provider, e)
                continue
        
        # All providers failed, try to return stale data
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        # If no stale data available, return empty list
        return []
    
    async def get_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        cache_key = f"fixtures:{matchday if matchday is not None else 'all'}"
        
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "fixtures", "hit")
            return cached_data
        
        monitor_api_call("cache", "fixtures", "miss")
        
        for provider in self.providers:
            if self.circuit_breaker_state[provider.provider_name] == "OPEN":
                continue
            
            try:
                result = await self._execute_with_retry(
                    lambda: provider.get_fixtures(matchday), provider
                )
                if isinstance(result, list) and not result:
                    continue
                if result is not None:
                    await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                    monitor_api_call(provider.provider_name, "fixtures", "success")
                    return result
            except Exception as e:
                monitor_api_call(provider.provider_name, "fixtures", "error")
                await self._handle_provider_failure(provider, e)
                continue
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return []
    
    async def get_match_by_id(self, match_id: int) -> Optional[MatchHistorical]:
        cache_key = f"match:{match_id}"
        
        # Try cache first
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "match_by_id", "hit")
            return cached_data
        
        monitor_api_call("cache", "match_by_id", "miss")
        
        # Try providers
        result = None
        
        for provider in self.providers:
            if self.circuit_breaker_state[provider.provider_name] == "OPEN":
                continue
                
            try:
                result = await self._execute_with_retry(
                    lambda: provider.get_match_by_id(match_id), provider
                )
                if result is not None:
                    await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                    monitor_api_call(provider.provider_name, "match_by_id", "success")
                    return result
                    
            except Exception as e:
                monitor_api_call(provider.provider_name, "match_by_id", "error")
                await self._handle_provider_failure(provider, e)
                continue
        
        # Try stale data
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return None
    
    async def get_standings(self) -> Optional[Standings]:
        cache_key = "standings:current"
        
        # Try cache first
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "standings", "hit")
            return cached_data
        
        monitor_api_call("cache", "standings", "miss")
        
        # Try providers
        result = None
        
        for provider in self.providers:
            if self.circuit_breaker_state[provider.provider_name] == "OPEN":
                continue
                
            try:
                result = await self._execute_with_retry(provider.get_standings, provider)
                if result is not None:
                    await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                    monitor_api_call(provider.provider_name, "standings", "success")
                    return result
                    
            except Exception as e:
                monitor_api_call(provider.provider_name, "standings", "error")
                await self._handle_provider_failure(provider, e)
                continue
        
        # Try stale data
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return None
    
    async def _execute_with_retry(self, func, provider, max_retries=2):
        for attempt in range(max_retries + 1):
            try:
                return await func()
            except Exception as e:
                if attempt == max_retries:
                    raise e
                await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
        return None
    
    async def _handle_provider_failure(self, provider, error):
        provider_name = provider.provider_name
        self.circuit_breaker_failures[provider_name] += 1
        
        if self.circuit_breaker_failures[provider_name] >= self.max_failures:
            self.circuit_breaker_state[provider_name] = "OPEN"
            print(f"Circuit breaker OPEN for {provider_name}")
            
            # Schedule circuit breaker reset
            asyncio.create_task(self._reset_circuit_breaker(provider_name))
    
    async def _reset_circuit_breaker(self, provider_name):
        await asyncio.sleep(self.reset_timeout)
        self.circuit_breaker_state[provider_name] = "CLOSED"
        self.circuit_breaker_failures[provider_name] = 0
        print(f"Circuit breaker CLOSED for {provider_name}")
    
    async def _get_from_cache(self, key: str):
        try:
            cached = await redis_client.get(key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            print(f"Cache read error: {e}")
        return None
    
    async def _set_to_cache(self, key: str, data, ttl: int):
        try:
            serialized = json.dumps(data, default=str)
            await redis_client.setex(key, ttl, serialized)
        except Exception as e:
            print(f"Cache write error: {e}")
    
    async def _get_stale_data(self, key: str):
        try:
            # Try to get any cached data regardless of TTL
            stale = await redis_client.get(key)
            if stale:
                return json.loads(stale)
        except Exception as e:
            print(f"Stale data read error: {e}")
        return None

def cache_with_fallback(ttl: int = 300):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            service = UnifiedDataService()
            cache_key = f"fallback:{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Try cache first
            cached = await service._get_from_cache(cache_key)
            if cached:
                return cached
            
            # Execute function
            try:
                result = await func(*args, **kwargs)
                if result is not None:
                    await service._set_to_cache(cache_key, result, ttl)
                return result
            except Exception as e:
                # On error, try to return stale data
                stale = await service._get_stale_data(cache_key)
                if stale:
                    track_fallback_activation(f"stale_{func.__name__}")
                    return stale
                raise e
        return wrapper
    return decorator

# Global instance
unified_data_service = UnifiedDataService()
