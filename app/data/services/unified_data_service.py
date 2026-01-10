from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import asyncio
import json
from functools import wraps

from app.data.providers.football_data import FootballDataProvider
from app.data.providers.api_football import ApiFootballProvider
from app.data.providers.statsbomb import StatsBombProvider
from app.data.models.common import MatchLive, MatchHistorical, Standings, Team, Provider, MatchStatus, Score, TeamStats
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
    
    async def get_live_matches_norway(self) -> List[MatchLive]:
        cache_key = f"live_matches_norway:{datetime.now().strftime('%Y-%m-%d')}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "live_matches_norway", "hit")
            return cached_data
        
        monitor_api_call("cache", "live_matches_norway", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_live_matches(league_id=settings.NORWAY_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_LIVE)
                monitor_api_call(provider.provider_name, "live_matches_norway", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "live_matches_norway", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return []
    
    async def get_fixtures_norway(self, matchday: Optional[int] = None) -> List[MatchLive]:
        cache_key = f"fixtures_norway:{matchday if matchday is not None else 'all'}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "fixtures_norway", "hit")
            return cached_data
        
        monitor_api_call("cache", "fixtures_norway", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_fixtures(matchday=matchday, league_id=settings.NORWAY_LEAGUE_ID)
            if isinstance(result, list) and not result:
                result = None
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "fixtures_norway", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "fixtures_norway", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_fixtures = self._get_mock_norway_fixtures(matchday)
        if mock_fixtures:
            track_fallback_activation("norway_mock_fixtures")
            return mock_fixtures
        
        return []
    
    async def get_live_matches_premier(self) -> List[MatchLive]:
        cache_key = f"live_matches_premier:{datetime.now().strftime('%Y-%m-%d')}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "live_matches_premier", "hit")
            return cached_data
        
        monitor_api_call("cache", "live_matches_premier", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_live_matches(league_id=settings.PREMIER_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_LIVE)
                monitor_api_call(provider.provider_name, "live_matches_premier", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "live_matches_premier", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return []
    
    async def get_live_matches_bundesliga(self) -> List[MatchLive]:
        cache_key = f"live_matches_bundesliga:{datetime.now().strftime('%Y-%m-%d')}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "live_matches_bundesliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "live_matches_bundesliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_live_matches(league_id=settings.BUNDESLIGA_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_LIVE)
                monitor_api_call(provider.provider_name, "live_matches_bundesliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "live_matches_bundesliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return []
    
    async def get_live_matches_laliga(self) -> List[MatchLive]:
        cache_key = f"live_matches_laliga:{datetime.now().strftime('%Y-%m-%d')}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "live_matches_laliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "live_matches_laliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_live_matches(league_id=settings.LA_LIGA_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_LIVE)
                monitor_api_call(provider.provider_name, "live_matches_laliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "live_matches_laliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        return []
    
    async def get_fixtures_premier(self, matchday: Optional[int] = None) -> List[MatchLive]:
        cache_key = f"fixtures_premier:{matchday if matchday is not None else 'all'}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "fixtures_premier", "hit")
            return cached_data
        
        monitor_api_call("cache", "fixtures_premier", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_fixtures(matchday=matchday, league_id=settings.PREMIER_LEAGUE_ID)
            if isinstance(result, list) and not result:
                result = None
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "fixtures_premier", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "fixtures_premier", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_fixtures = self._get_mock_premier_fixtures(matchday)
        if mock_fixtures:
            track_fallback_activation("premier_mock_fixtures")
            return mock_fixtures
        
        return []
    
    async def get_fixtures_bundesliga(self, matchday: Optional[int] = None) -> List[MatchLive]:
        cache_key = f"fixtures_bundesliga:{matchday if matchday is not None else 'all'}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "fixtures_bundesliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "fixtures_bundesliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_fixtures(matchday=matchday, league_id=settings.BUNDESLIGA_LEAGUE_ID)
            if isinstance(result, list) and not result:
                result = None
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "fixtures_bundesliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "fixtures_bundesliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_fixtures = self._get_mock_bundesliga_fixtures(matchday)
        if mock_fixtures:
            track_fallback_activation("bundesliga_mock_fixtures")
            return mock_fixtures
        
        return []
    
    async def get_fixtures_laliga(self, matchday: Optional[int] = None) -> List[MatchLive]:
        cache_key = f"fixtures_laliga:{matchday if matchday is not None else 'all'}"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "fixtures_laliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "fixtures_laliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_fixtures(matchday=matchday, league_id=settings.LA_LIGA_LEAGUE_ID)
            if isinstance(result, list) and not result:
                result = None
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "fixtures_laliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "fixtures_laliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_fixtures = self._get_mock_laliga_fixtures(matchday)
        if mock_fixtures:
            track_fallback_activation("laliga_mock_fixtures")
            return mock_fixtures
        
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
    
    async def get_standings_norway(self) -> Optional[Standings]:
        cache_key = "standings_norway:current"
        
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "standings_norway", "hit")
            return cached_data
        
        monitor_api_call("cache", "standings_norway", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_standings(league_id=settings.NORWAY_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "standings_norway", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "standings_norway", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_standings = self._get_mock_norway_standings()
        if mock_standings:
            track_fallback_activation("norway_mock_standings")
            return mock_standings
        
        return None

    async def get_standings_premier(self) -> Optional[Standings]:
        cache_key = "standings_premier:current"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "standings_premier", "hit")
            return cached_data
        
        monitor_api_call("cache", "standings_premier", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_standings(league_id=settings.PREMIER_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "standings_premier", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "standings_premier", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_standings = self._get_mock_premier_standings()
        if mock_standings:
            track_fallback_activation("premier_mock_standings")
            return mock_standings
        
        return None

    async def get_standings_bundesliga(self) -> Optional[Standings]:
        cache_key = "standings_bundesliga:current"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "standings_bundesliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "standings_bundesliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_standings(league_id=settings.BUNDESLIGA_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "standings_bundesliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "standings_bundesliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_standings = self._get_mock_bundesliga_standings()
        if mock_standings:
            track_fallback_activation("bundesliga_mock_standings")
            return mock_standings
        
        return None

    async def get_standings_laliga(self) -> Optional[Standings]:
        cache_key = "standings_laliga:current"
        cached_data = await self._get_from_cache(cache_key)
        if cached_data:
            monitor_api_call("cache", "standings_laliga", "hit")
            return cached_data
        
        monitor_api_call("cache", "standings_laliga", "miss")
        
        try:
            provider = ApiFootballProvider()
            result = await provider.get_standings(league_id=settings.LA_LIGA_LEAGUE_ID)
            if result is not None:
                await self._set_to_cache(cache_key, result, settings.REDIS_CACHE_TTL_STATIC)
                monitor_api_call(provider.provider_name, "standings_laliga", "success")
                return result
        except Exception as e:
            monitor_api_call("api_football", "standings_laliga", "error")
            await self._handle_provider_failure(provider, e)
        
        stale_data = await self._get_stale_data(cache_key)
        if stale_data:
            track_fallback_activation("stale_cache")
            return stale_data
        
        mock_standings = self._get_mock_laliga_standings()
        if mock_standings:
            track_fallback_activation("laliga_mock_standings")
            return mock_standings
        
        return None

    def _get_mock_norway_standings(self) -> Optional[Standings]:
        from datetime import datetime
        teams = [
            {"pos": 1, "name": "Bodø/Glimt", "id": 2001, "played": 18, "won": 12, "drawn": 3, "lost": 3, "gf": 38, "ga": 18, "pts": 39},
            {"pos": 2, "name": "Molde", "id": 2002, "played": 18, "won": 11, "drawn": 3, "lost": 4, "gf": 35, "ga": 20, "pts": 36},
            {"pos": 3, "name": "Rosenborg", "id": 2003, "played": 18, "won": 9, "drawn": 5, "lost": 4, "gf": 30, "ga": 22, "pts": 32},
            {"pos": 4, "name": "Vålerenga", "id": 2004, "played": 18, "won": 8, "drawn": 4, "lost": 6, "gf": 28, "ga": 24, "pts": 28},
            {"pos": 5, "name": "Brann", "id": 2005, "played": 18, "won": 7, "drawn": 6, "lost": 5, "gf": 26, "ga": 23, "pts": 27},
            {"pos": 6, "name": "Sarpsborg 08", "id": 2006, "played": 18, "won": 7, "drawn": 5, "lost": 6, "gf": 25, "ga": 24, "pts": 26},
            {"pos": 7, "name": "Lillestrøm", "id": 2007, "played": 18, "won": 6, "drawn": 6, "lost": 6, "gf": 23, "ga": 23, "pts": 24},
            {"pos": 8, "name": "Odd", "id": 2008, "played": 18, "won": 5, "drawn": 7, "lost": 6, "gf": 21, "ga": 23, "pts": 22},
            {"pos": 9, "name": "Haugesund", "id": 2009, "played": 18, "won": 5, "drawn": 6, "lost": 7, "gf": 20, "ga": 24, "pts": 21},
            {"pos": 10, "name": "Tromsø", "id": 2010, "played": 18, "won": 5, "drawn": 5, "lost": 8, "gf": 19, "ga": 25, "pts": 20},
            {"pos": 11, "name": "Sandefjord", "id": 2011, "played": 18, "won": 4, "drawn": 7, "lost": 7, "gf": 18, "ga": 24, "pts": 19},
            {"pos": 12, "name": "Stabæk", "id": 2012, "played": 18, "won": 4, "drawn": 6, "lost": 8, "gf": 17, "ga": 24, "pts": 18},
            {"pos": 13, "name": "Strømsgodset", "id": 2013, "played": 18, "won": 4, "drawn": 5, "lost": 9, "gf": 18, "ga": 27, "pts": 17},
            {"pos": 14, "name": "HamKam", "id": 2014, "played": 18, "won": 3, "drawn": 7, "lost": 8, "gf": 16, "ga": 25, "pts": 16},
            {"pos": 15, "name": "Kristiansund", "id": 2015, "played": 18, "won": 3, "drawn": 6, "lost": 9, "gf": 15, "ga": 26, "pts": 15},
            {"pos": 16, "name": "Aalesund", "id": 2016, "played": 18, "won": 2, "drawn": 6, "lost": 10, "gf": 13, "ga": 27, "pts": 12},
        ]
        standings = []
        for t in teams:
            standings.append(
                TeamStats(
                    team=Team(id=t["id"], name=t["name"]),
                    played=t["played"],
                    won=t["won"],
                    drawn=t["drawn"],
                    lost=t["lost"],
                    goals_for=t["gf"],
                    goals_against=t["ga"],
                    goal_difference=t["gf"] - t["ga"],
                    points=t["pts"],
                    position=t["pos"],
                )
            )
        return Standings(
            competition="Eliteserien",
            season=settings.SEASON,
            standings=standings,
            last_updated=datetime.now(),
            data_provider=Provider.FOOTBALL_DATA,
        )
    
    def _get_mock_premier_standings(self) -> Optional[Standings]:
        from datetime import datetime
        teams = [
            {"pos": 1, "name": "Manchester City", "id": 3001, "played": 20, "won": 15, "drawn": 3, "lost": 2, "gf": 48, "ga": 18, "pts": 48},
            {"pos": 2, "name": "Liverpool", "id": 3002, "played": 20, "won": 14, "drawn": 4, "lost": 2, "gf": 45, "ga": 19, "pts": 46},
            {"pos": 3, "name": "Arsenal", "id": 3003, "played": 20, "won": 13, "drawn": 4, "lost": 3, "gf": 40, "ga": 20, "pts": 43},
            {"pos": 4, "name": "Tottenham", "id": 3004, "played": 20, "won": 12, "drawn": 4, "lost": 4, "gf": 38, "ga": 24, "pts": 40},
            {"pos": 5, "name": "Aston Villa", "id": 3005, "played": 20, "won": 11, "drawn": 5, "lost": 4, "gf": 36, "ga": 25, "pts": 38},
            {"pos": 6, "name": "Newcastle", "id": 3006, "played": 20, "won": 10, "drawn": 4, "lost": 6, "gf": 34, "ga": 26, "pts": 34},
            {"pos": 7, "name": "Manchester United", "id": 3007, "played": 20, "won": 10, "drawn": 3, "lost": 7, "gf": 30, "ga": 27, "pts": 33},
            {"pos": 8, "name": "Chelsea", "id": 3008, "played": 20, "won": 9, "drawn": 5, "lost": 6, "gf": 32, "ga": 28, "pts": 32},
            {"pos": 9, "name": "Brighton", "id": 3009, "played": 20, "won": 8, "drawn": 6, "lost": 6, "gf": 31, "ga": 29, "pts": 30},
            {"pos": 10, "name": "West Ham", "id": 3010, "played": 20, "won": 8, "drawn": 5, "lost": 7, "gf": 29, "ga": 30, "pts": 29},
            {"pos": 11, "name": "Brentford", "id": 3011, "played": 20, "won": 7, "drawn": 5, "lost": 8, "gf": 27, "ga": 30, "pts": 26},
            {"pos": 12, "name": "Crystal Palace", "id": 3012, "played": 20, "won": 6, "drawn": 7, "lost": 7, "gf": 23, "ga": 27, "pts": 25},
            {"pos": 13, "name": "Fulham", "id": 3013, "played": 20, "won": 6, "drawn": 6, "lost": 8, "gf": 24, "ga": 30, "pts": 24},
            {"pos": 14, "name": "Wolves", "id": 3014, "played": 20, "won": 6, "drawn": 5, "lost": 9, "gf": 22, "ga": 30, "pts": 23},
            {"pos": 15, "name": "Everton", "id": 3015, "played": 20, "won": 5, "drawn": 6, "lost": 9, "gf": 20, "ga": 29, "pts": 21},
            {"pos": 16, "name": "Nottingham Forest", "id": 3016, "played": 20, "won": 5, "drawn": 5, "lost": 10, "gf": 19, "ga": 31, "pts": 20},
            {"pos": 17, "name": "Bournemouth", "id": 3017, "played": 20, "won": 5, "drawn": 5, "lost": 10, "gf": 21, "ga": 34, "pts": 20},
            {"pos": 18, "name": "Burnley", "id": 3018, "played": 20, "won": 4, "drawn": 4, "lost": 12, "gf": 18, "ga": 34, "pts": 16},
            {"pos": 19, "name": "Sheffield United", "id": 3019, "played": 20, "won": 3, "drawn": 5, "lost": 12, "gf": 16, "ga": 35, "pts": 14},
            {"pos": 20, "name": "Luton Town", "id": 3020, "played": 20, "won": 3, "drawn": 4, "lost": 13, "gf": 15, "ga": 36, "pts": 13},
        ]
        standings = []
        for t in teams:
            standings.append(
                TeamStats(
                    team=Team(id=t["id"], name=t["name"]),
                    played=t["played"],
                    won=t["won"],
                    drawn=t["drawn"],
                    lost=t["lost"],
                    goals_for=t["gf"],
                    goals_against=t["ga"],
                    goal_difference=t["gf"] - t["ga"],
                    points=t["pts"],
                    position=t["pos"],
                )
            )
        return Standings(
            competition="Premier League",
            season=settings.SEASON,
            standings=standings,
            last_updated=datetime.now(),
            data_provider=Provider.API_FOOTBALL,
        )
    
    def _get_mock_bundesliga_standings(self) -> Optional[Standings]:
        from datetime import datetime
        teams = [
            {"pos": 1, "name": "Bayern München", "id": 3101, "played": 18, "won": 13, "drawn": 3, "lost": 2, "gf": 50, "ga": 16, "pts": 42},
            {"pos": 2, "name": "Bayer Leverkusen", "id": 3102, "played": 18, "won": 12, "drawn": 4, "lost": 2, "gf": 44, "ga": 18, "pts": 40},
            {"pos": 3, "name": "RB Leipzig", "id": 3103, "played": 18, "won": 11, "drawn": 4, "lost": 3, "gf": 40, "ga": 20, "pts": 37},
            {"pos": 4, "name": "Borussia Dortmund", "id": 3104, "played": 18, "won": 10, "drawn": 5, "lost": 3, "gf": 38, "ga": 22, "pts": 35},
            {"pos": 5, "name": "Stuttgart", "id": 3105, "played": 18, "won": 10, "drawn": 3, "lost": 5, "gf": 35, "ga": 24, "pts": 33},
            {"pos": 6, "name": "Eintracht Frankfurt", "id": 3106, "played": 18, "won": 8, "drawn": 6, "lost": 4, "gf": 30, "ga": 23, "pts": 30},
            {"pos": 7, "name": "Freiburg", "id": 3107, "played": 18, "won": 8, "drawn": 4, "lost": 6, "gf": 27, "ga": 26, "pts": 28},
            {"pos": 8, "name": "Hoffenheim", "id": 3108, "played": 18, "won": 7, "drawn": 5, "lost": 6, "gf": 29, "ga": 28, "pts": 26},
            {"pos": 9, "name": "Wolfsburg", "id": 3109, "played": 18, "won": 7, "drawn": 3, "lost": 8, "gf": 25, "ga": 29, "pts": 24},
            {"pos": 10, "name": "Union Berlin", "id": 3110, "played": 18, "won": 6, "drawn": 5, "lost": 7, "gf": 22, "ga": 26, "pts": 23},
            {"pos": 11, "name": "Mainz", "id": 3111, "played": 18, "won": 5, "drawn": 7, "lost": 6, "gf": 21, "ga": 25, "pts": 22},
            {"pos": 12, "name": "Gladbach", "id": 3112, "played": 18, "won": 5, "drawn": 6, "lost": 7, "gf": 24, "ga": 29, "pts": 21},
            {"pos": 13, "name": "Augsburg", "id": 3113, "played": 18, "won": 5, "drawn": 5, "lost": 8, "gf": 23, "ga": 30, "pts": 20},
            {"pos": 14, "name": "Bochum", "id": 3114, "played": 18, "won": 4, "drawn": 7, "lost": 7, "gf": 20, "ga": 30, "pts": 19},
            {"pos": 15, "name": "Werder Bremen", "id": 3115, "played": 18, "won": 4, "drawn": 6, "lost": 8, "gf": 21, "ga": 32, "pts": 18},
            {"pos": 16, "name": "Köln", "id": 3116, "played": 18, "won": 3, "drawn": 6, "lost": 9, "gf": 18, "ga": 31, "pts": 15},
            {"pos": 17, "name": "Heidenheim", "id": 3117, "played": 18, "won": 3, "drawn": 5, "lost": 10, "gf": 17, "ga": 33, "pts": 14},
            {"pos": 18, "name": "Darmstadt", "id": 3118, "played": 18, "won": 2, "drawn": 5, "lost": 11, "gf": 15, "ga": 36, "pts": 11},
        ]
        standings = []
        for t in teams:
            standings.append(
                TeamStats(
                    team=Team(id=t["id"], name=t["name"]),
                    played=t["played"],
                    won=t["won"],
                    drawn=t["drawn"],
                    lost=t["lost"],
                    goals_for=t["gf"],
                    goals_against=t["ga"],
                    goal_difference=t["gf"] - t["ga"],
                    points=t["pts"],
                    position=t["pos"],
                )
            )
        return Standings(
            competition="Bundesliga",
            season=settings.SEASON,
            standings=standings,
            last_updated=datetime.now(),
            data_provider=Provider.API_FOOTBALL,
        )
    
    def _get_mock_laliga_standings(self) -> Optional[Standings]:
        from datetime import datetime
        teams = [
            {"pos": 1, "name": "Real Madrid", "id": 3201, "played": 20, "won": 15, "drawn": 3, "lost": 2, "gf": 42, "ga": 15, "pts": 48},
            {"pos": 2, "name": "Barcelona", "id": 3202, "played": 20, "won": 14, "drawn": 3, "lost": 3, "gf": 40, "ga": 18, "pts": 45},
            {"pos": 3, "name": "Girona", "id": 3203, "played": 20, "won": 13, "drawn": 4, "lost": 3, "gf": 38, "ga": 20, "pts": 43},
            {"pos": 4, "name": "Atlético Madrid", "id": 3204, "played": 20, "won": 12, "drawn": 4, "lost": 4, "gf": 35, "ga": 21, "pts": 40},
            {"pos": 5, "name": "Athletic Bilbao", "id": 3205, "played": 20, "won": 11, "drawn": 4, "lost": 5, "gf": 32, "ga": 22, "pts": 37},
            {"pos": 6, "name": "Real Sociedad", "id": 3206, "played": 20, "won": 9, "drawn": 7, "lost": 4, "gf": 29, "ga": 22, "pts": 34},
            {"pos": 7, "name": "Betis", "id": 3207, "played": 20, "won": 8, "drawn": 8, "lost": 4, "gf": 27, "ga": 23, "pts": 32},
            {"pos": 8, "name": "Villarreal", "id": 3208, "played": 20, "won": 8, "drawn": 5, "lost": 7, "gf": 28, "ga": 27, "pts": 29},
            {"pos": 9, "name": "Valencia", "id": 3209, "played": 20, "won": 7, "drawn": 6, "lost": 7, "gf": 24, "ga": 24, "pts": 27},
            {"pos": 10, "name": "Osasuna", "id": 3210, "played": 20, "won": 7, "drawn": 5, "lost": 8, "gf": 22, "ga": 25, "pts": 26},
            {"pos": 11, "name": "Getafe", "id": 3211, "played": 20, "won": 6, "drawn": 7, "lost": 7, "gf": 21, "ga": 25, "pts": 25},
            {"pos": 12, "name": "Celta Vigo", "id": 3212, "played": 20, "won": 6, "drawn": 6, "lost": 8, "gf": 20, "ga": 24, "pts": 24},
            {"pos": 13, "name": "Sevilla", "id": 3213, "played": 20, "won": 5, "drawn": 7, "lost": 8, "gf": 21, "ga": 27, "pts": 22},
            {"pos": 14, "name": "Rayo Vallecano", "id": 3214, "played": 20, "won": 5, "drawn": 7, "lost": 8, "gf": 19, "ga": 26, "pts": 22},
            {"pos": 15, "name": "Mallorca", "id": 3215, "played": 20, "won": 4, "drawn": 8, "lost": 8, "gf": 18, "ga": 25, "pts": 20},
            {"pos": 16, "name": "Granada", "id": 3216, "played": 20, "won": 4, "drawn": 6, "lost": 10, "gf": 18, "ga": 30, "pts": 18},
            {"pos": 17, "name": "Cádiz", "id": 3217, "played": 20, "won": 3, "drawn": 8, "lost": 9, "gf": 16, "ga": 29, "pts": 17},
            {"pos": 18, "name": "Alavés", "id": 3218, "played": 20, "won": 3, "drawn": 7, "lost": 10, "gf": 15, "ga": 28, "pts": 16},
            {"pos": 19, "name": "Las Palmas", "id": 3219, "played": 20, "won": 3, "drawn": 7, "lost": 10, "gf": 14, "ga": 27, "pts": 16},
            {"pos": 20, "name": "Almería", "id": 3220, "played": 20, "won": 2, "drawn": 6, "lost": 12, "gf": 13, "ga": 32, "pts": 12},
        ]
        standings = []
        for t in teams:
            standings.append(
                TeamStats(
                    team=Team(id=t["id"], name=t["name"]),
                    played=t["played"],
                    won=t["won"],
                    drawn=t["drawn"],
                    lost=t["lost"],
                    goals_for=t["gf"],
                    goals_against=t["ga"],
                    goal_difference=t["gf"] - t["ga"],
                    points=t["pts"],
                    position=t["pos"],
                )
            )
        return Standings(
            competition="La Liga",
            season=settings.SEASON,
            standings=standings,
            last_updated=datetime.now(),
            data_provider=Provider.API_FOOTBALL,
        )
    
    def _get_mock_premier_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        from datetime import datetime, timedelta
        today = datetime.now()
        base_day = datetime(today.year, today.month, today.day, 18, 0)
        finished_matches = [
            MatchLive(
                id=4001,
                competition="Premier League",
                season=settings.SEASON,
                matchday=18,
                home_team=Team(id=3001, name="Manchester City"),
                away_team=Team(id=3002, name="Liverpool"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 2, "away": 1}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4002,
                competition="Premier League",
                season=settings.SEASON,
                matchday=18,
                home_team=Team(id=3003, name="Arsenal"),
                away_team=Team(id=3004, name="Tottenham"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 3, "away": 2}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        upcoming_matches = [
            MatchLive(
                id=4003,
                competition="Premier League",
                season=settings.SEASON,
                matchday=19,
                home_team=Team(id=3007, name="Manchester United"),
                away_team=Team(id=3008, name="Chelsea"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4004,
                competition="Premier League",
                season=settings.SEASON,
                matchday=19,
                home_team=Team(id=3006, name="Newcastle"),
                away_team=Team(id=3009, name="Brighton"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        if matchday == 18:
            return finished_matches
        if matchday == 19:
            return upcoming_matches
        return finished_matches + upcoming_matches
    
    def _get_mock_bundesliga_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        from datetime import datetime, timedelta
        today = datetime.now()
        base_day = datetime(today.year, today.month, today.day, 18, 0)
        finished_matches = [
            MatchLive(
                id=4101,
                competition="Bundesliga",
                season=settings.SEASON,
                matchday=18,
                home_team=Team(id=3101, name="Bayern München"),
                away_team=Team(id=3104, name="Borussia Dortmund"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 2, "away": 1}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4102,
                competition="Bundesliga",
                season=settings.SEASON,
                matchday=18,
                home_team=Team(id=3102, name="Bayer Leverkusen"),
                away_team=Team(id=3103, name="RB Leipzig"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 3, "away": 2}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        upcoming_matches = [
            MatchLive(
                id=4103,
                competition="Bundesliga",
                season=settings.SEASON,
                matchday=19,
                home_team=Team(id=3105, name="Stuttgart"),
                away_team=Team(id=3106, name="Eintracht Frankfurt"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4104,
                competition="Bundesliga",
                season=settings.SEASON,
                matchday=19,
                home_team=Team(id=3107, name="Freiburg"),
                away_team=Team(id=3108, name="Hoffenheim"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        if matchday == 18:
            return finished_matches
        if matchday == 19:
            return upcoming_matches
        return finished_matches + upcoming_matches
    
    def _get_mock_laliga_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        from datetime import datetime, timedelta
        today = datetime.now()
        base_day = datetime(today.year, today.month, today.day, 18, 0)
        finished_matches = [
            MatchLive(
                id=4201,
                competition="La Liga",
                season=settings.SEASON,
                matchday=20,
                home_team=Team(id=3201, name="Real Madrid"),
                away_team=Team(id=3202, name="Barcelona"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 2, "away": 2}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4202,
                competition="La Liga",
                season=settings.SEASON,
                matchday=20,
                home_team=Team(id=3203, name="Girona"),
                away_team=Team(id=3204, name="Atlético Madrid"),
                utc_date=base_day - timedelta(days=2),
                status=MatchStatus.FINISHED,
                score=Score(full_time={"home": 1, "away": 0}),
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        upcoming_matches = [
            MatchLive(
                id=4203,
                competition="La Liga",
                season=settings.SEASON,
                matchday=21,
                home_team=Team(id=3205, name="Athletic Bilbao"),
                away_team=Team(id=3206, name="Real Sociedad"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
            MatchLive(
                id=4204,
                competition="La Liga",
                season=settings.SEASON,
                matchday=21,
                home_team=Team(id=3207, name="Betis"),
                away_team=Team(id=3208, name="Villarreal"),
                utc_date=base_day + timedelta(days=1),
                status=MatchStatus.SCHEDULED,
                score=None,
                last_updated=datetime.now(),
                data_provider=Provider.API_FOOTBALL,
            ),
        ]
        if matchday == 20:
            return finished_matches
        if matchday == 21:
            return upcoming_matches
        return finished_matches + upcoming_matches
    
    def _get_mock_norway_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        from datetime import datetime, timedelta
        today = datetime.now()
        base_day = datetime(today.year, today.month, today.day, 18, 0)
        finished_matches = [
            self._create_norway_mock_match(
                3001,
                "Bodø/Glimt",
                "Rosenborg",
                3,
                1,
                MatchStatus.FINISHED,
                1,
                utc_date=base_day - timedelta(days=2),
            ),
            self._create_norway_mock_match(
                3002,
                "Molde",
                "Vålerenga",
                2,
                2,
                MatchStatus.FINISHED,
                1,
                utc_date=base_day - timedelta(days=2),
            ),
            self._create_norway_mock_match(
                3003,
                "Brann",
                "Lillestrøm",
                1,
                0,
                MatchStatus.FINISHED,
                1,
                utc_date=base_day - timedelta(days=1),
            ),
            self._create_norway_mock_match(
                3004,
                "Sarpsborg 08",
                "Odd",
                0,
                0,
                MatchStatus.FINISHED,
                1,
                utc_date=base_day - timedelta(days=1),
            ),
        ]
        upcoming_matches = [
            self._create_norway_mock_match(
                3005,
                "Haugesund",
                "Tromsø",
                0,
                0,
                MatchStatus.SCHEDULED,
                2,
                utc_date=base_day + timedelta(days=1),
            ),
            self._create_norway_mock_match(
                3006,
                "Sandefjord",
                "Stabæk",
                0,
                0,
                MatchStatus.SCHEDULED,
                2,
                utc_date=base_day + timedelta(days=1),
            ),
            self._create_norway_mock_match(
                3007,
                "Strømsgodset",
                "HamKam",
                0,
                0,
                MatchStatus.SCHEDULED,
                2,
                utc_date=base_day + timedelta(days=2),
            ),
            self._create_norway_mock_match(
                3008,
                "Kristiansund",
                "Aalesund",
                0,
                0,
                MatchStatus.SCHEDULED,
                2,
                utc_date=base_day + timedelta(days=2),
            ),
        ]
        if matchday == 1:
            return finished_matches
        if matchday == 2:
            return upcoming_matches
        return finished_matches + upcoming_matches

    def _create_norway_mock_match(
        self,
        id: int,
        home: str,
        away: str,
        home_score: int,
        away_score: int,
        status: MatchStatus,
        matchday: int,
        utc_date,
    ) -> MatchLive:
        match_date = utc_date
        score = None
        if status == MatchStatus.FINISHED:
            score = Score(full_time={"home": home_score, "away": away_score})
        return MatchLive(
            id=id,
            competition="Eliteserien",
            season=settings.SEASON,
            matchday=matchday,
            home_team=Team(id=id * 10, name=home),
            away_team=Team(id=id * 10 + 1, name=away),
            utc_date=match_date,
            status=status,
            score=score,
            last_updated=datetime.now(),
            data_provider=Provider.FOOTBALL_DATA,
        )
    
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
