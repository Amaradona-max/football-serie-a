from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.data.models.common import MatchLive, MatchHistorical, Standings, Team, Provider
from app.core.config import settings

class BaseDataProvider(ABC):
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.provider_name: Provider
    
    @abstractmethod
    async def get_live_matches(self) -> List[MatchLive]:
        pass
    
    @abstractmethod
    async def get_match_by_id(self, match_id: int) -> Optional[MatchHistorical]:
        pass
    
    @abstractmethod
    async def get_standings(self) -> Optional[Standings]:
        pass
    
    @abstractmethod
    async def get_team(self, team_id: int) -> Optional[Team]:
        pass
    
    @abstractmethod
    async def get_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        pass
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _make_request(self, url: str, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        try:
            response = await self.client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            print(f"HTTP error from {self.provider_name}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error from {self.provider_name}: {e}")
            return None
    
    async def close(self):
        await self.client.aclose()
    
    def __del__(self):
        try:
            import asyncio
            asyncio.create_task(self.close())
        except:
            pass