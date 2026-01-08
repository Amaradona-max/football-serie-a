from typing import Optional, List, Dict, Any
from datetime import datetime
from app.data.providers.base import BaseDataProvider
from app.data.models.common import MatchLive, MatchHistorical, Standings, Team, MatchStatus, MatchEvent, MatchEventType, Score, Provider
from app.core.config import settings

class ApiFootballProvider(BaseDataProvider):
    def __init__(self):
        super().__init__()
        self.provider_name = Provider.API_FOOTBALL
        self.base_url = "https://api-football-v1.p.rapidapi.com/v3"
        api_key = settings.API_FOOTBALL_KEY or settings.RAPIDAPI_KEY
        self.headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }
    
    async def get_live_matches(self) -> List[MatchLive]:
        url = f"{self.base_url}/fixtures?league={settings.SERIE_A_LEAGUE_ID}&season={settings.SEASON}&live=all"
        data = await self._make_request(url, self.headers)
        if not data or "response" not in data:
            return []
        
        return [self._parse_match(match_data) for match_data in data["response"]]
    
    async def get_match_by_id(self, match_id: int) -> Optional[MatchHistorical]:
        url = f"{self.base_url}/fixtures?id={match_id}"
        data = await self._make_request(url, self.headers)
        if not data or "response" not in data or not data["response"]:
            return None
        
        return self._parse_match(data["response"][0])
    
    async def get_standings(self) -> Optional[Standings]:
        url = f"{self.base_url}/standings?league={settings.SERIE_A_LEAGUE_ID}&season={settings.SEASON}"
        data = await self._make_request(url, self.headers)
        if not data or "response" not in data or not data["response"]:
            return None
        
        return self._parse_standings(data["response"][0])
    
    async def get_team(self, team_id: int) -> Optional[Team]:
        url = f"{self.base_url}/teams?id={team_id}"
        data = await self._make_request(url, self.headers)
        if not data or "response" not in data or not data["response"]:
            return None
        
        return self._parse_team(data["response"][0])
    
    async def get_fixtures(self, matchday: Optional[int] = None) -> List[MatchLive]:
        url = f"{self.base_url}/fixtures?league={settings.SERIE_A_LEAGUE_ID}&season={settings.SEASON}"
        if matchday:
            url += f"&round=Regular Season - {matchday}"
        
        data = await self._make_request(url, self.headers)
        if not data or "response" not in data:
            return []
        
        return [self._parse_match(match_data) for match_data in data["response"]]
    
    def _parse_match(self, match_data: Dict[str, Any]) -> MatchLive:
        fixture = match_data["fixture"]
        teams = match_data["teams"]
        goals = match_data["goals"]

        status_short = fixture["status"].get("short")
        status_map = {
            "NS": MatchStatus.SCHEDULED,
            "TBD": MatchStatus.SCHEDULED,
            "1H": MatchStatus.IN_PLAY,
            "2H": MatchStatus.IN_PLAY,
            "ET": MatchStatus.IN_PLAY,
            "P": MatchStatus.IN_PLAY,
            "HT": MatchStatus.PAUSED,
            "BT": MatchStatus.PAUSED,
            "FT": MatchStatus.FINISHED,
            "AET": MatchStatus.FINISHED,
            "PEN": MatchStatus.FINISHED,
            "PST": MatchStatus.POSTPONED,
            "SUSP": MatchStatus.POSTPONED,
            "INT": MatchStatus.POSTPONED,
            "CANC": MatchStatus.CANCELLED,
            "ABD": MatchStatus.CANCELLED,
            "AWD": MatchStatus.FINISHED,
            "WO": MatchStatus.FINISHED,
            "LIVE": MatchStatus.LIVE,
        }

        mapped_status = status_map.get(status_short, MatchStatus.SCHEDULED)
        
        return MatchLive(
            id=fixture["id"],
            competition="Serie A",
            season=settings.SEASON,
            matchday=match_data["league"].get("round", "Unknown").split(" - ")[-1],
            home_team=self._parse_team({"id": teams["home"]["id"], "name": teams["home"]["name"]}),
            away_team=self._parse_team({"id": teams["away"]["id"], "name": teams["away"]["name"]}),
            utc_date=datetime.fromisoformat(fixture["date"].replace("Z", "+00:00")),
            status=mapped_status,
            minute=fixture["status"].get("elapsed"),
            score=Score(full_time=goals) if goals["home"] is not None and goals["away"] is not None else None,
            events=[],
            venue=fixture["venue"]["name"] if fixture.get("venue") else None,
            referee=fixture["referee"] if fixture.get("referee") else None,
            last_updated=datetime.now(),
            data_provider=self.provider_name
        )
    
    def _parse_team(self, team_data: Dict[str, Any]) -> Team:
        return Team(
            id=team_data["id"],
            name=team_data["name"],
            short_name=team_data.get("shortName"),
            crest=team_data.get("logo"),
            country="Italy",
            founded=None,
            venue=None
        )
    
    def _parse_standings(self, standings_data: Dict[str, Any]) -> Standings:
        league = standings_data["league"]
        standings = league["standings"][0]
        team_stats = []
        
        for team_data in standings:
            team_stats.append(
                TeamStats(
                    team=self._parse_team(team_data["team"]),
                    played=team_data["all"]["played"],
                    won=team_data["all"]["win"],
                    drawn=team_data["all"]["draw"],
                    lost=team_data["all"]["lose"],
                    goals_for=team_data["all"]["goals"]["for"],
                    goals_against=team_data["all"]["goals"]["against"],
                    goal_difference=team_data["goalsDiff"],
                    points=team_data["points"],
                    form=team_data.get("form"),
                    position=team_data["rank"]
                )
            )
        
        return Standings(
            competition=league["name"],
            season=league["season"],
            standings=team_stats,
            last_updated=datetime.now(),
            data_provider=self.provider_name
        )
