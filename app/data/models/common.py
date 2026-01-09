from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class Provider(str, Enum):
    FOOTBALL_DATA = "football_data"
    API_FOOTBALL = "api_football"
    STATSBOMB = "statsbomb"
    CACHE = "cache"

class Team(BaseModel):
    id: int
    name: str
    short_name: Optional[str] = None
    crest: Optional[str] = None
    country: Optional[str] = None
    founded: Optional[int] = None
    venue: Optional[str] = None

class Score(BaseModel):
    full_time: Optional[Dict[str, int]] = None
    half_time: Optional[Dict[str, int]] = None
    extra_time: Optional[Dict[str, int]] = None
    penalties: Optional[Dict[str, int]] = None

class MatchStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    LIVE = "LIVE"
    IN_PLAY = "IN_PLAY"
    PAUSED = "PAUSED"
    FINISHED = "FINISHED"
    POSTPONED = "POSTPONED"
    CANCELLED = "CANCELLED"

class MatchEventType(str, Enum):
    GOAL = "GOAL"
    YELLOW_CARD = "YELLOW_CARD"
    RED_CARD = "RED_CARD"
    SUBSTITUTION = "SUBSTITUTION"
    PENALTY = "PENALTY"
    CORNER = "CORNER"
    FOUL = "FOUL"
    OFFSIDE = "OFFSIDE"

class MatchEvent(BaseModel):
    minute: int
    type: MatchEventType
    team: str
    player: Optional[str] = None
    description: Optional[str] = None
    extra_time: Optional[int] = None

class BaseMatch(BaseModel):
    id: int
    competition: str
    season: int
    matchday: int
    home_team: Team
    away_team: Team
    utc_date: datetime
    status: MatchStatus
    stage: Optional[str] = None
    group: Optional[str] = None
    last_updated: datetime
    data_provider: Provider

class MatchLive(BaseMatch):
    minute: Optional[int] = None
    score: Optional[Score] = None
    events: List[MatchEvent] = Field(default_factory=list)
    venue: Optional[str] = None
    referee: Optional[str] = None

class MatchHistorical(BaseMatch):
    score: Score
    events: List[MatchEvent] = Field(default_factory=list)
    statistics: Optional[Dict[str, Any]] = None
    venue: Optional[str] = None
    referee: Optional[str] = None
    attendance: Optional[int] = None

class TeamStats(BaseModel):
    team: Team
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    goal_difference: int
    points: int
    form: Optional[str] = None
    position: int

class Standings(BaseModel):
    competition: str
    season: int
    standings: List[TeamStats]
    last_updated: datetime
    data_provider: Provider

class PredictionInput(BaseModel):
    home_team_id: int
    away_team_id: int
    home_form: str
    away_form: str
    home_position: int
    away_position: int
    is_home_advantage: bool
    previous_meetings: List[Dict[str, Any]]

class PredictionOutput(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    predicted_score: Optional[Dict[str, int]] = None
    confidence: float
    model_version: str
