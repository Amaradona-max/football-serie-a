from datetime import datetime, date
from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field

class CardType(str, Enum):
    YELLOW = "yellow"
    RED = "red"
    SECOND_YELLOW = "second_yellow"

class SuspensionReason(str, Enum):
    RED_CARD = "red_card"
    YELLOW_CARDS = "yellow_cards"
    DISCIPLINARY = "disciplinary"
    OTHER = "other"

class PlayerBioRhythm(BaseModel):
    physical: float = Field(..., ge=0, le=100, description="Bioritmo fisico (0-100)")
    emotional: float = Field(..., ge=0, le=100, description="Bioritmo emotivo (0-100)")
    intellectual: float = Field(..., ge=0, le=100, description="Bioritmo intellettuale (0-100)")
    overall: float = Field(..., ge=0, le=100, description="Bioritmo complessivo (0-100)")
    
class PlayerDetailed(BaseModel):
    id: int
    name: str
    position: str
    date_of_birth: date
    nationality: str
    height: Optional[float] = None
    weight: Optional[float] = None
    bio_rhythm: Optional[PlayerBioRhythm] = None
    
class TopScorer(BaseModel):
    player: PlayerDetailed
    goals: int
    assists: Optional[int] = None
    minutes_played: Optional[int] = None
    
class CardEvent(BaseModel):
    player: PlayerDetailed
    card_type: CardType
    minute: int
    reason: Optional[str] = None
    match_id: int
    
class Suspension(BaseModel):
    player: PlayerDetailed
    reason: SuspensionReason
    matches_suspended: int
    end_date: Optional[date] = None
    description: Optional[str] = None
    
class TeamStatsDetailed(BaseModel):
    team_id: int
    team_name: str
    yellow_cards: int
    red_cards: int
    total_cards: int
    fouls_committed: Optional[int] = None
    fouls_suffered: Optional[int] = None
    
class MatchLineup(BaseModel):
    formation: str
    starting_xi: List[PlayerDetailed]
    substitutes: List[PlayerDetailed]
    coach: str
    
class ExpectedGoals(BaseModel):
    home: float
    away: float
    total: float
    
class MatchPredictionDetailed(BaseModel):
    match_id: int
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    expected_goals: ExpectedGoals
    both_teams_to_score_prob: float
    over_under_25_prob: float
    most_likely_scoreline: str
    home_fair_odds: Optional[float] = None
    draw_fair_odds: Optional[float] = None
    away_fair_odds: Optional[float] = None
    
class DailyAnalysis(BaseModel):
    date: date
    total_matches: int
    total_goals: int
    average_goals_per_match: float
    home_goals: int
    away_goals: int
    total_cards: int
    average_cards_per_match: float
    home_wins: int
    away_wins: int
    draws: int
    home_win_percentage: float
    away_win_percentage: float
    draw_percentage: float
    matches_over_25_goals: int
    matches_both_teams_score: int
    clean_sheets: int
    matches_with_red_card: int
    most_productive_match: Optional[str] = None
    most_disciplined_match: Optional[str] = None
    biggest_win: Optional[str] = None
    
class LiveMatchCard(BaseModel):
    match_id: int
    home_team: str
    away_team: str
    status: str
    minute: Optional[int] = None
    score: Optional[str] = None
    prediction: MatchPredictionDetailed
    expected_lineups: Dict[str, MatchLineup]  # home and away
    top_scorers: List[TopScorer]
    bio_rhythm_analysis: Dict[str, PlayerBioRhythm]  # Key players analysis
    cards_summary: Dict[str, int]  # yellow, red per team
    
class SerieAStats(BaseModel):
    standings: Any  # Will use existing Standings model
    top_scorers: List[TopScorer]
    card_stats: List[TeamStatsDetailed]
    suspensions: List[Suspension]
    last_updated: datetime
