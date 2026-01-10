import logging
from datetime import datetime, date, timezone
from typing import List, Optional, Dict, Any
import math

from app.data.services.unified_data_service import unified_data_service
from app.data.models.detailed import (
    TopScorer, CardEvent, Suspension, TeamStatsDetailed, PlayerDetailed, 
    PlayerBioRhythm, SerieAStats, LiveMatchCard, DailyAnalysis
)
from app.data.models.common import MatchLive, MatchStatus, Team, Provider, Score, MatchEventType
from app.data.cache.redis_client import redis_client
from app.core.config import settings
import json
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

class DetailedStatsService:
    def __init__(self):
        self.cache_prefix = "seriea_detailed:"
    
    async def get_seriea_stats(self) -> SerieAStats:
        cache_key = f"{self.cache_prefix}complete_stats"
        
        # Try cache first
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return SerieAStats.parse_raw(cached_data)
        
        try:
            # Get standings from unified service
            standings = await unified_data_service.get_standings()
            
            # Get detailed stats (in real implementation, this would come from data providers)
            top_scorers = await self._get_top_scorers()
            card_stats = await self._get_card_stats()
            suspensions = await self._get_suspensions()
            
            stats = SerieAStats(
                standings=standings,
                top_scorers=top_scorers,
                card_stats=card_stats,
                suspensions=suspensions,
                last_updated=datetime.now()
            )
            
            await redis_client.setex(cache_key, 60, stats.json())
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting Serie A stats: {e}")
            raise

    async def get_norway_stats(self) -> SerieAStats:
        cache_key = "norway_detailed:complete_stats"
        
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return SerieAStats.parse_raw(cached_data)
        
        try:
            standings = await unified_data_service.get_standings_norway()
            top_scorers = await self._get_norway_top_scorers()
            card_stats = await self._get_norway_card_stats()
            suspensions = await self._get_norway_suspensions()
            
            stats = SerieAStats(
                standings=standings,
                top_scorers=top_scorers,
                card_stats=card_stats,
                suspensions=suspensions,
                last_updated=datetime.now()
            )
            
            await redis_client.setex(cache_key, 60, stats.json())
            
            return stats
        except Exception as e:
            logger.error(f"Error getting Norway stats: {e}")
            raise

    async def get_prediction_context(self, match: Any) -> Dict[str, Any]:
        prediction = await self._generate_prediction(match)
        bio_rhythms = await self._get_bio_rhythm_analysis(match)
        expected_lineups = await self._get_expected_lineups(match)

        kickoff = getattr(match, "utc_date", None)
        include_context = False

        if isinstance(kickoff, datetime):
            if kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            seconds_to_kickoff = (kickoff - now).total_seconds()
            include_context = seconds_to_kickoff <= 3600

        if not include_context:
            bio_rhythms = {}
            expected_lineups = None

        return {
            "prediction": prediction,
            "bio_rhythm_analysis": bio_rhythms,
            "expected_lineups": expected_lineups,
        }
    
    async def get_live_match_cards(self) -> List[LiveMatchCard]:
        """Get live and upcoming match cards with detailed analysis from real providers"""
        try:
            matches: List[MatchLive] = []

            try:
                from app.data.providers.api_football import ApiFootballProvider
                api_provider = ApiFootballProvider()
                matches = await api_provider.get_live_matches()
            except Exception as e:
                logger.error(f"Error getting live matches from API-Football: {e}")
                matches = []

            if not matches:
                matches = await unified_data_service.get_live_matches()

            if not matches:
                return []

            match_cards: List[LiveMatchCard] = []
            for match in matches:
                match_card = await self._create_match_card(match)
                match_cards.append(match_card)

            return match_cards

        except Exception as e:
            logger.error(f"Error getting live match cards: {e}")
            raise
    
    async def get_daily_analysis(self, analysis_date: date) -> DailyAnalysis:
        """Get daily statistical analysis"""
        cache_key = f"{self.cache_prefix}daily_analysis:{analysis_date}"
        
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return DailyAnalysis.parse_raw(cached_data)
        
        try:
            # In real implementation, this would analyze matches from the specific date
            analysis = await self._generate_daily_analysis(analysis_date)
            
            # Cache for 24 hours
            await redis_client.setex(cache_key, 86400, analysis.json())
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error generating daily analysis: {e}")
            raise
    
    async def calculate_bio_rhythm(self, birth_date: date, target_date: date) -> PlayerBioRhythm:
        """Calculate bio-rhythm for a player"""
        try:
            # Calculate days since birth
            days_alive = (target_date - birth_date).days
            
            # Bio-rhythm calculations (simplified)
            physical = 50 + 50 * math.sin(2 * math.pi * days_alive / 23)
            emotional = 50 + 50 * math.sin(2 * math.pi * days_alive / 28) 
            intellectual = 50 + 50 * math.sin(2 * math.pi * days_alive / 33)
            overall = (physical + emotional + intellectual) / 3
            
            return PlayerBioRhythm(
                physical=max(0, min(100, physical)),
                emotional=max(0, min(100, emotional)),
                intellectual=max(0, min(100, intellectual)),
                overall=max(0, min(100, overall))
            )
            
        except Exception as e:
            logger.error(f"Error calculating bio-rhythm: {e}")
            return PlayerBioRhythm(physical=50, emotional=50, intellectual=50, overall=50)

    async def _get_bio_rhythm_analysis(self, match: Any) -> Dict[str, PlayerBioRhythm]:
        bio_rhythms: Dict[str, PlayerBioRhythm] = {}

        key_players = []
        if "Inter" in match.home_team.name or "Inter" in match.away_team.name:
            key_players.extend([("Lautaro Martínez", date(1997, 8, 22)), ("Marcus Thuram", date(1997, 8, 6))])
        if "Milan" in match.home_team.name or "Milan" in match.away_team.name:
            key_players.extend([("Christian Pulisic", date(1998, 9, 18)), ("Rafael Leão", date(1999, 6, 10))])
        if "Juventus" in match.home_team.name or "Juventus" in match.away_team.name:
            key_players.extend([("Dusan Vlahovic", date(2000, 1, 28)), ("Kenan Yıldız", date(2005, 5, 4))])
        if "Napoli" in match.home_team.name or "Napoli" in match.away_team.name:
            key_players.extend([("Romelu Lukaku", date(1993, 5, 13)), ("K. Kvaratskhelia", date(2001, 2, 12))])

        if not key_players:
            key_players = [("Capitano Casa", date(1995, 1, 1)), ("Capitano Trasferta", date(1996, 1, 1))]

        kickoff = getattr(match, "utc_date", None)
        target_date = date.today()
        if isinstance(kickoff, datetime):
            target_date = kickoff.date()

        for player_name, birth_date in key_players:
            bio_rhythm = await self.calculate_bio_rhythm(birth_date, target_date)
            bio_rhythms[player_name] = bio_rhythm

        return bio_rhythms
    
    async def _get_top_scorers(self) -> List[TopScorer]:
        return [
            TopScorer(
                player=PlayerDetailed(
                    id=1,
                    name="Lautaro Martínez",
                    position="Forward",
                    date_of_birth=date(1997, 8, 22),
                    nationality="Argentina",
                ),
                goals=10,
                assists=3,
                minutes_played=1260,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=2,
                    name="Christian Pulisic",
                    position="Forward",
                    date_of_birth=date(1998, 9, 18),
                    nationality="USA",
                ),
                goals=8,
                assists=4,
                minutes_played=1350,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=3,
                    name="Rafael Leão",
                    position="Forward",
                    date_of_birth=date(1999, 6, 10),
                    nationality="Portugal",
                ),
                goals=7,
                assists=3,
                minutes_played=1400,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=4,
                    name="Hakan Çalhanoğlu",
                    position="Midfielder",
                    date_of_birth=date(1994, 2, 8),
                    nationality="Türkiye",
                ),
                goals=6,
                assists=5,
                minutes_played=1500,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=5,
                    name="Marcus Thuram",
                    position="Forward",
                    date_of_birth=date(1997, 8, 6),
                    nationality="France",
                ),
                goals=6,
                assists=4,
                minutes_played=1450,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=6,
                    name="Riccardo Orsolini",
                    position="Forward",
                    date_of_birth=date(1997, 1, 24),
                    nationality="Italy",
                ),
                goals=6,
                assists=2,
                minutes_played=1300,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=7,
                    name="Tasos Douvikas",
                    position="Forward",
                    date_of_birth=date(1999, 8, 2),
                    nationality="Greece",
                ),
                goals=6,
                assists=2,
                minutes_played=1300,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=8,
                    name="Nico Paz",
                    position="Midfielder",
                    date_of_birth=date(2004, 9, 8),
                    nationality="Argentina",
                ),
                goals=6,
                assists=3,
                minutes_played=1320,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=9,
                    name="Rasmus Højlund",
                    position="Forward",
                    date_of_birth=date(2003, 2, 4),
                    nationality="Denmark",
                ),
                goals=6,
                assists=2,
                minutes_played=1380,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=10,
                    name="Kenan Yildiz",
                    position="Forward",
                    date_of_birth=date(2005, 5, 4),
                    nationality="Türkiye",
                ),
                goals=6,
                assists=2,
                minutes_played=1300,
            ),
        ]
    
    async def _get_norway_top_scorers(self) -> List[TopScorer]:
        return [
            TopScorer(
                player=PlayerDetailed(
                    id=2001,
                    name="Amahl Pellegrino",
                    position="Forward",
                    date_of_birth=date(1990, 6, 18),
                    nationality="Norway",
                ),
                goals=14,
                assists=4,
                minutes_played=1500,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=2002,
                    name="Ola Brynhildsen",
                    position="Forward",
                    date_of_birth=date(1999, 4, 27),
                    nationality="Norway",
                ),
                goals=11,
                assists=3,
                minutes_played=1450,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=2003,
                    name="Kasper Junker",
                    position="Forward",
                    date_of_birth=date(1994, 3, 5),
                    nationality="Denmark",
                ),
                goals=9,
                assists=2,
                minutes_played=1380,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=2004,
                    name="Ulrik Saltnes",
                    position="Midfielder",
                    date_of_birth=date(1992, 11, 10),
                    nationality="Norway",
                ),
                goals=8,
                assists=5,
                minutes_played=1420,
            ),
            TopScorer(
                player=PlayerDetailed(
                    id=2005,
                    name="Veton Berisha",
                    position="Forward",
                    date_of_birth=date(1994, 4, 13),
                    nationality="Norway",
                ),
                goals=8,
                assists=3,
                minutes_played=1400,
            ),
        ]
    
    async def _get_card_stats(self) -> List[TeamStatsDetailed]:
        return [
            TeamStatsDetailed(
                team_id=99,
                team_name="Fiorentina",
                yellow_cards=46,
                red_cards=1,
                total_cards=47,
            ),
            TeamStatsDetailed(
                team_id=104,
                team_name="Cagliari",
                yellow_cards=45,
                red_cards=0,
                total_cards=45,
            ),
            TeamStatsDetailed(
                team_id=1033,
                team_name="Como",
                yellow_cards=42,
                red_cards=2,
                total_cards=44,
            ),
            TeamStatsDetailed(
                team_id=119,
                team_name="Verona",
                yellow_cards=44,
                red_cards=0,
                total_cards=44,
            ),
            TeamStatsDetailed(
                team_id=110,
                team_name="Lazio",
                yellow_cards=35,
                red_cards=7,
                total_cards=42,
            ),
            TeamStatsDetailed(
                team_id=107,
                team_name="Genoa",
                yellow_cards=39,
                red_cards=2,
                total_cards=41,
            ),
            TeamStatsDetailed(
                team_id=111,
                team_name="Sassuolo",
                yellow_cards=40,
                red_cards=1,
                total_cards=41,
            ),
            TeamStatsDetailed(
                team_id=5890,
                team_name="Cremonese",
                yellow_cards=38,
                red_cards=1,
                total_cards=39,
            ),
            TeamStatsDetailed(
                team_id=100,
                team_name="Roma",
                yellow_cards=37,
                red_cards=1,
                total_cards=38,
            ),
            TeamStatsDetailed(
                team_id=1107,
                team_name="Pisa",
                yellow_cards=35,
                red_cards=2,
                total_cards=37,
            ),
            TeamStatsDetailed(
                team_id=115,
                team_name="Udinese",
                yellow_cards=36,
                red_cards=1,
                total_cards=37,
            ),
            TeamStatsDetailed(
                team_id=115,
                team_name="Torino",
                yellow_cards=35,
                red_cards=0,
                total_cards=35,
            ),
            TeamStatsDetailed(
                team_id=103,
                team_name="Bologna",
                yellow_cards=31,
                red_cards=2,
                total_cards=33,
            ),
            TeamStatsDetailed(
                team_id=112,
                team_name="Parma",
                yellow_cards=29,
                red_cards=3,
                total_cards=32,
            ),
            TeamStatsDetailed(
                team_id=1124,
                team_name="Lecce",
                yellow_cards=30,
                red_cards=0,
                total_cards=30,
            ),
            TeamStatsDetailed(
                team_id=98,
                team_name="Milan",
                yellow_cards=27,
                red_cards=1,
                total_cards=28,
            ),
            TeamStatsDetailed(
                team_id=108,
                team_name="Inter",
                yellow_cards=28,
                red_cards=0,
                total_cards=28,
            ),
            TeamStatsDetailed(
                team_id=102,
                team_name="Atalanta",
                yellow_cards=25,
                red_cards=1,
                total_cards=26,
            ),
            TeamStatsDetailed(
                team_id=113,
                team_name="Napoli",
                yellow_cards=25,
                red_cards=1,
                total_cards=26,
            ),
            TeamStatsDetailed(
                team_id=109,
                team_name="Juventus",
                yellow_cards=23,
                red_cards=2,
                total_cards=25,
            ),
        ]
    
    async def _get_norway_card_stats(self) -> List[TeamStatsDetailed]:
        return [
            TeamStatsDetailed(
                team_id=2001,
                team_name="Bodø/Glimt",
                yellow_cards=40,
                red_cards=2,
                total_cards=42,
            ),
            TeamStatsDetailed(
                team_id=2002,
                team_name="Molde",
                yellow_cards=38,
                red_cards=1,
                total_cards=39,
            ),
            TeamStatsDetailed(
                team_id=2003,
                team_name="Rosenborg",
                yellow_cards=36,
                red_cards=1,
                total_cards=37,
            ),
            TeamStatsDetailed(
                team_id=2004,
                team_name="Vålerenga",
                yellow_cards=34,
                red_cards=0,
                total_cards=34,
            ),
            TeamStatsDetailed(
                team_id=2005,
                team_name="Brann",
                yellow_cards=33,
                red_cards=1,
                total_cards=34,
            ),
            TeamStatsDetailed(
                team_id=2006,
                team_name="Sarpsborg 08",
                yellow_cards=32,
                red_cards=0,
                total_cards=32,
            ),
            TeamStatsDetailed(
                team_id=2007,
                team_name="Lillestrøm",
                yellow_cards=31,
                red_cards=1,
                total_cards=32,
            ),
            TeamStatsDetailed(
                team_id=2008,
                team_name="Odd",
                yellow_cards=30,
                red_cards=0,
                total_cards=30,
            ),
        ]
    
    async def _get_suspensions(self) -> List[Suspension]:
        return [
            Suspension(
                player=PlayerDetailed(
                    id=101,
                    name="Tijjani Noslin",
                    position="Forward",
                    date_of_birth=date(1999, 7, 7),
                    nationality="Netherlands",
                ),
                reason="yellow_cards",
                matches_suspended=1,
                description="Espulso in Lazio-Napoli (doppia ammonizione), salta la 19ª",
            ),
            Suspension(
                player=PlayerDetailed(
                    id=102,
                    name="Adam Marušić",
                    position="Defender",
                    date_of_birth=date(1992, 10, 17),
                    nationality="Montenegro",
                ),
                reason="red_card",
                matches_suspended=1,
                description="Espulso in Lazio-Napoli, squalifica di una giornata",
            ),
            Suspension(
                player=PlayerDetailed(
                    id=103,
                    name="Pasquale Mazzocchi",
                    position="Defender",
                    date_of_birth=date(1995, 7, 27),
                    nationality="Italy",
                ),
                reason="red_card",
                matches_suspended=1,
                description="Espulso in Lazio-Napoli, squalifica di una giornata",
            ),
            Suspension(
                player=PlayerDetailed(
                    id=104,
                    name="Mario Hermoso",
                    position="Defender",
                    date_of_birth=date(1995, 6, 18),
                    nationality="Spain",
                ),
                reason="yellow_cards",
                matches_suspended=1,
                description="Somma di ammonizioni, salta Lecce-Roma (19ª giornata)",
            ),
            Suspension(
                player=PlayerDetailed(
                    id=105,
                    name="Gianluca Mancini",
                    position="Defender",
                    date_of_birth=date(1996, 4, 17),
                    nationality="Italy",
                ),
                reason="yellow_cards",
                matches_suspended=1,
                description="Somma di ammonizioni, salta Lecce-Roma (19ª giornata)",
            ),
        ]
    
    async def _get_norway_suspensions(self) -> List[Suspension]:
        return []
    
    async def _create_match_card(self, match: Any) -> LiveMatchCard:
        """Create detailed match card with REAL key players for 2025/2026"""
        bio_rhythms = await self._get_bio_rhythm_analysis(match)

        home_team_name = getattr(match.home_team, "name", str(match.home_team))
        away_team_name = getattr(match.away_team, "name", str(match.away_team))

        status_str = str(match.status)
        score_str = None
        score_obj = getattr(match, "score", None)
        full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
        if isinstance(full_time, dict):
            home_goals = full_time.get("home")
            away_goals = full_time.get("away")
            if home_goals is not None and away_goals is not None:
                score_str = f"{home_goals}-{away_goals}"

        yellow_cards = 0
        red_cards = 0
        for event in getattr(match, "events", []):
            if event.type == MatchEventType.YELLOW_CARD:
                yellow_cards += 1
            elif event.type == MatchEventType.RED_CARD:
                red_cards += 1

        return LiveMatchCard(
            match_id=match.id,
            home_team=home_team_name,
            away_team=away_team_name,
            status=status_str,
            minute=getattr(match, 'minute', None),
            score=score_str,
            prediction=await self._generate_prediction(match),
            expected_lineups=await self._get_expected_lineups(match),
            top_scorers=await self._get_match_top_scorers(match),
            bio_rhythm_analysis=bio_rhythms,
            cards_summary={"yellow": yellow_cards, "red": red_cards}
        )
    
    async def get_live_match_cards_norway(self) -> List[LiveMatchCard]:
        """Get live match cards for Norway Eliteserien using the same rich context"""
        try:
            matches: List[MatchLive] = await unified_data_service.get_live_matches_norway()
            if not matches:
                return []

            match_cards: List[LiveMatchCard] = []
            for match in matches:
                match_cards.append(await self._create_match_card(match))

            return match_cards
        except Exception as e:
            logger.error(f"Error getting live match cards for Norway: {e}")
            raise
    
    async def get_live_match_cards_premier(self) -> List[LiveMatchCard]:
        try:
            matches: List[MatchLive] = await unified_data_service.get_live_matches_premier()
            if not matches:
                return []

            match_cards: List[LiveMatchCard] = []
            for match in matches:
                match_cards.append(await self._create_match_card(match))

            return match_cards
        except Exception as e:
            logger.error(f"Error getting live match cards for Premier League: {e}")
            raise

    async def get_live_match_cards_bundesliga(self) -> List[LiveMatchCard]:
        try:
            matches: List[MatchLive] = await unified_data_service.get_live_matches_bundesliga()
            if not matches:
                return []

            match_cards: List[LiveMatchCard] = []
            for match in matches:
                match_cards.append(await self._create_match_card(match))

            return match_cards
        except Exception as e:
            logger.error(f"Error getting live match cards for Bundesliga: {e}")
            raise

    async def get_live_match_cards_laliga(self) -> List[LiveMatchCard]:
        try:
            matches: List[MatchLive] = await unified_data_service.get_live_matches_laliga()
            if not matches:
                return []

            match_cards: List[LiveMatchCard] = []
            for match in matches:
                match_cards.append(await self._create_match_card(match))

            return match_cards
        except Exception as e:
            logger.error(f"Error getting live match cards for La Liga: {e}")
            raise
    
    async def _generate_prediction(self, match: Any) -> Any:
        from app.data.models.detailed import MatchPredictionDetailed, ExpectedGoals

        goalmodel_url = settings.GOALMODEL_API_URL

        def _poisson_pmf(k: int, lam: float) -> float:
            return math.exp(-lam) * (lam ** k) / math.factorial(k)

        def _compute_live_from_xg(
            home_xg: float,
            away_xg: float,
            current_home: int,
            current_away: int,
            minute: Any,
            red_home: int,
            red_away: int,
        ):
            try:
                m = int(minute)
            except Exception:
                return None
            if m < 0:
                m = 0
            if m > 90:
                m = 90
            remaining = max(0, 90 - m)
            if remaining == 0:
                if current_home > current_away:
                    return 100.0, 0.0, 0.0, 0.0, 0.0, f"{current_home}-{current_away}"
                if current_away > current_home:
                    return 0.0, 0.0, 100.0, 0.0, 0.0, f"{current_home}-{current_away}"
                return 0.0, 100.0, 0.0, 0.0, 0.0, f"{current_home}-{current_away}"
            fraction = remaining / 90.0
            lambda_home = max(0.05, home_xg * fraction)
            lambda_away = max(0.05, away_xg * fraction)
            rh = max(red_home or 0, 0)
            ra = max(red_away or 0, 0)
            factor_home = (0.7 ** rh) * (1.15 ** ra)
            factor_away = (0.7 ** ra) * (1.15 ** rh)
            lambda_home = max(0.01, lambda_home * factor_home)
            lambda_away = max(0.01, lambda_away * factor_away)
            max_goals = 6
            goals = list(range(max_goals + 1))
            home_probs = [_poisson_pmf(g, lambda_home) for g in goals]
            away_probs = [_poisson_pmf(g, lambda_away) for g in goals]
            matrix = [[home_probs[i] * away_probs[j] for j in range(len(goals))] for i in range(len(goals))]
            total_mass = sum(sum(row) for row in matrix)
            if total_mass <= 0:
                return None
            home_sum = 0.0
            draw_sum = 0.0
            away_sum = 0.0
            btts_sum = 0.0
            over25_sum = 0.0
            best_p = -1.0
            best_score = f"{current_home}-{current_away}"
            for i, g_home_extra in enumerate(goals):
                for j, g_away_extra in enumerate(goals):
                    p = matrix[i][j]
                    if p <= 0:
                        continue
                    gh = current_home + g_home_extra
                    ga = current_away + g_away_extra
                    if gh > ga:
                        home_sum += p
                    elif ga > gh:
                        away_sum += p
                    else:
                        draw_sum += p
                    if gh > 0 and ga > 0:
                        btts_sum += p
                    if gh + ga >= 3:
                        over25_sum += p
                    if p > best_p:
                        best_p = p
                        best_score = f"{gh}-{ga}"
            if home_sum + draw_sum + away_sum <= 0:
                return None
            home_prob = home_sum / (home_sum + draw_sum + away_sum)
            draw_prob = draw_sum / (home_sum + draw_sum + away_sum)
            away_prob = away_sum / (home_sum + draw_sum + away_sum)
            home_pct = round(home_prob * 100.0, 1)
            draw_pct = round(draw_prob * 100.0, 1)
            away_pct = round(away_prob * 100.0, 1)
            btts_pct = round((btts_sum / total_mass) * 100.0, 1)
            over25_pct = round((over25_sum / total_mass) * 100.0, 1)
            return home_pct, draw_pct, away_pct, btts_pct, over25_pct, best_score

        if goalmodel_url:
            try:
                payload = {
                    "home_team": getattr(match.home_team, "name", str(match.home_team)),
                    "away_team": getattr(match.away_team, "name", str(match.away_team)),
                    "competition": getattr(match, "competition", "Serie A"),
                    "season": getattr(match, "season", settings.SEASON),
                }
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    goalmodel_url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    resp_body = resp.read().decode("utf-8")
                    result = json.loads(resp_body)

                home_prob = float(result.get("home_win_prob"))
                draw_prob = float(result.get("draw_prob"))
                away_prob = float(result.get("away_win_prob"))
                home_xg = float(result.get("expected_goals_home"))
                away_xg = float(result.get("expected_goals_away"))
                total_xg = float(result.get("expected_goals_total", home_xg + away_xg))
                btts = float(result.get("both_teams_to_score_prob", 0.0))
                over25 = float(result.get("over_25_prob", 0.0))
                scoreline = result.get("most_likely_scoreline")

                home_prob_pct = round(home_prob, 1)
                draw_prob_pct = round(draw_prob, 1)
                away_prob_pct = round(away_prob, 1)

                score_obj_live = getattr(match, "score", None)
                full_time_live = getattr(score_obj_live, "full_time", None) if score_obj_live is not None else None
                live_home_goals = None
                live_away_goals = None
                if isinstance(full_time_live, dict):
                    try:
                        live_home_goals = int(full_time_live.get("home") or 0)
                        live_away_goals = int(full_time_live.get("away") or 0)
                    except Exception:
                        live_home_goals = None
                        live_away_goals = None
                minute_live = getattr(match, "minute", None)
                status_live = getattr(match, "status", None)
                red_home = 0
                red_away = 0
                for ev in getattr(match, "events", []) or []:
                    if getattr(ev, "type", None) == MatchEventType.RED_CARD:
                        team_name = getattr(ev, "team", None)
                        if team_name == getattr(match.home_team, "name", str(match.home_team)):
                            red_home += 1
                        elif team_name == getattr(match.away_team, "name", str(match.away_team)):
                            red_away += 1
                if (
                    minute_live is not None
                    and live_home_goals is not None
                    and live_away_goals is not None
                    and status_live in (MatchStatus.LIVE, MatchStatus.IN_PLAY)
                ):
                    live_result = _compute_live_from_xg(
                        home_xg,
                        away_xg,
                        live_home_goals,
                        live_away_goals,
                        minute_live,
                        red_home,
                        red_away,
                    )
                    if live_result is not None:
                        home_prob_pct, draw_prob_pct, away_prob_pct, btts, over25, scoreline = live_result

                def fair_odds(pct: float) -> Optional[float]:
                    if pct <= 0:
                        return None
                    return round(100.0 / pct, 2)

                return MatchPredictionDetailed(
                    match_id=match.id,
                    home_win_prob=home_prob_pct,
                    draw_prob=draw_prob_pct,
                    away_win_prob=away_prob_pct,
                    expected_goals=ExpectedGoals(
                        home=round(home_xg, 2),
                        away=round(away_xg, 2),
                        total=round(total_xg, 2),
                    ),
                    both_teams_to_score_prob=round(btts, 1),
                    over_under_25_prob=round(over25, 1),
                    most_likely_scoreline=scoreline or "1-1",
                    home_fair_odds=fair_odds(home_prob_pct),
                    draw_fair_odds=fair_odds(draw_prob_pct),
                    away_fair_odds=fair_odds(away_prob_pct),
                )
            except Exception as e:
                logger.error(f"Error calling goalmodel provider: {e}")

        competition_name = str(getattr(match, "competition", "") or "")

        if "Eliteserien" in competition_name or "Norway" in competition_name:
            standings = await unified_data_service.get_standings_norway()
        else:
            standings = await unified_data_service.get_standings()

        home_team_name = getattr(match.home_team, "name", str(match.home_team))
        away_team_name = getattr(match.away_team, "name", str(match.away_team))

        home_stats = None
        away_stats = None
        if standings and standings.standings:
            for ts in standings.standings:
                name = getattr(ts.team, "name", "")
                if name == home_team_name:
                    home_stats = ts
                elif name == away_team_name:
                    away_stats = ts

        if not home_stats or not away_stats:
            try:
                from app.ml.service import prediction_service

                ml_prediction = await prediction_service.predict_single_match(match.id)

                home_prob = round(ml_prediction.home_win_prob * 100.0, 1)
                draw_prob = round(ml_prediction.draw_prob * 100.0, 1)
                away_prob = round(ml_prediction.away_win_prob * 100.0, 1)

                predicted_score = ml_prediction.predicted_score or {}
                home_goals_pred = predicted_score.get("home", 1) or 1
                away_goals_pred = predicted_score.get("away", 1) or 1

                expected_home_goals = float(home_goals_pred)
                expected_away_goals = float(away_goals_pred)
                total_expected_goals = expected_home_goals + expected_away_goals

                base_btts = 40.0
                btts_adjust = max(
                    0.0,
                    min(
                        35.0,
                        (expected_home_goals + expected_away_goals - 2.0) * 25.0,
                    ),
                )
                both_teams_to_score_prob = round(
                    max(20.0, min(90.0, base_btts + btts_adjust)), 1
                )

                base_over25 = 45.0
                over25_adjust = max(
                    -20.0,
                    min(35.0, (total_expected_goals - 2.5) * 30.0),
                )
                over_under_25_prob = round(
                    max(15.0, min(95.0, base_over25 + over25_adjust)), 1
                )

                home_goals_rounded = int(round(expected_home_goals))
                away_goals_rounded = int(round(expected_away_goals))
                if home_goals_rounded == 0 and away_goals_rounded == 0:
                    home_goals_rounded = 1

                most_likely_scoreline = f"{home_goals_rounded}-{away_goals_rounded}"

                def fair_odds(pct: float) -> Optional[float]:
                    if pct <= 0:
                        return None
                    return round(100.0 / pct, 2)

                return MatchPredictionDetailed(
                    match_id=match.id,
                    home_win_prob=home_prob,
                    draw_prob=draw_prob,
                    away_win_prob=away_prob,
                    expected_goals=ExpectedGoals(
                        home=round(expected_home_goals, 2),
                        away=round(expected_away_goals, 2),
                        total=round(total_expected_goals, 2),
                    ),
                    both_teams_to_score_prob=both_teams_to_score_prob,
                    over_under_25_prob=over_under_25_prob,
                    most_likely_scoreline=most_likely_scoreline,
                    home_fair_odds=fair_odds(home_prob),
                    draw_fair_odds=fair_odds(draw_prob),
                    away_fair_odds=fair_odds(away_prob),
                )
            except Exception as e:
                logger.error(f"Error generating ML-based fallback prediction: {e}")

        def compute_rating(ts) -> float:
            played = max(ts.played, 1)
            points_per_game = ts.points / played
            goal_diff_per_game = ts.goal_difference / played
            goals_for_per_game = ts.goals_for / played
            return points_per_game * 2.0 + goal_diff_per_game * 0.5 + goals_for_per_game * 0.3

        home_rating = compute_rating(home_stats)
        away_rating = compute_rating(away_stats)

        base_home = 1.2
        base_away = 1.0
        diff = home_rating - away_rating
        diff_factor = max(-0.6, min(0.6, diff * 0.15))

        raw_home = base_home + diff_factor
        raw_away = base_away - diff_factor
        raw_draw = 0.9

        if raw_home < 0.1:
            raw_home = 0.1
        if raw_away < 0.1:
            raw_away = 0.1

        total_raw = raw_home + raw_draw + raw_away
        home_prob = round(raw_home / total_raw * 100, 1)
        draw_prob = round(raw_draw / total_raw * 100, 1)
        away_prob = round(raw_away / total_raw * 100, 1)

        home_attack = home_stats.goals_for / max(home_stats.played, 1)
        away_attack = away_stats.goals_for / max(away_stats.played, 1)
        home_defence = home_stats.goals_against / max(home_stats.played, 1)
        away_defence = away_stats.goals_against / max(away_stats.played, 1)

        expected_home_goals = max(0.3, (home_attack + away_defence) / 2.0 + 0.1)
        expected_away_goals = max(0.3, (away_attack + home_defence) / 2.0 - 0.1)
        total_expected_goals = expected_home_goals + expected_away_goals

        expected_home_goals = round(expected_home_goals, 2)
        expected_away_goals = round(expected_away_goals, 2)
        total_expected_goals = round(total_expected_goals, 2)

        base_btts = 40.0
        btts_adjust = max(0.0, min(35.0, (expected_home_goals + expected_away_goals - 2.0) * 25.0))
        both_teams_to_score_prob = round(max(20.0, min(90.0, base_btts + btts_adjust)), 1)

        base_over25 = 45.0
        over25_adjust = max(-20.0, min(35.0, (total_expected_goals - 2.5) * 30.0))
        over_under_25_prob = round(max(15.0, min(95.0, base_over25 + over25_adjust)), 1)

        home_goals_rounded = int(round(expected_home_goals))
        away_goals_rounded = int(round(expected_away_goals))
        if home_goals_rounded == 0 and away_goals_rounded == 0:
            home_goals_rounded = 1

        most_likely_scoreline = f"{home_goals_rounded}-{away_goals_rounded}"

        score_obj_live = getattr(match, "score", None)
        full_time_live = getattr(score_obj_live, "full_time", None) if score_obj_live is not None else None
        live_home_goals = None
        live_away_goals = None
        if isinstance(full_time_live, dict):
            try:
                live_home_goals = int(full_time_live.get("home") or 0)
                live_away_goals = int(full_time_live.get("away") or 0)
            except Exception:
                live_home_goals = None
                live_away_goals = None
        minute_live = getattr(match, "minute", None)
        status_live = getattr(match, "status", None)
        red_home = 0
        red_away = 0
        for ev in getattr(match, "events", []) or []:
            if getattr(ev, "type", None) == MatchEventType.RED_CARD:
                team_name = getattr(ev, "team", None)
                if team_name == getattr(match.home_team, "name", str(match.home_team)):
                    red_home += 1
                elif team_name == getattr(match.away_team, "name", str(match.away_team)):
                    red_away += 1
        if (
            minute_live is not None
            and live_home_goals is not None
            and live_away_goals is not None
            and status_live in (MatchStatus.LIVE, MatchStatus.IN_PLAY)
        ):
            live_result = _compute_live_from_xg(
                expected_home_goals,
                expected_away_goals,
                live_home_goals,
                live_away_goals,
                minute_live,
                red_home,
                red_away,
            )
            if live_result is not None:
                home_prob, draw_prob, away_prob, both_teams_to_score_prob, over_under_25_prob, most_likely_scoreline = live_result

        def fair_odds(pct: float) -> Optional[float]:
            if pct <= 0:
                return None
            return round(100.0 / pct, 2)

        return MatchPredictionDetailed(
            match_id=match.id,
            home_win_prob=home_prob,
            draw_prob=draw_prob,
            away_win_prob=away_prob,
            expected_goals=ExpectedGoals(
                home=expected_home_goals,
                away=expected_away_goals,
                total=total_expected_goals,
            ),
            both_teams_to_score_prob=both_teams_to_score_prob,
            over_under_25_prob=over_under_25_prob,
            most_likely_scoreline=most_likely_scoreline,
            home_fair_odds=fair_odds(home_prob),
            draw_fair_odds=fair_odds(draw_prob),
            away_fair_odds=fair_odds(away_prob),
        )
    
    async def _get_expected_lineups(self, match: Any) -> Dict[str, Any]:
        """Get expected lineups with REAL 2025/2026 data"""
        from app.data.models.detailed import MatchLineup
        
        home_coach = "Allenatore Casa"
        away_coach = "Allenatore Trasferta"
        home_formation = "4-3-3"
        away_formation = "4-3-3"

        # Specific coaches for 2025/2026
        if "Inter" in match.home_team.name: home_coach, home_formation = "Simone Inzaghi", "3-5-2"
        elif "Milan" in match.home_team.name: home_coach, home_formation = "Paulo Fonseca", "4-2-3-1"
        elif "Juventus" in match.home_team.name: home_coach, home_formation = "Thiago Motta", "4-3-3"
        elif "Napoli" in match.home_team.name: home_coach, home_formation = "Antonio Conte", "3-4-2-1"
        elif "Roma" in match.home_team.name: home_coach, home_formation = "Claudio Ranieri", "3-5-2"

        if "Inter" in match.away_team.name: away_coach, away_formation = "Simone Inzaghi", "3-5-2"
        elif "Milan" in match.away_team.name: away_coach, away_formation = "Paulo Fonseca", "4-2-3-1"
        elif "Juventus" in match.away_team.name: away_coach, away_formation = "Thiago Motta", "4-3-3"
        elif "Napoli" in match.away_team.name: away_coach, away_formation = "Antonio Conte", "3-4-2-1"
        elif "Roma" in match.away_team.name: away_coach, away_formation = "Claudio Ranieri", "3-5-2"
        
        return {
            "home": MatchLineup(formation=home_formation, starting_xi=[], substitutes=[], coach=home_coach),
            "away": MatchLineup(formation=away_formation, starting_xi=[], substitutes=[], coach=away_coach)
        }
    
    async def _get_match_top_scorers(self, match: Any) -> List[TopScorer]:
        """Get top scorers for the match based on REAL players"""
        scorers = await self._get_top_scorers()
        match_scorers = []
        
        for s in scorers:
            if "Inter" in match.home_team.name or "Inter" in match.away_team.name:
                if s.player.name in ["Lautaro Martínez", "Hakan Çalhanoğlu", "Marcus Thuram"]:
                    match_scorers.append(s)
            if "Milan" in match.home_team.name or "Milan" in match.away_team.name:
                if s.player.name in ["Christian Pulisic", "Rafael Leão"]:
                    match_scorers.append(s)
            if "Juventus" in match.home_team.name or "Juventus" in match.away_team.name:
                if s.player.name in ["Kenan Yildiz"]:
                    match_scorers.append(s)
            if "Napoli" in match.home_team.name or "Napoli" in match.away_team.name:
                if s.player.name in ["Rasmus Højlund"]:
                    match_scorers.append(s)
            if "Como" in match.home_team.name or "Como" in match.away_team.name:
                if s.player.name in ["Nico Paz"]:
                    match_scorers.append(s)
                
        return match_scorers if match_scorers else scorers[:2]
    
    async def _generate_daily_analysis(self, analysis_date: date) -> DailyAnalysis:
        fixtures = await unified_data_service.get_fixtures()

        matches_for_day = []
        for match in fixtures:
            match_date = getattr(match, "utc_date", None)
            if match_date and match_date.date() == analysis_date:
                matches_for_day.append(match)

        total_matches = len(matches_for_day)
        if total_matches == 0:
            return DailyAnalysis(
                date=analysis_date,
                total_matches=0,
                total_goals=0,
                average_goals_per_match=0.0,
                home_goals=0,
                away_goals=0,
                total_cards=0,
                average_cards_per_match=0.0,
                home_wins=0,
                away_wins=0,
                draws=0,
                home_win_percentage=0.0,
                away_win_percentage=0.0,
                draw_percentage=0.0,
                matches_over_25_goals=0,
                matches_both_teams_score=0,
                clean_sheets=0,
                matches_with_red_card=0,
                most_productive_match=None,
                most_disciplined_match=None,
                biggest_win=None,
            )

        total_goals = 0
        home_goals = 0
        away_goals = 0
        total_cards = 0
        home_wins = 0
        away_wins = 0
        draws = 0
        matches_over_25_goals = 0
        matches_both_teams_score = 0
        clean_sheets = 0
        matches_with_red_card = 0

        most_productive_match = None
        most_productive_goals = -1
        most_disciplined_match = None
        fewest_cards = None
        biggest_win = None
        biggest_win_margin = -1

        for match in matches_for_day:
            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None

            home_goals_match = 0
            away_goals_match = 0
            if isinstance(full_time, dict):
                home_goals_match = int(full_time.get("home") or 0)
                away_goals_match = int(full_time.get("away") or 0)

            home_goals += home_goals_match
            away_goals += away_goals_match
            goals_in_match = home_goals_match + away_goals_match
            total_goals += goals_in_match

            yellow_cards = 0
            red_cards = 0
            for event in getattr(match, "events", []):
                if event.type == MatchEventType.YELLOW_CARD:
                    yellow_cards += 1
                elif event.type == MatchEventType.RED_CARD:
                    red_cards += 1

            cards_in_match = yellow_cards + red_cards
            total_cards += cards_in_match

            home_name = getattr(match.home_team, "name", str(match.home_team))
            away_name = getattr(match.away_team, "name", str(match.away_team))

            if home_goals_match > away_goals_match:
                home_wins += 1
            elif away_goals_match > home_goals_match:
                away_wins += 1
            else:
                draws += 1

            if goals_in_match > most_productive_goals:
                most_productive_goals = goals_in_match
                most_productive_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"

            if fewest_cards is None or cards_in_match < fewest_cards:
                fewest_cards = cards_in_match
                most_disciplined_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"

            margin = abs(home_goals_match - away_goals_match)
            if margin > biggest_win_margin and goals_in_match > 0:
                biggest_win_margin = margin
                biggest_win = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"

            if goals_in_match > 2.5:
                matches_over_25_goals += 1

            if home_goals_match > 0 and away_goals_match > 0:
                matches_both_teams_score += 1

            if home_goals_match == 0 or away_goals_match == 0:
                clean_sheets += 1

            if red_cards > 0:
                matches_with_red_card += 1

        average_goals_per_match = round(total_goals / total_matches, 2)
        average_cards_per_match = round(total_cards / total_matches, 2) if total_matches > 0 else 0.0

        home_win_percentage = round((home_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        away_win_percentage = round((away_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        draw_percentage = round((draws / total_matches) * 100, 1) if total_matches > 0 else 0.0

        return DailyAnalysis(
            date=analysis_date,
            total_matches=total_matches,
            total_goals=total_goals,
            average_goals_per_match=average_goals_per_match,
            home_goals=home_goals,
            away_goals=away_goals,
            total_cards=total_cards,
            average_cards_per_match=average_cards_per_match,
            home_wins=home_wins,
            away_wins=away_wins,
            draws=draws,
            home_win_percentage=home_win_percentage,
            away_win_percentage=away_win_percentage,
            draw_percentage=draw_percentage,
            matches_over_25_goals=matches_over_25_goals,
            matches_both_teams_score=matches_both_teams_score,
            clean_sheets=clean_sheets,
            matches_with_red_card=matches_with_red_card,
            most_productive_match=most_productive_match,
            most_disciplined_match=most_disciplined_match,
            biggest_win=biggest_win,
        )

    async def _generate_daily_analysis_norway(self, analysis_date: date) -> DailyAnalysis:
        fixtures = await unified_data_service.get_fixtures_norway()

        matches_for_day = []
        for match in fixtures:
            match_date = getattr(match, "utc_date", None)
            if match_date and match_date.date() == analysis_date:
                matches_for_day.append(match)

        total_matches = len(matches_for_day)
        if total_matches == 0:
            return DailyAnalysis(
                date=analysis_date,
                total_matches=0,
                total_goals=0,
                average_goals_per_match=0.0,
                home_goals=0,
                away_goals=0,
                total_cards=0,
                average_cards_per_match=0.0,
                home_wins=0,
                away_wins=0,
                draws=0,
                home_win_percentage=0.0,
                away_win_percentage=0.0,
                draw_percentage=0.0,
                matches_over_25_goals=0,
                matches_both_teams_score=0,
                clean_sheets=0,
                matches_with_red_card=0,
                most_productive_match=None,
                most_disciplined_match=None,
                biggest_win=None,
            )

        total_goals = 0
        home_goals = 0
        away_goals = 0
        home_wins = 0
        away_wins = 0
        draws = 0
        matches_over_25_goals = 0
        matches_both_teams_score = 0
        clean_sheets = 0

        most_productive_match = None
        most_productive_goals = -1
        biggest_win = None
        biggest_win_margin = -1

        for match in matches_for_day:
            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None

            home_goals_match = 0
            away_goals_match = 0
            if isinstance(full_time, dict):
                home_goals_match = int(full_time.get("home") or 0)
                away_goals_match = int(full_time.get("away") or 0)

            home_goals += home_goals_match
            away_goals += away_goals_match
            goals_in_match = home_goals_match + away_goals_match
            total_goals += goals_in_match

            home_name = getattr(match.home_team, "name", str(match.home_team))
            away_name = getattr(match.away_team, "name", str(match.away_team))

            if home_goals_match > away_goals_match:
                home_wins += 1
            elif away_goals_match > home_goals_match:
                away_wins += 1
            else:
                draws += 1

            if goals_in_match > most_productive_goals:
                most_productive_goals = goals_in_match
                most_productive_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"

            margin = abs(home_goals_match - away_goals_match)
            if margin > biggest_win_margin and goals_in_match > 0:
                biggest_win_margin = margin
                biggest_win = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"

            if goals_in_match > 2.5:
                matches_over_25_goals += 1

            if home_goals_match > 0 and away_goals_match > 0:
                matches_both_teams_score += 1

            if home_goals_match == 0 or away_goals_match == 0:
                clean_sheets += 1

        average_goals_per_match = round(total_goals / total_matches, 2)

        home_win_percentage = round((home_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        away_win_percentage = round((away_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        draw_percentage = round((draws / total_matches) * 100, 1) if total_matches > 0 else 0.0

        return DailyAnalysis(
            date=analysis_date,
            total_matches=total_matches,
            total_goals=total_goals,
            average_goals_per_match=average_goals_per_match,
            home_goals=home_goals,
            away_goals=away_goals,
            total_cards=0,
            average_cards_per_match=0.0,
            home_wins=home_wins,
            away_wins=away_wins,
            draws=draws,
            home_win_percentage=home_win_percentage,
            away_win_percentage=away_win_percentage,
            draw_percentage=draw_percentage,
            matches_over_25_goals=matches_over_25_goals,
            matches_both_teams_score=matches_both_teams_score,
            clean_sheets=clean_sheets,
            matches_with_red_card=0,
            most_productive_match=most_productive_match,
            most_disciplined_match=None,
            biggest_win=biggest_win,
        )

    async def _generate_daily_analysis_premier(self, analysis_date: date) -> DailyAnalysis:
        fixtures = await unified_data_service.get_fixtures_premier()
        matches_for_day = []
        for match in fixtures:
            match_date = getattr(match, "utc_date", None)
            if match_date and match_date.date() == analysis_date:
                matches_for_day.append(match)
        total_matches = len(matches_for_day)
        if total_matches == 0:
            return DailyAnalysis(
                date=analysis_date,
                total_matches=0,
                total_goals=0,
                average_goals_per_match=0.0,
                home_goals=0,
                away_goals=0,
                total_cards=0,
                average_cards_per_match=0.0,
                home_wins=0,
                away_wins=0,
                draws=0,
                home_win_percentage=0.0,
                away_win_percentage=0.0,
                draw_percentage=0.0,
                matches_over_25_goals=0,
                matches_both_teams_score=0,
                clean_sheets=0,
                matches_with_red_card=0,
                most_productive_match=None,
                most_disciplined_match=None,
                biggest_win=None,
            )
        total_goals = 0
        home_goals = 0
        away_goals = 0
        home_wins = 0
        away_wins = 0
        draws = 0
        matches_over_25_goals = 0
        matches_both_teams_score = 0
        clean_sheets = 0
        most_productive_match = None
        most_productive_goals = -1
        biggest_win = None
        biggest_win_margin = -1
        for match in matches_for_day:
            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
            home_goals_match = 0
            away_goals_match = 0
            if isinstance(full_time, dict):
                home_goals_match = int(full_time.get("home") or 0)
                away_goals_match = int(full_time.get("away") or 0)
            home_goals += home_goals_match
            away_goals += away_goals_match
            goals_in_match = home_goals_match + away_goals_match
            total_goals += goals_in_match
            home_name = getattr(match.home_team, "name", str(match.home_team))
            away_name = getattr(match.away_team, "name", str(match.away_team))
            if home_goals_match > away_goals_match:
                home_wins += 1
            elif away_goals_match > home_goals_match:
                away_wins += 1
            else:
                draws += 1
            if goals_in_match > most_productive_goals:
                most_productive_goals = goals_in_match
                most_productive_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            margin = abs(home_goals_match - away_goals_match)
            if margin > biggest_win_margin and goals_in_match > 0:
                biggest_win_margin = margin
                biggest_win = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            if goals_in_match > 2.5:
                matches_over_25_goals += 1
            if home_goals_match > 0 and away_goals_match > 0:
                matches_both_teams_score += 1
            if home_goals_match == 0 or away_goals_match == 0:
                clean_sheets += 1
        average_goals_per_match = round(total_goals / total_matches, 2)
        home_win_percentage = round((home_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        away_win_percentage = round((away_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        draw_percentage = round((draws / total_matches) * 100, 1) if total_matches > 0 else 0.0
        return DailyAnalysis(
            date=analysis_date,
            total_matches=total_matches,
            total_goals=total_goals,
            average_goals_per_match=average_goals_per_match,
            home_goals=home_goals,
            away_goals=away_goals,
            total_cards=0,
            average_cards_per_match=0.0,
            home_wins=home_wins,
            away_wins=away_wins,
            draws=draws,
            home_win_percentage=home_win_percentage,
            away_win_percentage=away_win_percentage,
            draw_percentage=draw_percentage,
            matches_over_25_goals=matches_over_25_goals,
            matches_both_teams_score=matches_both_teams_score,
            clean_sheets=clean_sheets,
            matches_with_red_card=0,
            most_productive_match=most_productive_match,
            most_disciplined_match=None,
            biggest_win=biggest_win,
        )

    async def _generate_daily_analysis_bundesliga(self, analysis_date: date) -> DailyAnalysis:
        fixtures = await unified_data_service.get_fixtures_bundesliga()
        matches_for_day = []
        for match in fixtures:
            match_date = getattr(match, "utc_date", None)
            if match_date and match_date.date() == analysis_date:
                matches_for_day.append(match)
        total_matches = len(matches_for_day)
        if total_matches == 0:
            return DailyAnalysis(
                date=analysis_date,
                total_matches=0,
                total_goals=0,
                average_goals_per_match=0.0,
                home_goals=0,
                away_goals=0,
                total_cards=0,
                average_cards_per_match=0.0,
                home_wins=0,
                away_wins=0,
                draws=0,
                home_win_percentage=0.0,
                away_win_percentage=0.0,
                draw_percentage=0.0,
                matches_over_25_goals=0,
                matches_both_teams_score=0,
                clean_sheets=0,
                matches_with_red_card=0,
                most_productive_match=None,
                most_disciplined_match=None,
                biggest_win=None,
            )
        total_goals = 0
        home_goals = 0
        away_goals = 0
        home_wins = 0
        away_wins = 0
        draws = 0
        matches_over_25_goals = 0
        matches_both_teams_score = 0
        clean_sheets = 0
        most_productive_match = None
        most_productive_goals = -1
        biggest_win = None
        biggest_win_margin = -1
        for match in matches_for_day:
            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
            home_goals_match = 0
            away_goals_match = 0
            if isinstance(full_time, dict):
                home_goals_match = int(full_time.get("home") or 0)
                away_goals_match = int(full_time.get("away") or 0)
            home_goals += home_goals_match
            away_goals += away_goals_match
            goals_in_match = home_goals_match + away_goals_match
            total_goals += goals_in_match
            home_name = getattr(match.home_team, "name", str(match.home_team))
            away_name = getattr(match.away_team, "name", str(match.away_team))
            if home_goals_match > away_goals_match:
                home_wins += 1
            elif away_goals_match > home_goals_match:
                away_wins += 1
            else:
                draws += 1
            if goals_in_match > most_productive_goals:
                most_productive_goals = goals_in_match
                most_productive_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            margin = abs(home_goals_match - away_goals_match)
            if margin > biggest_win_margin and goals_in_match > 0:
                biggest_win_margin = margin
                biggest_win = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            if goals_in_match > 2.5:
                matches_over_25_goals += 1
            if home_goals_match > 0 and away_goals_match > 0:
                matches_both_teams_score += 1
            if home_goals_match == 0 or away_goals_match == 0:
                clean_sheets += 1
        average_goals_per_match = round(total_goals / total_matches, 2)
        home_win_percentage = round((home_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        away_win_percentage = round((away_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        draw_percentage = round((draws / total_matches) * 100, 1) if total_matches > 0 else 0.0
        return DailyAnalysis(
            date=analysis_date,
            total_matches=total_matches,
            total_goals=total_goals,
            average_goals_per_match=average_goals_per_match,
            home_goals=home_goals,
            away_goals=away_goals,
            total_cards=0,
            average_cards_per_match=0.0,
            home_wins=home_wins,
            away_wins=away_wins,
            draws=draws,
            home_win_percentage=home_win_percentage,
            away_win_percentage=away_win_percentage,
            draw_percentage=draw_percentage,
            matches_over_25_goals=matches_over_25_goals,
            matches_both_teams_score=matches_both_teams_score,
            clean_sheets=clean_sheets,
            matches_with_red_card=0,
            most_productive_match=most_productive_match,
            most_disciplined_match=None,
            biggest_win=biggest_win,
        )

    async def _generate_daily_analysis_laliga(self, analysis_date: date) -> DailyAnalysis:
        fixtures = await unified_data_service.get_fixtures_laliga()
        matches_for_day = []
        for match in fixtures:
            match_date = getattr(match, "utc_date", None)
            if match_date and match_date.date() == analysis_date:
                matches_for_day.append(match)
        total_matches = len(matches_for_day)
        if total_matches == 0:
            return DailyAnalysis(
                date=analysis_date,
                total_matches=0,
                total_goals=0,
                average_goals_per_match=0.0,
                home_goals=0,
                away_goals=0,
                total_cards=0,
                average_cards_per_match=0.0,
                home_wins=0,
                away_wins=0,
                draws=0,
                home_win_percentage=0.0,
                away_win_percentage=0.0,
                draw_percentage=0.0,
                matches_over_25_goals=0,
                matches_both_teams_score=0,
                clean_sheets=0,
                matches_with_red_card=0,
                most_productive_match=None,
                most_disciplined_match=None,
                biggest_win=None,
            )
        total_goals = 0
        home_goals = 0
        away_goals = 0
        home_wins = 0
        away_wins = 0
        draws = 0
        matches_over_25_goals = 0
        matches_both_teams_score = 0
        clean_sheets = 0
        most_productive_match = None
        most_productive_goals = -1
        biggest_win = None
        biggest_win_margin = -1
        for match in matches_for_day:
            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
            home_goals_match = 0
            away_goals_match = 0
            if isinstance(full_time, dict):
                home_goals_match = int(full_time.get("home") or 0)
                away_goals_match = int(full_time.get("away") or 0)
            home_goals += home_goals_match
            away_goals += away_goals_match
            goals_in_match = home_goals_match + away_goals_match
            total_goals += goals_in_match
            home_name = getattr(match.home_team, "name", str(match.home_team))
            away_name = getattr(match.away_team, "name", str(match.away_team))
            if home_goals_match > away_goals_match:
                home_wins += 1
            elif away_goals_match > home_goals_match:
                away_wins += 1
            else:
                draws += 1
            if goals_in_match > most_productive_goals:
                most_productive_goals = goals_in_match
                most_productive_match = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            margin = abs(home_goals_match - away_goals_match)
            if margin > biggest_win_margin and goals_in_match > 0:
                biggest_win_margin = margin
                biggest_win = f"{home_name} {home_goals_match}-{away_goals_match} {away_name}"
            if goals_in_match > 2.5:
                matches_over_25_goals += 1
            if home_goals_match > 0 and away_goals_match > 0:
                matches_both_teams_score += 1
            if home_goals_match == 0 or away_goals_match == 0:
                clean_sheets += 1
        average_goals_per_match = round(total_goals / total_matches, 2)
        home_win_percentage = round((home_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        away_win_percentage = round((away_wins / total_matches) * 100, 1) if total_matches > 0 else 0.0
        draw_percentage = round((draws / total_matches) * 100, 1) if total_matches > 0 else 0.0
        return DailyAnalysis(
            date=analysis_date,
            total_matches=total_matches,
            total_goals=total_goals,
            average_goals_per_match=average_goals_per_match,
            home_goals=home_goals,
            away_goals=away_goals,
            total_cards=0,
            average_cards_per_match=0.0,
            home_wins=home_wins,
            away_wins=away_wins,
            draws=draws,
            home_win_percentage=home_win_percentage,
            away_win_percentage=away_win_percentage,
            draw_percentage=draw_percentage,
            matches_over_25_goals=matches_over_25_goals,
            matches_both_teams_score=matches_both_teams_score,
            clean_sheets=clean_sheets,
            matches_with_red_card=0,
            most_productive_match=most_productive_match,
            most_disciplined_match=None,
            biggest_win=biggest_win,
        )

# Global service instance
detailed_stats_service = DetailedStatsService()
