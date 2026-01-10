from datetime import date
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from app.data.services.detailed_stats_service import detailed_stats_service
from app.data.models.detailed import (
    SerieAStats, LiveMatchCard, DailyAnalysis, PlayerBioRhythm
)
from app.services.biorhythm_service import bio_rhythm_service
from app.data.services.unified_data_service import unified_data_service
from app.data.models.common import MatchStatus, Score, MatchEventType
from app.core.dependencies import verify_api_key
from app.monitoring.metrics import monitor_api_call

router = APIRouter()

@router.get("/stats/seriea", response_model=SerieAStats)
async def get_seriea_detailed_stats(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "seriea_detailed_stats", "request")
        stats = await detailed_stats_service.get_seriea_stats()
        monitor_api_call("api", "seriea_detailed_stats", "success")
        return stats
    except Exception as e:
        monitor_api_call("api", "seriea_detailed_stats", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats/norway", response_model=SerieAStats)
async def get_norway_detailed_stats(api_key: str = Depends(verify_api_key)):
    try:
        monitor_api_call("api", "norway_detailed_stats", "request")
        stats = await detailed_stats_service.get_norway_stats()
        monitor_api_call("api", "norway_detailed_stats", "success")
        return stats
    except Exception as e:
        monitor_api_call("api", "norway_detailed_stats", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/matches/next/predictions")
async def get_next_matchday_predictions(
    matchday: int = Query(20, description="Giornata per le previsioni (default: prossima giornata nota)"),
    api_key: str = Depends(verify_api_key)
):
    try:
        monitor_api_call("api", "next_matchday_predictions", "request")
        
        fixtures = await unified_data_service.get_fixtures(matchday)
        upcoming = [
            match for match in fixtures
            if getattr(match, "status", None) in (MatchStatus.SCHEDULED, MatchStatus.LIVE, MatchStatus.IN_PLAY)
        ]
        
        result = []
        for match in upcoming:
            context = await detailed_stats_service.get_prediction_context(match)
            prediction = context["prediction"]
            bio_rhythm_analysis = context["bio_rhythm_analysis"]
            expected_lineups = context["expected_lineups"]

            result.append(
                {
                    "match_id": match.id,
                    "home_team": getattr(match.home_team, "name", str(match.home_team)),
                    "away_team": getattr(match.away_team, "name", str(match.away_team)),
                    "matchday": match.matchday,
                    "kickoff": match.utc_date.isoformat(),
                    "prediction": prediction,
                    "bio_rhythm_analysis": bio_rhythm_analysis,
                    "expected_lineups": expected_lineups,
                }
            )
        
        result = sorted(
            result,
            key=lambda m: (m["matchday"], m["kickoff"]),
        )
        
        monitor_api_call("api", "next_matchday_predictions", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "next_matchday_predictions", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/matches/next/predictions/norway")
async def get_next_matchday_predictions_norway(
    matchday: Optional[int] = Query(None, description="Giornata per le previsioni Norvegia (default: prossima giornata nota)"),
    api_key: str = Depends(verify_api_key)
):
    try:
        monitor_api_call("api", "next_matchday_predictions_norway", "request")
        
        fixtures = await unified_data_service.get_fixtures_norway(matchday)
        upcoming = [
            match for match in fixtures
            if getattr(match, "status", None) in (MatchStatus.SCHEDULED, MatchStatus.LIVE, MatchStatus.IN_PLAY)
        ]
        
        result = []
        for match in upcoming:
            context = await detailed_stats_service.get_prediction_context(match)
            prediction = context["prediction"]
            bio_rhythm_analysis = context["bio_rhythm_analysis"]
            expected_lineups = context["expected_lineups"]

            result.append(
                {
                    "match_id": match.id,
                    "home_team": getattr(match.home_team, "name", str(match.home_team)),
                    "away_team": getattr(match.away_team, "name", str(match.away_team)),
                    "matchday": getattr(match, "matchday", None),
                    "kickoff": match.utc_date.isoformat() if getattr(match, "utc_date", None) else None,
                    "prediction": prediction,
                    "bio_rhythm_analysis": bio_rhythm_analysis,
                    "expected_lineups": expected_lineups,
                }
            )
        
        result = sorted(
            result,
            key=lambda m: (m["matchday"] or 0, m["kickoff"] or ""),
        )
        
        monitor_api_call("api", "next_matchday_predictions_norway", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "next_matchday_predictions_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/matches/live/cards", response_model=List[LiveMatchCard])
async def get_live_match_cards(api_key: str = Depends(verify_api_key)):
    """
    Get live match cards with detailed analysis including:
    - Pronostici dettagliati
    - Goal attesi
    - Formazioni probabili
    - Top marcatori della partita
    - Bioritmo calciatori
    - Cartellini e statistiche
    """
    try:
        monitor_api_call("api", "live_match_cards", "request")
        match_cards = await detailed_stats_service.get_live_match_cards()
        monitor_api_call("api", "live_match_cards", "success")
        return match_cards
    except Exception as e:
        monitor_api_call("api", "live_match_cards", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/matches/live/cards/norway", response_model=List[LiveMatchCard])
async def get_live_match_cards_norway(api_key: str = Depends(verify_api_key)):
    """
    Get live match cards for Norway Eliteserien with detailed analysis.
    """
    try:
        monitor_api_call("api", "live_match_cards_norway", "request")
        match_cards = await detailed_stats_service.get_live_match_cards_norway()
        monitor_api_call("api", "live_match_cards_norway", "success")
        return match_cards
    except Exception as e:
        monitor_api_call("api", "live_match_cards_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/daily", response_model=DailyAnalysis)
async def get_daily_analysis(
    analysis_date: Optional[date] = Query(None, description="Data per l'analisi (default: oggi)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get daily statistical analysis for Serie A matches:
    - Partite totali
    - Goal totali e media
    - Cartellini totali
    - Vittorie casa/trasferta/pareggi
    - Partita più produttiva
    - Partita più disciplinata
    """
    try:
        monitor_api_call("api", "daily_analysis", "request")
        
        if analysis_date is None:
            analysis_date = date.today()
            
        analysis = await detailed_stats_service.get_daily_analysis(analysis_date)
        monitor_api_call("api", "daily_analysis", "success")
        return analysis
    except Exception as e:
        monitor_api_call("api", "daily_analysis", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analysis/norway/daily", response_model=DailyAnalysis)
async def get_daily_analysis_norway(
    analysis_date: Optional[date] = Query(None, description="Data per l'analisi (default: oggi)"),
    api_key: str = Depends(verify_api_key)
):
    try:
        monitor_api_call("api", "daily_analysis_norway", "request")
        
        if analysis_date is None:
            analysis_date = date.today()
            
        analysis = await detailed_stats_service._generate_daily_analysis_norway(analysis_date)
        monitor_api_call("api", "daily_analysis_norway", "success")
        return analysis
    except Exception as e:
        monitor_api_call("api", "daily_analysis_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bio-rhythm/player")
async def calculate_player_bio_rhythm(
    birth_date: date = Query(..., description="Data di nascita del calciatore (YYYY-MM-DD)"),
    target_date: Optional[date] = Query(None, description="Data target per il calcolo (default: oggi)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Calculate bio-rhythm for a specific player
    Restituisce:
    - Bioritmo fisico, emotivo, intellettuale
    - Punteggio complessivo
    - Interpretazione umana
    """
    try:
        monitor_api_call("api", "player_bio_rhythm", "request")
        
        if target_date is None:
            target_date = date.today()
            
        bio_rhythm = await detailed_stats_service.calculate_bio_rhythm(birth_date, target_date)
        interpretation = bio_rhythm_service.get_rhythm_interpretation(bio_rhythm)
        
        result = {
            "bio_rhythm": bio_rhythm,
            "interpretation": interpretation,
            "calculation_date": target_date,
            "birth_date": birth_date
        }
        
        monitor_api_call("api", "player_bio_rhythm", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "player_bio_rhythm", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bio-rhythm/team")
async def calculate_team_bio_rhythm(
    players: List[str] = Query(..., description="Lista di giocatori nel formato 'nome:data_nascita' (es: 'Lautaro Martínez:1997-08-22')"),
    target_date: Optional[date] = Query(None, description="Data target per il calcolo (default: oggi)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Calculate bio-rhythm for an entire team
    Restituisce:
    - Giocatori ordinati per bioritmo
    - Medie della squadra
    - Top performers
    - Condizione complessiva
    """
    try:
        monitor_api_call("api", "team_bio_rhythm", "request")
        
        if target_date is None:
            target_date = date.today()
        
        # Parse players
        parsed_players = []
        for player_str in players:
            if ":" in player_str:
                name, dob_str = player_str.split(":", 1)
                try:
                    dob = date.fromisoformat(dob_str)
                    parsed_players.append({
                        "name": name.strip(),
                        "date_of_birth": dob
                    })
                except ValueError:
                    continue
        
        if not parsed_players:
            raise HTTPException(status_code=400, detail="Formato giocatori non valido")
        
        # Get optimal players
        optimal_players = bio_rhythm_service.get_optimal_players(parsed_players, target_date)
        team_summary = bio_rhythm_service.get_team_bio_rhythm_summary(parsed_players, target_date)
        
        result = {
            "team_summary": team_summary,
            "optimal_players": optimal_players,
            "calculation_date": target_date,
            "total_players_analyzed": len(optimal_players)
        }
        
        monitor_api_call("api", "team_bio_rhythm", "success")
        return result
    except HTTPException:
        raise
    except Exception as e:
        monitor_api_call("api", "team_bio_rhythm", "error")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/matches/finished")
async def get_finished_matches(
    matchday: Optional[int] = Query(None, description="Giornata specifica (default: ultima giocata)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get finished matches with detailed results and statistics
    """
    try:
        monitor_api_call("api", "finished_matches", "request")
        fixtures = await unified_data_service.get_fixtures(matchday)
        finished = [
            match for match in fixtures
            if getattr(match, "status", None) == MatchStatus.FINISHED
        ]

        result = []
        for match in finished:
            events = getattr(match, "events", []) or []
            goals = []
            cards = []

            for event in events:
                event_type = getattr(event, "type", None)
                if event_type == MatchEventType.GOAL:
                    minute = getattr(event, "minute", None)
                    extra_time = getattr(event, "extra_time", None)
                    minute_display = f"{minute}+{extra_time}" if extra_time not in (None, 0) else minute
                    goals.append(
                        {
                            "player": getattr(event, "player", None),
                            "minute": minute_display,
                            "team": getattr(event, "team", None),
                        }
                    )
                elif event_type in (MatchEventType.YELLOW_CARD, MatchEventType.RED_CARD):
                    minute = getattr(event, "minute", None)
                    extra_time = getattr(event, "extra_time", None)
                    minute_display = f"{minute}+{extra_time}" if extra_time not in (None, 0) else minute
                    cards.append(
                        {
                            "player": getattr(event, "player", None),
                            "minute": minute_display,
                            "team": getattr(event, "team", None),
                            "card": "yellow" if event_type == MatchEventType.YELLOW_CARD else "red",
                        }
                    )

            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if isinstance(score_obj, Score) else None
            score_str = None
            if isinstance(full_time, dict):
                home_goals = full_time.get("home")
                away_goals = full_time.get("away")
                if home_goals is not None and away_goals is not None:
                    score_str = f"{home_goals}-{away_goals}"

            result.append(
                {
                    "match_id": match.id,
                    "home_team": getattr(match.home_team, "name", str(match.home_team)),
                    "away_team": getattr(match.away_team, "name", str(match.away_team)),
                    "score": score_str,
                    "date": match.utc_date.date().isoformat(),
                    "matchday": match.matchday,
                    "goals": goals,
                    "cards": cards,
                }
            )

        result = sorted(
            result,
            key=lambda m: (m["date"], m["match_id"]),
            reverse=True,
        )

        monitor_api_call("api", "finished_matches", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "finished_matches", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/matches/finished/norway")
async def get_finished_matches_norway(
    matchday: Optional[int] = Query(None, description="Giornata specifica (default: ultima giocata)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get finished matches for Norway Eliteserien with basic results.
    """
    try:
        monitor_api_call("api", "finished_matches_norway", "request")
        fixtures = await unified_data_service.get_fixtures_norway(matchday)
        finished = [
            match for match in fixtures
            if getattr(match, "status", None) == MatchStatus.FINISHED
        ]

        result = []
        for match in finished:
            events = getattr(match, "events", []) or []
            goals = []
            cards = []

            for event in events:
                event_type = getattr(event, "type", None)
                if event_type == MatchEventType.GOAL:
                    minute = getattr(event, "minute", None)
                    extra_time = getattr(event, "extra_time", None)
                    minute_display = f"{minute}+{extra_time}" if extra_time not in (None, 0) else minute
                    goals.append(
                        {
                            "player": getattr(event, "player", None),
                            "minute": minute_display,
                            "team": getattr(event, "team", None),
                        }
                    )
                elif event_type in (MatchEventType.YELLOW_CARD, MatchEventType.RED_CARD):
                    minute = getattr(event, "minute", None)
                    extra_time = getattr(event, "extra_time", None)
                    minute_display = f"{minute}+{extra_time}" if extra_time not in (None, 0) else minute
                    cards.append(
                        {
                            "player": getattr(event, "player", None),
                            "minute": minute_display,
                            "team": getattr(event, "team", None),
                            "card": "yellow" if event_type == MatchEventType.YELLOW_CARD else "red",
                        }
                    )

            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if isinstance(score_obj, Score) else None
            score_str = None
            if isinstance(full_time, dict):
                home_goals = full_time.get("home")
                away_goals = full_time.get("away")
                if home_goals is not None and away_goals is not None:
                    score_str = f"{home_goals}-{away_goals}"

            result.append(
                {
                    "match_id": match.id,
                    "home_team": getattr(match.home_team, "name", str(match.home_team)),
                    "away_team": getattr(match.away_team, "name", str(match.away_team)),
                    "score": score_str,
                    "date": match.utc_date.date().isoformat(),
                    "matchday": match.matchday,
                    "goals": goals,
                    "cards": cards,
                }
            )

        result = sorted(
            result,
            key=lambda m: (m["date"], m["match_id"]),
            reverse=True,
        )

        monitor_api_call("api", "finished_matches_norway", "success")
        return result
    except Exception as e:
        monitor_api_call("api", "finished_matches_norway", "error")
        raise HTTPException(status_code=500, detail=str(e))
