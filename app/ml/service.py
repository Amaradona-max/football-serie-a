from typing import List, Optional
from datetime import datetime, date
import math
import logging

from app.data.services.unified_data_service import unified_data_service
from app.data.models.common import Team, Standings, PredictionInput, PredictionOutput, MatchStatus
from app.ml.models import prediction_model

logger = logging.getLogger(__name__)

class PredictionService:
    def __init__(self):
        self.model = prediction_model
    
    async def predict_next_matchday(self) -> List[PredictionOutput]:
        """
        Predict outcomes for all matches in the next matchday.
        
        Returns:
            List of predictions for each match
        """
        try:
            training_stats_2025 = await self.train_on_2025_history()
            fixtures = await unified_data_service.get_fixtures()
            standings = await unified_data_service.get_standings()
            
            upcoming_matches = []
            for match in fixtures:
                kickoff = getattr(match, "utc_date", None)
                if kickoff and kickoff >= datetime.now() and (kickoff - datetime.now()).days <= 7:
                    upcoming_matches.append(match)
            
            predictions = []
            for match in upcoming_matches:
                # Prepare prediction input
                prediction_input = await self._prepare_prediction_input(match, standings)
                
                # Generate prediction
                prediction = self.model.predict(prediction_input)
                predictions.append(prediction)
            
            logger.info(
                f"Generated {len(predictions)} predictions for next matchday "
                f"(train_accuracy={training_stats_2025.get('training_accuracy', 0.0):.3f}, "
                f"matches_training={training_stats_2025.get('training_matches', 0)}, "
                f"matches_2025={training_stats_2025.get('matches_2025', 0)}, "
                f"matches_2026={training_stats_2025.get('matches_2026', 0)})"
            )
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting next matchday: {e}")
            raise
    
    async def predict_next_matchday_norway(self) -> List[dict]:
        try:
            training_stats_2025 = await self.train_on_2025_history()
            fixtures = await unified_data_service.get_fixtures_norway()
            standings = await unified_data_service.get_standings_norway()
            
            if not fixtures or not standings:
                return []
            
            upcoming_matches = []
            for match in fixtures:
                kickoff = getattr(match, "utc_date", None)
                if kickoff and kickoff >= datetime.now() and (kickoff - datetime.now()).days <= 7:
                    upcoming_matches.append(match)
            
            results = []
            for match in upcoming_matches:
                prediction_input = await self._prepare_prediction_input(match, standings)
                prediction = self.model.predict(prediction_input)
                
                home_team_name = getattr(match.home_team, "name", str(match.home_team))
                away_team_name = getattr(match.away_team, "name", str(match.away_team))
                kickoff = getattr(match, "utc_date", None)
                
                results.append(
                    {
                        "match_id": match.id,
                        "home_team": home_team_name,
                        "away_team": away_team_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff.isoformat() if kickoff else None,
                        "prediction": prediction,
                    }
                )
            
            logger.info(f"Generated {len(results)} Norway predictions for next matchday")
            return results
        except Exception as e:
            logger.error(f"Error predicting next matchday for Norway: {e}")
            raise
    
    async def predict_next_matchday_premier(self) -> List[dict]:
        try:
            training_stats_2025 = await self.train_on_2025_history()
            fixtures = await unified_data_service.get_fixtures_premier()
            standings = await unified_data_service.get_standings_premier()
            
            if not fixtures or not standings:
                return []
            
            upcoming_matches = []
            for match in fixtures:
                kickoff = getattr(match, "utc_date", None)
                if kickoff and kickoff >= datetime.now() and (kickoff - datetime.now()).days <= 7:
                    upcoming_matches.append(match)
            
            results = []
            for match in upcoming_matches:
                prediction_input = await self._prepare_prediction_input(match, standings)
                prediction = self.model.predict(prediction_input)
                
                home_team_name = getattr(match.home_team, "name", str(match.home_team))
                away_team_name = getattr(match.away_team, "name", str(match.away_team))
                kickoff = getattr(match, "utc_date", None)
                
                results.append(
                    {
                        "match_id": match.id,
                        "home_team": home_team_name,
                        "away_team": away_team_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff.isoformat() if kickoff else None,
                        "prediction": prediction,
                    }
                )
            
            logger.info(f"Generated {len(results)} Premier League predictions for next matchday")
            return results
        except Exception as e:
            logger.error(f"Error predicting next matchday for Premier League: {e}")
            raise
    
    async def predict_next_matchday_bundesliga(self) -> List[dict]:
        try:
            training_stats_2025 = await self.train_on_2025_history()
            fixtures = await unified_data_service.get_fixtures_bundesliga()
            standings = await unified_data_service.get_standings_bundesliga()
            
            if not fixtures or not standings:
                return []
            
            upcoming_matches = []
            for match in fixtures:
                kickoff = getattr(match, "utc_date", None)
                if kickoff and kickoff >= datetime.now() and (kickoff - datetime.now()).days <= 7:
                    upcoming_matches.append(match)
            
            results = []
            for match in upcoming_matches:
                prediction_input = await self._prepare_prediction_input(match, standings)
                prediction = self.model.predict(prediction_input)
                
                home_team_name = getattr(match.home_team, "name", str(match.home_team))
                away_team_name = getattr(match.away_team, "name", str(match.away_team))
                kickoff = getattr(match, "utc_date", None)
                
                results.append(
                    {
                        "match_id": match.id,
                        "home_team": home_team_name,
                        "away_team": away_team_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff.isoformat() if kickoff else None,
                        "prediction": prediction,
                    }
                )
            
            logger.info(f"Generated {len(results)} Bundesliga predictions for next matchday")
            return results
        except Exception as e:
            logger.error(f"Error predicting next matchday for Bundesliga: {e}")
            raise
    
    async def predict_next_matchday_laliga(self) -> List[dict]:
        try:
            training_stats_2025 = await self.train_on_2025_history()
            fixtures = await unified_data_service.get_fixtures_laliga()
            standings = await unified_data_service.get_standings_laliga()
            
            if not fixtures or not standings:
                return []
            
            upcoming_matches = []
            for match in fixtures:
                kickoff = getattr(match, "utc_date", None)
                if kickoff and kickoff >= datetime.now() and (kickoff - datetime.now()).days <= 7:
                    upcoming_matches.append(match)
            
            results = []
            for match in upcoming_matches:
                prediction_input = await self._prepare_prediction_input(match, standings)
                prediction = self.model.predict(prediction_input)
                
                home_team_name = getattr(match.home_team, "name", str(match.home_team))
                away_team_name = getattr(match.away_team, "name", str(match.away_team))
                kickoff = getattr(match, "utc_date", None)
                
                results.append(
                    {
                        "match_id": match.id,
                        "home_team": home_team_name,
                        "away_team": away_team_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff.isoformat() if kickoff else None,
                        "prediction": prediction,
                    }
                )
            
            logger.info(f"Generated {len(results)} La Liga predictions for next matchday")
            return results
        except Exception as e:
            logger.error(f"Error predicting next matchday for La Liga: {e}")
            raise
    
    async def predict_single_match(self, match_id: int) -> PredictionOutput:
        """
        Predict outcome for a specific match.
        
        Args:
            match_id: ID of the match to predict
            
        Returns:
            Prediction for the specified match
        """
        try:
            # Get match details
            match = await unified_data_service.get_match_by_id(match_id)
            
            if not match:
                raise ValueError(f"Match with ID {match_id} not found")
            
            # Get current standings
            standings = await unified_data_service.get_standings()
            
            # Prepare prediction input
            prediction_input = await self._prepare_prediction_input(match, standings)
            
            # Generate prediction
            prediction = self.model.predict(prediction_input)
            
            logger.info(f"Generated prediction for match {match_id}")
            return prediction
            
        except Exception as e:
            logger.error(f"Error predicting match {match_id}: {e}")
            raise
    
    async def _prepare_prediction_input(self, match, standings: Standings) -> PredictionInput:
        """
        Prepare prediction input from match and standings data.
        
        Args:
            match: Match data
            standings: Current league standings
            
        Returns:
            Prepared prediction input
        """
        home_team_name = getattr(match.home_team, "name", str(match.home_team))
        away_team_name = getattr(match.away_team, "name", str(match.away_team))
        
        home_team_info = self._get_team_info(home_team_name, standings)
        away_team_info = self._get_team_info(away_team_name, standings)
        
        home_form = home_team_info.get('form') or self._get_team_form(home_team_name)
        away_form = away_team_info.get('form') or self._get_team_form(away_team_name)
        
        previous_meetings = await self._get_previous_meetings(
            home_team_name, away_team_name
        )
        
        return PredictionInput(
            home_team_id=home_team_info.get('id', 0),
            away_team_id=away_team_info.get('id', 0),
            home_form=home_form,
            away_form=away_form,
            home_position=home_team_info.get('position', 20),
            away_position=away_team_info.get('position', 20),
            is_home_advantage=True,
            previous_meetings=previous_meetings,
            home_goals_for=home_team_info.get('goals_for'),
            home_goals_against=home_team_info.get('goals_against'),
            away_goals_for=away_team_info.get('goals_for'),
            away_goals_against=away_team_info.get('goals_against'),
            home_played=home_team_info.get('played'),
            away_played=away_team_info.get('played'),
        )
    
    def _get_team_info(self, team_name: str, standings: Standings) -> dict:
        """Get team information from standings"""
        for team_standing in standings.standings:
            if team_standing.team.name.lower() == team_name.lower():
                return {
                    'played': team_standing.played,
                    'position': team_standing.position,
                    'points': team_standing.points,
                    'form': team_standing.form,
                    'goals_for': team_standing.goals_for,
                    'goals_against': team_standing.goals_against
                }
        
        # Return default values if team not found
        return {
            'position': 20,  # Bottom of table
            'points': 0,
            'form': None,
            'goals_for': 0,
            'goals_against': 0
        }
    
    def _get_team_form(self, team_name: str) -> Optional[str]:
        """Get team's recent form if available from real data"""
        return None
    
    async def _get_previous_meetings(self, home_team: str, away_team: str) -> List[dict]:
        """Get previous meetings between two teams"""
        return []
    
    async def train_model(self, historical_data: List[dict]) -> float:
        """
        Train the prediction model on historical data.
        
        Args:
            historical_data: List of historical match data with outcomes
            
        Returns:
            Training accuracy
        """
        try:
            accuracy = self.model.train(historical_data)
            logger.info(f"Model trained with accuracy: {accuracy:.3f}")
            return accuracy
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise

    async def _build_2024_2025_training_data(self, league: Optional[str] = None) -> List[dict]:
        fixtures_serie_a = None
        standings_serie_a = None
        fixtures_norway = None
        standings_norway = None
        fixtures_premier = None
        standings_premier = None
        fixtures_bundesliga = None
        standings_bundesliga = None
        fixtures_laliga = None
        standings_laliga = None

        if league in (None, "seriea"):
            fixtures_serie_a = await unified_data_service.get_fixtures(None)
            standings_serie_a = await unified_data_service.get_standings()

        if league in (None, "norway"):
            fixtures_norway = await unified_data_service.get_fixtures_norway(None)
            standings_norway = await unified_data_service.get_standings_norway()

        if league in (None, "premier"):
            fixtures_premier = await unified_data_service.get_fixtures_premier(None)
            standings_premier = await unified_data_service.get_standings_premier()

        if league in (None, "bundesliga"):
            fixtures_bundesliga = await unified_data_service.get_fixtures_bundesliga(None)
            standings_bundesliga = await unified_data_service.get_standings_bundesliga()

        if league in (None, "laliga"):
            fixtures_laliga = await unified_data_service.get_fixtures_laliga(None)
            standings_laliga = await unified_data_service.get_standings_laliga()

        start_date = date(2024, 1, 1)
        end_date = date(2025, 12, 31)

        training_data: List[dict] = []

        async def add_matches(matches: List, standings: Standings, league_name: str) -> None:
            if not matches or not standings:
                return

            for match in matches:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue

                kickoff = getattr(match, "utc_date", None)
                if not isinstance(kickoff, datetime):
                    continue

                match_date = kickoff.date()
                if match_date < start_date or match_date > end_date:
                    continue

                prediction_input = await self._prepare_prediction_input(match, standings)

                score_obj = getattr(match, "score", None)
                full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
                if not isinstance(full_time, dict):
                    continue

                home_goals = int(full_time.get("home") or 0)
                away_goals = int(full_time.get("away") or 0)

                if home_goals > away_goals:
                    actual = "H"
                elif away_goals > home_goals:
                    actual = "A"
                else:
                    actual = "D"

                training_data.append(
                    {
                        "prediction_input": prediction_input,
                        "actual_outcome": actual,
                        "league": league_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff,
                    }
                )

        await add_matches(fixtures_serie_a or [], standings_serie_a, "seriea")
        await add_matches(fixtures_norway or [], standings_norway, "norway")
        await add_matches(fixtures_premier or [], standings_premier, "premier")
        await add_matches(fixtures_bundesliga or [], standings_bundesliga, "bundesliga")
        await add_matches(fixtures_laliga or [], standings_laliga, "laliga")

        return training_data

    async def _build_2026_training_data(self) -> List[dict]:
        fixtures_serie_a = await unified_data_service.get_fixtures(None)
        standings_serie_a = await unified_data_service.get_standings()
        fixtures_norway = await unified_data_service.get_fixtures_norway(None)
        standings_norway = await unified_data_service.get_standings_norway()
        fixtures_premier = await unified_data_service.get_fixtures_premier(None)
        standings_premier = await unified_data_service.get_standings_premier()
        fixtures_bundesliga = await unified_data_service.get_fixtures_bundesliga(None)
        standings_bundesliga = await unified_data_service.get_standings_bundesliga()
        fixtures_laliga = await unified_data_service.get_fixtures_laliga(None)
        standings_laliga = await unified_data_service.get_standings_laliga()

        start_date = date(2026, 1, 1)
        end_date = date.today()

        training_data: List[dict] = []

        async def add_matches(matches: List, standings: Standings, league_name: str) -> None:
            if not matches or not standings:
                return

            for match in matches:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue

                kickoff = getattr(match, "utc_date", None)
                if not isinstance(kickoff, datetime):
                    continue

                match_date = kickoff.date()
                if match_date < start_date or match_date > end_date:
                    continue

                prediction_input = await self._prepare_prediction_input(match, standings)

                score_obj = getattr(match, "score", None)
                full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
                if not isinstance(full_time, dict):
                    continue

                home_goals = int(full_time.get("home") or 0)
                away_goals = int(full_time.get("away") or 0)

                if home_goals > away_goals:
                    actual = "H"
                elif away_goals > home_goals:
                    actual = "A"
                else:
                    actual = "D"

                training_data.append(
                    {
                        "prediction_input": prediction_input,
                        "actual_outcome": actual,
                        "league": league_name,
                        "matchday": getattr(match, "matchday", None),
                        "kickoff": kickoff,
                    }
                )

        await add_matches(fixtures_serie_a or [], standings_serie_a, "seriea")
        await add_matches(fixtures_norway or [], standings_norway, "norway")
        await add_matches(fixtures_premier or [], standings_premier, "premier")
        await add_matches(fixtures_bundesliga or [], standings_bundesliga, "bundesliga")
        await add_matches(fixtures_laliga or [], standings_laliga, "laliga")

        return training_data

    async def train_on_2025_history(self) -> dict:
        data_2024_2025 = await self._build_2024_2025_training_data()
        data_2026 = await self._build_2026_training_data()

        historical_data = data_2024_2025 + data_2026
        if not historical_data:
            logger.info("No 2025/2026 historical data available for training")
            return {
                "training_accuracy": 0.0,
                "training_matches": 0,
                "matches_2024_2025": 0,
                "matches_2026": 0,
            }
        accuracy = await self.train_model(historical_data)
        return {
            "training_accuracy": accuracy,
            "training_matches": len(historical_data),
            "matches_2024_2025": len(data_2024_2025),
            "matches_2026": len(data_2026),
        }

    async def get_2025_stats(self, league: Optional[str] = None) -> dict:
        historical_data = await self._build_2024_2025_training_data(league=None)
        if not historical_data:
            return {
                "accuracy_2025": 0.0,
                "matches_2025": 0,
            }

        correct = 0
        total = 0
        per_league_stats = {}

        for item in historical_data:
            input_data = item.get("prediction_input")
            actual = item.get("actual_outcome")
            if input_data is None or actual not in ("H", "D", "A"):
                continue

            item_league = item.get("league")
            if league is not None and item_league != league:
                continue

            prediction = self.model.predict(input_data)
            probabilities = {
                "H": prediction.home_win_prob,
                "D": prediction.draw_prob,
                "A": prediction.away_win_prob,
            }
            predicted_label = max(probabilities.items(), key=lambda x: x[1])[0]

            if predicted_label == actual:
                correct += 1
            total += 1

            if item_league:
                if item_league not in per_league_stats:
                    per_league_stats[item_league] = {"correct": 0, "total": 0}
                if predicted_label == actual:
                    per_league_stats[item_league]["correct"] += 1
                per_league_stats[item_league]["total"] += 1

        if total == 0:
            accuracy = 0.0
        else:
            accuracy = correct / total

        result = {
            "accuracy_2025": accuracy,
            "matches_2025": total,
        }

        for league_name, stats in per_league_stats.items():
            league_total = stats["total"]
            league_accuracy = stats["correct"] / league_total if league_total > 0 else 0.0
            result[league_name] = {
                "accuracy_2025": league_accuracy,
                "matches_2025": league_total,
            }

        return result

    async def get_2026_stats(self, league: Optional[str] = None) -> dict:
        historical_data = await self._build_2026_training_data()
        if not historical_data:
            return {
                "accuracy_2026": 0.0,
                "matches_2026": 0,
            }

        correct = 0
        total = 0
        per_league_stats = {}

        for item in historical_data:
            input_data = item.get("prediction_input")
            actual = item.get("actual_outcome")
            if input_data is None or actual not in ("H", "D", "A"):
                continue

            item_league = item.get("league")
            if league is not None and item_league != league:
                continue

            prediction = self.model.predict(input_data)
            probabilities = {
                "H": prediction.home_win_prob,
                "D": prediction.draw_prob,
                "A": prediction.away_win_prob,
            }
            predicted_label = max(probabilities.items(), key=lambda x: x[1])[0]

            if predicted_label == actual:
                correct += 1
            total += 1

            if item_league:
                if item_league not in per_league_stats:
                    per_league_stats[item_league] = {"correct": 0, "total": 0}
                if predicted_label == actual:
                    per_league_stats[item_league]["correct"] += 1
                per_league_stats[item_league]["total"] += 1

        if total == 0:
            accuracy = 0.0
        else:
            accuracy = correct / total

        result = {
            "accuracy_2026": accuracy,
            "matches_2026": total,
        }

        for league_name, stats in per_league_stats.items():
            league_total = stats["total"]
            league_accuracy = stats["correct"] / league_total if league_total > 0 else 0.0
            result[league_name] = {
                "accuracy_2026": league_accuracy,
                "matches_2026": league_total,
            }

        return result

    async def evaluate_predictions_serie_a(self, matchday: Optional[int] = None) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures(matchday)
            standings = await unified_data_service.get_standings()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_matches = [
                match for match in fixtures
                if getattr(match, "status", None) == MatchStatus.FINISHED
            ]

            return await self._evaluate_matches(finished_matches, standings)
        except Exception as e:
            logger.error(f"Error evaluating predictions for Serie A: {e}")
            raise

    async def evaluate_predictions_norway(self, matchday: Optional[int] = None) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_norway(matchday)
            standings = await unified_data_service.get_standings_norway()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_matches = [
                match for match in fixtures
                if getattr(match, "status", None) == MatchStatus.FINISHED
            ]

            return await self._evaluate_matches(finished_matches, standings)
        except Exception as e:
            logger.error(f"Error evaluating predictions for Norway: {e}")
            raise

    async def evaluate_predictions_premier(self, matchday: Optional[int] = None) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_premier(matchday)
            standings = await unified_data_service.get_standings_premier()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_matches = [
                match for match in fixtures
                if getattr(match, "status", None) == MatchStatus.FINISHED
            ]

            return await self._evaluate_matches(finished_matches, standings)
        except Exception as e:
            logger.error(f"Error evaluating predictions for Premier League: {e}")
            raise

    async def evaluate_predictions_bundesliga(self, matchday: Optional[int] = None) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_bundesliga(matchday)
            standings = await unified_data_service.get_standings_bundesliga()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_matches = [
                match for match in fixtures
                if getattr(match, "status", None) == MatchStatus.FINISHED
            ]

            return await self._evaluate_matches(finished_matches, standings)
        except Exception as e:
            logger.error(f"Error evaluating predictions for Bundesliga: {e}")
            raise

    async def evaluate_predictions_laliga(self, matchday: Optional[int] = None) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_laliga(matchday)
            standings = await unified_data_service.get_standings_laliga()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_matches = [
                match for match in fixtures
                if getattr(match, "status", None) == MatchStatus.FINISHED
            ]

            return await self._evaluate_matches(finished_matches, standings)
        except Exception as e:
            logger.error(f"Error evaluating predictions for La Liga: {e}")
            raise

    async def evaluate_recent_matchdays_serie_a(self, last_n_matchdays: int = 18) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures(None)
            standings = await unified_data_service.get_standings()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_by_matchday = {}
            for match in fixtures:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue
                matchday = getattr(match, "matchday", None)
                if matchday is None:
                    continue
                finished_by_matchday.setdefault(matchday, []).append(match)

            if not finished_by_matchday:
                return {"matches_evaluated": 0}

            sorted_matchdays = sorted(finished_by_matchday.keys())
            if last_n_matchdays > 0 and len(sorted_matchdays) > last_n_matchdays:
                selected_matchdays = sorted_matchdays[-last_n_matchdays:]
            else:
                selected_matchdays = sorted_matchdays

            selected_matches = []
            for md in selected_matchdays:
                selected_matches.extend(finished_by_matchday[md])

            if not selected_matches:
                return {"matches_evaluated": 0}

            evaluation = await self._evaluate_matches(selected_matches, standings)
            evaluation["matchdays"] = selected_matchdays
            return evaluation
        except Exception as e:
            logger.error(f"Error evaluating recent matchdays for Serie A: {e}")
            raise

    async def evaluate_recent_matchdays_norway(self, last_n_matchdays: int = 18) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_norway(None)
            standings = await unified_data_service.get_standings_norway()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_by_matchday = {}
            for match in fixtures:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue
                matchday = getattr(match, "matchday", None)
                if matchday is None:
                    continue
                finished_by_matchday.setdefault(matchday, []).append(match)

            if not finished_by_matchday:
                return {"matches_evaluated": 0}

            sorted_matchdays = sorted(finished_by_matchday.keys())
            if last_n_matchdays > 0 and len(sorted_matchdays) > last_n_matchdays:
                selected_matchdays = sorted_matchdays[-last_n_matchdays:]
            else:
                selected_matchdays = sorted_matchdays

            selected_matches = []
            for md in selected_matchdays:
                selected_matches.extend(finished_by_matchday[md])

            if not selected_matches:
                return {"matches_evaluated": 0}

            evaluation = await self._evaluate_matches(selected_matches, standings)
            evaluation["matchdays"] = selected_matchdays
            return evaluation
        except Exception as e:
            logger.error(f"Error evaluating recent matchdays for Norway: {e}")
            raise

    async def evaluate_recent_matchdays_premier(self, last_n_matchdays: int = 18) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_premier(None)
            standings = await unified_data_service.get_standings_premier()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_by_matchday = {}
            for match in fixtures:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue
                matchday = getattr(match, "matchday", None)
                if matchday is None:
                    continue
                finished_by_matchday.setdefault(matchday, []).append(match)

            if not finished_by_matchday:
                return {"matches_evaluated": 0}

            sorted_matchdays = sorted(finished_by_matchday.keys())
            if last_n_matchdays > 0 and len(sorted_matchdays) > last_n_matchdays:
                selected_matchdays = sorted_matchdays[-last_n_matchdays:]
            else:
                selected_matchdays = sorted_matchdays

            selected_matches = []
            for md in selected_matchdays:
                selected_matches.extend(finished_by_matchday[md])

            if not selected_matches:
                return {"matches_evaluated": 0}

            evaluation = await self._evaluate_matches(selected_matches, standings)
            evaluation["matchdays"] = selected_matchdays
            return evaluation
        except Exception as e:
            logger.error(f"Error evaluating recent matchdays for Premier League: {e}")
            raise

    async def evaluate_recent_matchdays_bundesliga(self, last_n_matchdays: int = 18) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_bundesliga(None)
            standings = await unified_data_service.get_standings_bundesliga()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_by_matchday = {}
            for match in fixtures:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue
                matchday = getattr(match, "matchday", None)
                if matchday is None:
                    continue
                finished_by_matchday.setdefault(matchday, []).append(match)

            if not finished_by_matchday:
                return {"matches_evaluated": 0}

            sorted_matchdays = sorted(finished_by_matchday.keys())
            if last_n_matchdays > 0 and len(sorted_matchdays) > last_n_matchdays:
                selected_matchdays = sorted_matchdays[-last_n_matchdays:]
            else:
                selected_matchdays = sorted_matchdays

            selected_matches = []
            for md in selected_matchdays:
                selected_matches.extend(finished_by_matchday[md])

            if not selected_matches:
                return {"matches_evaluated": 0}

            evaluation = await self._evaluate_matches(selected_matches, standings)
            evaluation["matchdays"] = selected_matchdays
            return evaluation
        except Exception as e:
            logger.error(f"Error evaluating recent matchdays for Bundesliga: {e}")
            raise

    async def evaluate_recent_matchdays_laliga(self, last_n_matchdays: int = 18) -> dict:
        try:
            fixtures = await unified_data_service.get_fixtures_laliga(None)
            standings = await unified_data_service.get_standings_laliga()

            if not fixtures or not standings:
                return {"matches_evaluated": 0}

            finished_by_matchday = {}
            for match in fixtures:
                if getattr(match, "status", None) != MatchStatus.FINISHED:
                    continue
                matchday = getattr(match, "matchday", None)
                if matchday is None:
                    continue
                finished_by_matchday.setdefault(matchday, []).append(match)

            if not finished_by_matchday:
                return {"matches_evaluated": 0}

            sorted_matchdays = sorted(finished_by_matchday.keys())
            if last_n_matchdays > 0 and len(sorted_matchdays) > last_n_matchdays:
                selected_matchdays = sorted_matchdays[-last_n_matchdays:]
            else:
                selected_matchdays = sorted_matchdays

            selected_matches = []
            for md in selected_matchdays:
                selected_matches.extend(finished_by_matchday[md])

            if not selected_matches:
                return {"matches_evaluated": 0}

            evaluation = await self._evaluate_matches(selected_matches, standings)
            evaluation["matchdays"] = selected_matchdays
            return evaluation
        except Exception as e:
            logger.error(f"Error evaluating recent matchdays for La Liga: {e}")
            raise

    async def _evaluate_matches(self, matches: List, standings: Standings) -> dict:
        if not matches:
            return {"matches_evaluated": 0}

        brier_scores: List[float] = []
        log_losses: List[float] = []
        accuracies: List[float] = []
        match_details: List[dict] = []

        for match in matches:
            prediction_input = await self._prepare_prediction_input(match, standings)
            prediction = self.model.predict(prediction_input)

            score_obj = getattr(match, "score", None)
            full_time = getattr(score_obj, "full_time", None) if score_obj is not None else None
            if not isinstance(full_time, dict):
                continue

            home_goals = int(full_time.get("home") or 0)
            away_goals = int(full_time.get("away") or 0)

            if home_goals > away_goals:
                actual = "H"
            elif away_goals > home_goals:
                actual = "A"
            else:
                actual = "D"

            probs = {
                "H": prediction.home_win_prob,
                "D": prediction.draw_prob,
                "A": prediction.away_win_prob,
            }

            y = {
                "H": 1.0 if actual == "H" else 0.0,
                "D": 1.0 if actual == "D" else 0.0,
                "A": 1.0 if actual == "A" else 0.0,
            }

            brier = (
                (probs["H"] - y["H"]) ** 2
                + (probs["D"] - y["D"]) ** 2
                + (probs["A"] - y["A"]) ** 2
            )
            brier_scores.append(brier)

            p_true = probs[actual]
            eps = 1e-15
            log_losses.append(-math.log(max(p_true, eps)))

            predicted_label = max(probs.items(), key=lambda x: x[1])[0]
            is_hit = predicted_label == actual
            accuracies.append(1.0 if is_hit else 0.0)

            confidence = max(probs.values())

            home_team_name = getattr(getattr(match, "home_team", None), "name", None)
            away_team_name = getattr(getattr(match, "away_team", None), "name", None)

            if home_team_name is None:
                home_team_name = str(getattr(match, "home_team", "Casa"))
            if away_team_name is None:
                away_team_name = str(getattr(match, "away_team", "Trasferta"))

            def _map_outcome_label(label: str) -> str:
                if label == "H":
                    return "1"
                if label == "A":
                    return "2"
                if label == "D":
                    return "X"
                return label

            match_details.append(
                {
                    "home_team": home_team_name,
                    "away_team": away_team_name,
                    "matchday": getattr(match, "matchday", None),
                    "predicted_outcome": _map_outcome_label(predicted_label),
                    "predicted_outcome_probability": confidence,
                    "prediction_success_probability": confidence,
                    "hit": is_hit,
                }
            )

        n = len(brier_scores)
        if n == 0:
            return {"matches_evaluated": 0}

        accuracy_value = sum(accuracies) / n

        return {
            "matches_evaluated": n,
            "brier_score_mean": sum(brier_scores) / n,
            "log_loss_mean": sum(log_losses) / n,
            "accuracy": accuracy_value,
            "overall_accuracy": accuracy_value,
            "matches": match_details,
        }

# Global prediction service instance
prediction_service = PredictionService()
