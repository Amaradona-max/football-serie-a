from typing import List, Optional
from datetime import datetime
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
            
            logger.info(f"Generated {len(predictions)} predictions for next matchday")
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting next matchday: {e}")
            raise
    
    async def predict_next_matchday_norway(self) -> List[dict]:
        try:
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

    async def _evaluate_matches(self, matches: List, standings: Standings) -> dict:
        if not matches:
            return {"matches_evaluated": 0}

        brier_scores: List[float] = []
        log_losses: List[float] = []
        accuracies: List[float] = []

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

            y = {"H": 1.0 if actual == "H" else 0.0,
                 "D": 1.0 if actual == "D" else 0.0,
                 "A": 1.0 if actual == "A" else 0.0}

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
            accuracies.append(1.0 if predicted_label == actual else 0.0)

        n = len(brier_scores)
        if n == 0:
            return {"matches_evaluated": 0}

        return {
            "matches_evaluated": n,
            "brier_score_mean": sum(brier_scores) / n,
            "log_loss_mean": sum(log_losses) / n,
            "accuracy": sum(accuracies) / n,
        }

# Global prediction service instance
prediction_service = PredictionService()
