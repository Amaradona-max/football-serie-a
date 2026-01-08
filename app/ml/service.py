from typing import List, Optional
from datetime import datetime
import logging

from app.data.services.unified_data_service import unified_data_service
from app.data.models.common import Match, Team, Standings, PredictionInput, PredictionOutput
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
            # Get fixtures for next matchday
            fixtures = await unified_data_service.get_fixtures()
            
            # Get current standings for team strength information
            standings = await unified_data_service.get_standings()
            
            # Filter for upcoming matches (next 7 days)
            upcoming_matches = [
                match for match in fixtures
                if match.date and match.date >= datetime.now()
                and (match.date - datetime.now()).days <= 7
            ]
            
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
    
    async def _prepare_prediction_input(self, match: Match, standings: Standings) -> PredictionInput:
        """
        Prepare prediction input from match and standings data.
        
        Args:
            match: Match data
            standings: Current league standings
            
        Returns:
            Prepared prediction input
        """
        # Get team information from standings
        home_team_info = self._get_team_info(match.home_team, standings)
        away_team_info = self._get_team_info(match.away_team, standings)
        
        # Get recent form (simplified - would come from data providers)
        home_form = self._get_team_form(match.home_team)
        away_form = self._get_team_form(match.away_team)
        
        # Get previous meetings (simplified)
        previous_meetings = await self._get_previous_meetings(
            match.home_team, match.away_team
        )
        
        return PredictionInput(
            home_team=match.home_team,
            away_team=match.away_team,
            home_position=home_team_info.get('position', 20),
            away_position=away_team_info.get('position', 20),
            home_form=home_form,
            away_form=away_form,
            is_home_advantage=True,  # Assuming home advantage
            previous_meetings=previous_meetings
        )
    
    def _get_team_info(self, team_name: str, standings: Standings) -> dict:
        """Get team information from standings"""
        for team_standing in standings.standings:
            if team_standing.team.name.lower() == team_name.lower():
                return {
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
        """Get team's recent form (simplified implementation)"""
        # In a real implementation, this would come from data providers
        # For now, return a mock form string
        return "WWLWD"  # Example form: Win, Win, Loss, Win, Draw
    
    async def _get_previous_meetings(self, home_team: str, away_team: str) -> List[dict]:
        """Get previous meetings between two teams (simplified)"""
        # In a real implementation, this would query historical data
        # For now, return mock data
        return [
            {"date": "2024-01-15", "home_goals": 2, "away_goals": 1},
            {"date": "2023-09-10", "home_goals": 1, "away_goals": 1},
            {"date": "2023-03-05", "home_goals": 3, "away_goals": 0}
        ]
    
    async def train_model(self, historical_data: List[dict]) -> float:
        """
        Train the prediction model on historical data.
        
        Args:
            historical_data: List of historical match data with outcomes
            
        Returns:
            Training accuracy
        """
        try:
            # Convert to DataFrame for training
            import pandas as pd
            df = pd.DataFrame(historical_data)
            
            # Train the model
            accuracy = self.model.train(df)
            
            logger.info(f"Model trained with accuracy: {accuracy:.3f}")
            return accuracy
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise

# Global prediction service instance
prediction_service = PredictionService()