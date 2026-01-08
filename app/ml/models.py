import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

from app.data.models.common import PredictionInput, PredictionOutput

class FootballPredictionModel:
    def __init__(self):
        self.model = None
        self.model_version = "v1.0"
        self.features = [
            'home_team_strength',
            'away_team_strength', 
            'home_form',
            'away_form',
            'home_position',
            'away_position',
            'is_home_advantage',
            'previous_meetings_win_rate'
        ]
        self.model_path = Path("models/football_predictor.joblib")
        
    def train(self, training_data: pd.DataFrame):
        """
        Train the prediction model on historical data.
        
        Args:
            training_data: DataFrame with historical match data and outcomes
        """
        # Prepare features and target
        X = training_data[self.features]
        y = training_data['result']  # 'H', 'D', 'A'
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Train model (using Random Forest for better performance)
        self.model = RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            max_depth=10
        )
        
        self.model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        print(f"Model trained with accuracy: {accuracy:.3f}")
        print("Classification Report:")
        print(classification_report(y_test, y_pred))
        
        # Save model
        self._save_model()
        
        return accuracy
    
    def predict(self, input_data: PredictionInput) -> PredictionOutput:
        """
        Predict match outcome based on input features.
        
        Args:
            input_data: Prediction input with team and match information
            
        Returns:
            Prediction output with probabilities and confidence
        """
        if self.model is None:
            self._load_model()
        
        # Prepare features for prediction
        features = self._prepare_features(input_data)
        
        # Get prediction probabilities
        probabilities = self.model.predict_proba([features])[0]
        
        # Map probabilities to outcomes
        class_mapping = {0: 'H', 1: 'D', 2: 'A'}
        prob_dict = {
            class_mapping[i]: float(prob)
            for i, prob in enumerate(probabilities)
        }
        
        # Calculate confidence (max probability)
        confidence = max(probabilities)
        
        # Generate predicted score (simplified)
        predicted_score = self._predict_score(prob_dict)
        
        return PredictionOutput(
            home_win_prob=prob_dict['H'],
            draw_prob=prob_dict['D'],
            away_win_prob=prob_dict['A'],
            predicted_score=predicted_score,
            confidence=confidence,
            model_version=self.model_version
        )
    
    def _prepare_features(self, input_data: PredictionInput) -> List[float]:
        """Prepare input features for model prediction"""
        
        # Convert team forms to numerical values (e.g., 'WWLWD' -> strength)
        home_form_strength = self._calculate_form_strength(input_data.home_form)
        away_form_strength = self._calculate_form_strength(input_data.away_form)
        
        # Calculate team strength based on position (inverse relationship)
        home_strength = 1.0 / max(1, input_data.home_position)
        away_strength = 1.0 / max(1, input_data.away_position)
        
        # Calculate win rate from previous meetings
        win_rate = self._calculate_win_rate(input_data.previous_meetings)
        
        features = [
            home_strength,                    # home_team_strength
            away_strength,                    # away_team_strength
            home_form_strength,               # home_form
            away_form_strength,               # away_form
            input_data.home_position,         # home_position
            input_data.away_position,         # away_position
            float(input_data.is_home_advantage),  # is_home_advantage
            win_rate                         # previous_meetings_win_rate
        ]
        
        return features
    
    def _calculate_form_strength(self, form_string: Optional[str]) -> float:
        """Convert form string to numerical strength"""
        if not form_string or len(form_string) == 0:
            return 0.5  # Neutral
        
        # Simple calculation: W=1, D=0.5, L=0
        values = []
        for char in form_string:
            if char == 'W':
                values.append(1.0)
            elif char == 'D':
                values.append(0.5)
            elif char == 'L':
                values.append(0.0)
            else:
                values.append(0.5)  # Unknown
        
        return sum(values) / len(values) if values else 0.5
    
    def _calculate_win_rate(self, previous_meetings: List[Dict[str, Any]]) -> float:
        """Calculate win rate from previous meetings"""
        if not previous_meetings:
            return 0.5  # Neutral
        
        wins = 0
        total = len(previous_meetings)
        
        for meeting in previous_meetings:
            # Simplified: assume home team perspective
            if meeting.get('home_goals', 0) > meeting.get('away_goals', 0):
                wins += 1
            elif meeting.get('home_goals', 0) == meeting.get('away_goals', 0):
                wins += 0.5
        
        return wins / total if total > 0 else 0.5
    
    def _predict_score(self, probabilities: Dict[str, float]) -> Optional[Dict[str, int]]:
        """Generate a predicted score based on probabilities"""
        # Simple heuristic for score prediction
        max_prob_outcome = max(probabilities.items(), key=lambda x: x[1])
        
        if max_prob_outcome[0] == 'H':  # Home win
            return {'home': 2, 'away': 1}
        elif max_prob_outcome[0] == 'A':  # Away win
            return {'home': 1, 'away': 2}
        else:  # Draw
            return {'home': 1, 'away': 1}
    
    def _save_model(self):
        """Save trained model to file"""
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.model, self.model_path)
        print(f"Model saved to {self.model_path}")
    
    def _load_model(self):
        """Load trained model from file"""
        if self.model_path.exists():
            self.model = joblib.load(self.model_path)
            print(f"Model loaded from {self.model_path}")
        else:
            # Fallback to simple model if no trained model exists
            self.model = LogisticRegression(random_state=42)
            # Train on dummy data for basic functionality
            X_dummy = np.random.rand(10, len(self.features))
            y_dummy = np.random.choice(['H', 'D', 'A'], 10)
            self.model.fit(X_dummy, y_dummy)
            print("Using fallback model (no trained model found)")

# Global model instance
prediction_model = FootballPredictionModel()