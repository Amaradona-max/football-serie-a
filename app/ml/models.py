from typing import Dict, List, Optional, Any
from app.data.models.common import PredictionInput, PredictionOutput


class FootballPredictionModel:
    def __init__(self):
        self.model_version = "v2.0-standings-based"

    def train(self, training_data: List[Dict[str, Any]]) -> float:
        return 1.0

    def predict(self, input_data: PredictionInput) -> PredictionOutput:
        home_form_strength = self._calculate_form_strength(input_data.home_form)
        away_form_strength = self._calculate_form_strength(input_data.away_form)

        max_position = 20
        home_position_factor = (max_position - min(input_data.home_position, max_position)) / max_position
        away_position_factor = (max_position - min(input_data.away_position, max_position)) / max_position

        base = 0.33
        home_advantage_bonus = 0.08 if input_data.is_home_advantage else 0.0

        form_diff = home_form_strength - away_form_strength
        position_diff = home_position_factor - away_position_factor
        goal_strength = self._calculate_goal_strength(input_data)

        score = base + home_advantage_bonus + 0.25 * form_diff + 0.35 * position_diff + 0.25 * goal_strength

        home_win_prob = max(0.05, min(0.85, score))

        strength_diff = abs(home_position_factor - away_position_factor)
        draw_centrality = 1.0 - strength_diff
        draw_prob = max(0.10, min(0.45, 0.25 + 0.25 * (draw_centrality - 0.5)))

        away_win_prob = max(0.05, 1.0 - home_win_prob - draw_prob)

        total = home_win_prob + draw_prob + away_win_prob
        if total > 0:
            home_win_prob /= total
            draw_prob /= total
            away_win_prob /= total

        probabilities = {"H": home_win_prob, "D": draw_prob, "A": away_win_prob}

        predicted_score = self._predict_score(probabilities)
        confidence = max(probabilities.values())

        return PredictionOutput(
            home_win_prob=probabilities["H"],
            draw_prob=probabilities["D"],
            away_win_prob=probabilities["A"],
            predicted_score=predicted_score,
            confidence=confidence,
            model_version=self.model_version,
        )

    def _calculate_form_strength(self, form_string: Optional[str]) -> float:
        if not form_string or len(form_string) == 0:
            return 0.5

        values = []
        for char in form_string:
            if char == "W":
                values.append(1.0)
            elif char == "D":
                values.append(0.5)
            elif char == "L":
                values.append(0.0)
            else:
                values.append(0.5)

        return sum(values) / len(values) if values else 0.5

    def _calculate_win_rate(self, previous_meetings: List[Dict[str, Any]]) -> float:
        if not previous_meetings:
            return 0.5

        wins = 0.0
        total = len(previous_meetings)

        for meeting in previous_meetings:
            home_goals = meeting.get("home_goals", 0)
            away_goals = meeting.get("away_goals", 0)
            if home_goals > away_goals:
                wins += 1.0
            elif home_goals == away_goals:
                wins += 0.5

        return wins / total if total > 0 else 0.5

    def _calculate_goal_strength(self, input_data: PredictionInput) -> float:
        home_played = input_data.home_played or 0
        away_played = input_data.away_played or 0
        home_gf = input_data.home_goals_for or 0
        home_ga = input_data.home_goals_against or 0
        away_gf = input_data.away_goals_for or 0
        away_ga = input_data.away_goals_against or 0

        if home_played <= 0 or away_played <= 0:
            return 0.0

        home_attack = home_gf / home_played
        home_defense = home_ga / home_played
        away_attack = away_gf / away_played
        away_defense = away_ga / away_played

        home_balance = home_attack - home_defense
        away_balance = away_attack - away_defense

        diff = home_balance - away_balance
        if diff > 1.5:
            diff = 1.5
        if diff < -1.5:
            diff = -1.5

        return diff / 3.0

    def _predict_score(self, probabilities: Dict[str, float]) -> Dict[str, int]:
        max_outcome = max(probabilities.items(), key=lambda x: x[1])[0]

        if max_outcome == "H":
            return {"home": 2, "away": 1}
        if max_outcome == "A":
            return {"home": 1, "away": 2}
        return {"home": 1, "away": 1}


prediction_model = FootballPredictionModel()
