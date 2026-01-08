import math
from datetime import date, datetime
from typing import Dict, List, Optional
import logging

from app.data.models.detailed import PlayerBioRhythm

logger = logging.getLogger(__name__)

class BioRhythmService:
    def __init__(self):
        self.physical_cycle = 23  # days
        self.emotional_cycle = 28  # days
        self.intellectual_cycle = 33  # days
    
    def calculate_bio_rhythm(self, birth_date: date, target_date: date) -> PlayerBioRhythm:
        """
        Calculate comprehensive bio-rhythm for a player
        
        Args:
            birth_date: Player's date of birth
            target_date: Date for which to calculate bio-rhythm
            
        Returns:
            PlayerBioRhythm object with all components
        """
        try:
            # Calculate days since birth
            days_alive = (target_date - birth_date).days
            
            # Calculate individual rhythms
            physical = self._calculate_rhythm_component(days_alive, self.physical_cycle)
            emotional = self._calculate_rhythm_component(days_alive, self.emotional_cycle)
            intellectual = self._calculate_rhythm_component(days_alive, self.intellectual_cycle)
            
            # Calculate overall score (weighted average)
            overall = self._calculate_overall_score(physical, emotional, intellectual)
            
            return PlayerBioRhythm(
                physical=physical,
                emotional=emotional,
                intellectual=intellectual,
                overall=overall
            )
            
        except Exception as e:
            logger.error(f"Error calculating bio-rhythm: {e}")
            return PlayerBioRhythm(
                physical=50.0,
                emotional=50.0,
                intellectual=50.0,
                overall=50.0
            )
    
    def _calculate_rhythm_component(self, days_alive: int, cycle_days: int) -> float:
        """Calculate individual rhythm component"""
        # Sine wave calculation for bio-rhythm
        radians = 2 * math.pi * days_alive / cycle_days
        rhythm_value = 50 + 50 * math.sin(radians)
        
        # Ensure value is within 0-100 range
        return max(0.0, min(100.0, round(rhythm_value, 1)))
    
    def _calculate_overall_score(self, physical: float, emotional: float, intellectual: float) -> float:
        """Calculate weighted overall score"""
        # Weights for different components (can be adjusted)
        weights = {
            'physical': 0.4,    # Physical condition is most important for athletes
            'emotional': 0.3,    # Emotional state affects performance
            'intellectual': 0.3  # Decision making and focus
        }
        
        overall = (
            physical * weights['physical'] +
            emotional * weights['emotional'] +
            intellectual * weights['intellectual']
        )
        
        return round(overall, 1)
    
    def get_rhythm_interpretation(self, bio_rhythm: PlayerBioRhythm) -> Dict[str, str]:
        """
        Get human-readable interpretation of bio-rhythm scores
        
        Returns:
            Dictionary with interpretations for each component
        """
        interpretations = {
            'physical': self._interpret_component(bio_rhythm.physical, "fisico"),
            'emotional': self._interpret_component(bio_rhythm.emotional, "emotivo"),
            'intellectual': self._interpret_component(bio_rhythm.intellectual, "intellettuale"),
            'overall': self._interpret_overall(bio_rhythm.overall)
        }
        
        return interpretations
    
    def _interpret_component(self, score: float, component_name: str) -> str:
        """Interpret individual component score"""
        if score >= 80:
            return f"Eccellente - {component_name} al massimo livello"
        elif score >= 65:
            return f"Molto buono - {component_name} in ottime condizioni"
        elif score >= 50:
            return f"Normale - {component_name} nella media"
        elif score >= 35:
            return f"Leggermente basso - {component_name} sotto la media"
        else:
            return f"Critico - {component_name} molto basso"
    
    def _interpret_overall(self, score: float) -> str:
        """Interpret overall bio-rhythm score"""
        if score >= 80:
            return "💪 Prestazione eccezionale attesa - Giornata ideale"
        elif score >= 70:
            return "👍 Prestazione ottima - Condizioni molto favorevoli"
        elif score >= 60:
            return "👌 Prestazione buona - Giornata positiva"
        elif score >= 50:
            return "🔶 Prestazione nella media - Giornata standard"
        elif score >= 40:
            return "⚠️ Prestazione sotto la media - Attenzione necessaria"
        else:
            return "🔴 Prestazione critica - Giornata difficile"
    
    def get_optimal_players(self, players: List[Dict], target_date: date) -> List[Dict]:
        """
        Sort players by overall bio-rhythm score (descending)
        
        Args:
            players: List of player dicts with 'name' and 'date_of_birth'
            target_date: Date for bio-rhythm calculation
            
        Returns:
            Sorted list of players with bio-rhythm scores
        """
        players_with_scores = []
        
        for player in players:
            if 'date_of_birth' in player:
                bio_rhythm = self.calculate_bio_rhythm(
                    player['date_of_birth'], target_date
                )
                
                player_data = {
                    'name': player['name'],
                    'position': player.get('position', 'N/A'),
                    'bio_rhythm': bio_rhythm,
                    'interpretation': self.get_rhythm_interpretation(bio_rhythm)
                }
                players_with_scores.append(player_data)
        
        # Sort by overall score descending
        players_with_scores.sort(key=lambda x: x['bio_rhythm'].overall, reverse=True)
        
        return players_with_scores
    
    def get_team_bio_rhythm_summary(self, team_players: List[Dict], target_date: date) -> Dict:
        """
        Get team-level bio-rhythm summary
        
        Returns:
            Team summary with average scores and top performers
        """
        players_with_scores = self.get_optimal_players(team_players, target_date)
        
        if not players_with_scores:
            return {
                'average_overall': 50.0,
                'average_physical': 50.0,
                'average_emotional': 50.0,
                'average_intellectual': 50.0,
                'top_performers': [],
                'team_condition': 'Dati non disponibili'
            }
        
        # Calculate averages
        avg_overall = sum(p['bio_rhythm'].overall for p in players_with_scores) / len(players_with_scores)
        avg_physical = sum(p['bio_rhythm'].physical for p in players_with_scores) / len(players_with_scores)
        avg_emotional = sum(p['bio_rhythm'].emotional for p in players_with_scores) / len(players_with_scores)
        avg_intellectual = sum(p['bio_rhythm'].intellectual for p in players_with_scores) / len(players_with_scores)
        
        # Get top 3 performers
        top_performers = players_with_scores[:3]
        
        # Determine team condition
        if avg_overall >= 70:
            team_condition = "💪 Condizioni eccellenti"
        elif avg_overall >= 60:
            team_condition = "👍 Condizioni molto buone"
        elif avg_overall >= 50:
            team_condition = "👌 Condizioni nella media"
        elif avg_overall >= 40:
            team_condition = "⚠️ Condizioni sotto la media"
        else:
            team_condition = "🔴 Condizioni critiche"
        
        return {
            'average_overall': round(avg_overall, 1),
            'average_physical': round(avg_physical, 1),
            'average_emotional': round(avg_emotional, 1),
            'average_intellectual': round(avg_intellectual, 1),
            'top_performers': top_performers,
            'team_condition': team_condition,
            'total_players': len(players_with_scores)
        }

# Global service instance
bio_rhythm_service = BioRhythmService()