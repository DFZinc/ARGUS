from typing import List, Dict


class PatternMatcher:
    @staticmethod
    def score_launch(launch: Dict, current_narratives: List[Dict]) -> Dict:
        """Score a launch against current narratives"""
        symbol = launch.get("symbol", "").lower()
        name = launch.get("name", "").lower()

        # Narrative fit - calculate weighted score
        narrative_score = 0
        matched_narratives = []
        
        for n in current_narratives:
            keyword = n.get("keyword", "").lower()
            if keyword and (keyword in symbol or keyword in name):
                velocity = min(100, n.get("velocity_score", 0))
                narrative_score += velocity
                if keyword not in matched_narratives:
                    matched_narratives.append(keyword)
        
        # Cap narrative_score to 100
        narrative_score = min(100, narrative_score)
        
        # Calculate narrative weight (more matches = higher weight)
        narrative_weight = min(1.0, len(matched_narratives) / 5.0)
        narrative_contribution = narrative_score * 0.3 * (1 + narrative_weight)

        # Virality proxy from volume
        volume = launch.get("volume_usd_1h", 0)
        virality = min(100, volume / 500) if volume > 0 else 0
        
        # Normalize transaction count to 0-100 scale
        txns = launch.get("txns_1h", 0)
        txns_score = min(100, txns / 100) if txns > 0 else 0

        # Calculate total score with adjusted weights
        total_score = (
            (narrative_contribution * 0.35) +  # Increased weight for narratives
            (virality * 0.40) +                 # Volume/virality
            (txns_score * 0.25)                 # Transaction activity
        )
        total_score = round(min(100, total_score))

        return {
            "address": launch.get("address", ""),
            "symbol": launch.get("symbol", "UNKNOWN"),
            "name": launch.get("name", ""),
            "score": total_score,
            "narrative_fit": round(narrative_score),
            "narrative_count": len(matched_narratives),
            "virality": round(virality),
            "txns_score": round(txns_score),
            "found_narratives": matched_narratives,
            "volume_usd_1h": volume,
            "txns_1h": txns
        }