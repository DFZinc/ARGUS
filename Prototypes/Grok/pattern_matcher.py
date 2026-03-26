class PatternMatcher:
    @staticmethod
    def score_launch(launch: dict, current_narratives: list[dict]) -> dict:
        symbol = launch.get("symbol", "").lower()
        name = launch.get("name", "").lower()

        # Narrative fit
        narrative_score = 0
        for n in current_narratives:
            if n["keyword"] in symbol or n["keyword"] in name:
                narrative_score += n["score"]

        # Virality (placeholder — expand with X mention count)
        virality = min(100, launch.get("volume_usd_1h", 0) / 500)  # simple volume proxy

        total_score = (narrative_score * 0.3) + (virality * 0.4) + (launch.get("txns_1h", 0) * 0.3)
        total_score = min(100, round(total_score))

        return {
            "address": launch["address"],
            "symbol": launch.get("symbol"),
            "score": total_score,
            "narrative_fit": round(narrative_score),
            "virality": round(virality),
            "found_narratives": [n["keyword"] for n in current_narratives if n["keyword"] in symbol or n["keyword"] in name]
        }