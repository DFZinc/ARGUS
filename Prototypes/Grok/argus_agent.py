import asyncio
import logging
import os
from datetime import datetime

from rate_limiter import RateLimiter
from narrative_scout import NarrativeScout
from launch_detector import LaunchDetector
from pattern_matcher import PatternMatcher
from argus_cache import ArgusCache   # create this file next if you want persistence

# Load config
from config import POLL_INTERVAL_SECONDS, LUNARCRUSH_BEARER, X_BEARER_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class ArgusAgent:
    def __init__(self):
        self.rl = RateLimiter(calls_per_second=3.0)
        self.scout = NarrativeScout(self.rl)
        self.detector = LaunchDetector(self.rl)
        self.cache = ArgusCache()   # simple SQLite or JSON

    async def run(self):
        log.info("🚀 ARGUS Narrative Intelligence Agent v2 started (real data)")
        while True:
            try:
                await self._cycle()
            except Exception as e:
                log.error(f"Cycle error: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _cycle(self):
        log.info("🔍 Scanning live narratives + new launches...")

        narratives = await self.scout.get_current_narratives()
        new_launches = await self.detector.get_new_launches()

        ranked = []
        for launch in new_launches:
            score_data = PatternMatcher.score_launch(launch, narratives)
            if score_data["score"] >= 60:
                ranked.append(score_data)
                log.info(f"🔥 HIGH MATCH → {score_data['symbol']} | Score {score_data['score']} | Narratives: {score_data['found_narratives']}")

        self.cache.save_findings(ranked, narratives)
        log.info(f"✅ ARGUS cycle done — {len(ranked)} high-potential launches found")