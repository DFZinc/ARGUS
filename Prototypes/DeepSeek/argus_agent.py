import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import List, Dict

from rate_limiter import RateLimiter
from narrative_scout import NarrativeScout
from launch_detector import LaunchDetector
from pattern_matcher import PatternMatcher
from argus_cache import ArgusCache
from llm_reasoner import LLMReasoner
from config import POLL_INTERVAL_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


class ArgusAgent:
    def __init__(self, enable_llm: bool = False):
        self.rl = RateLimiter(calls_per_second=3.0)
        self.scout = NarrativeScout(self.rl)
        self.detector = LaunchDetector(self.rl)
        self.cache = ArgusCache()
        self.llm = LLMReasoner() if enable_llm else None
        self._running = True
        self._cycle_count = 0
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        log.info(f"Received signal {signum}, shutting down...")
        self._running = False

    async def run(self):
        """Main agent loop"""
        log.info("🚀 ARGUS Narrative Intelligence Agent v2 started")
        log.info(f"Poll interval: {POLL_INTERVAL_SECONDS} seconds")
        
        while self._running:
            try:
                await self._cycle()
                self._cycle_count += 1
            except asyncio.CancelledError:
                log.info("Agent loop cancelled")
                break
            except Exception as e:
                log.error(f"Cycle error: {e}", exc_info=True)
            
            # Graceful shutdown check
            if not self._running:
                break
                
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        
        log.info("🛑 ARGUS Agent stopped")

    async def _cycle(self):
        """Single analysis cycle"""
        cycle_start = datetime.now()
        log.info(f"🔍 Cycle {self._cycle_count + 1} starting...")
        
        # Fetch data
        narratives = await self.scout.get_current_narratives()
        new_launches = await self.detector.get_new_launches()
        
        if not narratives:
            log.warning("No narratives found in this cycle")
            return
        
        if not new_launches:
            log.info("No new launches found in this cycle")
            return
        
        # Score launches
        ranked = []
        for launch in new_launches:
            try:
                score_data = PatternMatcher.score_launch(launch, narratives)
                if score_data["score"] >= 60:
                    ranked.append(score_data)
                    log.info(
                        f"🔥 HIGH MATCH → {score_data['symbol']} | "
                        f"Score {score_data['score']} | "
                        f"Narratives: {score_data['found_narratives'][:3]}"
                    )
            except Exception as e:
                log.error(f"Error scoring launch {launch.get('symbol', 'unknown')}: {e}")
        
        # Save to cache
        self.cache.save_findings(ranked, narratives)
        
        # LLM analysis if enabled and we have findings
        llm_analysis = None
        if self.llm and ranked:
            try:
                llm_analysis = await self.llm.analyze(narratives, ranked)
                log.info(f"📝 LLM Analysis:\n{llm_analysis[:500]}...")
            except Exception as e:
                log.error(f"LLM analysis failed: {e}")
        
        # Cycle summary
        cycle_time = (datetime.now() - cycle_start).total_seconds()
        log.info(
            f"✅ Cycle {self._cycle_count + 1} complete — "
            f"{len(ranked)} high-potential launches found, "
            f"took {cycle_time:.1f}s"
        )
        
        # Emit metrics
        await self._emit_metrics(len(ranked), len(narratives), cycle_time)

    async def _emit_metrics(self, findings_count: int, narratives_count: int, cycle_time: float):
        """Emit metrics for monitoring"""
        # Get stats from cache
        stats = self.cache.get_stats()
        
        log.info(
            f"📊 Metrics: {findings_count} findings | "
            f"{narratives_count} narratives | "
            f"Avg score 24h: {stats.get('avg_score_24h', 0)} | "
            f"Cycle time: {cycle_time:.1f}s"
        )

    async def health_check(self) -> Dict:
        """Health check endpoint for monitoring"""
        stats = self.cache.get_stats()
        return {
            "status": "healthy" if self._running else "stopped",
            "cycle_count": self._cycle_count,
            "last_cycle": datetime.now().isoformat(),
            "cache_stats": stats
        }


async def main():
    """Entry point for standalone execution"""
    agent = ArgusAgent(enable_llm=False)  # Set to True if Ollama is running
    
    try:
        await agent.run()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received")
    except Exception as e:
        log.error(f"Agent crashed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())