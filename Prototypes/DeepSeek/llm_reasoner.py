# llm_reasoner.py
import aiohttp
import json
import logging
import asyncio
from typing import List, Dict, Optional

from config import LLM_BASE_URL, LLM_MODEL, LLM_TEMPERATURE

log = logging.getLogger(__name__)


class LLMReasoner:
    def __init__(self, model: str = LLM_MODEL, base_url: str = LLM_BASE_URL):
        self.model = model
        self.base_url = base_url
        self.temperature = LLM_TEMPERATURE

    async def analyze(self, narratives: List[Dict], launches: List[Dict], retries: int = 3) -> str:
        """Analyze narratives and launches with LLM"""
        prompt = self._build_prompt(narratives, launches)
        
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.base_url}/api/chat",
                        json={
                            "model": self.model,
                            "messages": [{"role": "user", "content": prompt}],
                            "stream": False,
                            "options": {"temperature": self.temperature}
                        },
                        timeout=aiohttp.ClientTimeout(total=60)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return data.get("message", {}).get("content", "No response from LLM")
                        else:
                            log.warning(f"LLM HTTP {resp.status}, attempt {attempt + 1}/{retries}")
                            
            except asyncio.TimeoutError:
                log.warning(f"LLM timeout, attempt {attempt + 1}/{retries}")
            except Exception as e:
                log.warning(f"LLM error: {e}, attempt {attempt + 1}/{retries}")
            
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        return "LLM reasoning unavailable — make sure Ollama is running with the model pulled."

    def _build_prompt(self, narratives: List[Dict], launches: List[Dict]) -> str:
        """Build analysis prompt"""
        # Top narratives by velocity
        top_narratives = sorted(narratives, key=lambda x: x.get("velocity_score", 0), reverse=True)[:15]
        
        # High-scoring launches
        high_score_launches = [l for l in launches if l.get("score", 0) >= 60][:10]
        
        return f"""You are ARGUS, a sharp crypto narrative analyst.

Current hot narratives (with velocity scores):
{json.dumps([
    {
        'keyword': n.get('keyword'),
        'velocity_score': n.get('velocity_score'),
        'source': n.get('source'),
        'mentions': n.get('mention_count')
    }
    for n in top_narratives
], indent=2)}

Promising new token launches (with ARGUS scores):
{json.dumps([
    {
        'symbol': l.get('symbol'),
        'score': l.get('score'),
        'narrative_fit': l.get('narrative_fit'),
        'matched_narratives': l.get('found_narratives', []),
        'volume_1h_usd': l.get('volume_usd_1h', 0),
        'txns_1h': l.get('txns_1h', 0)
    }
    for l in high_score_launches
], indent=2)}

Analyze in clear bullet points:
1. **Dominant Meta**: What's the main trend right now? (1-2 sentences)
2. **Top Opportunities**: 2-3 launches that best fit current narratives + why
3. **Red Flags**: Any obvious copycats, honeypots, or suspicious patterns
4. **Actionable Alpha**: One-sentence summary for traders (conviction + timing)

Be concise, skeptical, and data-driven. Focus on conviction and timing."""