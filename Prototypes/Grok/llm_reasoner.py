# llm_reasoner.py
import aiohttp
import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)

class LLMReasoner:
    def __init__(self, model: str = "deepseek-r1:8b"):
        self.model = model
        self.base_url = "http://localhost:11434"   # Ollama default

    async def analyze(self, narratives: list, launches: list) -> str:
        prompt = f"""You are ARGUS, a sharp crypto narrative analyst.

Current hot narratives:
{json.dumps([n.get('keyword') for n in narratives[:15]], indent=2)}

Promising new token launches (with ARGUS raw scores):
{json.dumps([{'symbol': l.get('symbol'), 'score': l.get('score'), 'matched': l.get('found_narratives', [])} for l in launches if l.get('score', 0) >= 60], indent=2)}

Analyze in clear bullet points:
1. Dominant meta / trend right now (1 sentence)
2. Top 2-3 launches that best fit the current narrative + why
3. Any red flags or obvious copycats
4. One-sentence actionable alpha summary for traders

Be concise, skeptical, and data-driven. Focus on conviction and timing."""

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/chat",
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False,
                        "options": {"temperature": 0.6}
                    },
                    timeout=60
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("message", {}).get("content", "No response from LLM")
                    else:
                        return f"LLM error: HTTP {resp.status}"
        except Exception as e:
            log.warning(f"Ollama LLM call failed: {e}")
            return "LLM reasoning unavailable — make sure Ollama is running with the model pulled."
