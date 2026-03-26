import asyncio
import aiohttp
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional

from config import LUNARCRUSH_BEARER, X_BEARER_TOKEN, APIFY_TOKEN
from rate_limiter import RateLimiter

log = logging.getLogger(__name__)


class NarrativeScout:
    def __init__(self, rate_limiter: RateLimiter):
        self.rl = rate_limiter
        self.apify_token = APIFY_TOKEN

    async def get_current_narratives(self) -> List[Dict]:
        """Real-time narratives from LunarCrush + X + TikTok"""
        narratives = []

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            # Fetch all sources concurrently
            tasks = [self._fetch_lunarcrush(session)]
            tasks.append(self._fetch_x_semantic(session))
            
            if self.apify_token:
                tasks.append(self._fetch_tiktok_trending(session))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    log.debug(f"Source fetch error: {result}")
                elif isinstance(result, list):
                    narratives.extend(result)

        # Dedupe & rank by velocity
        seen = {}
        for n in narratives:
            key = n["keyword"].lower()
            if key not in seen or n["velocity_score"] > seen[key]["velocity_score"]:
                seen[key] = n

        ranked = sorted(seen.values(), key=lambda x: x["velocity_score"], reverse=True)
        log.info(f"🔥 ARGUS found {len(ranked)} live narratives")
        return ranked[:20]

    async def _fetch_lunarcrush(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Free LunarCrush public trending topics"""
        try:
            await self.rl.acquire()
            url = "https://lunarcrush.com/api4/public/topics/list/v1"
            headers = {"Authorization": f"Bearer {LUNARCRUSH_BEARER}"}
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    topics = data.get("data", [])[:15]
                    return [{
                        "keyword": t.get("topic", "").lower(),
                        "velocity_score": min(100, int(t.get("interactions_1h", 0) / 10) + int(t.get("mentions_1h", 0))),
                        "mention_count": t.get("mentions_1h", 0),
                        "engagement": t.get("interactions_1h", 0),
                        "source": "LunarCrush",
                        "first_seen": datetime.now().isoformat()
                    } for t in topics if t.get("interactions_1h", 0) > 50]
        except asyncio.TimeoutError:
            log.debug("LunarCrush timeout")
        except Exception as e:
            log.debug(f"LunarCrush failed: {e}")
        return []

    async def _fetch_x_semantic(self, session: aiohttp.ClientSession) -> List[Dict]:
        """X recent search with concurrent queries"""
        queries = ["memecoin launch", "new pump", "fair launch", "agent coin", "gen z pepe"]
        
        async def fetch_query(q: str) -> Optional[Dict]:
            try:
                await self.rl.acquire()
                url = "https://api.x.com/2/tweets/search/recent"
                params = {
                    "query": q,
                    "max_results": 50,
                    "tweet.fields": "public_metrics,created_at"
                }
                headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
                
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        tweets = data.get("data", [])
                        if tweets:
                            score = self._calc_virality(tweets)
                            return {
                                "keyword": q,
                                "velocity_score": score,
                                "mention_count": len(tweets),
                                "engagement": sum(
                                    t["public_metrics"]["like_count"] + t["public_metrics"]["retweet_count"]
                                    for t in tweets
                                ),
                                "source": "X",
                                "first_seen": tweets[0]["created_at"]
                            }
            except asyncio.TimeoutError:
                log.debug(f"X timeout for query: {q}")
            except Exception as e:
                log.debug(f"X failed for {q}: {e}")
            return None
        
        tasks = [fetch_query(q) for q in queries]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def _fetch_tiktok_trending(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Apify TikTok scraper with proper dataset fetching"""
        if not self.apify_token:
            return []
        
        try:
            await self.rl.acquire()
            
            # Start TikTok scraper run
            url = "https://api.apify.com/v2/acts/clockworks~tiktok-scraper/runs"
            payload = {
                "input": {
                    "hashtags": ["memecoin", "launch", "pepe", "pump"],
                    "limit": 30,
                    "searchType": "hashtag"
                },
                "maxItems": 30
            }
            headers = {
                "Authorization": f"Bearer {self.apify_token}",
                "Content-Type": "application/json"
            }
            
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    run_id = data.get("data", {}).get("id")
                    
                    if run_id:
                        # Wait briefly for results
                        await asyncio.sleep(5)
                        
                        # Fetch results
                        dataset_url = f"https://api.apify.com/v2/datasets/{run_id}/items"
                        async with session.get(dataset_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as dataset_resp:
                            if dataset_resp.status == 200:
                                items = await dataset_resp.json()
                                if items:
                                    # Aggregate hashtag frequencies
                                    hashtag_counts = {}
                                    for item in items[:30]:
                                        hashtags = item.get("hashtags", [])
                                        for tag in hashtags:
                                            tag_lower = tag.lower()
                                            hashtag_counts[tag_lower] = hashtag_counts.get(tag_lower, 0) + 1
                                    
                                    # Return top hashtags
                                    top_hashtags = sorted(hashtag_counts.items(), key=lambda x: x[1], reverse=True)[:10]
                                    return [{
                                        "keyword": tag,
                                        "velocity_score": min(100, count * 20),
                                        "mention_count": count,
                                        "engagement": count * 50,
                                        "source": "TikTok",
                                        "first_seen": datetime.now().isoformat()
                                    } for tag, count in top_hashtags]
        except asyncio.TimeoutError:
            log.debug("TikTok Apify timeout")
        except Exception as e:
            log.debug(f"TikTok Apify failed: {e}")
        return []

    def _calc_virality(self, tweets: List[Dict]) -> int:
        """Calculate virality score from tweets"""
        count = len(tweets)
        total_eng = sum(
            t["public_metrics"]["like_count"] + t["public_metrics"]["retweet_count"]
            for t in tweets
        )
        avg_eng = total_eng / max(count, 1)
        return min(100, int(count * 3 + avg_eng * 0.8))