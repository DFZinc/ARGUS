from datetime import datetime
from typing import List, Dict
import logging

from volume_monitor import VolumeMonitor
from rate_limiter import RateLimiter

log = logging.getLogger(__name__)


class LaunchDetector:
    def __init__(self, rate_limiter: RateLimiter):
        self.monitor = VolumeMonitor(rate_limiter)
        self._seen_launches = set()  # Track seen launches to avoid duplicates

    async def get_new_launches(self) -> List[Dict]:
        """Get newly launched tokens with volume"""
        all_tokens = await self.monitor.get_active_tokens()
        
        # Filter to launches < 6 hours old with volume
        cutoff = datetime.now().timestamp() - (6 * 3600)
        
        new_launches = []
        for token in all_tokens:
            # Check if token is new (not seen before)
            token_addr = token.get("address", "")
            if token_addr in self._seen_launches:
                continue
            
            # Check creation time (fallback to current time if not available)
            created_at = token.get("pair_created_at", datetime.now().timestamp())
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at).timestamp()
                except (ValueError, TypeError):
                    created_at = datetime.now().timestamp()
            
            if created_at > cutoff:
                self._seen_launches.add(token_addr)
                new_launches.append(token)
        
        # Clean seen launches set occasionally
        if len(self._seen_launches) > 1000:
            self._seen_launches = set(list(self._seen_launches)[-500:])
        
        log.info(f"Found {len(new_launches)} new launches in last 6 hours")
        return new_launches[:20]  # top 20 newest