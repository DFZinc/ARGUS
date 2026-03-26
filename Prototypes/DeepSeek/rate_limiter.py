"""
Rate Limiter
------------
Enforces API rate limits with a token bucket algorithm.
"""

import asyncio
import time
import logging

log = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, calls_per_second: int = 5):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second  # seconds between calls
        self._lock = asyncio.Lock()
        self._last_call = 0.0
        self._tokens = calls_per_second
        self._last_refill = time.monotonic()

    async def acquire(self):
        """Call before every API request to enforce rate limits."""
        async with self._lock:
            now = time.monotonic()
            
            # Refill tokens based on time elapsed
            elapsed = now - self._last_refill
            new_tokens = elapsed * self.calls_per_second
            self._tokens = min(self.calls_per_second, self._tokens + new_tokens)
            self._last_refill = now
            
            # If no tokens available, wait
            if self._tokens < 1:
                wait_time = (1 - self._tokens) / self.calls_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= 1
            
            # Ensure minimum interval between calls
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            
            self._last_call = time.monotonic()