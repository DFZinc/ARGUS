"""
Volume Monitor
--------------
Detects ETH tokens with genuine volume activity.
"""

import asyncio
import aiohttp
import logging
import json
import os
from typing import List, Dict, Optional, Set

from rate_limiter import RateLimiter
from config import (
    ETHERSCAN_API_KEY,
    MIN_VOLUME_USD_1H,
    MIN_LIQUIDITY_USD,
    MIN_TXNS_1H,
    DEXSCREENER_CONCURRENCY,
    GECKO_CONCURRENCY,
    GECKO_DELAY_SECS
)

log = logging.getLogger(__name__)

# API Endpoints
DEXSCREENER = "https://api.dexscreener.com"
GECKOTERMINAL = "https://api.geckoterminal.com/api/v2"
ETHERSCAN = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1

# Uniswap factory addresses
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

# Event topic signatures
V3_POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
V2_PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# Router addresses
UNISWAP_V2_ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
UNISWAP_V3_ROUTER = "0xe592427a0aece92de3edee1f18e0157c05861564"
UNISWAP_V3_ROUTER2 = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
UNISWAP_UNIVERSAL = "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad"
UNISWAP_UNIVERSAL_V4 = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"

# Infrastructure tokens — never trading opportunities
EXCLUDED_TOKENS: Set[str] = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0x0000000000000000000000000000000000000000",  # Null
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  # USD1
}

BLOCK_LOOKBACK = 1800  # ~6 hours of blocks at 12s/block


class VolumeMonitor:
    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rl = rate_limiter
        self.api_key = ETHERSCAN_API_KEY
        
        self.MIN_VOLUME_USD_1H = MIN_VOLUME_USD_1H
        self.MIN_LIQUIDITY_USD = MIN_LIQUIDITY_USD
        self.MIN_TXNS_1H = MIN_TXNS_1H
        
        self._seen_this_cycle: Set[str] = set()
        self._latest_block: Optional[int] = None

    async def get_active_tokens(self) -> List[Dict]:
        """Get all active tokens with real volume"""
        self._seen_this_cycle.clear()

        # Step 1: Get latest block
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            self._latest_block = await self._get_latest_block(session)

        if not self._latest_block:
            log.warning("Could not fetch latest block number")
            return []

        log.info(f"Block range: {self._latest_block - BLOCK_LOOKBACK} -> {self._latest_block}")

        # Step 2: Run all discovery sources concurrently
        gecko_semaphore = asyncio.Semaphore(GECKO_CONCURRENCY)
        
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            v3_new, v2_new, active, gecko = await asyncio.gather(
                self._fetch_v3_new_pools(session),
                self._fetch_v2_new_pairs(session),
                self._fetch_active_swaps(session),
                self._fetch_gecko_trending(session, gecko_semaphore),
                return_exceptions=True
            )

        v3_list = v3_new if isinstance(v3_new, list) else []
        v2_list = v2_new if isinstance(v2_new, list) else []
        act_list = active if isinstance(active, list) else []
        gecko_list = gecko if isinstance(gecko, list) else []

        log.info(
            f"Discovery: {len(v3_list)} V3 new | {len(v2_list)} V2 new | "
            f"{len(act_list)} active swaps | {len(gecko_list)} Gecko trending"
        )

        # Combine candidates that have confirmed trading activity
        active_set = set(act_list) | set(gecko_list)
        all_candidates = list(active_set)

        log.info(f"Validating {len(all_candidates)} unique candidates...")

        # Validate against DexScreener with controlled concurrency
        dex_semaphore = asyncio.Semaphore(DEXSCREENER_CONCURRENCY)
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            tasks = [
                self._fetch_pair_data(session, addr, dex_semaphore, gecko_semaphore)
                for addr in all_candidates
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        active_tokens = []
        failed = 0
        
        for token in results:
            if isinstance(token, Exception):
                failed += 1
                log.debug(f"Validation failed: {token}")
                continue
            if not token:
                continue
                
            address = token.get("address", "").lower()
            if not address or address in self._seen_this_cycle or address in EXCLUDED_TOKENS:
                continue
                
            if self._passes_thresholds(token):
                self._seen_this_cycle.add(address)
                active_tokens.append(token)
                log.info(
                    f"Active: {token.get('symbol')} ({token.get('dex','?')}) | "
                    f"Vol 1h: ${token.get('volume_usd_1h', 0):,.0f} | "
                    f"Txns: {token.get('txns_1h', 0)} | "
                    f"Liq: ${token.get('liquidity_usd', 0):,.0f}"
                )

        if failed:
            log.warning(f"{failed} DexScreener lookups failed")

        log.info(f"Found {len(active_tokens)} active token(s)")
        return active_tokens

    async def _fetch_v3_new_pools(self, session: aiohttp.ClientSession) -> List[str]:
        """Fetch V3 new pool creation events"""
        try:
            params = {
                "chainid": CHAIN_ID,
                "module": "logs",
                "action": "getLogs",
                "address": UNISWAP_V3_FACTORY,
                "topic0": V3_POOL_CREATED_TOPIC,
                "fromBlock": self._latest_block - BLOCK_LOOKBACK,
                "toBlock": self._latest_block,
                "page": 1,
                "offset": 100,
                "apikey": self.api_key,
            }
            if self.rl:
                await self.rl.acquire()
                
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning(f"V3 pool events HTTP {resp.status}")
                    return []
                    
                data = await resp.json()
                logs = data.get("result", [])
                if not isinstance(logs, list):
                    return []
                    
                tokens = []
                for entry in logs:
                    topics = entry.get("topics", [])
                    for i in [1, 2]:
                        if i < len(topics):
                            addr = ("0x" + topics[i][-40:]).lower()
                            if addr not in EXCLUDED_TOKENS:
                                tokens.append(addr)
                return list(set(tokens))
        except asyncio.TimeoutError:
            log.debug("V3 pool events timeout")
        except Exception as e:
            log.warning(f"V3 pool events error: {e}")
        return []

    async def _fetch_v2_new_pairs(self, session: aiohttp.ClientSession) -> List[str]:
        """Fetch V2 new pair creation events"""
        try:
            params = {
                "chainid": CHAIN_ID,
                "module": "logs",
                "action": "getLogs",
                "address": UNISWAP_V2_FACTORY,
                "topic0": V2_PAIR_CREATED_TOPIC,
                "fromBlock": self._latest_block - BLOCK_LOOKBACK,
                "toBlock": self._latest_block,
                "page": 1,
                "offset": 100,
                "apikey": self.api_key,
            }
            if self.rl:
                await self.rl.acquire()
                
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    log.warning(f"V2 pair events HTTP {resp.status}")
                    return []
                    
                data = await resp.json()
                logs = data.get("result", [])
                if not isinstance(logs, list):
                    return []
                    
                tokens = []
                for entry in logs:
                    topics = entry.get("topics", [])
                    for i in [1, 2]:
                        if i < len(topics):
                            addr = ("0x" + topics[i][-40:]).lower()
                            if addr not in EXCLUDED_TOKENS:
                                tokens.append(addr)
                return list(set(tokens))
        except asyncio.TimeoutError:
            log.debug("V2 pair events timeout")
        except Exception as e:
            log.warning(f"V2 pair events error: {e}")
        return []

    async def _fetch_active_swaps(self, session: aiohttp.ClientSession) -> List[str]:
        """Fetch tokens with recent swap activity"""
        token_counts: Dict[str, int] = {}
        routers = [
            UNISWAP_V2_ROUTER, UNISWAP_V3_ROUTER, UNISWAP_V3_ROUTER2,
            UNISWAP_UNIVERSAL, UNISWAP_UNIVERSAL_V4
        ]
        
        for router in routers:
            try:
                params = {
                    "chainid": CHAIN_ID,
                    "module": "account",
                    "action": "tokentx",
                    "address": router,
                    "page": 1,
                    "offset": 1000,
                    "sort": "desc",
                    "apikey": self.api_key,
                }
                if self.rl:
                    await self.rl.acquire()
                    
                async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        continue
                        
                    data = await resp.json()
                    txs = data.get("result", [])
                    if not isinstance(txs, list):
                        continue
                        
                    for tx in txs:
                        addr = tx.get("contractAddress", "").lower()
                        if addr and addr not in EXCLUDED_TOKENS:
                            token_counts[addr] = token_counts.get(addr, 0) + 1
            except asyncio.TimeoutError:
                log.debug(f"Router {router} timeout")
            except Exception as e:
                log.debug(f"Router activity error: {e}")
                
        return list(token_counts.keys())

    async def _fetch_gecko_trending(self, session: aiohttp.ClientSession, gecko_semaphore: asyncio.Semaphore) -> List[str]:
        """Fetch trending pools from GeckoTerminal"""
        async with gecko_semaphore:
            try:
                await asyncio.sleep(GECKO_DELAY_SECS)
                url = f"{GECKOTERMINAL}/networks/eth/trending_pools"
                params = {"include": "base_token", "page": 1}
                
                async with session.get(
                    url,
                    params=params,
                    headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"GeckoTerminal trending HTTP {resp.status}")
                        return []
                        
                    data = await resp.json()
                    pools = data.get("data", [])
                    tokens = []
                    
                    for pool in pools:
                        relationships = pool.get("relationships", {})
                        base_token = relationships.get("base_token", {}).get("data", {})
                        token_id = base_token.get("id", "")
                        
                        if token_id.startswith("eth_"):
                            addr = token_id[4:].lower()
                            if addr not in EXCLUDED_TOKENS:
                                tokens.append(addr)
                                
                    log.debug(f"GeckoTerminal returned {len(tokens)} trending tokens")
                    return tokens
            except asyncio.TimeoutError:
                log.debug("GeckoTerminal timeout")
            except Exception as e:
                log.warning(f"GeckoTerminal error: {e}")
                return []

    async def _fetch_pair_data(
        self,
        session: aiohttp.ClientSession,
        token_address: str,
        semaphore: asyncio.Semaphore,
        gecko_semaphore: asyncio.Semaphore
    ) -> Optional[Dict]:
        """Fetch pair data from DexScreener with GeckoTerminal fallback"""
        # Try DexScreener first
        async with semaphore:
            try:
                url = f"{DEXSCREENER}/token-pairs/v1/ethereum/{token_address}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = data if isinstance(data, list) else data.get("pairs", [])
                        eth_pairs = [p for p in pairs if p.get("chainId") == "ethereum"]
                        
                        if eth_pairs:
                            best = max(eth_pairs, key=lambda p: float((p.get("volume") or {}).get("h1", 0) or 0))
                            return self._normalize_dex_pair(best)
                    elif resp.status == 429:
                        log.debug(f"DexScreener rate limited for {token_address}")
            except asyncio.TimeoutError:
                log.debug(f"DexScreener timeout for {token_address}")
            except Exception as e:
                log.debug(f"DexScreener error for {token_address}: {e}")

        # Fallback to GeckoTerminal
        async with gecko_semaphore:
            try:
                await asyncio.sleep(GECKO_DELAY_SECS)
                url = f"{GECKOTERMINAL}/networks/eth/tokens/{token_address}/pools"
                params = {"page": 1}
                
                async with session.get(
                    url,
                    params=params,
                    headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return None
                        
                    data = await resp.json()
                    pools = data.get("data", [])
                    if not pools:
                        return None
                        
                    best = max(
                        pools,
                        key=lambda p: float(p.get("attributes", {}).get("volume_usd", {}).get("h24", 0) or 0)
                    )
                    return self._normalize_gecko_pool(best, token_address)
            except Exception as e:
                log.debug(f"GeckoTerminal fallback error for {token_address}: {e}")
                return None

    def _normalize_dex_pair(self, pair: Dict) -> Dict:
        """Normalize DexScreener pair data"""
        vol = pair.get("volume", {})
        pc = pair.get("priceChange", {})
        txns = pair.get("txns", {}).get("h1", {})
        
        return {
            "address": pair.get("baseToken", {}).get("address", "").lower(),
            "symbol": pair.get("baseToken", {}).get("symbol", "UNKNOWN"),
            "name": pair.get("baseToken", {}).get("name", ""),
            "pair_address": pair.get("pairAddress", ""),
            "dex": pair.get("dexId", ""),
            "volume_usd_1h": float(vol.get("h1", 0) or 0),
            "volume_usd_6h": float(vol.get("h6", 0) or 0),
            "volume_usd_24h": float(vol.get("h24", 0) or 0),
            "liquidity_usd": float((pair.get("liquidity") or {}).get("usd", 0) or 0),
            "price_change_pct_1h": float(pc.get("h1", 0) or 0),
            "txns_1h": int(txns.get("buys", 0)) + int(txns.get("sells", 0)),
            "market_cap": float(pair.get("marketCap", 0) or 0),
            "pair_created_at": pair.get("pairCreatedAt", 0),
        }

    def _normalize_gecko_pool(self, pool: Dict, token_address: str) -> Dict:
        """Normalize GeckoTerminal pool data"""
        attrs = pool.get("attributes", {})
        vol = attrs.get("volume_usd", {})
        pc = attrs.get("price_change_percentage", {})
        
        return {
            "address": token_address.lower(),
            "symbol": attrs.get("name", "UNKNOWN").split("/")[0].strip(),
            "name": attrs.get("name", ""),
            "pair_address": attrs.get("address", ""),
            "dex": "geckoterminal",
            "volume_usd_1h": float(vol.get("h1", 0) or 0),
            "volume_usd_6h": float(vol.get("h6", 0) or 0),
            "volume_usd_24h": float(vol.get("h24", 0) or 0),
            "liquidity_usd": float(attrs.get("reserve_in_usd", 0) or 0),
            "price_change_pct_1h": float(pc.get("h1", 0) or 0),
            "txns_1h": (
                int(attrs.get("transactions", {}).get("h1", {}).get("buys", 0)) +
                int(attrs.get("transactions", {}).get("h1", {}).get("sells", 0))
            ),
            "market_cap": float(attrs.get("market_cap_usd", 0) or 0),
            "pair_created_at": 0,  # Not available from Gecko
        }

    def _passes_thresholds(self, token: Dict) -> bool:
        """Check if token meets minimum thresholds"""
        if token.get("volume_usd_1h", 0) < self.MIN_VOLUME_USD_1H:
            return False
        if token.get("liquidity_usd", 0) < self.MIN_LIQUIDITY_USD:
            return False
        if token.get("txns_1h", 0) < self.MIN_TXNS_1H:
            return False
        return True

    async def _get_latest_block(self, session: aiohttp.ClientSession) -> Optional[int]:
        """Get latest Ethereum block number"""
        try:
            params = {
                "chainid": CHAIN_ID,
                "module": "proxy",
                "action": "eth_blockNumber",
                "apikey": self.api_key,
            }
            if self.rl:
                await self.rl.acquire()
                
            async with session.get(ETHERSCAN, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = data.get("result", "")
                    if result and isinstance(result, str) and result.startswith("0x"):
                        return int(result, 16)
        except Exception as e:
            log.warning(f"Block number error: {e}")
        return None