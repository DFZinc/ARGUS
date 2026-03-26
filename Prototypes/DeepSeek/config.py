# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Polling & Agent Settings
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 300))

# API Keys
LUNARCRUSH_BEARER = os.getenv("LUNARCRUSH_BEARER", "")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Rate Limits & Concurrency
DEXSCREENER_CONCURRENCY = int(os.getenv("DEXSCREENER_CONCURRENCY", 5))
GECKO_CONCURRENCY = int(os.getenv("GECKO_CONCURRENCY", 1))
GECKO_DELAY_SECS = float(os.getenv("GECKO_DELAY_SECS", 5.0))

# Token Filters
MIN_VOLUME_USD_1H = int(os.getenv("MIN_VOLUME_USD_1H", 5000))
MIN_LIQUIDITY_USD = int(os.getenv("MIN_LIQUIDITY_USD", 5000))
MIN_TXNS_1H = int(os.getenv("MIN_TXNS_1H", 10))

# LLM Settings
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek-r1:8b")
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", 0.6))

# Dashboard Settings
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", 8002))