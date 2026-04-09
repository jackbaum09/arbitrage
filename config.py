"""
Configuration for the arbitrage scanner.
Reads from the same Supabase instance as prediction_markets-master.
"""

from __future__ import annotations

import os
import logging
import psycopg2
from urllib.parse import quote_plus
from typing import Any

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

log: logging.Logger = logging.getLogger(__name__)

DB_PASSWORD: str = os.environ.get("DB_PASSWORD", "")
DB_HOST: str = os.environ.get("DB_HOST", "")
DB_PORT: str = os.environ.get("DB_PORT", "6543")
DB_NAME: str = os.environ.get("DB_NAME", "postgres")
DB_USER: str = os.environ.get("DB_USER", "")


def get_db_connection() -> Any:
    password: str = quote_plus(DB_PASSWORD)
    conn_string: str = f"postgresql://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return psycopg2.connect(conn_string)


# ---------------------------------------------------------------------------
# Fee structures (per contract / per dollar)
# ---------------------------------------------------------------------------

# Kalshi: no trading fee on most contracts, but 7c settlement fee on winning
# contracts (i.e., if you win, you get $1 - $0.07 = $0.93 net per contract).
# Some contracts are fee-free — we use the conservative default.
KALSHI_SETTLEMENT_FEE: float = 0.07

# Polymarket: no explicit trading fee, but there is spread cost baked into
# the order book. We model a conservative 2% effective fee to account for
# slippage and the UMA resolution bond / gas costs on withdrawal.
POLYMARKET_EFFECTIVE_FEE: float = 0.02


# ---------------------------------------------------------------------------
# Scanner settings
# ---------------------------------------------------------------------------

# Minimum ROI (after fees) to flag as an opportunity
MIN_ROI_THRESHOLD: float = 0.005  # 0.5%

# Minimum combined liquidity across both legs (in dollars)
MIN_LIQUIDITY: float = 100.0

# Scan interval (overridable via --interval CLI arg)
SCAN_INTERVAL_MINUTES: int = 5

# ---------------------------------------------------------------------------
# Order book / liquidity settings
# ---------------------------------------------------------------------------

# Target position size ($) for VWAP executable price calculation
ORDERBOOK_TARGET_SIZE: float = 100.0

# Timeout (seconds) per order book API call
ORDERBOOK_FETCH_TIMEOUT: float = 5.0

# Max parallel API calls for order book fetching
ORDERBOOK_MAX_WORKERS: int = 10

# API base URLs for live order book data
KALSHI_API_BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_CLOB_BASE_URL: str = "https://clob.polymarket.com"
POLYMARKET_GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"

# Futures tables (contain both Kalshi and Polymarket rows)
FUTURES_TABLES: list[str] = [
    "nba_prediction_futures",
    "cbb_prediction_futures",
    "mlb_prediction_futures",
    "nhl_prediction_futures",
    "nfl_prediction_futures",
]

# Cross-table game market pairs: (Kalshi game table, Polymarket futures table)
GAME_MARKET_PAIRS: list[tuple[str, str]] = [
    ("nba_prediction_game_markets", "nba_prediction_futures"),
    ("cbb_prediction_game_markets", "cbb_prediction_futures"),
    ("mlb_prediction_game_markets", "mlb_prediction_futures"),
    ("nhl_prediction_game_markets", "nhl_prediction_futures"),
]


# ---------------------------------------------------------------------------
# Execution settings
# ---------------------------------------------------------------------------

# Master kill switch — must be "true" AND --execute flag set to trade
EXECUTION_ENABLED: bool = os.environ.get("EXECUTION_ENABLED", "false").lower() == "true"

# Kalshi credentials
KALSHI_API_KEY_ID: str = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH: str = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
KALSHI_API_MODE: str = os.environ.get("KALSHI_API_MODE", "demo")  # "demo" or "live"

# Polymarket credentials
POLYMARKET_PRIVATE_KEY: str = os.environ.get("POLYMARKET_PRIVATE_KEY", "")

# Risk limits (conservative defaults)
MAX_POSITION_SIZE: float = float(os.environ.get("MAX_POSITION_SIZE", "50.0"))
MAX_TOTAL_CAPITAL: float = float(os.environ.get("MAX_TOTAL_CAPITAL", "500.0"))
MAX_SINGLE_TRADE: float = float(os.environ.get("MAX_SINGLE_TRADE", "100.0"))
MIN_EXECUTION_ROI: float = float(os.environ.get("MIN_EXECUTION_ROI", "0.01"))
MAX_OPEN_POSITIONS: int = int(os.environ.get("MAX_OPEN_POSITIONS", "10"))
FILL_TIMEOUT_SECONDS: float = float(os.environ.get("FILL_TIMEOUT_SECONDS", "30"))
