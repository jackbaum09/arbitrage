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

# Scan interval
SCAN_INTERVAL_MINUTES: int = 30

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
