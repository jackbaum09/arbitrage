"""
Data models for arbitrage opportunities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Opportunity:
    """A detected arbitrage opportunity with fee-adjusted ROI."""

    # Identification
    sport: str                  # e.g. "nba", "mlb"
    market_type: str            # e.g. "championship", "mvp", "game"
    outcome: str                # e.g. "Boston Celtics", "Shohei Ohtani"

    # Prices (raw, before fees)
    buy_yes_platform: str       # platform where we buy YES
    buy_yes_price: float        # price paid for YES contract
    buy_no_platform: str        # platform where we buy NO
    buy_no_price: float         # price paid for NO contract

    # Fee-adjusted financials
    total_cost: float           # buy_yes_price + buy_no_price
    gross_profit: float         # 1.0 - total_cost (per $1 of contracts)
    fees: float                 # total fees across both legs
    net_profit: float           # gross_profit - fees
    roi: float                  # net_profit / total_cost

    # Liquidity / sizing
    kalshi_volume: float | None = None
    polymarket_liquidity: float | None = None

    # Executable prices (from live order book VWAP)
    buy_yes_executable_price: float | None = None
    buy_no_executable_price: float | None = None
    buy_yes_depth: float | None = None          # $ fillable on YES leg
    buy_no_depth: float | None = None           # $ fillable on NO leg
    max_executable_size: float | None = None    # min(buy_yes_depth, buy_no_depth)
    liquidity_verified: bool = False

    # Original midpoint prices (preserved when overwritten by executable prices)
    buy_yes_midpoint: float | None = None
    buy_no_midpoint: float | None = None

    # Platform market IDs (for order book lookups)
    kalshi_market_id: str | None = None
    polymarket_market_id: str | None = None

    # Capital requirements for a $100 notional position
    capital_required: float = 0.0

    # Metadata
    source_table: str = ""
    detected_at: datetime = field(default_factory=datetime.utcnow)

    # Unique key for deduplication across scans
    @property
    def opportunity_key(self) -> str:
        return f"{self.sport}|{self.market_type}|{self.outcome}|{self.buy_yes_platform}"
