"""
Data models for trade execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TradeOrder:
    """A single order to be placed on one platform."""

    platform: str           # "kalshi" or "polymarket"
    market_id: str          # ticker (Kalshi) or token_id (Polymarket)
    side: str               # "yes" or "no"
    action: str             # "buy" or "sell"
    price: float            # limit price (0-1 scale, e.g. 0.55)
    size: int               # number of contracts/shares
    client_order_id: str    # UUID for deduplication


@dataclass
class TradeResult:
    """Result of placing a single order."""

    order: TradeOrder
    order_id: str | None = None     # platform-assigned order ID
    status: str = "pending"         # placed, filled, partial, rejected, error, cancelled
    filled_size: int = 0
    filled_price: float = 0.0
    error: str | None = None


@dataclass
class ArbitrageExecution:
    """Result of executing a full two-leg arbitrage trade."""

    opportunity_key: str
    yes_leg: TradeResult | None = None
    no_leg: TradeResult | None = None
    status: str = "pending"         # success, partial, failed, risk_blocked
    total_cost: float = 0.0
    expected_profit: float = 0.0
    error: str | None = None
    executed_at: datetime = field(default_factory=datetime.utcnow)
