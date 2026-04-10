"""
Pre-trade risk controls.

Every check must pass before an order is placed. These are conservative
defaults — adjust via config.py as you gain confidence in the system.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from scanner.models import Opportunity

log = logging.getLogger(__name__)


@dataclass
class RiskLimits:
    """Configurable risk parameters."""

    max_position_size: float = 50.0         # max $ per single leg
    max_total_capital: float = 500.0        # max $ deployed across all positions
    max_single_trade: float = 100.0         # max $ per individual trade
    min_roi_threshold: float = 0.01         # minimum 1% ROI to execute
    max_open_positions: int = 10            # max concurrent arb positions
    require_liquidity_verified: bool = True  # only trade verified opportunities


def check_risk(
    opportunity: Opportunity,
    deployed_capital: float,
    open_position_count: int,
    limits: RiskLimits,
) -> tuple[bool, str]:
    """
    Run all pre-trade risk checks.

    Returns (allowed, reason). If allowed is False, reason explains why.
    """
    # 1. Liquidity verification
    if limits.require_liquidity_verified and not opportunity.liquidity_verified:
        return False, "Opportunity not liquidity-verified"

    # 2. ROI threshold
    if opportunity.roi < limits.min_roi_threshold:
        return False, f"ROI {opportunity.roi:.2%} below minimum {limits.min_roi_threshold:.2%}"

    # 3. Open position count
    if open_position_count >= limits.max_open_positions:
        return False, f"At max open positions ({limits.max_open_positions})"

    # 4. Trade size
    trade_cost = opportunity.capital_required
    if trade_cost > limits.max_single_trade:
        return False, f"Trade cost ${trade_cost:.2f} exceeds max ${limits.max_single_trade:.2f}"

    # 5. Total deployed capital
    if deployed_capital + trade_cost > limits.max_total_capital:
        return False, (
            f"Would exceed max capital: ${deployed_capital:.2f} + ${trade_cost:.2f} "
            f"> ${limits.max_total_capital:.2f}"
        )

    # 6. Individual leg size
    # Compute the number of contracts that would actually be placed,
    # matching the sizing formula in execution/manager.py, then check the
    # resulting per-leg dollar exposure against max_position_size.
    max_spend = min(
        opportunity.capital_required,
        limits.max_single_trade,
        opportunity.max_executable_size or limits.max_single_trade,
    )
    num_contracts = max(1, int(max_spend / max(opportunity.total_cost, 0.01)))
    yes_leg_dollars = num_contracts * opportunity.buy_yes_price
    no_leg_dollars = num_contracts * opportunity.buy_no_price
    if yes_leg_dollars > limits.max_position_size or no_leg_dollars > limits.max_position_size:
        return False, (
            f"Leg dollars (YES=${yes_leg_dollars:.2f}, NO=${no_leg_dollars:.2f}) "
            f"exceed max position size ${limits.max_position_size:.2f}"
        )

    # 7. Order book depth
    if opportunity.buy_yes_depth is not None and opportunity.buy_no_depth is not None:
        min_depth = min(opportunity.buy_yes_depth, opportunity.buy_no_depth)
        if min_depth < trade_cost:
            return False, f"Order book depth ${min_depth:.2f} insufficient for trade ${trade_cost:.2f}"

    return True, "All risk checks passed"
