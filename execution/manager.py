"""
Two-leg arbitrage execution orchestrator.

Coordinates placing orders on both platforms, monitors fills,
and handles partial fills / unwinds.
"""

from __future__ import annotations

import logging
import time
import uuid

from scanner.models import Opportunity
from scanner.orderbook import get_polymarket_token_ids
from execution.models import TradeOrder, TradeResult, ArbitrageExecution
from execution.kalshi_client import KalshiClient
from execution.polymarket_client import PolymarketClient
from execution.risk import RiskLimits, check_risk
from execution.store import record_execution, get_open_execution_count, get_deployed_capital

log = logging.getLogger(__name__)

# How long to wait for a fill before giving up (seconds)
DEFAULT_FILL_TIMEOUT = 30


def _resolve_polymarket_token(
    polymarket_market_id: str | None, side: str
) -> str | None:
    """Resolve the correct Polymarket CLOB token ID for a given side."""
    if not polymarket_market_id:
        return None

    token_ids = get_polymarket_token_ids(polymarket_market_id)
    if not token_ids:
        return None

    # index 0 = YES token, index 1 = NO token
    if side == "yes" and len(token_ids) >= 1:
        return token_ids[0]
    if side == "no" and len(token_ids) >= 2:
        return token_ids[1]

    return None


def _wait_for_fill(
    kalshi_client: KalshiClient | None,
    polymarket_client: PolymarketClient | None,
    order_id: str,
    platform: str,
    timeout: float = DEFAULT_FILL_TIMEOUT,
) -> str:
    """
    Poll for order fill status on either platform.

    Returns final status: "filled", "cancelled", "resting", or "placed"
    when polling isn't possible.
    """
    if not order_id:
        return "placed"

    deadline = time.time() + timeout

    if platform == "kalshi":
        if not kalshi_client:
            return "placed"
        while time.time() < deadline:
            try:
                data = kalshi_client.get_order(order_id)
                order = data.get("order", {})
                status = order.get("status", "resting")
                if status == "executed":
                    return "filled"
                if status == "canceled":
                    return "cancelled"
                time.sleep(2)
            except Exception as e:
                log.warning(f"Error polling Kalshi order {order_id}: {e}")
                time.sleep(2)
        return "resting"

    if platform == "polymarket":
        if not polymarket_client:
            return "placed"
        while time.time() < deadline:
            try:
                data = polymarket_client.get_order(order_id)
                # py-clob-client returns a dict with 'status' (e.g.,
                # 'MATCHED', 'LIVE', 'CANCELED'). Map to our taxonomy.
                status = (data.get("status") or "").upper()
                size_matched = float(data.get("size_matched") or 0)
                original_size = float(data.get("original_size") or 0)
                if status == "MATCHED" or (
                    original_size and size_matched >= original_size
                ):
                    return "filled"
                if status in ("CANCELED", "CANCELLED"):
                    return "cancelled"
                time.sleep(2)
            except Exception as e:
                log.warning(f"Error polling Polymarket order {order_id}: {e}")
                time.sleep(2)
        return "resting"

    return "placed"


def execute_opportunity(
    opp: Opportunity,
    kalshi_client: KalshiClient | None,
    polymarket_client: PolymarketClient | None,
    risk_limits: RiskLimits,
    fill_timeout: float = DEFAULT_FILL_TIMEOUT,
) -> ArbitrageExecution:
    """
    Execute a two-leg arbitrage trade.

    Places orders on both platforms for the given opportunity.
    Uses limit orders at the VWAP executable price.
    """
    execution = ArbitrageExecution(opportunity_key=opp.opportunity_key)

    # ----- Risk checks -----
    deployed = get_deployed_capital()
    open_count = get_open_execution_count()
    allowed, reason = check_risk(opp, deployed, open_count, risk_limits)

    if not allowed:
        log.info(f"Risk blocked: {opp.outcome} — {reason}")
        execution.status = "risk_blocked"
        execution.error = reason
        record_execution(execution)
        return execution

    # ----- Resolve market IDs -----
    # Determine which platform gets the YES buy and which gets the NO buy
    yes_platform = opp.buy_yes_platform
    no_platform = opp.buy_no_platform

    # Calculate order size (1 contract = $1 payout, cost = price * contracts)
    # Use min of max_executable_size and risk limit, then convert to contracts
    max_spend = min(
        opp.capital_required,
        risk_limits.max_single_trade,
        opp.max_executable_size or risk_limits.max_single_trade,
    )
    # Number of contracts: spend / (yes_price + no_price) = spend / total_cost
    num_contracts = max(1, int(max_spend / max(opp.total_cost, 0.01)))

    log.info(
        f"Executing: {opp.outcome} | {num_contracts} contracts | "
        f"YES@{opp.buy_yes_price:.2f} on {yes_platform} + "
        f"NO@{opp.buy_no_price:.2f} on {no_platform}"
    )

    # ----- Place YES leg -----
    yes_result = _place_leg(
        platform=yes_platform,
        side="yes",
        price=opp.buy_yes_price,
        size=num_contracts,
        kalshi_market_id=opp.kalshi_market_id,
        polymarket_market_id=opp.polymarket_market_id,
        kalshi_client=kalshi_client,
        polymarket_client=polymarket_client,
    )
    execution.yes_leg = yes_result

    if yes_result.status == "error":
        execution.status = "failed"
        execution.error = f"YES leg failed: {yes_result.error}"
        log.error(f"YES leg failed for {opp.outcome}: {yes_result.error}")
        record_execution(execution)
        return execution

    # Wait for YES leg fill on whichever platform it landed on
    if yes_result.order_id:
        fill_status = _wait_for_fill(
            kalshi_client,
            polymarket_client,
            yes_result.order_id,
            yes_platform,
            fill_timeout,
        )
        yes_result.status = fill_status

        if fill_status not in ("filled", "placed"):
            # YES leg didn't fill — cancel and abort before placing the NO leg
            log.warning(f"YES leg didn't fill ({fill_status}), cancelling")
            if yes_platform == "kalshi" and kalshi_client:
                kalshi_client.cancel_order(yes_result.order_id)
            elif yes_platform == "polymarket" and polymarket_client:
                polymarket_client.cancel_order(yes_result.order_id)
            execution.status = "failed"
            execution.error = f"YES leg timed out ({fill_status})"
            record_execution(execution)
            return execution

    # ----- Place NO leg -----
    no_result = _place_leg(
        platform=no_platform,
        side="no",
        price=opp.buy_no_price,
        size=num_contracts,
        kalshi_market_id=opp.kalshi_market_id,
        polymarket_market_id=opp.polymarket_market_id,
        kalshi_client=kalshi_client,
        polymarket_client=polymarket_client,
    )
    execution.no_leg = no_result

    if no_result.status == "error":
        # NO leg failed — attempt to unwind YES leg
        log.error(f"NO leg failed for {opp.outcome}: {no_result.error}")
        _attempt_unwind(yes_result, kalshi_client, polymarket_client)
        execution.status = "partial"
        execution.error = f"NO leg failed: {no_result.error}"
        record_execution(execution)
        return execution

    # ----- Success -----
    execution.status = "success"
    execution.total_cost = round(
        (opp.buy_yes_price + opp.buy_no_price) * num_contracts, 2
    )
    execution.expected_profit = round(opp.net_profit * num_contracts, 2)

    log.info(
        f"Execution SUCCESS: {opp.outcome} | "
        f"{num_contracts} contracts | cost=${execution.total_cost:.2f} | "
        f"expected profit=${execution.expected_profit:.2f}"
    )

    record_execution(execution)
    return execution


def _place_leg(
    platform: str,
    side: str,
    price: float,
    size: int,
    kalshi_market_id: str | None,
    polymarket_market_id: str | None,
    kalshi_client: KalshiClient | None,
    polymarket_client: PolymarketClient | None,
) -> TradeResult:
    """Place a single leg of the arbitrage trade."""
    cid = str(uuid.uuid4())

    if platform == "kalshi":
        if not kalshi_client:
            return TradeResult(
                order=TradeOrder("kalshi", "", side, "buy", price, size, cid),
                status="error",
                error="Kalshi client not configured",
            )
        if not kalshi_market_id:
            return TradeResult(
                order=TradeOrder("kalshi", "", side, "buy", price, size, cid),
                status="error",
                error="No Kalshi market ID",
            )
        price_cents = max(1, min(99, round(price * 100)))
        return kalshi_client.place_order(
            ticker=kalshi_market_id,
            side=side,
            action="buy",
            count=size,
            price_cents=price_cents,
            client_order_id=cid,
        )

    elif platform == "polymarket":
        if not polymarket_client:
            return TradeResult(
                order=TradeOrder("polymarket", "", side, "buy", price, size, cid),
                status="error",
                error="Polymarket client not configured",
            )
        token_id = _resolve_polymarket_token(polymarket_market_id, side)
        if not token_id:
            return TradeResult(
                order=TradeOrder("polymarket", "", side, "buy", price, size, cid),
                status="error",
                error=f"Could not resolve Polymarket token for {side}",
            )
        # Buying a YES or NO token = BUY on the respective token's book
        return polymarket_client.place_order(
            token_id=token_id,
            side="BUY",
            price=price,
            size=size,
            client_order_id=cid,
            outcome_side=side,
        )

    else:
        return TradeResult(
            order=TradeOrder(platform, "", side, "buy", price, size, cid),
            status="error",
            error=f"Unknown platform: {platform}",
        )


def _attempt_unwind(
    leg: TradeResult,
    kalshi_client: KalshiClient | None,
    polymarket_client: PolymarketClient | None,
) -> None:
    """Attempt to cancel or unwind a placed leg after the other leg fails."""
    if not leg.order_id:
        return

    log.warning(f"Attempting to unwind {leg.order.platform} order {leg.order_id}")

    if leg.order.platform == "kalshi" and kalshi_client:
        kalshi_client.cancel_order(leg.order_id)
    elif leg.order.platform == "polymarket" and polymarket_client:
        polymarket_client.cancel_order(leg.order_id)
