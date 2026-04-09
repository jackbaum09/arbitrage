"""
Live order book fetching and executable price calculation.

Fetches order book depth from Polymarket CLOB API and Kalshi REST API,
computes VWAP executable prices at a target position size, and provides
liquidity depth information for filtering false-positive arbitrage signals.

Both APIs are public (no authentication required for reading order books).
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests

from config import (
    KALSHI_API_BASE_URL,
    POLYMARKET_CLOB_BASE_URL,
    POLYMARKET_GAMMA_BASE_URL,
    ORDERBOOK_FETCH_TIMEOUT,
    ORDERBOOK_MAX_WORKERS,
    ORDERBOOK_TARGET_SIZE,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Polymarket token_id resolution (cached per process lifetime)
# ---------------------------------------------------------------------------

_token_id_cache: dict[str, list[str]] = {}


def get_polymarket_token_ids(gamma_market_id: str) -> list[str]:
    """
    Resolve a Polymarket Gamma API market ID to CLOB token IDs.

    Binary markets return 2 token IDs: index 0 = YES, index 1 = NO.
    Results are cached in-memory so each market is resolved at most once.

    Returns an empty list on failure (graceful degradation).
    """
    if gamma_market_id in _token_id_cache:
        return _token_id_cache[gamma_market_id]

    try:
        resp = requests.get(
            f"{POLYMARKET_GAMMA_BASE_URL}/markets/{gamma_market_id}",
            timeout=ORDERBOOK_FETCH_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        tokens = data.get("clobTokenIds")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not isinstance(tokens, list):
            tokens = []

        _token_id_cache[gamma_market_id] = tokens
        return tokens

    except Exception as e:
        log.warning(f"Failed to resolve Polymarket token IDs for {gamma_market_id}: {e}")
        _token_id_cache[gamma_market_id] = []
        return []


# ---------------------------------------------------------------------------
# Order book fetching
# ---------------------------------------------------------------------------


def fetch_polymarket_orderbook(token_id: str) -> dict | None:
    """
    Fetch the full order book from Polymarket CLOB API.

    Returns dict with 'bids' and 'asks' arrays, each containing
    {'price': str, 'size': str} entries. Returns None on failure.
    """
    try:
        resp = requests.get(
            f"{POLYMARKET_CLOB_BASE_URL}/book",
            params={"token_id": token_id},
            timeout=ORDERBOOK_FETCH_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"Polymarket orderbook fetch failed for {token_id}: {e}")
        return None


def fetch_kalshi_orderbook(ticker: str) -> dict | None:
    """
    Fetch the order book from Kalshi API.

    Returns the parsed JSON response containing orderbook data.
    Returns None on failure.
    """
    try:
        resp = requests.get(
            f"{KALSHI_API_BASE_URL}/markets/{ticker}/orderbook",
            timeout=ORDERBOOK_FETCH_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"Kalshi orderbook fetch failed for {ticker}: {e}")
        return None


# ---------------------------------------------------------------------------
# VWAP calculation
# ---------------------------------------------------------------------------


def compute_executable_price(
    orders: list[tuple[float, float]],
    target_size: float,
) -> tuple[float, float] | None:
    """
    Walk an order book ladder and compute the volume-weighted average price
    (VWAP) to fill a target dollar size.

    Args:
        orders: List of (price, size_in_dollars) sorted best-to-worst
                (lowest price first for asks / buy side).
        target_size: Target fill size in dollars.

    Returns:
        (vwap, fillable_size) tuple, or None if the book is empty.
        If total available size < target_size, returns VWAP for whatever
        is available with the actual fillable size.
    """
    if not orders:
        return None

    filled = 0.0
    cost = 0.0

    for price, size in orders:
        if price <= 0:
            continue
        remaining = target_size - filled
        fill_at_level = min(size, remaining)
        cost += price * fill_at_level
        filled += fill_at_level
        if filled >= target_size:
            break

    if filled <= 0:
        return None

    vwap = cost / filled
    return (round(vwap, 4), round(filled, 2))


def _parse_polymarket_asks(book: dict) -> list[tuple[float, float]]:
    """Extract ask-side orders from Polymarket book response, sorted price ascending."""
    asks = book.get("asks", [])
    parsed = []
    for order in asks:
        try:
            price = float(order.get("price", 0))
            size = float(order.get("size", 0))
            if price > 0 and size > 0:
                parsed.append((price, size))
        except (ValueError, TypeError):
            continue
    parsed.sort(key=lambda x: x[0])
    return parsed


def _parse_polymarket_bids(book: dict) -> list[tuple[float, float]]:
    """Extract bid-side orders from Polymarket book response, sorted price descending."""
    bids = book.get("bids", [])
    parsed = []
    for order in bids:
        try:
            price = float(order.get("price", 0))
            size = float(order.get("size", 0))
            if price > 0 and size > 0:
                parsed.append((price, size))
        except (ValueError, TypeError):
            continue
    parsed.sort(key=lambda x: x[0], reverse=True)
    return parsed


def _parse_kalshi_book_side(levels: list) -> list[tuple[float, float]]:
    """
    Parse Kalshi orderbook levels (list of [price_str, size_str] pairs).

    Kalshi returns bid levels sorted ascending by price. The best bid is the
    last element (highest price). For asks, we derive them from the opposite
    side: YES ask = 1.0 - NO bid.

    Returns list of (price, size) tuples sorted price ascending.
    """
    parsed = []
    for level in levels:
        try:
            price = float(level[0])
            size = float(level[1])
            if price > 0 and size > 0:
                parsed.append((price, size))
        except (ValueError, TypeError, IndexError):
            continue
    parsed.sort(key=lambda x: x[0])
    return parsed


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


@dataclass
class ExecutablePrices:
    """Executable price data for both platforms on both sides."""

    kalshi_yes_ask_vwap: float | None = None
    kalshi_no_ask_vwap: float | None = None
    kalshi_yes_depth: float = 0.0
    kalshi_no_depth: float = 0.0

    polymarket_yes_ask_vwap: float | None = None
    polymarket_no_ask_vwap: float | None = None
    polymarket_yes_depth: float = 0.0
    polymarket_no_depth: float = 0.0


def _compute_kalshi_executable(book: dict, target_size: float) -> dict:
    """
    Compute executable prices from a Kalshi orderbook response.

    Kalshi binary markets: to BUY YES, you lift the YES ask side.
    The orderbook_fp contains yes_dollars (YES bids) and no_dollars (NO bids).
    YES ask = derived from NO bids: ask_price = 1.0 - no_bid_price.
    Similarly, NO ask = derived from YES bids: ask_price = 1.0 - yes_bid_price.
    """
    result = {
        "yes_ask_vwap": None, "no_ask_vwap": None,
        "yes_depth": 0.0, "no_depth": 0.0,
    }

    orderbook = book.get("orderbook_fp") or book.get("orderbook", {})
    yes_bids = _parse_kalshi_book_side(orderbook.get("yes_dollars") or orderbook.get("yes", []))
    no_bids = _parse_kalshi_book_side(orderbook.get("no_dollars") or orderbook.get("no", []))

    # YES ask side: derived from NO bids (buy YES = sell NO)
    # NO bid at price P means YES ask at (1.0 - P)
    # Best YES ask = lowest (1.0 - no_bid), so we want highest NO bids first
    yes_asks = [(round(1.0 - price, 4), size) for price, size in reversed(no_bids) if price < 1.0]
    r = compute_executable_price(yes_asks, target_size)
    if r:
        result["yes_ask_vwap"], result["yes_depth"] = r

    # NO ask side: derived from YES bids
    no_asks = [(round(1.0 - price, 4), size) for price, size in reversed(yes_bids) if price < 1.0]
    r = compute_executable_price(no_asks, target_size)
    if r:
        result["no_ask_vwap"], result["no_depth"] = r

    return result


def _compute_polymarket_executable(
    yes_book: dict | None,
    no_book: dict | None,
    target_size: float,
) -> dict:
    """
    Compute executable prices from Polymarket order books.

    To BUY YES: lift the asks on the YES token book.
    To BUY NO: lift the asks on the NO token book.
    If only the YES book is available, buying NO = selling YES (lift YES bids),
    and the effective NO ask price = 1.0 - YES bid price.
    """
    result = {
        "yes_ask_vwap": None, "no_ask_vwap": None,
        "yes_depth": 0.0, "no_depth": 0.0,
    }

    # YES side: use asks from the YES token book
    if yes_book:
        asks = _parse_polymarket_asks(yes_book)
        r = compute_executable_price(asks, target_size)
        if r:
            result["yes_ask_vwap"], result["yes_depth"] = r

    # NO side: prefer asks from the NO token book if available
    if no_book:
        asks = _parse_polymarket_asks(no_book)
        r = compute_executable_price(asks, target_size)
        if r:
            result["no_ask_vwap"], result["no_depth"] = r
    elif yes_book:
        # Fallback: buying NO = selling YES = lifting YES bids
        # Effective NO ask price = 1.0 - YES bid price
        bids = _parse_polymarket_bids(yes_book)
        no_asks = [(round(1.0 - price, 4), size) for price, size in bids if price < 1.0]
        r = compute_executable_price(no_asks, target_size)
        if r:
            result["no_ask_vwap"], result["no_depth"] = r

    return result


def get_executable_prices(
    kalshi_ticker: str | None,
    polymarket_gamma_id: str | None,
    target_size: float = ORDERBOOK_TARGET_SIZE,
) -> ExecutablePrices | None:
    """
    Fetch live order books and compute executable VWAP prices for both platforms.

    Returns an ExecutablePrices dataclass, or None if both API calls fail.
    Partial results (one platform succeeds, the other fails) are returned
    with None values for the failed platform.
    """
    kalshi_book = None
    pm_yes_book = None
    pm_no_book = None

    # Fetch order books in parallel
    with ThreadPoolExecutor(max_workers=ORDERBOOK_MAX_WORKERS) as pool:
        futures = {}

        if kalshi_ticker:
            futures[pool.submit(fetch_kalshi_orderbook, kalshi_ticker)] = "kalshi"

        pm_token_ids = []
        if polymarket_gamma_id:
            pm_token_ids = get_polymarket_token_ids(polymarket_gamma_id)
            if len(pm_token_ids) >= 1:
                futures[pool.submit(fetch_polymarket_orderbook, pm_token_ids[0])] = "pm_yes"
            if len(pm_token_ids) >= 2:
                futures[pool.submit(fetch_polymarket_orderbook, pm_token_ids[1])] = "pm_no"

        for future in as_completed(futures):
            label = futures[future]
            try:
                result = future.result()
                if label == "kalshi":
                    kalshi_book = result
                elif label == "pm_yes":
                    pm_yes_book = result
                elif label == "pm_no":
                    pm_no_book = result
            except Exception as e:
                log.warning(f"Order book fetch failed ({label}): {e}")

    if kalshi_book is None and pm_yes_book is None:
        return None

    prices = ExecutablePrices()

    if kalshi_book:
        k = _compute_kalshi_executable(kalshi_book, target_size)
        prices.kalshi_yes_ask_vwap = k["yes_ask_vwap"]
        prices.kalshi_no_ask_vwap = k["no_ask_vwap"]
        prices.kalshi_yes_depth = k["yes_depth"]
        prices.kalshi_no_depth = k["no_depth"]

    pm = _compute_polymarket_executable(pm_yes_book, pm_no_book, target_size)
    prices.polymarket_yes_ask_vwap = pm["yes_ask_vwap"]
    prices.polymarket_no_ask_vwap = pm["no_ask_vwap"]
    prices.polymarket_yes_depth = pm["yes_depth"]
    prices.polymarket_no_depth = pm["no_depth"]

    return prices
