"""
Polymarket US trading client.

Wraps the `polymarket-us` SDK (PyPI, v0.1.2+) for order placement and
account queries against the US-regulated Polymarket exchange
(gateway.polymarket.us + api.polymarket.us).

This client is deliberately API-compatible with execution/polymarket_client.py
where the method names are identical (get_balance_dollars, get_address,
get_order, cancel_order, cancel_all, get_open_orders) so execution/manager.py
can branch on venue with minimal signature changes.

`place_order` takes a different signature because PM US market semantics
differ from international Gamma:
  - Markets are slug-keyed (`aec-{sport}-{away}-{home}-{YYYY-MM-DD}`), not
    twin-token keyed. There is no YES/NO token pair — a single slug has two
    `marketSides` (e.g. CLE + ATL), and a trader picks which side to go
    long/short on via an `intent` field.
  - Quantities are integer share counts; prices are 3-decimal dollar values
    packed into a dict: `{"value": "0.480", "currency": "USD"}`.

Auth is Ed25519 request signing (X-PM-Access-Key / X-PM-Timestamp /
X-PM-Signature) — the SDK handles this internally.

Wiring into execution/manager.py and main._init_execution is deferred to
Path X Phase 4.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from execution.models import TradeOrder, TradeResult

log = logging.getLogger(__name__)

# Retry / backoff config for transient network and 5xx failures.
#
# Like the Polygon client, we only retry the network call — we do NOT
# retry a successful submission. The SDK generates a fresh signature per
# request so retrying a duplicate-POST risks a double-fill on a REST
# endpoint that accepted the first attempt but returned a flaky 5xx.
_PMUS_MAX_RETRIES = 3
_PMUS_BACKOFF_BASE = 0.5  # seconds


def _is_transient_error(exc: Exception) -> bool:
    """
    Classify a polymarket-us SDK exception as transient (safe-ish to retry).

    We retry only when the server provably did not accept the order: raw
    connection errors, timeouts, explicit 5xx, and 429 rate-limit. All 4xx
    (400 bad request, 401 auth, 403 permission, 404 not found) are
    non-transient and must surface immediately.
    """
    try:
        from polymarket_us.errors import (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        )
    except ImportError:
        # SDK not installed — treat nothing as transient so the underlying
        # ImportError bubbles up from the caller instead.
        return False

    if isinstance(exc, (APIConnectionError, APITimeoutError, InternalServerError, RateLimitError)):
        return True
    # Best-effort string match for transient-looking errors that slip
    # through as a generic APIStatusError.
    msg = str(exc).lower()
    return any(
        tok in msg
        for tok in ("timeout", "timed out", "connection reset", "bad gateway",
                    "service unavailable", "gateway timeout", "temporarily")
    )


def _with_retries(label: str, fn, *, max_retries: int = _PMUS_MAX_RETRIES):
    """Invoke fn() with exponential backoff on transient errors."""
    delay = _PMUS_BACKOFF_BASE
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            if not _is_transient_error(exc):
                raise
            last_exc = exc
            if attempt == max_retries:
                break
            log.warning(
                f"Polymarket US {label} transient error (attempt {attempt}/{max_retries}): "
                f"{exc} — retrying in {delay:.1f}s"
            )
            time.sleep(delay)
            delay *= 2
    assert last_exc is not None
    raise last_exc


def _format_amount(price: float) -> dict[str, str]:
    """
    Convert a float limit price into PM US Amount shape.

    Market `orderPriceMinTickSize` is 0.001, so prices are rounded to 3
    decimals. `currency` is always USD for sports markets.
    """
    return {"value": f"{round(float(price), 3):.3f}", "currency": "USD"}


def _parse_amount(amt: Any) -> float:
    """Parse an Amount dict back to a float, defaulting to 0.0 on any error."""
    if not isinstance(amt, dict):
        return 0.0
    raw = amt.get("value")
    try:
        return float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


# ---------- Intent / TIF mapping ----------

# Canonical (lowercase) intent the caller supplies ↔ PM US string constant.
# For arbitrage we place buys only, but sell_* is included for completeness
# so position unwinds go through the same client.
_INTENT_MAP = {
    "buy_long": "ORDER_INTENT_BUY_LONG",
    "buy_short": "ORDER_INTENT_BUY_SHORT",
    "sell_long": "ORDER_INTENT_SELL_LONG",
    "sell_short": "ORDER_INTENT_SELL_SHORT",
}

# IOC is the correct TIF for arbitrage: we computed the ROI using currently
# displayed depth, so if the fill doesn't happen immediately we'd rather
# bail than leave resting orders that could be picked off later.
_TIF_MAP = {
    "IOC": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    "FOK": "TIME_IN_FORCE_FILL_OR_KILL",
    "GTC": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    "GTD": "TIME_IN_FORCE_GOOD_TILL_DATE",
}


class PolymarketUSClient:
    """Authenticated Polymarket US trading client."""

    def __init__(self, key_id: str, secret_key: str):
        if not key_id or not secret_key:
            raise ValueError(
                "PolymarketUSClient requires both key_id and secret_key. "
                "Generate them at https://polymarket.us/developer and set "
                "PM_US_KEY_ID / PM_US_SECRET_KEY in .env."
            )

        try:
            from polymarket_us import PolymarketUS
        except ImportError:
            raise ImportError(
                "polymarket-us is required for Polymarket US trading. "
                "Install with: pip install polymarket-us"
            )

        self.key_id = key_id
        self._client = PolymarketUS(key_id=key_id, secret_key=secret_key)
        log.info("Polymarket US client initialized")

    # ----- Trading -----

    def place_order(
        self,
        market_slug: str,
        intent: str,
        price: float,
        size: int,
        client_order_id: str | None = None,
        outcome_side: str | None = None,
        tif: str = "IOC",
    ) -> TradeResult:
        """
        Place an order on Polymarket US.

        Args:
            market_slug: The market slug (e.g. `aec-mlb-cle-atl-2026-04-11`).
            intent: One of "buy_long", "buy_short", "sell_long", "sell_short".
                PM US markets have two marketSides; "long" means betting the
                market resolves true for the long side, "short" means betting
                it resolves false. Arbitrage callers use only buy_*.
            price: Limit price (0.001-0.999, rounded to 3 decimals).
            size: Number of shares (integer).
            client_order_id: Optional UUID for local dedupe/tracking. PM US
                does not accept a client_order_id in the create request
                itself, so this is tracked only on the TradeOrder record.
            outcome_side: Optional label ("yes"/"no" or "long"/"short") for
                tracking purposes. Not sent to the API.
            tif: Time-in-force code. Default "IOC" (immediate-or-cancel)
                which is the right choice for arbitrage.

        Returns:
            TradeResult with status "placed", "filled", "partial", or "error".
        """
        intent_normalized = intent.lower().replace("-", "_")
        pm_intent = _INTENT_MAP.get(intent_normalized)
        if not pm_intent:
            return TradeResult(
                order=TradeOrder(
                    platform="polymarket_us",
                    market_id=market_slug,
                    side=outcome_side or intent_normalized,
                    action="buy" if intent_normalized.startswith("buy") else "sell",
                    price=price,
                    size=size,
                    client_order_id=client_order_id or str(uuid.uuid4()),
                ),
                status="error",
                error=f"Unknown intent '{intent}'; expected one of {list(_INTENT_MAP)}",
            )

        pm_tif = _TIF_MAP.get(tif.upper(), _TIF_MAP["IOC"])

        cid = client_order_id or str(uuid.uuid4())
        tracked_side = outcome_side or (
            "long" if "long" in intent_normalized else "short"
        )
        action = "buy" if intent_normalized.startswith("buy") else "sell"

        order = TradeOrder(
            platform="polymarket_us",
            market_id=market_slug,
            side=tracked_side,
            action=action,
            price=price,
            size=size,
            client_order_id=cid,
        )

        params = {
            "marketSlug": market_slug,
            "intent": pm_intent,
            "type": "ORDER_TYPE_LIMIT",
            "price": _format_amount(price),
            "quantity": int(size),
            "tif": pm_tif,
        }

        try:
            resp = _with_retries(
                "create_order", lambda: self._client.orders.create(params)
            )

            order_id = None
            status = "placed"
            filled_size = 0
            filled_price = price

            if isinstance(resp, dict):
                order_id = resp.get("id")
                execs = resp.get("executions") or []
                if execs:
                    # IOC orders return any fills inline in `executions[]`.
                    fills = [e for e in execs if isinstance(e, dict)]
                    filled_size = sum(
                        int(float(e.get("lastShares") or 0)) for e in fills
                    )
                    # Weighted-avg fill price across all executions, falling
                    # back to the limit price if we can't compute it.
                    qty_price_pairs = [
                        (float(e.get("lastShares") or 0), _parse_amount(e.get("lastPx")))
                        for e in fills
                    ]
                    total_qty = sum(q for q, _ in qty_price_pairs)
                    if total_qty > 0:
                        filled_price = (
                            sum(q * p for q, p in qty_price_pairs) / total_qty
                        )
                    if filled_size >= size:
                        status = "filled"
                    elif filled_size > 0:
                        status = "partial"
                    else:
                        # IOC with no fill → cancelled by the engine
                        status = "cancelled"

            return TradeResult(
                order=order,
                order_id=order_id,
                status=status,
                filled_size=filled_size,
                filled_price=filled_price,
            )

        except Exception as e:
            log.error(f"Polymarket US order failed: {e}")
            return TradeResult(order=order, status="error", error=str(e))

    def preview_order(
        self,
        market_slug: str,
        intent: str,
        price: float,
        size: int,
        tif: str = "IOC",
    ) -> dict:
        """
        Dry-run an order via POST /v1/order/preview.

        Useful for smoke-testing credentials without risking a fill. Returns
        the raw SDK response dict (or an empty dict on error).
        """
        pm_intent = _INTENT_MAP.get(intent.lower().replace("-", "_"))
        if not pm_intent:
            return {"error": f"Unknown intent '{intent}'"}
        params = {
            "request": {
                "marketSlug": market_slug,
                "intent": pm_intent,
                "type": "ORDER_TYPE_LIMIT",
                "price": _format_amount(price),
                "quantity": int(size),
                "tif": _TIF_MAP.get(tif.upper(), _TIF_MAP["IOC"]),
            }
        }
        try:
            return _with_retries(
                "preview_order", lambda: self._client.orders.preview(params)
            ) or {}
        except Exception as e:
            log.warning(f"Polymarket US preview_order failed: {e}")
            return {"error": str(e)}

    def get_order(self, order_id: str) -> dict:
        """Get a single order by ID (for fill polling)."""
        try:
            return _with_retries(
                "get_order", lambda: self._client.orders.retrieve(order_id)
            ) or {}
        except Exception as e:
            log.warning(f"Failed to fetch Polymarket US order {order_id}: {e}")
            return {}

    def cancel_order(self, order_id: str, market_slug: str | None = None) -> bool:
        """
        Cancel an open order. Returns True on success.

        PM US requires the `marketSlug` alongside the order ID because the
        exchange is sharded per-slug. Callers must track the slug for any
        order they might want to cancel.
        """
        if not market_slug:
            log.error(
                f"cancel_order requires market_slug for PM US; got None for {order_id}"
            )
            return False
        try:
            self._client.orders.cancel(order_id, {"marketSlug": market_slug})
            return True
        except Exception as e:
            log.error(f"Failed to cancel Polymarket US order {order_id}: {e}")
            return False

    def cancel_all(self, market_slugs: list[str] | None = None) -> bool:
        """
        Cancel all open orders, optionally scoped to a list of slugs.
        """
        try:
            params = {"slugs": market_slugs} if market_slugs else None
            self._client.orders.cancel_all(params)
            return True
        except Exception as e:
            log.error(f"Failed to cancel all Polymarket US orders: {e}")
            return False

    def get_open_orders(self, market_slugs: list[str] | None = None) -> list:
        """Get all open orders, optionally scoped to a list of slugs."""
        try:
            params = {"slugs": market_slugs} if market_slugs else None
            resp = self._client.orders.list(params)
            if isinstance(resp, dict):
                return resp.get("orders") or []
            return []
        except Exception as e:
            log.error(f"Failed to fetch Polymarket US open orders: {e}")
            return []

    # ----- Account / balance -----

    def get_balance_allowance(self) -> dict:
        """
        Return PM US account balance in a shape roughly compatible with
        the Polygon PolymarketClient.get_balance_allowance() return shape.

        PM US has no on-chain allowance concept (funds are exchange-
        custodied, not ERC-20 approved), so `allowance` is a sentinel
        large value so downstream risk checks that gate on allowance > 0
        don't false-fire.

        Returns:
            dict like {"balance": "<usdc_base_units>", "allowance": "<sentinel>"}
            where balance is in USDC base units (6 decimals) for parity
            with the Polygon client.
        """
        try:
            resp = self._client.account.balances()
        except Exception as e:
            log.error(f"Failed to fetch Polymarket US balance: {e}")
            return {}
        if not isinstance(resp, dict):
            return {}
        balances = resp.get("balances") or []
        if not balances:
            return {"balance": "0", "allowance": "0"}

        # The response is a list in case of multi-currency; pick the USD
        # entry (should be the only one for sports).
        usd = next(
            (b for b in balances if isinstance(b, dict) and b.get("currency", "USD") == "USD"),
            balances[0],
        )
        # `buyingPower` is the most useful number for risk gating — it's
        # currentBalance minus openOrders minus marginRequirement. If the
        # SDK returns a float dollars value, convert to USDC base units so
        # the caller gets the same shape as the Polygon client.
        buying_power = usd.get("buyingPower")
        current_balance = usd.get("currentBalance")
        dollars = (
            float(buying_power) if buying_power is not None
            else float(current_balance or 0)
        )
        base_units = int(round(dollars * 1e6))
        # Sentinel allowance: PM US always "allows" — set to balance so
        # any sanity check that compares `allowance >= balance` passes.
        return {"balance": str(base_units), "allowance": str(base_units)}

    def get_balance_dollars(self) -> float:
        """Return USDC buying power in dollars (convenience for risk checks)."""
        ba = self.get_balance_allowance()
        raw = ba.get("balance")
        if raw is None:
            return 0.0
        try:
            return float(raw) / 1e6
        except (TypeError, ValueError):
            return 0.0

    def get_address(self) -> str | None:
        """
        Return an account identifier for logging / execution records.

        PM US is not a wallet-based venue — there is no Polygon address.
        The closest analog is the API key ID, which uniquely identifies
        the developer account the scanner is trading from.
        """
        return self.key_id

    # ----- Context manager -----

    def close(self) -> None:
        """Close the underlying HTTP client."""
        try:
            self._client.close()
        except Exception:
            pass

    def __enter__(self) -> "PolymarketUSClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
