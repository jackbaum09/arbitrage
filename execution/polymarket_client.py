"""
Polymarket CLOB trading client.

Wraps the py-clob-client SDK for order placement and management.
Requires a funded Polygon wallet with USDC.
"""

from __future__ import annotations

import logging
import uuid

from execution.models import TradeOrder, TradeResult

log = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"

# Chain ID for Polygon mainnet
POLYGON_CHAIN_ID = 137


class PolymarketClient:
    """Authenticated Polymarket CLOB trading client."""

    def __init__(self, private_key: str, host: str = CLOB_HOST):
        try:
            from py_clob_client.client import ClobClient
        except ImportError:
            raise ImportError(
                "py-clob-client is required for Polymarket trading. "
                "Install with: pip install py-clob-client"
            )

        self.host = host
        self._client = ClobClient(
            host=host,
            key=private_key,
            chain_id=POLYGON_CHAIN_ID,
        )

        # Derive API credentials from the private key
        try:
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            log.info("Polymarket client initialized and authenticated")
        except Exception as e:
            log.error(f"Failed to derive Polymarket API credentials: {e}")
            raise

    # ----- Trading -----

    def place_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: int,
        client_order_id: str | None = None,
        outcome_side: str | None = None,
    ) -> TradeResult:
        """
        Place a limit order on Polymarket.

        Args:
            token_id: CLOB token ID for the outcome
            side: "buy" or "sell" (the order action)
            price: Limit price (0.01 - 0.99)
            size: Number of shares
            client_order_id: UUID for deduplication
            outcome_side: "yes" or "no" — which outcome token this token_id
                          represents, for accurate execution tracking. If None,
                          falls back to mapping side ("buy"->"yes", "sell"->"no").
        """
        from py_clob_client.order_builder.constants import BUY, SELL

        cid = client_order_id or str(uuid.uuid4())
        tracked_outcome = outcome_side or ("yes" if side.upper() == "BUY" else "no")
        order = TradeOrder(
            platform="polymarket",
            market_id=token_id,
            side=tracked_outcome,
            action=side.lower(),
            price=price,
            size=size,
            client_order_id=cid,
        )

        clob_side = BUY if side.upper() == "BUY" else SELL

        try:
            signed_order = self._client.create_order(
                token_id=token_id,
                price=price,
                size=size,
                side=clob_side,
            )
            resp = self._client.post_order(signed_order)

            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("id")

            return TradeResult(
                order=order,
                order_id=order_id,
                status="placed",
                filled_price=price,
            )

        except Exception as e:
            log.error(f"Polymarket order failed: {e}")
            return TradeResult(order=order, status="error", error=str(e))

    def get_order(self, order_id: str) -> dict:
        """Get the current state of a single order (for fill polling)."""
        try:
            return self._client.get_order(order_id) or {}
        except Exception as e:
            log.warning(f"Failed to fetch Polymarket order {order_id}: {e}")
            return {}

    def get_balance_allowance(self) -> dict:
        """
        Return USDC balance and CTF Exchange spending allowance for the
        wallet. Both values must be sufficient before placing an order.

        Returns a dict like {"balance": "1000000", "allowance": "500000"}
        in USDC base units (6 decimals). Convert to dollars by dividing by 1e6.
        """
        try:
            return self._client.get_balance_allowance() or {}
        except Exception as e:
            log.error(f"Failed to fetch Polymarket balance/allowance: {e}")
            return {}

    def get_balance_dollars(self) -> float:
        """Return USDC balance in dollars (convenience for risk checks)."""
        ba = self.get_balance_allowance()
        raw = ba.get("balance")
        if raw is None:
            return 0.0
        try:
            return float(raw) / 1e6
        except (TypeError, ValueError):
            return 0.0

    def get_address(self) -> str | None:
        """Return the wallet address derived from the private key."""
        try:
            return self._client.get_address()
        except Exception:
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        try:
            self._client.cancel(order_id)
            return True
        except Exception as e:
            log.error(f"Failed to cancel Polymarket order {order_id}: {e}")
            return False

    def cancel_all(self) -> bool:
        """Cancel all open orders."""
        try:
            self._client.cancel_all()
            return True
        except Exception as e:
            log.error(f"Failed to cancel all Polymarket orders: {e}")
            return False

    def get_open_orders(self) -> list:
        """Get all open orders."""
        try:
            return self._client.get_orders() or []
        except Exception as e:
            log.error(f"Failed to fetch Polymarket orders: {e}")
            return []
