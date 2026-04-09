"""
Kalshi trading API client.

Handles RSA-PSS authentication and order management.
Defaults to the demo API for safe testing.
"""

from __future__ import annotations

import base64
import logging
import time
import uuid

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from execution.models import TradeOrder, TradeResult

log = logging.getLogger(__name__)

DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient:
    """Authenticated Kalshi trading client."""

    def __init__(
        self,
        api_key_id: str,
        private_key_path: str,
        base_url: str = DEMO_BASE_URL,
        timeout: float = 10.0,
    ):
        self.api_key_id = api_key_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._private_key = self._load_private_key(private_key_path)
        log.info(f"Kalshi client initialized (base={self.base_url})")

    @staticmethod
    def _load_private_key(path: str) -> rsa.RSAPrivateKey:
        with open(path, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Generate authentication headers for a Kalshi API request."""
        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{path}"

        signature = self._private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, json_body: dict | None = None) -> dict:
        """Make an authenticated request to the Kalshi API."""
        headers = self._sign_request(method, path)
        url = f"{self.base_url}{path}"

        resp = requests.request(
            method, url, headers=headers, json=json_body, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    # ----- Trading -----

    def place_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price_cents: int,
        client_order_id: str | None = None,
    ) -> TradeResult:
        """
        Place a limit order on Kalshi.

        Args:
            ticker: Market ticker (e.g., "KXNHL-26-BOS")
            side: "yes" or "no"
            action: "buy" or "sell"
            count: Number of contracts
            price_cents: Price in cents (1-99)
            client_order_id: UUID for deduplication (auto-generated if None)
        """
        cid = client_order_id or str(uuid.uuid4())
        order = TradeOrder(
            platform="kalshi",
            market_id=ticker,
            side=side,
            action=action,
            price=price_cents / 100.0,
            size=count,
            client_order_id=cid,
        )

        price_field = f"{side}_price"
        body = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            price_field: price_cents,
            "client_order_id": cid,
            "type": "limit",
        }

        try:
            data = self._request("POST", "/portfolio/orders", body)
            order_data = data.get("order", {})
            order_id = order_data.get("order_id")
            status = order_data.get("status", "resting")

            # Map Kalshi status to our status
            result_status = {
                "resting": "placed",
                "executed": "filled",
                "canceled": "cancelled",
            }.get(status, "placed")

            return TradeResult(
                order=order,
                order_id=order_id,
                status=result_status,
                filled_size=order_data.get("remaining_count", 0),
                filled_price=price_cents / 100.0,
            )

        except requests.HTTPError as e:
            log.error(f"Kalshi order failed: {e.response.status_code} {e.response.text}")
            return TradeResult(order=order, status="error", error=str(e))
        except Exception as e:
            log.error(f"Kalshi order failed: {e}")
            return TradeResult(order=order, status="error", error=str(e))

    def get_order(self, order_id: str) -> dict:
        """Get the status of a specific order."""
        return self._request("GET", f"/portfolio/orders/{order_id}")

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if successful."""
        try:
            self._request("DELETE", f"/portfolio/orders/{order_id}")
            return True
        except Exception as e:
            log.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def get_balance(self) -> float:
        """Get available balance in dollars."""
        data = self._request("GET", "/portfolio/balance")
        return data.get("balance", 0) / 100.0

    def get_positions(self) -> list[dict]:
        """Get all current positions."""
        data = self._request("GET", "/portfolio/positions")
        return data.get("market_positions", [])

    def get_open_orders(self) -> list[dict]:
        """Get all resting (open) orders."""
        data = self._request("GET", "/portfolio/orders?status=resting")
        return data.get("orders", [])
