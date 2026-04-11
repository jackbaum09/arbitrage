#!/usr/bin/env python3
"""
Polymarket US account health check.

One-shot diagnostic that reads PM_US_KEY_ID / PM_US_SECRET_KEY from .env,
initializes the SDK, and prints:
  - account identifier (key_id)
  - USDC buying power + current balance
  - open orders count
  - whether the account is ready for live trading

Exit codes:
  0  account fully ready (buying power > 0)
  1  credentials not set — can't run check
  2  SDK not installed or client init failed
  3  balance fetch failed
  5  buying power is zero — account not funded

Usage:
    cd /home/ec2-user/arbitrage
    source venv/bin/activate
    python3 deploy/check_polymarket_us.py

Generate credentials at https://polymarket.us/developer, then set:
    PM_US_KEY_ID=<uuid>
    PM_US_SECRET_KEY=<base64-ed25519-seed>
in /home/ec2-user/arbitrage/.env (chmod 600 after).
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(HERE), ".env"))


def main() -> int:
    key_id = os.environ.get("PM_US_KEY_ID", "")
    secret_key = os.environ.get("PM_US_SECRET_KEY", "")

    if not key_id or not secret_key:
        print("PM_US_KEY_ID / PM_US_SECRET_KEY not set in .env", file=sys.stderr)
        print("  Generate at https://polymarket.us/developer and paste them")
        print("  into /home/ec2-user/arbitrage/.env (then chmod 600 .env).")
        return 1

    try:
        from execution.polymarket_us_client import PolymarketUSClient
    except ImportError as e:
        print(f"ERROR: cannot import PolymarketUSClient: {e}", file=sys.stderr)
        print("  Is polymarket-us installed in the venv? `pip install polymarket-us`",
              file=sys.stderr)
        return 2

    try:
        client = PolymarketUSClient(key_id=key_id, secret_key=secret_key)
    except Exception as e:
        print(f"ERROR: Polymarket US client init failed: {e}", file=sys.stderr)
        return 2

    print("=" * 56)
    print("Polymarket US account health check")
    print("=" * 56)
    print(f"account (key_id): {client.get_address()}")

    # Raw balance response, for transparency.
    try:
        raw = client._client.account.balances()
    except Exception as e:
        print(f"ERROR: account.balances() failed: {e}", file=sys.stderr)
        return 3

    if not isinstance(raw, dict):
        print(f"ERROR: unexpected balances response type: {type(raw).__name__}",
              file=sys.stderr)
        return 3

    balances = raw.get("balances") or []
    if not balances:
        print("ERROR: balances response was empty — auth or API issue?",
              file=sys.stderr)
        return 3

    usd = next(
        (b for b in balances if isinstance(b, dict) and b.get("currency", "USD") == "USD"),
        balances[0],
    )

    current = float(usd.get("currentBalance") or 0)
    buying_power = float(usd.get("buyingPower") or 0)
    asset_notional = float(usd.get("assetNotional") or 0)
    open_orders_value = float(usd.get("openOrders") or 0)
    unsettled = float(usd.get("unsettledFunds") or 0)

    print(f"USD current balance: ${current:,.2f}")
    print(f"USD buying power:    ${buying_power:,.2f}")
    print(f"open orders notional: ${open_orders_value:,.2f}")
    print(f"asset notional:       ${asset_notional:,.2f}")
    print(f"unsettled funds:      ${unsettled:,.2f}")

    # Open orders count
    try:
        open_orders = client.get_open_orders()
        print(f"open orders count:    {len(open_orders)}")
    except Exception as e:
        print(f"WARN: could not fetch open orders: {e}", file=sys.stderr)

    print()

    if buying_power <= 0 and current <= 0:
        print("STATUS: NOT FUNDED — no USD balance on Polymarket US.")
        print()
        print("Next steps:")
        print("  1. Log into polymarket.us, deposit USD via Aeropay or ACH.")
        print("  2. Wait for the deposit to clear (ACH can take a day).")
        print("  3. Re-run this script.")
        return 5

    if buying_power <= 0:
        print("STATUS: FUNDED BUT NO BUYING POWER — all balance tied up in")
        print("        open orders or pending settlement. Cancel stale orders")
        print("        or wait for settlement before trying to execute.")
        return 5

    print(f"STATUS: OK — ${buying_power:,.2f} buying power available for trading.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
