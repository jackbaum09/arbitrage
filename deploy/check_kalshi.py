#!/usr/bin/env python3
"""
Kalshi account health check.

One-shot diagnostic that reads KALSHI_* from the repo .env, connects in
the configured mode (live/demo), and prints:
  - mode + base URL
  - free cash balance
  - open positions (ticker + exposure)
  - resting orders

Exits non-zero if anything prevents a clean connection, so cron or a
shell gate can use `python3 deploy/check_kalshi.py && python3 main.py`
to block scanner startup on broken Kalshi auth.

Usage:
    cd /home/ec2-user/arbitrage
    source venv/bin/activate
    python3 deploy/check_kalshi.py
"""

from __future__ import annotations

import os
import sys

# Make the repo importable when this script is run directly
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(HERE), ".env"))

from execution.kalshi_client import (  # noqa: E402
    KalshiClient,
    DEMO_BASE_URL,
    LIVE_BASE_URL,
)


def main() -> int:
    key_id = os.environ.get("KALSHI_API_KEY_ID", "")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
    mode = os.environ.get("KALSHI_API_MODE", "demo")

    if not key_id or not key_path:
        print("ERROR: KALSHI_API_KEY_ID or KALSHI_PRIVATE_KEY_PATH not set", file=sys.stderr)
        return 2
    if not os.path.isfile(key_path):
        print(f"ERROR: Kalshi private key file not found at {key_path}", file=sys.stderr)
        return 2

    base_url = LIVE_BASE_URL if mode == "live" else DEMO_BASE_URL

    try:
        client = KalshiClient(
            api_key_id=key_id, private_key_path=key_path, base_url=base_url
        )
    except Exception as e:
        print(f"ERROR: Kalshi client init failed: {e}", file=sys.stderr)
        return 3

    print("=" * 56)
    print(f"Kalshi account health check ({mode} mode)")
    print(f"  base_url: {base_url}")
    print("=" * 56)

    try:
        balance = client.get_balance()
    except Exception as e:
        print(f"ERROR: get_balance failed: {e}", file=sys.stderr)
        return 4
    print(f"free cash: ${balance:.2f}")

    try:
        positions = client.get_positions()
    except Exception as e:
        print(f"WARN: get_positions failed: {e}", file=sys.stderr)
        positions = []

    # Only show positions with non-zero exposure — closed/settled positions
    # linger in the response with market_exposure_dollars='0' and realized_pnl
    open_positions = [
        p for p in positions
        if float(p.get("market_exposure_dollars") or 0) > 0
    ]
    print(f"\nopen positions: {len(open_positions)}")
    total_exposure = 0.0
    for p in open_positions:
        ticker = p.get("ticker", "?")
        exp = float(p.get("market_exposure_dollars") or 0)
        fees = float(p.get("fees_paid_dollars") or 0)
        total_exposure += exp
        print(f"  {ticker:<50} ${exp:>8.2f}  (fees paid: ${fees:.2f})")
    if open_positions:
        print(f"  {'TOTAL OPEN EXPOSURE':<50} ${total_exposure:>8.2f}")

    try:
        orders = client.get_open_orders()
    except Exception as e:
        print(f"WARN: get_open_orders failed: {e}", file=sys.stderr)
        orders = []
    print(f"\nresting orders: {len(orders)}")
    for o in orders:
        print(f"  {o.get('ticker', '?')} {o.get('side', '?')} "
              f"{o.get('action', '?')} @ {o.get('price', '?')}")

    print()
    if balance <= 0.50:
        print("STATUS: NOT FUNDED — balance too low for live execution")
        return 1
    print(f"STATUS: OK — ${balance:.2f} free cash available for scanner arb trades")
    return 0


if __name__ == "__main__":
    sys.exit(main())
