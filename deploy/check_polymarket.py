#!/usr/bin/env python3
"""
Polymarket wallet health check.

One-shot diagnostic that reads POLYMARKET_PRIVATE_KEY from .env, derives
the wallet address, and prints:
  - wallet address
  - USDC balance
  - USDC allowance to the CTF Exchange contract
  - whether the wallet is ready for live trading

Exit codes:
  0  wallet fully ready (balance > 0 AND allowance > 0)
  1  private key not set — can't run check
  2  client init failed (bad key, RPC issue)
  3  balance fetch failed
  4  wallet has USDC but zero allowance — needs approval
  5  wallet has zero USDC balance

Usage:
    cd /home/ec2-user/arbitrage
    source venv/bin/activate
    python3 deploy/check_polymarket.py

If exit code 4 (no allowance), the next step is to run an
`update_balance_allowance` / `approve` transaction against the CTF
Exchange contract from the wallet. This script deliberately does NOT
sign that transaction itself — approval should be a manual, reviewed
step the first time (double-check contract address and amount).
"""

from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(HERE), ".env"))


def main() -> int:
    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        print("POLYMARKET_PRIVATE_KEY not set in .env", file=sys.stderr)
        print("  Set it to the hex private key of a funded Polygon wallet.")
        return 1

    try:
        from execution.polymarket_client import PolymarketClient
    except ImportError as e:
        print(f"ERROR: cannot import PolymarketClient: {e}", file=sys.stderr)
        print("  Is py-clob-client installed in the venv?", file=sys.stderr)
        return 2

    try:
        client = PolymarketClient(private_key=pk)
    except Exception as e:
        print(f"ERROR: Polymarket client init failed: {e}", file=sys.stderr)
        return 2

    print("=" * 56)
    print("Polymarket wallet health check")
    print("=" * 56)

    try:
        address = client.get_address()
    except Exception as e:
        print(f"WARN: get_address failed: {e}", file=sys.stderr)
        address = "(unknown)"
    print(f"wallet: {address}")

    try:
        ba = client.get_balance_allowance()
    except Exception as e:
        print(f"ERROR: get_balance_allowance failed: {e}", file=sys.stderr)
        return 3

    bal_raw = ba.get("balance")
    allow_raw = ba.get("allowance")

    if bal_raw is None and allow_raw is None:
        print("ERROR: balance/allowance response was empty — CLOB API issue?", file=sys.stderr)
        return 3

    bal = float(bal_raw or 0) / 1e6
    allow = float(allow_raw or 0) / 1e6

    print(f"USDC balance:   ${bal:,.2f}")
    print(f"CTF allowance:  ${allow:,.2f}")
    print()

    if bal <= 0:
        print("STATUS: NOT FUNDED — wallet has no USDC.")
        print("  Next: bridge or on-ramp USDC (Polygon PoS) to the address above.")
        print("  Also ensure the wallet has a small MATIC balance for gas.")
        return 5

    if allow <= 0:
        print("STATUS: NOT APPROVED — wallet has USDC but no CTF Exchange allowance.")
        print("  The allowance is the permission for Polymarket's exchange contract")
        print("  to move your USDC when a trade fills. Without it, signed orders")
        print("  revert on-chain.")
        print()
        print("  Next: run an `approve` transaction from this wallet granting")
        print("  USDC spending to the CTF Exchange proxy. The Polymarket docs")
        print("  and the py-clob-client README cover the exact contract address")
        print("  and call. Many users do this once via the Polymarket UI when")
        print("  placing their first order there.")
        return 4

    print(f"STATUS: OK — ${bal:,.2f} USDC available, ${allow:,.2f} approved for trading")
    return 0


if __name__ == "__main__":
    sys.exit(main())
