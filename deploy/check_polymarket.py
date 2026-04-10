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

    # Pull real contract addresses from the client so we never drift
    # from what py-clob-client thinks the exchange proxies are.
    try:
        usdc_addr = client._client.get_collateral_address()
        ctf_exchange = client._client.get_exchange_address(neg_risk=False)
        ctf_exchange_neg = client._client.get_exchange_address(neg_risk=True)
    except Exception:
        usdc_addr = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # Polygon bridged USDC
        ctf_exchange = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        ctf_exchange_neg = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

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
        print("STATUS: NOT FUNDED — wallet has no USDC on Polygon.")
        print()
        print("Next steps:")
        print(f"  1. Send USDC (Polygon bridged, contract {usdc_addr})")
        print(f"     to {address}")
        print("     Common sources: Coinbase withdraw (Polygon network),")
        print("     Binance withdraw, or a bridge like across.to / hop.exchange.")
        print("  2. Send a small amount of MATIC (~$1 worth) to the same wallet")
        print("     for gas. On-chain approvals and any sell txns will need it.")
        print("  3. Re-run this script to confirm the balance lands, then")
        print("     handle the CTF Exchange approval (see exit-code-4 path).")
        return 5

    if allow <= 0:
        print("STATUS: NOT APPROVED — wallet has USDC but no CTF Exchange allowance.")
        print()
        print("The allowance is an on-chain ERC-20 approve() granting Polymarket's")
        print("exchange proxy the right to move your USDC when a trade fills.")
        print("Without it, signed orders submit fine but revert at settlement.")
        print()
        print("Contract addresses (Polygon mainnet):")
        print(f"  USDC (bridged, spender of approve):  {usdc_addr}")
        print(f"  CTF Exchange proxy (standard):       {ctf_exchange}")
        print(f"  CTF Exchange proxy (neg-risk):       {ctf_exchange_neg}")
        print()
        print("Two paths to do the approval:")
        print()
        print("  Path A (easiest, no script):")
        print("    Visit polymarket.com, connect this wallet, and place one $1")
        print("    order on any market. The UI prompts for the USDC approval on")
        print("    your first trade and handles the signing + submission. After")
        print("    confirmation, re-run this script — allowance should be > 0.")
        print()
        print("  Path B (scripted, no UI):")
        print("    Run deploy/approve_polymarket.py (also in this repo). It")
        print("    dry-runs by default; pass --confirm to actually send the")
        print("    approve transactions to both exchange proxies. Requires a")
        print("    small MATIC balance for gas.")
        print()
        print("NOTE: The scanner places buy-only legs on Polymarket, so only the")
        print("USDC→CTF approval is strictly required for arb execution. Path A")
        print("and Path B both handle this; approving the neg-risk proxy is only")
        print("needed if you start buying neg-risk markets (championships, etc).")
        return 4

    print(f"STATUS: OK — ${bal:,.2f} USDC available, ${allow:,.2f} approved for trading")
    return 0


if __name__ == "__main__":
    sys.exit(main())
