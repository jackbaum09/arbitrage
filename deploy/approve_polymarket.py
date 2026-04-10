#!/usr/bin/env python3
"""
Sign and submit Polygon ERC-20 approve() transactions from the Polymarket
scanner wallet to the CTF Exchange proxies.

This is a one-time setup script. Run it after funding your wallet with
USDC + a small MATIC balance on Polygon, and before trying to execute
arbs on Polymarket. It:

  1. Loads POLYMARKET_PRIVATE_KEY from .env
  2. Queries current USDC allowance to both exchange proxies via
     eth_call (no signing)
  3. If --confirm is passed, signs + submits approve(spender, uint256.max)
     transactions for any proxy whose allowance is zero; otherwise prints
     a dry-run summary
  4. Waits for each tx to confirm, prints the receipt, and re-queries

Safety:
  - Dry-run by default: NO transaction is sent unless --confirm is passed
  - Hardcoded contract addresses (pulled from py-clob-client on first
    deploy; see check_polymarket.py for the source of truth)
  - Approve amount is uint256.max which is the Polymarket UI default
    (unlimited until revoked); use --amount to pass a specific USDC amount
  - Refuses to run on any chain other than Polygon mainnet (chainId 137)

Usage:
  python3 deploy/approve_polymarket.py              # dry-run
  python3 deploy/approve_polymarket.py --confirm    # actually submit

Exit codes:
  0  everything approved (either already, or successfully after --confirm)
  1  POLYMARKET_PRIVATE_KEY not set
  2  RPC / signing error
  3  insufficient MATIC for gas
  4  dry-run completed; re-run with --confirm to actually send
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(HERE), ".env"))

import requests  # noqa: E402

# eth_account is imported lazily inside main() so that --help and the
# "key not set" error path work on machines where py-clob-client (and
# its eth_account transitive dep) isn't installed.

# Polygon mainnet
POLYGON_CHAIN_ID = 137
POLYGON_RPC = os.environ.get("POLYGON_RPC", "https://polygon-rpc.com")

# Contract addresses (verified via py-clob-client.get_collateral_address()
# and get_exchange_address() — see deploy/check_polymarket.py).
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTF_EXCHANGE_NEG_RISK = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# ERC-20 function selectors
SEL_APPROVE = "0x095ea7b3"        # approve(address,uint256)
SEL_ALLOWANCE = "0xdd62ed3e"      # allowance(address,address)
SEL_BALANCE_OF = "0x70a08231"     # balanceOf(address)

UINT256_MAX = 2**256 - 1
USDC_DECIMALS = 6


# ---------------------------------------------------------------------------
# Low-level JSON-RPC helpers (avoids a full web3.py dependency)
# ---------------------------------------------------------------------------


def _rpc(method: str, params: list) -> object:
    resp = requests.post(
        POLYGON_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"RPC {method} error: {data['error']}")
    return data["result"]


def _addr_to_topic(addr: str) -> str:
    """Left-pad a 0x-prefixed 20-byte address to 32 bytes (hex, no 0x)."""
    return addr.lower().replace("0x", "").rjust(64, "0")


def _uint_to_hex32(v: int) -> str:
    return hex(v)[2:].rjust(64, "0")


def _eth_call(to: str, data: str) -> str:
    return _rpc("eth_call", [{"to": to, "data": data}, "latest"])


def read_allowance(owner: str, spender: str) -> int:
    call_data = SEL_ALLOWANCE + _addr_to_topic(owner) + _addr_to_topic(spender)
    raw = _eth_call(USDC_ADDRESS, call_data)
    return int(raw, 16) if raw and raw != "0x" else 0


def read_usdc_balance(addr: str) -> int:
    call_data = SEL_BALANCE_OF + _addr_to_topic(addr)
    raw = _eth_call(USDC_ADDRESS, call_data)
    return int(raw, 16) if raw and raw != "0x" else 0


def read_matic_balance(addr: str) -> int:
    raw = _rpc("eth_getBalance", [addr, "latest"])
    return int(raw, 16) if raw and raw != "0x" else 0


def build_approve_calldata(spender: str, amount: int) -> str:
    return "0x" + SEL_APPROVE[2:] + _addr_to_topic(spender) + _uint_to_hex32(amount)


# ---------------------------------------------------------------------------
# Transaction signing / submission
# ---------------------------------------------------------------------------


def sign_and_send_approve(
    acct, spender: str, amount: int, dry_run: bool
) -> dict:
    """Sign and submit one approve() transaction. Returns a summary dict."""
    nonce_hex = _rpc("eth_getTransactionCount", [acct.address, "pending"])
    nonce = int(nonce_hex, 16)

    calldata = build_approve_calldata(spender, amount)

    # Estimate gas via eth_estimateGas
    try:
        est_hex = _rpc(
            "eth_estimateGas",
            [{"from": acct.address, "to": USDC_ADDRESS, "data": calldata}],
        )
        gas_limit = int(est_hex, 16)
        # 25% cushion in case the estimate is tight
        gas_limit = int(gas_limit * 1.25)
    except Exception as e:
        print(f"  WARN: gas estimate failed ({e}); falling back to 100k")
        gas_limit = 100_000

    # Use EIP-1559 priced against current base fee if available, else legacy
    try:
        # Polygon supports both; pull current gas price and add a priority tip
        gas_price_hex = _rpc("eth_gasPrice", [])
        gas_price = int(gas_price_hex, 16)
    except Exception:
        gas_price = 50_000_000_000  # 50 gwei fallback

    tx = {
        "chainId": POLYGON_CHAIN_ID,
        "nonce": nonce,
        "to": USDC_ADDRESS,
        "value": 0,
        "gas": gas_limit,
        "gasPrice": gas_price,
        "data": calldata,
    }

    summary = {
        "spender": spender,
        "amount": amount,
        "nonce": nonce,
        "gas_limit": gas_limit,
        "gas_price_gwei": gas_price / 1e9,
        "est_cost_matic": (gas_limit * gas_price) / 1e18,
    }

    if dry_run:
        summary["status"] = "dry-run"
        return summary

    signed = acct.sign_transaction(tx)
    # Newer eth_account versions: signed.raw_transaction; older: signed.rawTransaction
    raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
    # JSON-RPC requires a 0x-prefixed hex string. Handle both bytes-like
    # objects and already-hex strings, and ensure the 0x prefix is present
    # either way — HexBytes.hex() returns unprefixed in some versions.
    if isinstance(raw, (bytes, bytearray)):
        raw_hex = "0x" + bytes(raw).hex()
    else:
        raw_str = str(raw)
        raw_hex = raw_str if raw_str.startswith("0x") else "0x" + raw_str
    tx_hash = _rpc("eth_sendRawTransaction", [raw_hex])
    summary["tx_hash"] = tx_hash
    summary["status"] = "submitted"

    # Poll for receipt
    print(f"  submitted {tx_hash}, waiting for confirmation...")
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            receipt = _rpc("eth_getTransactionReceipt", [tx_hash])
            if receipt:
                summary["block"] = int(receipt.get("blockNumber", "0x0"), 16)
                summary["receipt_status"] = int(receipt.get("status", "0x0"), 16)
                summary["status"] = "confirmed" if summary["receipt_status"] == 1 else "failed"
                return summary
        except Exception as e:
            print(f"  poll error: {e}")
        time.sleep(3)

    summary["status"] = "pending_timeout"
    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually sign and submit transactions (dry-run by default).",
    )
    parser.add_argument(
        "--amount",
        type=float,
        default=None,
        help="Approve a specific USDC amount instead of unlimited (uint256.max).",
    )
    parser.add_argument(
        "--skip-neg-risk",
        action="store_true",
        help="Only approve the standard CTF Exchange proxy; skip neg-risk "
        "(saves one approve tx if you only trade binary markets).",
    )
    args = parser.parse_args()

    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set in .env", file=sys.stderr)
        return 1

    if not pk.startswith("0x"):
        pk = "0x" + pk

    try:
        from eth_account import Account
    except ImportError as e:
        print(f"ERROR: eth_account not installed ({e}).", file=sys.stderr)
        print("  This script must run from the repo venv where py-clob-client", file=sys.stderr)
        print("  is installed (eth_account comes in as a transitive dep).", file=sys.stderr)
        return 2

    try:
        acct = Account.from_key(pk)
    except Exception as e:
        print(f"ERROR: bad private key: {e}", file=sys.stderr)
        return 2

    print("=" * 64)
    print(f"Polymarket USDC approval on Polygon ({POLYGON_RPC})")
    print("=" * 64)
    print(f"wallet: {acct.address}")

    # Sanity-check chain ID
    try:
        chain_id_hex = _rpc("eth_chainId", [])
        cid = int(chain_id_hex, 16)
        if cid != POLYGON_CHAIN_ID:
            print(f"ERROR: RPC reports chainId={cid}, expected {POLYGON_CHAIN_ID} (Polygon)", file=sys.stderr)
            return 2
    except Exception as e:
        print(f"ERROR: RPC unreachable: {e}", file=sys.stderr)
        return 2

    # Read current state
    try:
        usdc_bal_raw = read_usdc_balance(acct.address)
        matic_bal_raw = read_matic_balance(acct.address)
        allow_std_raw = read_allowance(acct.address, CTF_EXCHANGE)
        allow_neg_raw = read_allowance(acct.address, CTF_EXCHANGE_NEG_RISK)
    except Exception as e:
        print(f"ERROR: on-chain read failed: {e}", file=sys.stderr)
        return 2

    usdc_bal = usdc_bal_raw / (10 ** USDC_DECIMALS)
    matic_bal = matic_bal_raw / 1e18

    print(f"USDC balance:   ${usdc_bal:,.2f} ({USDC_ADDRESS})")
    print(f"MATIC balance:  {matic_bal:.4f} (for gas)")
    print()
    print("Current allowances:")
    print(f"  CTF Exchange (standard) {CTF_EXCHANGE}")
    print(f"    allowance: ${allow_std_raw / 10**USDC_DECIMALS:,.2f}")
    print(f"  CTF Exchange (neg-risk) {CTF_EXCHANGE_NEG_RISK}")
    print(f"    allowance: ${allow_neg_raw / 10**USDC_DECIMALS:,.2f}")
    print()

    # Gas sanity
    if matic_bal < 0.05:
        print("WARN: MATIC balance is below 0.05 — this may not cover 2 approve txns.")
        print("  Fund with ~$1 worth of MATIC from any Polygon exchange before --confirm.")
        if args.confirm:
            print("ABORT: refusing to attempt signing with near-zero MATIC.", file=sys.stderr)
            return 3

    # Determine which approvals are needed
    approve_amount = UINT256_MAX
    if args.amount is not None:
        approve_amount = int(args.amount * (10 ** USDC_DECIMALS))

    targets = []
    if allow_std_raw == 0:
        targets.append(("standard CTF Exchange", CTF_EXCHANGE))
    if allow_neg_raw == 0 and not args.skip_neg_risk:
        targets.append(("neg-risk CTF Exchange", CTF_EXCHANGE_NEG_RISK))

    if not targets:
        print("STATUS: All required approvals already in place. Nothing to do.")
        return 0

    print(f"Plan: {len(targets)} approve() txn(s) with amount={approve_amount} "
          f"({'uint256.max (unlimited)' if approve_amount == UINT256_MAX else f'${args.amount:,.2f}'})")
    for label, spender in targets:
        print(f"  - {label} ({spender})")
    print()

    if not args.confirm:
        print("DRY-RUN: No transactions submitted. Re-run with --confirm to sign and send.")
        # Show what each tx would look like
        for label, spender in targets:
            summary = sign_and_send_approve(acct, spender, approve_amount, dry_run=True)
            print(f"  {label}:")
            print(f"    nonce={summary['nonce']}  gas_limit={summary['gas_limit']}  "
                  f"gas_price={summary['gas_price_gwei']:.2f} gwei  "
                  f"est_cost≈{summary['est_cost_matic']:.6f} MATIC")
        return 4

    # Actually submit
    all_ok = True
    for label, spender in targets:
        print(f"Submitting approve for {label}...")
        try:
            summary = sign_and_send_approve(acct, spender, approve_amount, dry_run=False)
        except Exception as e:
            print(f"  FAILED: {e}")
            all_ok = False
            continue
        print(f"  status={summary['status']}  block={summary.get('block')}  "
              f"tx_hash={summary.get('tx_hash')}")
        if summary.get("status") != "confirmed":
            all_ok = False

    # Re-query after
    print()
    print("Post-approval state:")
    try:
        new_std = read_allowance(acct.address, CTF_EXCHANGE)
        new_neg = read_allowance(acct.address, CTF_EXCHANGE_NEG_RISK)
        print(f"  standard: ${new_std / 10**USDC_DECIMALS:,.2f}")
        print(f"  neg-risk: ${new_neg / 10**USDC_DECIMALS:,.2f}")
    except Exception as e:
        print(f"  (re-read failed: {e})")

    return 0 if all_ok else 2


if __name__ == "__main__":
    sys.exit(main())
