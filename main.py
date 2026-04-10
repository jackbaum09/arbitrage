"""
Arbitrage scanner entry point.

Runs a scan immediately, then repeats on the configured interval.
Can also be invoked as a one-shot with --once.
With --execute, attempts to trade qualifying opportunities.
"""

from __future__ import annotations

import argparse
import logging
import time
import sys

from config import (
    SCAN_INTERVAL_MINUTES,
    EXECUTION_ENABLED,
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_API_MODE,
    POLYMARKET_PRIVATE_KEY,
    MAX_POSITION_SIZE,
    MAX_TOTAL_CAPITAL,
    MAX_SINGLE_TRADE,
    MIN_EXECUTION_ROI,
    MAX_OPEN_POSITIONS,
    FILL_TIMEOUT_SECONDS,
)
from datetime import datetime, timezone
from scanner.detect import scan_all
from scanner.store import ensure_table, write_opportunities, record_scan_run

log = logging.getLogger("arbitrage")


def _init_execution():
    """Initialize execution clients and risk limits. Returns (kalshi, polymarket, risk_limits) or None."""
    from execution.kalshi_client import KalshiClient, DEMO_BASE_URL, LIVE_BASE_URL
    from execution.polymarket_client import PolymarketClient
    from execution.risk import RiskLimits
    from execution.store import ensure_execution_table

    ensure_execution_table()

    risk_limits = RiskLimits(
        max_position_size=MAX_POSITION_SIZE,
        max_total_capital=MAX_TOTAL_CAPITAL,
        max_single_trade=MAX_SINGLE_TRADE,
        min_roi_threshold=MIN_EXECUTION_ROI,
        max_open_positions=MAX_OPEN_POSITIONS,
    )

    # Initialize Kalshi client
    kalshi_client = None
    if KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH:
        base_url = LIVE_BASE_URL if KALSHI_API_MODE == "live" else DEMO_BASE_URL
        try:
            kalshi_client = KalshiClient(
                api_key_id=KALSHI_API_KEY_ID,
                private_key_path=KALSHI_PRIVATE_KEY_PATH,
                base_url=base_url,
            )
            balance = kalshi_client.get_balance()
            log.info(f"Kalshi connected ({KALSHI_API_MODE} mode) — balance: ${balance:.2f}")
        except Exception as e:
            log.error(f"Failed to initialize Kalshi client: {e}")
    else:
        log.warning("Kalshi credentials not configured — Kalshi trading disabled")

    # Initialize Polymarket client
    polymarket_client = None
    if POLYMARKET_PRIVATE_KEY:
        try:
            polymarket_client = PolymarketClient(private_key=POLYMARKET_PRIVATE_KEY)
            try:
                addr = polymarket_client.get_address()
                ba = polymarket_client.get_balance_allowance()
                bal_raw = ba.get("balance")
                allow_raw = ba.get("allowance")
                bal_dollars = float(bal_raw) / 1e6 if bal_raw is not None else 0.0
                allow_dollars = float(allow_raw) / 1e6 if allow_raw is not None else 0.0
                log.info(
                    f"Polymarket connected — wallet {addr} "
                    f"balance ${bal_dollars:.2f} USDC, "
                    f"CTF Exchange allowance ${allow_dollars:.2f}"
                )
                # Pre-flight: require a non-zero USDC allowance to the CTF Exchange
                # contract. Without this, create_order will sign fine but fills
                # will revert on-chain. Fail loud here instead of at trade time.
                if allow_dollars <= 0:
                    log.error(
                        "Polymarket USDC allowance to CTF Exchange is $0 — "
                        "wallet must approve USDC spending before trading. "
                        "Disabling Polymarket trading for this run."
                    )
                    polymarket_client = None
            except Exception as e:
                log.error(
                    f"Polymarket balance/allowance fetch failed: {e} — "
                    "disabling Polymarket trading for this run."
                )
                polymarket_client = None
        except Exception as e:
            log.error(f"Failed to initialize Polymarket client: {e}")
    else:
        log.warning("Polymarket private key not configured — Polymarket trading disabled")

    # Cross-platform arbs always place one leg on each platform, so BOTH
    # clients must be armed for execution to be meaningful. A partial
    # configuration (Kalshi only, or Polymarket only) would spam failed
    # leg placements and potentially leave orphaned single-leg exposure
    # — refuse to arm execution in that case.
    if not kalshi_client or not polymarket_client:
        missing = []
        if not kalshi_client:
            missing.append("Kalshi")
        if not polymarket_client:
            missing.append("Polymarket")
        log.error(
            f"Execution requires both Kalshi and Polymarket clients armed — "
            f"missing: {', '.join(missing)}. Disabling execution for this run."
        )
        return None

    return kalshi_client, polymarket_client, risk_limits


def run_scan(execute: bool = False, execution_ctx=None) -> int:
    """Execute one scan cycle. Returns number of opportunities found."""
    log.info("=" * 60)
    log.info("Starting arbitrage scan")
    log.info("=" * 60)

    started_at = datetime.now(timezone.utc)
    opportunities = []
    try:
        opportunities = scan_all()

        for opp in opportunities:
            verified = " [VERIFIED]" if opp.liquidity_verified else ""
            log.info(
                f"  {opp.sport.upper()} | {opp.market_type} | {opp.outcome}: "
                f"ROI={opp.roi:.2%} | net=${opp.net_profit:.4f}/contract | "
                f"cost=${opp.total_cost:.4f} | "
                f"{opp.buy_yes_platform} YES@{opp.buy_yes_price:.2f} + "
                f"{opp.buy_no_platform} NO@{opp.buy_no_price:.2f}{verified}"
            )

        count = write_opportunities(opportunities)
        log.info(f"Scan complete: {count} active opportunity(ies)")

        # Send alerts for high-ROI opportunities
        try:
            from scanner.alerts import send_opportunity_alerts
            send_opportunity_alerts(opportunities)
        except Exception as e:
            log.debug(f"Alert sending skipped: {e}")

        # Execute trades if enabled
        if execute and execution_ctx and opportunities:
            kalshi_client, polymarket_client, risk_limits = execution_ctx
            from execution.manager import execute_opportunity
            from execution.risk import fetch_balances

            # Fetch live per-platform balances once at the start of the
            # execute loop. If either configured client fails, skip execution
            # for this scan — safer than trading against a stale snapshot.
            balances = fetch_balances(kalshi_client, polymarket_client)
            if balances is None:
                log.error(
                    "Live balance fetch failed — skipping execution for this scan"
                )
            else:
                log.info(
                    f"Live balances: Kalshi=${balances.kalshi:.2f}, "
                    f"Polymarket=${balances.polymarket:.2f}"
                )
                log.info(
                    f"Evaluating {len(opportunities)} opportunities for execution..."
                )
                for opp in opportunities:
                    result = execute_opportunity(
                        opp,
                        kalshi_client=kalshi_client,
                        polymarket_client=polymarket_client,
                        risk_limits=risk_limits,
                        fill_timeout=FILL_TIMEOUT_SECONDS,
                        balances=balances,
                    )
                    if result.status == "success":
                        log.info(
                            f"  TRADED: {opp.outcome} | "
                            f"cost=${result.total_cost:.2f} | "
                            f"expected profit=${result.expected_profit:.2f}"
                        )
                    elif result.status == "risk_blocked":
                        log.debug(f"  Blocked: {opp.outcome} — {result.error}")

        record_scan_run(
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            opportunities=opportunities,
            status="success",
        )
        return count

    except Exception as e:
        log.exception("Scan failed")
        record_scan_run(
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            opportunities=opportunities,
            status="error",
            error_message=str(e)[:500],
        )
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Prediction market arbitrage scanner")
    parser.add_argument("--once", action="store_true", help="Run a single scan and exit")
    parser.add_argument("--setup", action="store_true", help="Create DB table and exit")
    parser.add_argument("--interval", type=int, default=None, help="Scan interval in minutes (default: 5)")
    parser.add_argument("--execute", action="store_true", help="Enable trade execution (requires EXECUTION_ENABLED=true)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    ensure_table()

    if args.setup:
        log.info("Table setup complete")
        return

    # Initialize execution if requested
    execute = False
    execution_ctx = None
    if args.execute:
        if not EXECUTION_ENABLED:
            log.error("--execute flag set but EXECUTION_ENABLED is not 'true' in env. Aborting.")
            sys.exit(1)
        execution_ctx = _init_execution()
        if execution_ctx:
            execute = True
            log.info("Trade execution ENABLED")
        else:
            log.error("Failed to initialize execution — running in scan-only mode")

    if args.once:
        run_scan(execute=execute, execution_ctx=execution_ctx)
        return

    # Continuous scanning loop
    interval = args.interval if args.interval is not None else SCAN_INTERVAL_MINUTES
    interval_seconds = interval * 60
    log.info(f"Starting continuous scanner (interval={interval}m)")

    while True:
        run_scan(execute=execute, execution_ctx=execution_ctx)
        log.info(f"Next scan in {interval} minutes...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
