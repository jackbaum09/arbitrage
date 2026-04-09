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
from scanner.detect import scan_all
from scanner.store import ensure_table, write_opportunities

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
        except Exception as e:
            log.error(f"Failed to initialize Polymarket client: {e}")
    else:
        log.warning("Polymarket private key not configured — Polymarket trading disabled")

    if not kalshi_client and not polymarket_client:
        log.error("No trading clients available — execution disabled")
        return None

    return kalshi_client, polymarket_client, risk_limits


def run_scan(execute: bool = False, execution_ctx=None) -> int:
    """Execute one scan cycle. Returns number of opportunities found."""
    log.info("=" * 60)
    log.info("Starting arbitrage scan")
    log.info("=" * 60)

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

            log.info(f"Evaluating {len(opportunities)} opportunities for execution...")
            for opp in opportunities:
                result = execute_opportunity(
                    opp,
                    kalshi_client=kalshi_client,
                    polymarket_client=polymarket_client,
                    risk_limits=risk_limits,
                    fill_timeout=FILL_TIMEOUT_SECONDS,
                )
                if result.status == "success":
                    log.info(
                        f"  TRADED: {opp.outcome} | "
                        f"cost=${result.total_cost:.2f} | "
                        f"expected profit=${result.expected_profit:.2f}"
                    )
                elif result.status == "risk_blocked":
                    log.debug(f"  Blocked: {opp.outcome} — {result.error}")

        return count

    except Exception:
        log.exception("Scan failed")
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
