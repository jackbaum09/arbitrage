"""
Arbitrage scanner entry point.

Runs a scan immediately, then repeats on the configured interval.
Can also be invoked as a one-shot with --once.
"""

from __future__ import annotations

import argparse
import logging
import time
import sys

from config import SCAN_INTERVAL_MINUTES
from scanner.detect import scan_all
from scanner.store import ensure_table, write_opportunities

log = logging.getLogger("arbitrage")


def run_scan() -> int:
    """Execute one scan cycle. Returns number of opportunities found."""
    log.info("=" * 60)
    log.info("Starting arbitrage scan")
    log.info("=" * 60)

    try:
        opportunities = scan_all()

        for opp in opportunities:
            log.info(
                f"  {opp.sport.upper()} | {opp.market_type} | {opp.outcome}: "
                f"ROI={opp.roi:.2%} | net=${opp.net_profit:.4f}/contract | "
                f"cost=${opp.total_cost:.4f} | "
                f"{opp.buy_yes_platform} YES@{opp.buy_yes_price:.2f} + "
                f"{opp.buy_no_platform} NO@{opp.buy_no_price:.2f}"
            )

        count = write_opportunities(opportunities)
        log.info(f"Scan complete: {count} active opportunity(ies)")
        return count

    except Exception:
        log.exception("Scan failed")
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Prediction market arbitrage scanner")
    parser.add_argument("--once", action="store_true", help="Run a single scan and exit")
    parser.add_argument("--setup", action="store_true", help="Create DB table and exit")
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

    if args.once:
        run_scan()
        return

    # Continuous scanning loop
    interval_seconds = SCAN_INTERVAL_MINUTES * 60
    log.info(f"Starting continuous scanner (interval={SCAN_INTERVAL_MINUTES}m)")

    while True:
        run_scan()
        log.info(f"Next scan in {SCAN_INTERVAL_MINUTES} minutes...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
