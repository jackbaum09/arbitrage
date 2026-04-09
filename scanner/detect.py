"""
Core arbitrage detection engine.

Reads from existing Supabase prediction market tables populated by
prediction_markets-master, compares cross-platform prices, and calculates
fee-adjusted ROI for each opportunity.
"""

from __future__ import annotations

import logging
from typing import Any

from config import (
    get_db_connection,
    KALSHI_SETTLEMENT_FEE,
    POLYMARKET_EFFECTIVE_FEE,
    MIN_ROI_THRESHOLD,
    MIN_LIQUIDITY,
    FUTURES_TABLES,
    GAME_MARKET_PAIRS,
)
from scanner.models import Opportunity

log = logging.getLogger(__name__)


def _sport_from_table(table: str) -> str:
    """Extract sport code from table name. e.g. 'nba_prediction_futures' -> 'nba'."""
    return table.split("_")[0]


def _calculate_fees(buy_yes_platform: str, buy_no_platform: str) -> float:
    """
    Calculate total fees for a two-leg arbitrage position.

    One leg wins, one loses. The winning leg pays the settlement/effective fee.
    We don't know which leg wins, so we take the max fee (conservative).
    """
    fee_map = {
        "kalshi": KALSHI_SETTLEMENT_FEE,
        "polymarket": POLYMARKET_EFFECTIVE_FEE,
    }
    # Worst case: the platform with the higher fee is the winning leg
    return max(fee_map.get(buy_yes_platform, 0), fee_map.get(buy_no_platform, 0))


def _build_opportunity(
    table: str,
    market_type: str,
    outcome: str,
    buy_yes_platform: str,
    buy_yes_price: float,
    buy_no_platform: str,
    buy_no_price: float,
    kalshi_volume: float | None = None,
    polymarket_liquidity: float | None = None,
) -> Opportunity | None:
    """
    Build an Opportunity if the fee-adjusted ROI exceeds the threshold.
    Returns None if the opportunity is not profitable after fees.
    """
    total_cost = buy_yes_price + buy_no_price
    gross_profit = 1.0 - total_cost

    if gross_profit <= 0:
        return None

    fees = _calculate_fees(buy_yes_platform, buy_no_platform)
    net_profit = gross_profit - fees

    if net_profit <= 0:
        return None

    roi = net_profit / total_cost

    if roi < MIN_ROI_THRESHOLD:
        return None

    # Capital required: for a $100 notional, you buy $100 worth of each leg
    # proportional to cost
    capital_required = round(total_cost * 100, 2)

    return Opportunity(
        sport=_sport_from_table(table),
        market_type=market_type,
        outcome=outcome,
        buy_yes_platform=buy_yes_platform,
        buy_yes_price=round(buy_yes_price, 4),
        buy_no_platform=buy_no_platform,
        buy_no_price=round(buy_no_price, 4),
        total_cost=round(total_cost, 4),
        gross_profit=round(gross_profit, 4),
        fees=round(fees, 4),
        net_profit=round(net_profit, 4),
        roi=round(roi, 4),
        kalshi_volume=kalshi_volume,
        polymarket_liquidity=polymarket_liquidity,
        capital_required=capital_required,
        source_table=table,
    )


def scan_futures() -> list[Opportunity]:
    """
    Scan futures tables for arbitrage between Kalshi and Polymarket.

    Matches by market_type + normalized outcome text.
    Checks both directions: YES on Kalshi + NO on Polymarket, and vice versa.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    opportunities: list[Opportunity] = []

    try:
        for table in FUTURES_TABLES:
            cursor.execute(f"""
                SELECT
                    k.market_type,
                    k.outcome,
                    k.yes_price AS k_yes,
                    k.no_price  AS k_no,
                    p.yes_price AS p_yes,
                    p.no_price  AS p_no,
                    k.volume    AS k_volume,
                    p.liquidity AS p_liquidity
                FROM {table} k
                JOIN {table} p
                    ON  k.market_type = p.market_type
                    AND LOWER(TRIM(k.outcome)) = LOWER(TRIM(p.outcome))
                WHERE k.platform = 'kalshi'
                  AND p.platform = 'polymarket'
                  AND k.yes_price IS NOT NULL AND k.no_price IS NOT NULL
                  AND p.yes_price IS NOT NULL AND p.no_price IS NOT NULL
                  AND k.yes_price > 0 AND p.yes_price > 0
            """)

            for row in cursor.fetchall():
                market_type, outcome, k_yes, k_no, p_yes, p_no, k_vol, p_liq = row

                # Strategy 1: Buy YES on Kalshi + NO on Polymarket
                opp = _build_opportunity(
                    table, market_type, outcome,
                    "kalshi", k_yes, "polymarket", p_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opportunities.append(opp)

                # Strategy 2: Buy YES on Polymarket + NO on Kalshi
                opp = _build_opportunity(
                    table, market_type, outcome,
                    "polymarket", p_yes, "kalshi", k_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opportunities.append(opp)

    finally:
        cursor.close()
        conn.close()

    return opportunities


def scan_game_markets() -> list[Opportunity]:
    """
    Scan game markets for arbitrage between Kalshi game tables and
    Polymarket entries in futures tables (market_type='game').
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    opportunities: list[Opportunity] = []

    try:
        for kalshi_table, pm_table in GAME_MARKET_PAIRS:
            # Check if market_subtype column exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_name = %s AND column_name = 'market_subtype'
                )
            """, (kalshi_table,))
            has_subtype = cursor.fetchone()[0]

            subtype_filter = (
                "(k.market_subtype IS NULL OR k.market_subtype = 'winner')"
                if has_subtype else "TRUE"
            )

            cursor.execute(f"""
                SELECT
                    k.matchup,
                    k.team,
                    k.yes_price AS k_yes,
                    k.no_price  AS k_no,
                    p.yes_price AS p_yes,
                    p.no_price  AS p_no,
                    k.volume    AS k_volume,
                    p.liquidity AS p_liquidity
                FROM {kalshi_table} k
                JOIN {pm_table} p
                    ON  p.platform = 'polymarket'
                    AND p.market_type = 'game'
                    AND REPLACE(k.matchup, ' @ ', ' vs ') = p.matchup
                    AND LOWER(TRIM(p.outcome)) LIKE '%%' || LOWER(k.team) || '%%'
                WHERE {subtype_filter}
                  AND k.yes_price IS NOT NULL AND k.no_price IS NOT NULL
                  AND p.yes_price IS NOT NULL AND p.no_price IS NOT NULL
                  AND k.yes_price > 0 AND p.yes_price > 0
            """)

            sport = _sport_from_table(kalshi_table)
            for row in cursor.fetchall():
                matchup, team, k_yes, k_no, p_yes, p_no, k_vol, p_liq = row
                outcome = f"{team} ({matchup})"

                # Strategy 1: Buy YES on Kalshi + NO on Polymarket
                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "kalshi", k_yes, "polymarket", p_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opp.sport = sport
                    opportunities.append(opp)

                # Strategy 2: Buy YES on Polymarket + NO on Kalshi
                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "polymarket", p_yes, "kalshi", k_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opp.sport = sport
                    opportunities.append(opp)

    finally:
        cursor.close()
        conn.close()

    return opportunities


def scan_all() -> list[Opportunity]:
    """Run all scanners and return combined, sorted results."""
    log.info("Scanning futures tables for arbitrage...")
    futures_opps = scan_futures()
    log.info(f"  Found {len(futures_opps)} futures opportunity(ies)")

    log.info("Scanning game markets for arbitrage...")
    game_opps = scan_game_markets()
    log.info(f"  Found {len(game_opps)} game market opportunity(ies)")

    all_opps = futures_opps + game_opps
    all_opps.sort(key=lambda o: o.roi, reverse=True)

    log.info(f"Total opportunities after fees: {len(all_opps)}")
    return all_opps
