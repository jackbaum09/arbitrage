"""
Core arbitrage detection engine.

Reads from existing Supabase prediction market tables populated by
prediction_markets-master, compares cross-platform prices, and calculates
fee-adjusted ROI for each opportunity.

Uses fuzzy matching to pair Kalshi and Polymarket outcomes since the two
platforms use different naming conventions.
"""

from __future__ import annotations

import logging

from config import (
    get_db_connection,
    KALSHI_SETTLEMENT_FEE,
    POLYMARKET_EFFECTIVE_FEE,
    MIN_ROI_THRESHOLD,
    MIN_LIQUIDITY,
    ORDERBOOK_TARGET_SIZE,
    FUTURES_TABLES,
    GAME_MARKET_PAIRS,
)
from scanner.models import Opportunity
from scanner.match import match_outcomes
from scanner.orderbook import get_executable_prices

log = logging.getLogger(__name__)


def _sport_from_table(table: str) -> str:
    return table.split("_")[0]


def _calculate_fees(buy_yes_platform: str, buy_no_platform: str) -> float:
    fee_map = {
        "kalshi": KALSHI_SETTLEMENT_FEE,
        "polymarket": POLYMARKET_EFFECTIVE_FEE,
    }
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


def _enrich_with_orderbook(
    opp: Opportunity,
    kalshi_market_id: str | None,
    polymarket_market_id: str | None,
) -> Opportunity | None:
    """
    Fetch live order books and replace midpoint prices with executable VWAP.

    Returns the enriched opportunity, or None if liquidity is insufficient.
    On API failure, returns the opportunity unchanged with liquidity_verified=False.
    """
    opp.kalshi_market_id = kalshi_market_id
    opp.polymarket_market_id = polymarket_market_id

    prices = get_executable_prices(kalshi_market_id, polymarket_market_id, ORDERBOOK_TARGET_SIZE)
    if prices is None:
        log.debug(f"Order book fetch failed for {opp.outcome}, keeping midpoint prices")
        return opp

    # Determine executable prices for this specific strategy
    # buy_yes_platform tells us where we're buying YES
    if opp.buy_yes_platform == "kalshi":
        exec_yes = prices.kalshi_yes_ask_vwap
        exec_no = prices.polymarket_no_ask_vwap
        yes_depth = prices.kalshi_yes_depth
        no_depth = prices.polymarket_no_depth
    else:
        exec_yes = prices.polymarket_yes_ask_vwap
        exec_no = prices.kalshi_no_ask_vwap
        yes_depth = prices.polymarket_yes_depth
        no_depth = prices.kalshi_no_depth

    # Check depth meets minimum liquidity
    if yes_depth < MIN_LIQUIDITY or no_depth < MIN_LIQUIDITY:
        log.debug(
            f"Insufficient liquidity for {opp.outcome}: "
            f"YES depth=${yes_depth}, NO depth=${no_depth} (min=${MIN_LIQUIDITY})"
        )
        return None

    if exec_yes is None or exec_no is None:
        log.debug(f"No executable price available for {opp.outcome}, keeping midpoint")
        return opp

    # Preserve midpoint prices and update with executable prices
    opp.buy_yes_midpoint = opp.buy_yes_price
    opp.buy_no_midpoint = opp.buy_no_price

    opp.buy_yes_executable_price = exec_yes
    opp.buy_no_executable_price = exec_no
    opp.buy_yes_depth = yes_depth
    opp.buy_no_depth = no_depth
    opp.max_executable_size = min(yes_depth, no_depth)
    opp.liquidity_verified = True

    # Recalculate financials with executable prices
    opp.buy_yes_price = round(exec_yes, 4)
    opp.buy_no_price = round(exec_no, 4)
    opp.total_cost = round(exec_yes + exec_no, 4)
    opp.gross_profit = round(1.0 - opp.total_cost, 4)

    if opp.gross_profit <= 0:
        return None

    opp.fees = round(_calculate_fees(opp.buy_yes_platform, opp.buy_no_platform), 4)
    opp.net_profit = round(opp.gross_profit - opp.fees, 4)

    if opp.net_profit <= 0:
        return None

    opp.roi = round(opp.net_profit / opp.total_cost, 4)
    opp.capital_required = round(opp.total_cost * 100, 2)

    if opp.roi < MIN_ROI_THRESHOLD:
        return None

    return opp


def _fetch_platform_rows(cursor, table: str, platform: str, market_type: str) -> list[dict]:
    """Fetch all rows for a given platform and market_type from a futures table."""
    cursor.execute(f"""
        SELECT outcome, yes_price, no_price, volume, liquidity, platform_market_id
        FROM {table}
        WHERE platform = %s
          AND market_type = %s
          AND yes_price IS NOT NULL
          AND no_price IS NOT NULL
          AND yes_price > 0
    """, (platform, market_type))

    return [
        {
            "outcome": row[0],
            "yes_price": float(row[1]),
            "no_price": float(row[2]),
            "volume": float(row[3]) if row[3] else None,
            "liquidity": float(row[4]) if row[4] else None,
            "platform_market_id": row[5],
        }
        for row in cursor.fetchall()
    ]


def scan_futures() -> list[Opportunity]:
    """
    Scan futures tables for arbitrage between Kalshi and Polymarket.
    Uses fuzzy matching to pair outcomes across platforms.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    opportunities: list[Opportunity] = []

    try:
        for table in FUTURES_TABLES:
            # Get market_types that exist on both platforms
            cursor.execute(f"""
                SELECT market_type
                FROM {table}
                WHERE platform IN ('kalshi', 'polymarket')
                  AND yes_price IS NOT NULL AND yes_price > 0
                GROUP BY market_type
                HAVING COUNT(DISTINCT platform) = 2
            """)
            shared_types = [row[0] for row in cursor.fetchall()]

            if not shared_types:
                continue

            log.info(f"  {table}: shared market types = {shared_types}")

            for market_type in shared_types:
                kalshi_rows = _fetch_platform_rows(cursor, table, "kalshi", market_type)
                pm_rows = _fetch_platform_rows(cursor, table, "polymarket", market_type)

                if not kalshi_rows or not pm_rows:
                    continue

                sport = _sport_from_table(table)
                matched = match_outcomes(kalshi_rows, pm_rows, sport=sport)
                log.info(
                    f"    {market_type}: {len(kalshi_rows)} kalshi x "
                    f"{len(pm_rows)} pm -> {len(matched)} matched pairs"
                )

                for k_row, p_row in matched:
                    # Use the Polymarket outcome as the canonical name
                    outcome = p_row["outcome"]
                    k_mid = k_row["platform_market_id"]
                    p_mid = p_row["platform_market_id"]

                    # Strategy 1: Buy YES on Kalshi + NO on Polymarket
                    opp = _build_opportunity(
                        table, market_type, outcome,
                        "kalshi", k_row["yes_price"], "polymarket", p_row["no_price"],
                        kalshi_volume=k_row["volume"],
                        polymarket_liquidity=p_row["liquidity"],
                    )
                    if opp:
                        opp = _enrich_with_orderbook(opp, k_mid, p_mid)
                        if opp:
                            opportunities.append(opp)

                    # Strategy 2: Buy YES on Polymarket + NO on Kalshi
                    opp = _build_opportunity(
                        table, market_type, outcome,
                        "polymarket", p_row["yes_price"], "kalshi", k_row["no_price"],
                        kalshi_volume=k_row["volume"],
                        polymarket_liquidity=p_row["liquidity"],
                    )
                    if opp:
                        opp = _enrich_with_orderbook(opp, k_mid, p_mid)
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
                    p.liquidity AS p_liquidity,
                    k.market_ticker AS k_market_id,
                    p.platform_market_id AS p_market_id
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
            rows = cursor.fetchall()
            log.info(f"  {kalshi_table} <-> {pm_table}: {len(rows)} game market pairs")

            for row in rows:
                matchup, team, k_yes, k_no, p_yes, p_no, k_vol, p_liq, k_mid, p_mid = row
                outcome = f"{team} ({matchup})"

                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "kalshi", k_yes, "polymarket", p_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opp.sport = sport
                    opp = _enrich_with_orderbook(opp, k_mid, p_mid)
                    if opp:
                        opportunities.append(opp)

                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "polymarket", p_yes, "kalshi", k_no,
                    kalshi_volume=k_vol, polymarket_liquidity=p_liq,
                )
                if opp:
                    opp.sport = sport
                    opp = _enrich_with_orderbook(opp, k_mid, p_mid)
                    if opp:
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
