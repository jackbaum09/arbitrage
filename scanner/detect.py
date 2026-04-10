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
import math

from config import (
    get_db_connection,
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
from scanner.teams import kalshi_code_to_pm_game_team

log = logging.getLogger(__name__)


def _sport_from_table(table: str) -> str:
    return table.split("_")[0]


def _kalshi_trading_fee(price: float) -> float:
    """
    Kalshi sports market trading fee per contract, in dollars.

    Kalshi charges ceil(7 * P * (1 - P)) cents per contract on sports
    markets, where P is the contract price in dollars. This is the actual
    fee schedule (not the flat 7c worst-case bound we used previously),
    and it's strongly asymmetric: ~2c near P=0.5, ~1c at P=0.9, 0c at the
    extremes. Charged at trade time on every Kalshi contract regardless
    of which side eventually wins.
    """
    if price <= 0 or price >= 1:
        return 0.0
    cents = math.ceil(7 * price * (1 - price))
    return cents / 100.0


def _leg_fee(platform: str, price: float) -> float:
    """Per-contract fee for a single leg, expressed in dollars of $1 face."""
    if platform == "kalshi":
        return _kalshi_trading_fee(price)
    if platform == "polymarket":
        return POLYMARKET_EFFECTIVE_FEE
    return 0.0


def _calculate_fees(
    buy_yes_platform: str,
    buy_yes_price: float,
    buy_no_platform: str,
    buy_no_price: float,
) -> float:
    """
    Total fees for a two-leg arb, in dollars per $1 of contract face.

    Fees from the two legs come from independent sources (Kalshi trading
    fee vs Polymarket slippage/withdrawal cost) and are both paid, so we
    sum them rather than taking the max as the old flat-fee model did.
    """
    return _leg_fee(buy_yes_platform, buy_yes_price) + _leg_fee(
        buy_no_platform, buy_no_price
    )


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

    fees = _calculate_fees(
        buy_yes_platform, buy_yes_price, buy_no_platform, buy_no_price
    )
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
    pm_outcome_token: str = "yes",
) -> Opportunity | None:
    """
    Fetch live order books and replace midpoint prices with executable VWAP.

    pm_outcome_token tells us which PM CLOB token represents "this outcome
    wins". For futures and away-team game markets it's "yes" (PM's YES
    token == this outcome wins). For home-team game markets it's "no"
    (PM's YES token == away team wins, so the home team's "this team
    wins" leg actually lives on PM's NO token). Without this hint the
    enrichment would replace our flipped midpoint prices with the wrong
    token's ask VWAP and produce nonsense opportunities.

    Returns the enriched opportunity, or None if liquidity is insufficient.
    On API failure, returns the opportunity unchanged with liquidity_verified=False.
    """
    opp.kalshi_market_id = kalshi_market_id
    opp.polymarket_market_id = polymarket_market_id

    prices = get_executable_prices(kalshi_market_id, polymarket_market_id, ORDERBOOK_TARGET_SIZE)
    if prices is None:
        log.debug(f"Order book fetch failed for {opp.outcome}, keeping midpoint prices")
        return opp

    # For this outcome, the PM token that represents "this outcome wins"
    # is pm_outcome_token; the opposite token represents "this outcome loses".
    if pm_outcome_token == "yes":
        pm_win_ask = prices.polymarket_yes_ask_vwap
        pm_win_depth = prices.polymarket_yes_depth
        pm_lose_ask = prices.polymarket_no_ask_vwap
        pm_lose_depth = prices.polymarket_no_depth
    else:
        pm_win_ask = prices.polymarket_no_ask_vwap
        pm_win_depth = prices.polymarket_no_depth
        pm_lose_ask = prices.polymarket_yes_ask_vwap
        pm_lose_depth = prices.polymarket_yes_depth

    # Determine executable prices for this specific strategy.
    # buy_yes_platform tells us where we're buying the "this team wins" leg.
    if opp.buy_yes_platform == "kalshi":
        exec_yes = prices.kalshi_yes_ask_vwap
        exec_no = pm_lose_ask
        yes_depth = prices.kalshi_yes_depth
        no_depth = pm_lose_depth
    else:
        exec_yes = pm_win_ask
        exec_no = prices.kalshi_no_ask_vwap
        yes_depth = pm_win_depth
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

    opp.fees = round(
        _calculate_fees(
            opp.buy_yes_platform, exec_yes, opp.buy_no_platform, exec_no
        ),
        4,
    )
    opp.net_profit = round(opp.gross_profit - opp.fees, 4)

    if opp.net_profit <= 0:
        return None

    opp.roi = round(opp.net_profit / opp.total_cost, 4)
    opp.capital_required = round(opp.total_cost * 100, 2)

    if opp.roi < MIN_ROI_THRESHOLD:
        return None

    return opp


def _fetch_platform_rows(cursor, table: str, platform: str, market_type: str) -> list[dict]:
    """
    Fetch all rows for a given platform and market_type from a futures table.

    Allows rows where one side has hit the 0.0 floor (e.g., a team that's
    been mathematically eliminated). Those edge cases occasionally produce
    real arbs against the other platform's residual price, and the
    downstream order book / depth check will filter out anything actually
    untradeable.
    """
    cursor.execute(f"""
        SELECT outcome, yes_price, no_price, volume, liquidity, platform_market_id
        FROM {table}
        WHERE platform = %s
          AND market_type = %s
          AND yes_price IS NOT NULL
          AND no_price IS NOT NULL
          AND (yes_price > 0 OR no_price > 0)
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


def _parse_kalshi_game_matchup(matchup: str) -> tuple[str, str] | None:
    """Parse Kalshi matchup 'AWAY @ HOME' into (away_code, home_code)."""
    if not matchup or " @ " not in matchup:
        return None
    parts = matchup.split(" @ ", 1)
    if len(parts) != 2:
        return None
    return parts[0].strip().upper(), parts[1].strip().upper()


GAME_SCAN_WINDOW_HOURS = 36


def _fetch_kalshi_game_rows(cursor, table: str) -> list[dict]:
    """
    Fetch moneyline-winner Kalshi game rows (one per team per game)
    for games tipping off in the next GAME_SCAN_WINDOW_HOURS. Within
    (matchup, team, commence_time) we pick the latest updated row to
    avoid mixing stale snapshots.
    """
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'market_subtype'
        )
    """, (table,))
    has_subtype = cursor.fetchone()[0]

    subtype_filter = (
        "(market_subtype IS NULL OR market_subtype = 'winner')"
        if has_subtype else "TRUE"
    )

    cursor.execute(f"""
        SELECT DISTINCT ON (matchup, team, commence_time)
            matchup, team, yes_price, no_price, volume,
            market_ticker, commence_time
        FROM {table}
        WHERE platform = 'kalshi'
          AND team IS NOT NULL
          AND {subtype_filter}
          AND yes_price IS NOT NULL AND no_price IS NOT NULL
          AND (yes_price > 0 OR no_price > 0)
          AND commence_time IS NOT NULL
          AND commence_time BETWEEN NOW() - INTERVAL '2 hours'
                                AND NOW() + INTERVAL '{GAME_SCAN_WINDOW_HOURS} hours'
        ORDER BY matchup, team, commence_time, updated_at DESC
    """)
    return [
        {
            "matchup": row[0],
            "team": row[1],
            "yes_price": float(row[2]),
            "no_price": float(row[3]),
            "volume": float(row[4]) if row[4] else None,
            "market_id": row[5],
            "commence_time": row[6],
        }
        for row in cursor.fetchall()
    ]


def _fetch_pm_game_rows(cursor, futures_table: str) -> list[dict]:
    """
    Fetch Polymarket moneyline game rows for upcoming games in the
    scan window. Filters out spreads, totals, non-league junk (AHL,
    soccer, KBO, etc.) so only plausible per-league moneyline rows
    remain. De-dupes to latest updated_at per (home, away, commence_time).
    """
    cursor.execute(f"""
        SELECT DISTINCT ON (home_team, away_team, commence_time)
            matchup, outcome, home_team, away_team,
            yes_price, no_price, liquidity, platform_market_id, commence_time
        FROM {futures_table}
        WHERE platform = 'polymarket'
          AND market_type = 'game'
          AND home_team IS NOT NULL
          AND away_team IS NOT NULL
          AND yes_price IS NOT NULL AND no_price IS NOT NULL
          AND (yes_price > 0 OR no_price > 0)
          AND commence_time IS NOT NULL
          AND commence_time BETWEEN NOW() - INTERVAL '2 hours'
                                AND NOW() + INTERVAL '{GAME_SCAN_WINDOW_HOURS} hours'
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND outcome NOT LIKE %s
          AND (matchup IS NULL OR matchup NOT LIKE 'AHL%%')
        ORDER BY home_team, away_team, commence_time, updated_at DESC
    """, (
        "%O/U%",
        "%Spread%",
        "%Both Teams%",
        "Will %",
        "%: Total%",
        "%qualify%",
        "Will there be %",
    ))

    rows = []
    for row in cursor.fetchall():
        rows.append({
            "matchup": row[0],
            "outcome": row[1],
            "home_team": row[2],
            "away_team": row[3],
            "yes_price": float(row[4]),
            "no_price": float(row[5]),
            "liquidity": float(row[6]) if row[6] else None,
            "market_id": row[7],
            "commence_time": row[8],
        })
    return rows


def _build_pm_game_index(rows: list[dict]) -> dict[tuple[str, str, str], dict]:
    """
    Index PM game rows by (away_team, home_team, commence_date) so
    back-to-back games between the same teams on different days are
    not conflated. commence_date is the UTC YYYY-MM-DD.
    """
    index: dict[tuple[str, str, str], dict] = {}
    for r in rows:
        commence = r.get("commence_time")
        if commence is None:
            continue
        key = (
            r["away_team"].strip(),
            r["home_team"].strip(),
            commence.date().isoformat(),
        )
        index.setdefault(key, r)
    return index


def scan_game_markets() -> list[Opportunity]:
    """
    Scan per-game moneyline markets for arbitrage.

    Kalshi stores one row per team per game ("winner" subtype) with a
    3-letter team code. Polymarket stores one row per (game, market) where
    `yes_price` = away team wins and `no_price` = home team wins on
    moneyline rows. We translate each Kalshi team code to the form PM uses
    for that sport (codes for NBA, nicknames for NHL, full names for MLB)
    and look up the matching PM game by (away, home) tuple.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    opportunities: list[Opportunity] = []

    try:
        for kalshi_table, pm_table in GAME_MARKET_PAIRS:
            sport = _sport_from_table(kalshi_table)

            kalshi_rows = _fetch_kalshi_game_rows(cursor, kalshi_table)
            pm_rows = _fetch_pm_game_rows(cursor, pm_table)
            pm_index = _build_pm_game_index(pm_rows)

            matched_count = 0
            for k in kalshi_rows:
                parsed = _parse_kalshi_game_matchup(k["matchup"])
                if not parsed:
                    continue
                away_code, home_code = parsed
                k_team = k["team"].strip().upper()

                away_pm = kalshi_code_to_pm_game_team(away_code, sport)
                home_pm = kalshi_code_to_pm_game_team(home_code, sport)
                if not away_pm or not home_pm:
                    continue

                commence = k.get("commence_time")
                if commence is None:
                    continue
                pm_row = pm_index.get(
                    (away_pm, home_pm, commence.date().isoformat())
                )
                if pm_row is None:
                    continue
                matched_count += 1

                # Determine which PM token represents "this team wins":
                # PM yes_price = away team wins, no_price = home team wins.
                if k_team == away_code:
                    pm_yes_equiv = pm_row["yes_price"]  # "this team wins"
                    pm_no_equiv = pm_row["no_price"]    # "this team loses"
                    pm_outcome_token = "yes"
                elif k_team == home_code:
                    pm_yes_equiv = pm_row["no_price"]   # home team wins -> no on PM
                    pm_no_equiv = pm_row["yes_price"]
                    pm_outcome_token = "no"
                else:
                    # Team doesn't match either side of the parsed matchup
                    continue

                outcome = f"{k_team} ({k['matchup']})"

                # Strategy 1: Buy YES on Kalshi + NO on Polymarket (equiv)
                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "kalshi", k["yes_price"], "polymarket", pm_no_equiv,
                    kalshi_volume=k.get("volume"),
                    polymarket_liquidity=pm_row.get("liquidity"),
                )
                if opp:
                    opp.sport = sport
                    opp = _enrich_with_orderbook(
                        opp, k["market_id"], pm_row["market_id"],
                        pm_outcome_token=pm_outcome_token,
                    )
                    if opp:
                        opportunities.append(opp)

                # Strategy 2: Buy YES on Polymarket (equiv) + NO on Kalshi
                opp = _build_opportunity(
                    kalshi_table, "game_winner", outcome,
                    "polymarket", pm_yes_equiv, "kalshi", k["no_price"],
                    kalshi_volume=k.get("volume"),
                    polymarket_liquidity=pm_row.get("liquidity"),
                )
                if opp:
                    opp.sport = sport
                    opp = _enrich_with_orderbook(
                        opp, k["market_id"], pm_row["market_id"],
                        pm_outcome_token=pm_outcome_token,
                    )
                    if opp:
                        opportunities.append(opp)

            log.info(
                f"  {kalshi_table} <-> {pm_table}: "
                f"{len(kalshi_rows)} kalshi x {len(pm_rows)} pm -> {matched_count} matched"
            )

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
