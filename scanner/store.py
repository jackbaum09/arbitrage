"""
Write detected arbitrage opportunities to the Supabase
arbitrage_opportunities table.
"""

from __future__ import annotations

import logging
from datetime import datetime

from config import get_db_connection
from scanner.models import Opportunity

log = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    id                    BIGSERIAL PRIMARY KEY,
    opportunity_key       TEXT NOT NULL,
    sport                 TEXT NOT NULL,
    market_type           TEXT NOT NULL,
    outcome               TEXT NOT NULL,
    buy_yes_platform      TEXT NOT NULL,
    buy_yes_price         NUMERIC(6,4) NOT NULL,
    buy_no_platform       TEXT NOT NULL,
    buy_no_price          NUMERIC(6,4) NOT NULL,
    total_cost            NUMERIC(6,4) NOT NULL,
    gross_profit          NUMERIC(6,4) NOT NULL,
    fees                  NUMERIC(6,4) NOT NULL,
    net_profit            NUMERIC(6,4) NOT NULL,
    roi                   NUMERIC(6,4) NOT NULL,
    kalshi_volume         NUMERIC,
    polymarket_liquidity  NUMERIC,
    capital_required      NUMERIC(10,2) NOT NULL,
    source_table          TEXT NOT NULL,
    detected_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expired_at            TIMESTAMPTZ,
    status                TEXT NOT NULL DEFAULT 'active'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_arb_opps_active_key
    ON arbitrage_opportunities(opportunity_key) WHERE (status = 'active');
CREATE INDEX IF NOT EXISTS idx_arb_opps_status ON arbitrage_opportunities(status);
CREATE INDEX IF NOT EXISTS idx_arb_opps_sport ON arbitrage_opportunities(sport);
CREATE INDEX IF NOT EXISTS idx_arb_opps_roi ON arbitrage_opportunities(roi DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opps_detected ON arbitrage_opportunities(detected_at DESC);
"""

UPSERT_SQL = """
INSERT INTO arbitrage_opportunities (
    opportunity_key, sport, market_type, outcome,
    buy_yes_platform, buy_yes_price, buy_no_platform, buy_no_price,
    total_cost, gross_profit, fees, net_profit, roi,
    kalshi_volume, polymarket_liquidity, capital_required,
    source_table, detected_at, status
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, 'active'
)
ON CONFLICT (opportunity_key) WHERE status = 'active'
DO UPDATE SET
    buy_yes_price        = EXCLUDED.buy_yes_price,
    buy_no_price         = EXCLUDED.buy_no_price,
    total_cost           = EXCLUDED.total_cost,
    gross_profit         = EXCLUDED.gross_profit,
    fees                 = EXCLUDED.fees,
    net_profit           = EXCLUDED.net_profit,
    roi                  = EXCLUDED.roi,
    kalshi_volume        = EXCLUDED.kalshi_volume,
    polymarket_liquidity = EXCLUDED.polymarket_liquidity,
    capital_required     = EXCLUDED.capital_required,
    detected_at          = EXCLUDED.detected_at
"""

EXPIRE_SQL = """
UPDATE arbitrage_opportunities
SET status = 'expired', expired_at = %s
WHERE status = 'active'
  AND opportunity_key NOT IN %s
"""

EXPIRE_ALL_SQL = """
UPDATE arbitrage_opportunities
SET status = 'expired', expired_at = %s
WHERE status = 'active'
"""


def ensure_table() -> None:
    """Create the arbitrage_opportunities table if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        log.info("arbitrage_opportunities table ready")
    finally:
        conn.close()


def write_opportunities(opportunities: list[Opportunity]) -> int:
    """
    Upsert active opportunities and expire stale ones.

    - New opportunities are inserted with status='active'.
    - Existing active opportunities are updated with fresh prices/ROI.
    - Previously active opportunities not in the current scan are marked 'expired'.

    Returns the number of active opportunities written.
    """
    conn = get_db_connection()
    now = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            # Upsert current opportunities
            for opp in opportunities:
                cur.execute(UPSERT_SQL, (
                    opp.opportunity_key, opp.sport, opp.market_type, opp.outcome,
                    opp.buy_yes_platform, opp.buy_yes_price,
                    opp.buy_no_platform, opp.buy_no_price,
                    opp.total_cost, opp.gross_profit, opp.fees, opp.net_profit, opp.roi,
                    opp.kalshi_volume, opp.polymarket_liquidity, opp.capital_required,
                    opp.source_table, now,
                ))

            # Expire opportunities no longer detected
            if opportunities:
                active_keys = tuple(o.opportunity_key for o in opportunities)
                cur.execute(EXPIRE_SQL, (now, active_keys))
                expired_count = cur.rowcount
            else:
                cur.execute(EXPIRE_ALL_SQL, (now,))
                expired_count = cur.rowcount

            if expired_count > 0:
                log.info(f"Expired {expired_count} stale opportunity(ies)")

        conn.commit()
        log.info(f"Wrote {len(opportunities)} active opportunity(ies)")
        return len(opportunities)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
