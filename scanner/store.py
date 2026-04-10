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

MIGRATE_COLUMNS_SQL = """
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_yes_executable_price NUMERIC(6,4);
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_no_executable_price NUMERIC(6,4);
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_yes_midpoint NUMERIC(6,4);
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_no_midpoint NUMERIC(6,4);
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_yes_depth NUMERIC;
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS buy_no_depth NUMERIC;
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS max_executable_size NUMERIC;
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS liquidity_verified BOOLEAN DEFAULT FALSE;
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS kalshi_market_id TEXT;
ALTER TABLE arbitrage_opportunities ADD COLUMN IF NOT EXISTS polymarket_market_id TEXT;
"""

CREATE_PRICE_HISTORY_SQL = """
CREATE TABLE IF NOT EXISTS opportunity_price_history (
    id                    BIGSERIAL PRIMARY KEY,
    opportunity_key       TEXT NOT NULL,
    snapshot_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    buy_yes_price         NUMERIC(6,4),
    buy_no_price          NUMERIC(6,4),
    buy_yes_executable    NUMERIC(6,4),
    buy_no_executable     NUMERIC(6,4),
    buy_yes_depth         NUMERIC,
    buy_no_depth          NUMERIC,
    roi                   NUMERIC(6,4),
    roi_executable        NUMERIC(6,4)
);

CREATE INDEX IF NOT EXISTS idx_price_hist_key
    ON opportunity_price_history(opportunity_key, snapshot_at DESC);
"""

CREATE_SCAN_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS scanner_runs (
    id                    BIGSERIAL PRIMARY KEY,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_seconds      NUMERIC,
    opportunities_found   INTEGER NOT NULL DEFAULT 0,
    opportunities_verified INTEGER NOT NULL DEFAULT 0,
    max_roi               NUMERIC(6,4),
    status                TEXT NOT NULL DEFAULT 'success',
    error_message         TEXT
);

CREATE INDEX IF NOT EXISTS idx_scanner_runs_finished
    ON scanner_runs(finished_at DESC);
"""

RECORD_RUN_SQL = """
INSERT INTO scanner_runs (
    started_at, finished_at, duration_seconds,
    opportunities_found, opportunities_verified, max_roi,
    status, error_message
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

UPSERT_SQL = """
INSERT INTO arbitrage_opportunities (
    opportunity_key, sport, market_type, outcome,
    buy_yes_platform, buy_yes_price, buy_no_platform, buy_no_price,
    total_cost, gross_profit, fees, net_profit, roi,
    kalshi_volume, polymarket_liquidity, capital_required,
    source_table, detected_at, status,
    buy_yes_executable_price, buy_no_executable_price,
    buy_yes_midpoint, buy_no_midpoint,
    buy_yes_depth, buy_no_depth, max_executable_size,
    liquidity_verified, kalshi_market_id, polymarket_market_id
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, 'active',
    %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (opportunity_key) WHERE status = 'active'
DO UPDATE SET
    buy_yes_price              = EXCLUDED.buy_yes_price,
    buy_no_price               = EXCLUDED.buy_no_price,
    total_cost                 = EXCLUDED.total_cost,
    gross_profit               = EXCLUDED.gross_profit,
    fees                       = EXCLUDED.fees,
    net_profit                 = EXCLUDED.net_profit,
    roi                        = EXCLUDED.roi,
    kalshi_volume              = EXCLUDED.kalshi_volume,
    polymarket_liquidity       = EXCLUDED.polymarket_liquidity,
    capital_required           = EXCLUDED.capital_required,
    detected_at                = EXCLUDED.detected_at,
    buy_yes_executable_price   = EXCLUDED.buy_yes_executable_price,
    buy_no_executable_price    = EXCLUDED.buy_no_executable_price,
    buy_yes_midpoint           = EXCLUDED.buy_yes_midpoint,
    buy_no_midpoint            = EXCLUDED.buy_no_midpoint,
    buy_yes_depth              = EXCLUDED.buy_yes_depth,
    buy_no_depth               = EXCLUDED.buy_no_depth,
    max_executable_size        = EXCLUDED.max_executable_size,
    liquidity_verified         = EXCLUDED.liquidity_verified,
    kalshi_market_id           = EXCLUDED.kalshi_market_id,
    polymarket_market_id       = EXCLUDED.polymarket_market_id
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

SNAPSHOT_SQL = """
INSERT INTO opportunity_price_history (
    opportunity_key, snapshot_at,
    buy_yes_price, buy_no_price,
    buy_yes_executable, buy_no_executable,
    buy_yes_depth, buy_no_depth,
    roi, roi_executable
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_table() -> None:
    """Create / migrate the arbitrage tables."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(MIGRATE_COLUMNS_SQL)
            cur.execute(CREATE_PRICE_HISTORY_SQL)
            cur.execute(CREATE_SCAN_RUNS_SQL)
        conn.commit()
        log.info("arbitrage tables ready (with price history + scan runs)")
    finally:
        conn.close()


def record_scan_run(
    started_at: datetime,
    finished_at: datetime,
    opportunities: list[Opportunity],
    status: str = "success",
    error_message: str | None = None,
) -> None:
    """Log a single scan run to scanner_runs table for heartbeat / observability."""
    duration = (finished_at - started_at).total_seconds()
    verified = sum(1 for o in opportunities if o.liquidity_verified)
    max_roi = max((float(o.roi) for o in opportunities), default=None)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(RECORD_RUN_SQL, (
                started_at, finished_at, duration,
                len(opportunities), verified, max_roi,
                status, error_message,
            ))
        conn.commit()
    except Exception as e:
        log.warning(f"Failed to record scan run: {e}")
    finally:
        conn.close()


def _record_price_snapshot(cur, opp: Opportunity, now: datetime) -> None:
    """Insert a price history snapshot for an opportunity."""
    # Calculate executable ROI if we have executable prices
    roi_executable = None
    if opp.buy_yes_executable_price and opp.buy_no_executable_price:
        exec_total = opp.buy_yes_executable_price + opp.buy_no_executable_price
        if exec_total > 0:
            exec_gross = 1.0 - exec_total
            roi_executable = round(exec_gross / exec_total, 4) if exec_gross > 0 else 0.0

    cur.execute(SNAPSHOT_SQL, (
        opp.opportunity_key, now,
        opp.buy_yes_midpoint or opp.buy_yes_price,
        opp.buy_no_midpoint or opp.buy_no_price,
        opp.buy_yes_executable_price,
        opp.buy_no_executable_price,
        opp.buy_yes_depth,
        opp.buy_no_depth,
        opp.roi,
        roi_executable,
    ))


def write_opportunities(opportunities: list[Opportunity]) -> int:
    """
    Upsert active opportunities, record price snapshots, and expire stale ones.

    - New opportunities are inserted with status='active'.
    - Existing active opportunities are updated with fresh prices/ROI.
    - Previously active opportunities not in the current scan are marked 'expired'.
    - A price history snapshot is recorded for each active opportunity.

    Returns the number of active opportunities written.
    """
    conn = get_db_connection()
    now = datetime.utcnow()

    try:
        with conn.cursor() as cur:
            # Upsert current opportunities and record snapshots
            for opp in opportunities:
                cur.execute(UPSERT_SQL, (
                    opp.opportunity_key, opp.sport, opp.market_type, opp.outcome,
                    opp.buy_yes_platform, opp.buy_yes_price,
                    opp.buy_no_platform, opp.buy_no_price,
                    opp.total_cost, opp.gross_profit, opp.fees, opp.net_profit, opp.roi,
                    opp.kalshi_volume, opp.polymarket_liquidity, opp.capital_required,
                    opp.source_table, now,
                    opp.buy_yes_executable_price, opp.buy_no_executable_price,
                    opp.buy_yes_midpoint, opp.buy_no_midpoint,
                    opp.buy_yes_depth, opp.buy_no_depth, opp.max_executable_size,
                    opp.liquidity_verified, opp.kalshi_market_id, opp.polymarket_market_id,
                ))
                _record_price_snapshot(cur, opp, now)

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
