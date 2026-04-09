"""
Persist trade execution records to the database.
"""

from __future__ import annotations

import logging

from config import get_db_connection
from execution.models import ArbitrageExecution

log = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS arbitrage_executions (
    id                    BIGSERIAL PRIMARY KEY,
    opportunity_key       TEXT NOT NULL,
    executed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status                TEXT NOT NULL,
    yes_platform          TEXT NOT NULL,
    yes_market_id         TEXT,
    yes_order_id          TEXT,
    yes_price             NUMERIC(6,4),
    yes_size              INTEGER,
    yes_filled_size       INTEGER,
    yes_filled_price      NUMERIC(6,4),
    no_platform           TEXT NOT NULL,
    no_market_id          TEXT,
    no_order_id           TEXT,
    no_price              NUMERIC(6,4),
    no_size               INTEGER,
    no_filled_size        INTEGER,
    no_filled_price       NUMERIC(6,4),
    total_cost            NUMERIC(10,2),
    expected_profit       NUMERIC(10,2),
    error                 TEXT
);

CREATE INDEX IF NOT EXISTS idx_arb_exec_key
    ON arbitrage_executions(opportunity_key, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_arb_exec_status
    ON arbitrage_executions(status);
"""

INSERT_SQL = """
INSERT INTO arbitrage_executions (
    opportunity_key, executed_at, status,
    yes_platform, yes_market_id, yes_order_id,
    yes_price, yes_size, yes_filled_size, yes_filled_price,
    no_platform, no_market_id, no_order_id,
    no_price, no_size, no_filled_size, no_filled_price,
    total_cost, expected_profit, error
) VALUES (
    %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)
"""


def ensure_execution_table() -> None:
    """Create the arbitrage_executions table if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        log.info("arbitrage_executions table ready")
    finally:
        conn.close()


def record_execution(execution: ArbitrageExecution) -> None:
    """Write an execution record to the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            yes = execution.yes_leg
            no = execution.no_leg

            cur.execute(INSERT_SQL, (
                execution.opportunity_key,
                execution.executed_at,
                execution.status,
                # YES leg
                yes.order.platform if yes else "",
                yes.order.market_id if yes else None,
                yes.order_id if yes else None,
                yes.order.price if yes else None,
                yes.order.size if yes else None,
                yes.filled_size if yes else None,
                yes.filled_price if yes else None,
                # NO leg
                no.order.platform if no else "",
                no.order.market_id if no else None,
                no.order_id if no else None,
                no.order.price if no else None,
                no.order.size if no else None,
                no.filled_size if no else None,
                no.filled_price if no else None,
                # Summary
                execution.total_cost,
                execution.expected_profit,
                execution.error,
            ))
        conn.commit()
        log.info(f"Recorded execution: {execution.opportunity_key} [{execution.status}]")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_open_execution_count() -> int:
    """Count executions with status 'success' (active positions)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM arbitrage_executions WHERE status = 'success'"
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_deployed_capital() -> float:
    """Sum total cost of all successful (active) executions."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(total_cost), 0) FROM arbitrage_executions WHERE status = 'success'"
            )
            return float(cur.fetchone()[0])
    finally:
        conn.close()
