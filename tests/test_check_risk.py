"""
Tests for execution.risk.check_risk.

check_risk runs eight ordered gates before allowing a trade. Each gate
is responsible for blocking one specific class of unsafe trade. We test
each gate independently by holding the others fixed in a known-good
state and varying just the field the gate inspects.
"""

from __future__ import annotations

import pytest

from execution.risk import BalanceSnapshot, RiskLimits, check_risk
from scanner.models import Opportunity


def make_opp(
    *,
    roi: float = 0.05,
    total_cost: float = 0.95,
    buy_yes_price: float = 0.55,
    buy_no_price: float = 0.40,
    buy_yes_platform: str = "kalshi",
    buy_no_platform: str = "polymarket",
    capital_required: float = 95.0,
    liquidity_verified: bool = True,
    max_executable_size: float = 1000.0,
    buy_yes_depth: float = 1000.0,
    buy_no_depth: float = 1000.0,
) -> Opportunity:
    """Construct an Opportunity that passes every gate by default. Each
    test then mutates a single field to exercise one specific gate."""
    return Opportunity(
        sport="nba", market_type="game", outcome="LAL",
        buy_yes_platform=buy_yes_platform, buy_yes_price=buy_yes_price,
        buy_no_platform=buy_no_platform, buy_no_price=buy_no_price,
        total_cost=total_cost, gross_profit=1.0 - total_cost,
        fees=0.025, net_profit=(1.0 - total_cost) - 0.025, roi=roi,
        liquidity_verified=liquidity_verified,
        max_executable_size=max_executable_size,
        buy_yes_depth=buy_yes_depth, buy_no_depth=buy_no_depth,
        capital_required=capital_required,
    )


def make_limits(**overrides) -> RiskLimits:
    """RiskLimits with defaults that the make_opp() default opportunity
    passes cleanly. Tests pass keyword overrides to tighten one limit."""
    base = dict(
        max_position_size=100.0,
        max_total_capital=1000.0,
        max_single_trade=200.0,
        min_roi_threshold=0.01,
        max_open_positions=10,
        require_liquidity_verified=True,
    )
    base.update(overrides)
    return RiskLimits(**base)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_passes_all_gates_in_default_state():
    """Sanity check: the default opp + default limits + default balances pass."""
    ok, reason = check_risk(
        opportunity=make_opp(),
        deployed_capital=0.0,
        open_position_count=0,
        limits=make_limits(),
    )
    assert ok is True, reason
    assert "passed" in reason.lower()


# ---------------------------------------------------------------------------
# Gate 1: liquidity verification
# ---------------------------------------------------------------------------

def test_blocks_unverified_when_required():
    ok, reason = check_risk(
        opportunity=make_opp(liquidity_verified=False),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
    )
    assert ok is False
    assert "liquidity-verified" in reason


def test_allows_unverified_when_check_disabled():
    ok, _ = check_risk(
        opportunity=make_opp(liquidity_verified=False),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(require_liquidity_verified=False),
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Gate 2: ROI threshold
# ---------------------------------------------------------------------------

def test_blocks_below_min_roi():
    ok, reason = check_risk(
        opportunity=make_opp(roi=0.005),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(min_roi_threshold=0.01),
    )
    assert ok is False
    assert "ROI" in reason


def test_allows_at_min_roi_boundary():
    """Inclusive boundary: roi == threshold should be allowed."""
    ok, _ = check_risk(
        opportunity=make_opp(roi=0.01),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(min_roi_threshold=0.01),
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Gate 3: open position count
# ---------------------------------------------------------------------------

def test_blocks_at_max_open_positions():
    ok, reason = check_risk(
        opportunity=make_opp(),
        deployed_capital=0.0, open_position_count=10,
        limits=make_limits(max_open_positions=10),
    )
    assert ok is False
    assert "max open positions" in reason


# ---------------------------------------------------------------------------
# Gate 4: max single trade
# ---------------------------------------------------------------------------

def test_blocks_when_trade_cost_exceeds_max_single_trade():
    ok, reason = check_risk(
        opportunity=make_opp(capital_required=150.0),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(max_single_trade=100.0),
    )
    assert ok is False
    assert "exceeds max" in reason


# ---------------------------------------------------------------------------
# Gate 5: max total capital
# ---------------------------------------------------------------------------

def test_blocks_when_total_deployed_would_exceed_max_capital():
    ok, reason = check_risk(
        opportunity=make_opp(capital_required=95.0),
        deployed_capital=950.0, open_position_count=0,
        limits=make_limits(max_total_capital=1000.0),
    )
    assert ok is False
    assert "max capital" in reason


# ---------------------------------------------------------------------------
# Gate 6: per-leg dollar exposure
# ---------------------------------------------------------------------------

def test_blocks_when_leg_dollars_exceed_max_position_size():
    """
    With max_position_size = $30 but per-contract YES price = $0.55, the
    sizing formula buys floor(min(95, 200, 1000) / 0.95) = 100 contracts,
    so the YES leg costs ~$55 — over the $30 cap.
    """
    ok, reason = check_risk(
        opportunity=make_opp(),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(max_position_size=30.0),
    )
    assert ok is False
    assert "max position size" in reason


# ---------------------------------------------------------------------------
# Gate 7: order book depth vs trade cost
# ---------------------------------------------------------------------------

def test_blocks_when_min_depth_below_trade_cost():
    """
    capital_required is $95; YES depth $50 → min depth < trade cost,
    so the gate fires before we'd ever try to size into a thin book.
    """
    ok, reason = check_risk(
        opportunity=make_opp(buy_yes_depth=50.0, buy_no_depth=200.0),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
    )
    assert ok is False
    assert "depth" in reason.lower()


def test_skips_depth_check_when_either_depth_is_none():
    """
    Depth gate only fires when both depths are populated. A None depth
    means the order book wasn't enriched (e.g. an opportunity that came
    in as midpoint-only) and shouldn't crash the gate.
    """
    opp = make_opp()
    opp.buy_yes_depth = None
    ok, _ = check_risk(
        opportunity=opp,
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Gate 8: live per-platform balances
# ---------------------------------------------------------------------------

def test_blocks_when_yes_platform_balance_insufficient():
    balances = BalanceSnapshot(kalshi=10.0, polymarket=1000.0)
    ok, reason = check_risk(
        opportunity=make_opp(),  # YES on kalshi at $0.55 * 100 contracts = $55
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
        balances=balances,
    )
    assert ok is False
    assert "kalshi" in reason.lower()
    assert "yes leg" in reason.lower()


def test_blocks_when_no_platform_balance_insufficient():
    balances = BalanceSnapshot(kalshi=1000.0, polymarket=10.0)
    ok, reason = check_risk(
        opportunity=make_opp(),  # NO on polymarket at $0.40 * 100 contracts = $40
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
        balances=balances,
    )
    assert ok is False
    assert "polymarket" in reason.lower()
    assert "no leg" in reason.lower()


def test_allows_when_balances_sufficient():
    balances = BalanceSnapshot(kalshi=200.0, polymarket=200.0)
    ok, _ = check_risk(
        opportunity=make_opp(),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
        balances=balances,
    )
    assert ok is True


def test_skips_balance_check_when_balances_none():
    """Scan-only mode: no balances passed -> skip the live-balance gate."""
    ok, _ = check_risk(
        opportunity=make_opp(),
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
        balances=None,
    )
    assert ok is True


def test_blocks_same_platform_combined_legs():
    """
    Defensive: if both legs land on the same platform (shouldn't happen for
    cross-platform arbs), the combined leg dollars must fit in the single
    balance. We construct an opp with both legs on Kalshi to exercise this.
    """
    opp = make_opp(
        buy_yes_platform="kalshi",
        buy_no_platform="kalshi",
    )
    # YES leg ~$55 + NO leg ~$40 = $95 combined; only $60 available
    balances = BalanceSnapshot(kalshi=60.0, polymarket=0.0)
    ok, reason = check_risk(
        opportunity=opp,
        deployed_capital=0.0, open_position_count=0,
        limits=make_limits(),
        balances=balances,
    )
    assert ok is False
    assert "combined" in reason.lower()
