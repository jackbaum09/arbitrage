"""
Tests for execution.risk.BalanceSnapshot + fetch_balances.

BalanceSnapshot tracks live per-platform wallet balances captured at the
start of an execute loop, with a debit() method the manager calls after
each successful placement so subsequent opportunities in the same scan
see correct remaining balances without re-fetching.

fetch_balances() must return None when EITHER configured client raises,
so callers can fail closed and skip execution for that scan.
"""

from __future__ import annotations

import pytest

from execution.risk import BalanceSnapshot, fetch_balances


# ---------------------------------------------------------------------------
# BalanceSnapshot
# ---------------------------------------------------------------------------

def test_post_init_populates_remaining_from_starting_balances():
    snap = BalanceSnapshot(kalshi=500.0, polymarket=1200.0)
    assert snap.remaining == {"kalshi": 500.0, "polymarket": 1200.0}


def test_post_init_preserves_explicit_remaining():
    """If a caller passes its own `remaining` dict, post_init should not stomp it."""
    snap = BalanceSnapshot(
        kalshi=500.0, polymarket=1200.0,
        remaining={"kalshi": 100.0, "polymarket": 50.0},
    )
    assert snap.remaining == {"kalshi": 100.0, "polymarket": 50.0}


def test_debit_decrements_remaining():
    snap = BalanceSnapshot(kalshi=500.0, polymarket=1200.0)
    snap.debit("kalshi", 75.0)
    assert snap.remaining["kalshi"] == 425.0
    assert snap.remaining["polymarket"] == 1200.0


def test_debit_clamps_at_zero_never_goes_negative():
    """A debit larger than remaining must clamp at 0, not produce a negative."""
    snap = BalanceSnapshot(kalshi=50.0, polymarket=0.0)
    snap.debit("kalshi", 75.0)
    assert snap.remaining["kalshi"] == 0.0


def test_debit_unknown_platform_creates_zero_then_clamps():
    """Defensive: debiting an unknown platform shouldn't crash."""
    snap = BalanceSnapshot(kalshi=100.0, polymarket=100.0)
    snap.debit("alpaca", 25.0)
    assert snap.remaining.get("alpaca", 0.0) == 0.0


def test_default_balances_are_zero():
    snap = BalanceSnapshot()
    assert snap.kalshi == 0.0
    assert snap.polymarket == 0.0
    assert snap.remaining == {"kalshi": 0.0, "polymarket": 0.0}


# ---------------------------------------------------------------------------
# fetch_balances
# ---------------------------------------------------------------------------

class FakeKalshi:
    def __init__(self, balance=None, raises=None):
        self._balance = balance
        self._raises = raises

    def get_balance(self):
        if self._raises:
            raise self._raises
        return self._balance


class FakePolymarket:
    def __init__(self, balance=None, raises=None):
        self._balance = balance
        self._raises = raises

    def get_balance_dollars(self):
        if self._raises:
            raise self._raises
        return self._balance


def test_fetch_balances_happy_path():
    snap = fetch_balances(FakeKalshi(balance=970.0), FakePolymarket(balance=1200.0))
    assert snap is not None
    assert snap.kalshi == 970.0
    assert snap.polymarket == 1200.0
    assert snap.remaining == {"kalshi": 970.0, "polymarket": 1200.0}


def test_fetch_balances_returns_none_when_kalshi_raises():
    """Fail-closed: any client error must return None so the caller skips execution."""
    snap = fetch_balances(
        FakeKalshi(raises=RuntimeError("network down")),
        FakePolymarket(balance=1200.0),
    )
    assert snap is None


def test_fetch_balances_returns_none_when_polymarket_raises():
    snap = fetch_balances(
        FakeKalshi(balance=970.0),
        FakePolymarket(raises=RuntimeError("auth expired")),
    )
    assert snap is None


def test_fetch_balances_treats_none_returns_as_zero():
    """A client returning None (e.g. unfunded account) should coerce to 0.0, not crash."""
    snap = fetch_balances(
        FakeKalshi(balance=None),
        FakePolymarket(balance=None),
    )
    assert snap is not None
    assert snap.kalshi == 0.0
    assert snap.polymarket == 0.0


def test_fetch_balances_handles_none_clients():
    """When a client is None (not configured), its balance contributes 0."""
    snap = fetch_balances(None, FakePolymarket(balance=1200.0))
    assert snap is not None
    assert snap.kalshi == 0.0
    assert snap.polymarket == 1200.0

    snap = fetch_balances(FakeKalshi(balance=970.0), None)
    assert snap is not None
    assert snap.kalshi == 970.0
    assert snap.polymarket == 0.0


def test_fetch_balances_both_none_returns_empty_snapshot():
    """Both clients None (no execution configured) should still return a snapshot
    rather than None — None is reserved for the 'a fetch failed' signal."""
    snap = fetch_balances(None, None)
    assert snap is not None
    assert snap.kalshi == 0.0
    assert snap.polymarket == 0.0
