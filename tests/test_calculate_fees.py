"""
Tests for scanner.detect._calculate_fees and the per-leg helper _leg_fee.

The two-leg arb pays both legs' fees independently. The Kalshi fee comes
from _kalshi_trading_fee (asymmetric, scales with the series multiplier);
the Polymarket "fee" is a small flat slippage buffer (POLYMARKET_EFFECTIVE_FEE
in config.py). Unknown platforms contribute zero — defensive default.
"""

from __future__ import annotations

import pytest

from config import POLYMARKET_EFFECTIVE_FEE
from scanner.detect import _calculate_fees, _kalshi_trading_fee, _leg_fee


def test_kalshi_yes_polymarket_no_default_multiplier():
    """The 'classic' arb shape: buy YES on Kalshi, NO on Polymarket."""
    fees = _calculate_fees(
        buy_yes_platform="kalshi", buy_yes_price=0.55,
        buy_no_platform="polymarket", buy_no_price=0.42,
    )
    expected = _kalshi_trading_fee(0.55, 1.0) + POLYMARKET_EFFECTIVE_FEE
    assert fees == expected
    # And concretely: 7*0.55*0.45 = 1.7325 -> ceil 2c -> 0.02 + 0.005
    assert fees == pytest.approx(0.025)


def test_polymarket_yes_kalshi_no_mirror():
    """The mirror direction: buy YES on Polymarket, NO on Kalshi. Fees still sum."""
    fees = _calculate_fees(
        buy_yes_platform="polymarket", buy_yes_price=0.42,
        buy_no_platform="kalshi", buy_no_price=0.55,
    )
    # Kalshi side now sees price=0.55 on the NO leg; same fee
    expected = POLYMARKET_EFFECTIVE_FEE + _kalshi_trading_fee(0.55, 1.0)
    assert fees == expected


def test_both_legs_kalshi_uses_multiplier_on_both():
    """
    Defensive: same-platform two-leggers shouldn't happen for cross-platform
    arbs, but the helper should still sum two Kalshi fees with the same
    multiplier applied to both.
    """
    fees = _calculate_fees(
        buy_yes_platform="kalshi", buy_yes_price=0.4,
        buy_no_platform="kalshi", buy_no_price=0.6,
    )
    # 7*0.4*0.6 = 1.68 -> ceil 2c on each side
    assert fees == pytest.approx(0.04)


def test_both_legs_polymarket_doubles_slippage_buffer():
    """Two PM legs (theoretical) means the slippage buffer is paid twice."""
    fees = _calculate_fees(
        buy_yes_platform="polymarket", buy_yes_price=0.5,
        buy_no_platform="polymarket", buy_no_price=0.5,
    )
    assert fees == pytest.approx(2 * POLYMARKET_EFFECTIVE_FEE)


def test_promotional_multiplier_zeros_only_kalshi_leg():
    """
    A fee-free Kalshi series should still leave the Polymarket slippage buffer
    intact on the PM leg — the multiplier only short-circuits the Kalshi fee.
    """
    fees = _calculate_fees(
        buy_yes_platform="kalshi", buy_yes_price=0.5,
        buy_no_platform="polymarket", buy_no_price=0.5,
        kalshi_fee_multiplier=0.0,
    )
    assert fees == pytest.approx(POLYMARKET_EFFECTIVE_FEE)


def test_higher_kalshi_multiplier_scales_only_kalshi_leg():
    """M=2.0 doubles the Kalshi cents-before-ceiling; PM leg unchanged."""
    fees = _calculate_fees(
        buy_yes_platform="kalshi", buy_yes_price=0.5,
        buy_no_platform="polymarket", buy_no_price=0.5,
        kalshi_fee_multiplier=2.0,
    )
    # 7*2*0.25 = 3.5 -> ceil 4c
    assert fees == pytest.approx(0.04 + POLYMARKET_EFFECTIVE_FEE)


def test_unknown_platform_contributes_zero_fee():
    """Defensive: a leg on an unrecognised platform shouldn't blow up; it
    just contributes zero fee. Wrong-but-safe rather than crash-on-typo."""
    assert _leg_fee("alpaca", 0.5) == 0.0
    fees = _calculate_fees(
        buy_yes_platform="kalshi", buy_yes_price=0.5,
        buy_no_platform="alpaca", buy_no_price=0.5,
    )
    assert fees == _kalshi_trading_fee(0.5, 1.0)


def test_default_multiplier_param_is_one():
    """Calling _calculate_fees without an explicit multiplier should match M=1.0."""
    a = _calculate_fees("kalshi", 0.5, "polymarket", 0.5)
    b = _calculate_fees("kalshi", 0.5, "polymarket", 0.5, kalshi_fee_multiplier=1.0)
    assert a == b
