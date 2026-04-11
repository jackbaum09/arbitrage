"""
Tests for scanner.detect._kalshi_trading_fee.

Kalshi sports markets charge ceil(7 * M * P * (1 - P)) cents per contract,
where P is the contract price in dollars and M is the series-level fee
multiplier. Standard sports series have M=1.0; promotional fee-free series
have M=0.0; the ceiling means small fractional cents always round up to
the next whole cent.
"""

from __future__ import annotations

import pytest

from scanner.detect import _kalshi_trading_fee


# (price, multiplier, expected_dollars)
STANDARD_CASES = [
    # M=1.0 sports series, the curve we actually trade
    (0.5, 1.0, 0.02),   # 7*0.25 = 1.75 -> ceil 2c
    (0.3, 1.0, 0.02),   # 7*0.21 = 1.47 -> ceil 2c
    (0.7, 1.0, 0.02),   # symmetric to 0.3
    (0.1, 1.0, 0.01),   # 7*0.09 = 0.63 -> ceil 1c
    (0.9, 1.0, 0.01),   # symmetric to 0.1
    (0.01, 1.0, 0.01),  # 7*0.0099 = 0.0693 -> ceil 1c (any tiny non-zero rounds up)
    (0.99, 1.0, 0.01),  # symmetric
]


@pytest.mark.parametrize("price,multiplier,expected", STANDARD_CASES)
def test_standard_multiplier_curve(price, multiplier, expected):
    assert _kalshi_trading_fee(price, multiplier) == expected


def test_promotional_fee_free_series_returns_zero():
    """M=0.0 (promotional fee-free series) zeros the fee at every price."""
    for price in [0.1, 0.3, 0.5, 0.7, 0.9]:
        assert _kalshi_trading_fee(price, fee_multiplier=0.0) == 0.0


def test_higher_multiplier_scales_linearly_then_ceiling():
    """M=2.0 doubles the cents-before-ceiling, then ceil to next whole cent."""
    # 7 * 2.0 * 0.5 * 0.5 = 3.5 -> ceil 4c
    assert _kalshi_trading_fee(0.5, fee_multiplier=2.0) == 0.04
    # 7 * 2.0 * 0.1 * 0.9 = 1.26 -> ceil 2c
    assert _kalshi_trading_fee(0.1, fee_multiplier=2.0) == 0.02


def test_extreme_prices_return_zero():
    """At P=0 or P=1 the contract is fully resolved; no fee charged."""
    assert _kalshi_trading_fee(0.0, 1.0) == 0.0
    assert _kalshi_trading_fee(1.0, 1.0) == 0.0


def test_out_of_band_prices_return_zero():
    """Defensive: prices outside [0, 1] never charge a fee."""
    assert _kalshi_trading_fee(-0.1, 1.0) == 0.0
    assert _kalshi_trading_fee(1.5, 1.0) == 0.0


def test_negative_multiplier_returns_zero():
    """Defensive: a negative multiplier (shouldn't happen) does not produce a credit."""
    assert _kalshi_trading_fee(0.5, fee_multiplier=-1.0) == 0.0


def test_default_multiplier_is_one():
    """Calling without an explicit multiplier should match M=1.0."""
    assert _kalshi_trading_fee(0.5) == _kalshi_trading_fee(0.5, fee_multiplier=1.0)
