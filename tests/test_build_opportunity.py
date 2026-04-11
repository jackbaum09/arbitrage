"""
Tests for scanner.detect._build_opportunity.

This is the gate where raw price pairs become Opportunity objects. It must:
  1. Reject pairs with no gross profit (total_cost >= 1.0)
  2. Reject pairs whose fees consume all the gross profit
  3. Reject pairs whose net ROI is below MIN_ROI_THRESHOLD
  4. Return a populated Opportunity for surviving pairs, with all
     dollar fields rounded to 4 decimal places and capital_required
     rounded to 2 decimals
  5. Derive sport from the table name's first underscore-separated token

We patch scanner.detect.get_kalshi_fee_multiplier to a constant so tests
never hit the Kalshi series-fee endpoint.
"""

from __future__ import annotations

import pytest

from scanner import detect
from scanner.detect import _build_opportunity
from scanner.models import Opportunity


@pytest.fixture(autouse=True)
def stub_kalshi_fee_multiplier(monkeypatch):
    """All tests in this module use multiplier=1.0 unless they override."""
    monkeypatch.setattr(detect, "get_kalshi_fee_multiplier", lambda _ticker: 1.0)


def test_returns_none_when_total_cost_exceeds_one():
    """No gross profit -> not an arbitrage, return None before fee math."""
    opp = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="LAL",
        buy_yes_platform="kalshi", buy_yes_price=0.6,
        buy_no_platform="polymarket", buy_no_price=0.5,
    )
    assert opp is None


def test_returns_none_when_total_cost_equals_one():
    """Exactly 1.0 means zero gross profit -> not worth executing."""
    opp = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="LAL",
        buy_yes_platform="kalshi", buy_yes_price=0.5,
        buy_no_platform="polymarket", buy_no_price=0.5,
    )
    assert opp is None


def test_returns_none_when_fees_consume_gross_profit():
    """A 1c gross can't survive the ~2c Kalshi fee + 0.5c PM buffer."""
    # gross = 1 - 0.51 - 0.48 = 0.01; fees ~ 0.025; net negative
    opp = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="LAL",
        buy_yes_platform="kalshi", buy_yes_price=0.51,
        buy_no_platform="polymarket", buy_no_price=0.48,
    )
    assert opp is None


def test_returns_none_when_roi_below_threshold(monkeypatch):
    """
    Net profit > 0 but ROI < MIN_ROI_THRESHOLD must still return None.
    We bump the threshold for this test so we can hit the ROI gate cleanly
    without contriving sub-cent prices.
    """
    monkeypatch.setattr(detect, "MIN_ROI_THRESHOLD", 0.10)  # 10%
    # gross = 0.05, fees ~ 0.025, net ~ 0.025, roi ~ 0.025/0.95 ~ 2.6%
    opp = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="LAL",
        buy_yes_platform="kalshi", buy_yes_price=0.55,
        buy_no_platform="polymarket", buy_no_price=0.40,
    )
    assert opp is None


def test_returns_opportunity_for_clear_arb():
    """The happy path: yes=0.55 + no=0.40 leaves ~2.6% ROI after fees."""
    opp = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="LAL",
        buy_yes_platform="kalshi", buy_yes_price=0.55,
        buy_no_platform="polymarket", buy_no_price=0.40,
        kalshi_volume=1234.5, polymarket_liquidity=678.9,
    )
    assert opp is not None
    assert isinstance(opp, Opportunity)
    assert opp.sport == "nba"
    assert opp.market_type == "championship"
    assert opp.outcome == "LAL"
    assert opp.buy_yes_platform == "kalshi"
    assert opp.buy_no_platform == "polymarket"
    assert opp.buy_yes_price == 0.55
    assert opp.buy_no_price == 0.40
    assert opp.total_cost == pytest.approx(0.95)
    assert opp.gross_profit == pytest.approx(0.05)
    assert opp.fees == pytest.approx(0.025)  # 0.02 Kalshi + 0.005 PM
    assert opp.net_profit == pytest.approx(0.025)
    assert opp.roi == pytest.approx(0.0263, abs=1e-3)  # 0.025/0.95
    assert opp.capital_required == 95.0
    assert opp.kalshi_volume == 1234.5
    assert opp.polymarket_liquidity == 678.9
    assert opp.source_table == "nba_prediction_futures"
    # Rounding contract: dollar fields are 4dp, capital_required is 2dp
    assert opp.fees == round(opp.fees, 4)
    assert opp.capital_required == round(opp.capital_required, 2)


def test_sport_derived_from_table_prefix():
    """The sport label is the first underscore-separated token of the table."""
    for table, expected_sport in [
        ("nba_prediction_futures", "nba"),
        ("mlb_prediction_futures", "mlb"),
        ("nhl_prediction_futures", "nhl"),
        ("nfl_prediction_futures", "nfl"),
        ("cbb_prediction_futures", "cbb"),
    ]:
        opp = _build_opportunity(
            table=table, market_type="championship", outcome="X",
            buy_yes_platform="kalshi", buy_yes_price=0.55,
            buy_no_platform="polymarket", buy_no_price=0.40,
        )
        assert opp is not None
        assert opp.sport == expected_sport


def test_capital_required_is_total_cost_times_100_rounded_2dp():
    """capital_required = $ to deploy for one $1-face contract pair, *100 for $100 notional."""
    opp = _build_opportunity(
        table="mlb_prediction_futures", market_type="world_series", outcome="LAD",
        buy_yes_platform="polymarket", buy_yes_price=0.36,
        buy_no_platform="kalshi", buy_no_price=0.58,
    )
    assert opp is not None
    # 0.36 + 0.58 = 0.94 → $94.00 to capture a $100 contract pair
    assert opp.capital_required == 94.0


def test_promotional_kalshi_multiplier_unlocks_sub_2pp_arbs(monkeypatch):
    """
    A 1.5pp gross arb that gets killed by the standard Kalshi fee should
    survive when the series multiplier is 0 (promotional fee-free series).
    Verifies the fee multiplier actually flows through to net/roi.
    """
    # gross = 0.015; standard fee ~0.025 → blocked
    monkeypatch.setattr(detect, "get_kalshi_fee_multiplier", lambda _t: 1.0)
    blocked = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="X",
        buy_yes_platform="kalshi", buy_yes_price=0.51,
        buy_no_platform="polymarket", buy_no_price=0.475,
    )
    assert blocked is None

    # Same prices, fee-free Kalshi series → fees = 0 + 0.005 → net 0.01 → ~1% ROI
    monkeypatch.setattr(detect, "get_kalshi_fee_multiplier", lambda _t: 0.0)
    promo = _build_opportunity(
        table="nba_prediction_futures", market_type="championship", outcome="X",
        buy_yes_platform="kalshi", buy_yes_price=0.51,
        buy_no_platform="polymarket", buy_no_price=0.475,
    )
    assert promo is not None
    assert promo.fees == pytest.approx(0.005)
