"""Tests for analytics/performance.py — TWR, price return, anchor dates."""

from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from portfolio_manager.analytics.performance import (
    calculate_stock_price_return,
    calculate_twr,
    resolve_anchor_dates,
)
from portfolio_manager.core.types import LedgerEvent

# ─── Helpers ────────────────────────────────────────────────────


def _make_candlestick_df(date_price_pairs: list[tuple[datetime.date, str]]) -> pd.DataFrame:
    """Create a minimal candlestick DataFrame for testing."""
    rows = [
        {
            "date": d,
            "open": Decimal(p),
            "high": Decimal(p),
            "low": Decimal(p),
            "close": Decimal(p),
            "volume": 1000,
        }
        for d, p in date_price_pairs
    ]
    return pd.DataFrame(rows)


# ─── resolve_anchor_dates ───────────────────────────────────────


def test_anchor_date_resolution_basic():
    """All dates should resolve to valid trading days."""
    trading_days = [
        datetime.date(2024, 12, 30),
        datetime.date(2024, 12, 31),
        datetime.date(2025, 1, 2),
        datetime.date(2025, 1, 3),
        datetime.date(2025, 1, 6),
        datetime.date(2025, 1, 7),
        datetime.date(2025, 1, 8),
        datetime.date(2025, 1, 9),
        datetime.date(2025, 1, 10),
        datetime.date(2025, 1, 13),
    ]
    inception = datetime.date(2024, 12, 30)

    anchors = resolve_anchor_dates(datetime.date(2025, 1, 13), inception, trading_days)

    # WoW: 7 days ago = Jan 6 (Monday) — Jan 6 is a trading day
    assert anchors["wow"] == datetime.date(2025, 1, 6)

    # MTD: last day of Dec = Dec 31
    assert anchors["mtd"] == datetime.date(2024, 12, 31)

    # YTD: Dec 31 of prev year = Dec 31, 2024
    assert anchors["ytd"] == datetime.date(2024, 12, 31)


def test_anchor_date_weekend_resolution():
    """Weekend dates should resolve to nearest prior trading day."""
    trading_days = [
        datetime.date(2025, 1, 24),  # Friday
        datetime.date(2025, 1, 27),  # Monday
        datetime.date(2025, 1, 28),
        datetime.date(2025, 1, 29),
        datetime.date(2025, 1, 30),
        datetime.date(2025, 1, 31),
        datetime.date(2025, 2, 3),
    ]
    inception = datetime.date(2025, 1, 24)

    # as_of = Feb 3 (Monday), WoW = 7 days ago = Jan 27
    anchors = resolve_anchor_dates(datetime.date(2025, 2, 3), inception, trading_days)

    assert anchors["wow"] == datetime.date(2025, 1, 27)
    # MTD: last day of Jan = Jan 31 (Friday, trading day)
    assert anchors["mtd"] == datetime.date(2025, 1, 31)


def test_anchor_date_before_inception():
    """Dates before inception should return None."""
    trading_days = [
        datetime.date(2025, 6, 1),
        datetime.date(2025, 6, 2),
    ]
    inception = datetime.date(2025, 6, 1)

    anchors = resolve_anchor_dates(datetime.date(2025, 6, 2), inception, trading_days)

    # WoW = 7 days ago = May 26, before inception
    assert anchors["wow"] is None
    # MTD = May 31, before inception
    assert anchors["mtd"] is None


# ─── TWR ────────────────────────────────────────────────────────


def test_twr_no_cash_flows():
    """Without cash flows, TWR should degenerate to simple return."""
    # Deposit $10000 at inception, buy 100 AAPL at $100
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("100"), "ORD-001"),
    ]

    # Mock _get_prices_at_date to return known prices
    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:

        def prices_side_effect(symbols, date, price_cache=None, today_prices=None):
            if date == datetime.date(2025, 1, 2):
                return {"AAPL.US": Decimal("100")}
            if date == datetime.date(2025, 1, 31):
                return {"AAPL.US": Decimal("110")}  # 10% gain
            return {}

        mock_prices.side_effect = prices_side_effect

        twr = calculate_twr(ledger, datetime.date(2025, 1, 2), datetime.date(2025, 1, 31))

    assert twr is not None
    # NAV start: 100 * 100 + 0 = 10000 (cash used for buy)
    # NAV end: 100 * 110 + 0 = 11000
    # Simple return: (11000 - 10000) / 10000 = 0.10
    assert twr == Decimal("0.1")


def test_twr_with_initial_cash_offset():
    """When deposits are missing from ledger, initial_cash corrects NAV.

    Without initial_cash: replayed cash = 0 - 100*100 = -10000,
    NAV = 100*100 + (-10000) = 0 → TWR returns None (division by zero).

    With initial_cash=10000: replayed cash = 10000 - 100*100 = 0,
    NAV = 100*100 + 0 = 10000 → TWR works correctly.
    """
    # Ledger has NO cash_in event — simulates missing deposit data
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("100"), "ORD-001"),
    ]

    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:

        def prices_side_effect(symbols, date, price_cache=None, today_prices=None):
            if date == datetime.date(2025, 1, 2):
                return {"AAPL.US": Decimal("100")}
            if date == datetime.date(2025, 1, 31):
                return {"AAPL.US": Decimal("110")}
            return {}

        mock_prices.side_effect = prices_side_effect

        # Without offset: NAV_start = 100*100 + (-10000) = 0 → None
        twr_no_offset = calculate_twr(
            ledger, datetime.date(2025, 1, 2), datetime.date(2025, 1, 31)
        )
        assert twr_no_offset is None

        # With offset: NAV_start = 100*100 + (10000 - 10000) = 10000 → works
        twr_with_offset = calculate_twr(
            ledger, datetime.date(2025, 1, 2), datetime.date(2025, 1, 31),
            initial_cash=Decimal("10000"),
        )

    assert twr_with_offset is not None
    assert twr_with_offset == Decimal("0.1")


def test_twr_with_deposit():
    """Deposit mid-period: TWR should NOT be skewed by the deposit.

    Sub-period 1 (Jan 2 → Jan 15):
      NAV_start = 100*100 + 0 = 10000
      NAV_end_raw = 100*105 + 5000 = 15500 (replay includes cash_in)
      NAV_end_adjusted = 15500 - 5000 = 10500 (pre-cash-flow)
      R1 = (10500 - 10000) / 10000 = 0.05
    Sub-period 2 (Jan 15 → Jan 31):
      NAV_start = 15500 (post-cash-flow, includes deposit)
      NAV_end = 100*110 + 5000 = 16000
      R2 = (16000 - 15500) / 15500 ≈ 0.032258
    TWR = (1.05)(1.032258) - 1 ≈ 0.083871
    """
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("100"), "ORD-001"),
        # Mid-period deposit
        LedgerEvent(datetime.date(2025, 1, 15), "cash_in", None, Decimal("5000"), Decimal("0"), None),
    ]

    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:

        def prices_side_effect(symbols, date, price_cache=None, today_prices=None):
            if date == datetime.date(2025, 1, 2):
                return {"AAPL.US": Decimal("100")}
            if date == datetime.date(2025, 1, 15):
                return {"AAPL.US": Decimal("105")}  # 5% gain in sub-period 1
            if date == datetime.date(2025, 1, 31):
                return {"AAPL.US": Decimal("110")}
            return {}

        mock_prices.side_effect = prices_side_effect

        twr = calculate_twr(ledger, datetime.date(2025, 1, 2), datetime.date(2025, 1, 31))

    assert twr is not None
    expected_twr = (Decimal("1.05") * (Decimal("16000") / Decimal("15500"))) - 1
    assert abs(twr - expected_twr) < Decimal("0.000001")


def test_twr_with_withdrawal():
    """Withdrawal mid-period."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("50"), Decimal("100"), "ORD-001"),
        # Mid-period withdrawal
        LedgerEvent(datetime.date(2025, 1, 15), "cash_out", None, Decimal("2000"), Decimal("0"), None),
    ]

    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:

        def prices_side_effect(symbols, date, price_cache=None, today_prices=None):
            if date == datetime.date(2025, 1, 2):
                return {"AAPL.US": Decimal("100")}
            if date == datetime.date(2025, 1, 15):
                return {"AAPL.US": Decimal("105")}
            if date == datetime.date(2025, 1, 31):
                return {"AAPL.US": Decimal("110")}
            return {}

        mock_prices.side_effect = prices_side_effect

        twr = calculate_twr(ledger, datetime.date(2025, 1, 2), datetime.date(2025, 1, 31))

    assert twr is not None
    assert isinstance(twr, Decimal)


def test_twr_empty_ledger():
    """Empty ledger should return None."""
    twr = calculate_twr([], datetime.date(2025, 1, 1), datetime.date(2025, 1, 31))
    assert twr is None


def test_twr_before_inception():
    """Start date before first event should return None."""
    ledger = [
        LedgerEvent(datetime.date(2025, 6, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
    ]
    twr = calculate_twr(ledger, datetime.date(2025, 1, 1), datetime.date(2025, 6, 30))
    assert twr is None


# ─── Stock price return ─────────────────────────────────────────


def test_stock_price_return():
    """Simple (close_end - close_start) / close_start."""
    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:

        def prices_side_effect(symbols, date, price_cache=None, today_prices=None):
            if date == datetime.date(2025, 1, 2):
                return {"AAPL.US": Decimal("100")}
            if date == datetime.date(2025, 6, 15):
                return {"AAPL.US": Decimal("115")}
            return {}

        mock_prices.side_effect = prices_side_effect

        ret = calculate_stock_price_return(
            "AAPL.US",
            datetime.date(2025, 1, 2),
            datetime.date(2025, 6, 15),
        )

    assert ret is not None
    assert ret == Decimal("0.15")  # (115 - 100) / 100


def test_stock_price_return_no_start_price():
    """No price at start_date should return None."""
    with patch("portfolio_manager.analytics.performance._get_prices_at_date") as mock_prices:
        mock_prices.return_value = {}

        ret = calculate_stock_price_return(
            "AAPL.US",
            datetime.date(2025, 1, 2),
            datetime.date(2025, 6, 15),
        )

    assert ret is None


def test_insufficient_history():
    """Period before inception should return None for TWR."""
    ledger = [
        LedgerEvent(datetime.date(2025, 6, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
    ]
    twr = calculate_twr(ledger, datetime.date(2025, 1, 1), datetime.date(2025, 6, 30))
    assert twr is None
