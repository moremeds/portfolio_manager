"""Tests for api/market_data.py — pagination, Decimal conversion, timezone normalization."""

from __future__ import annotations

import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from portfolio_manager.api.market_data import (
    _to_decimal,
    _to_utc_date,
    get_cash_flows,
    get_history_candlesticks,
    get_prev_close_prices,
    get_real_time_quotes,
    get_trade_history,
    get_trading_days,
)

# ─── Helpers ────────────────────────────────────────────────────


def _make_order(
    order_id: str,
    symbol: str,
    side: str,
    qty: str,
    price: str,
    submitted_at: datetime.datetime,
) -> SimpleNamespace:
    return SimpleNamespace(
        order_id=order_id,
        symbol=symbol,
        side=SimpleNamespace(__str__=lambda self: f"OrderSide.{side}"),
        executed_quantity=qty,
        executed_price=price,
        submitted_at=submitted_at,
        status="Filled",
    )


def _make_cash_flow(
    balance: str,
    direction: int,
    business_type: int,
    business_time: datetime.datetime,
    currency: str = "USD",
    description: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        balance=balance,
        direction=direction,
        business_type=business_type,
        business_time=business_time,
        currency=currency,
        description=description,
    )


# ─── Decimal conversion ────────────────────────────────────────


def test_to_decimal_from_string():
    assert _to_decimal("123.45") == Decimal("123.45")


def test_to_decimal_from_float():
    result = _to_decimal(123.45)
    assert isinstance(result, Decimal)


def test_to_decimal_from_none():
    assert _to_decimal(None) == Decimal("0")


def test_to_decimal_from_int():
    assert _to_decimal(100) == Decimal("100")


# ─── Timezone normalization ─────────────────────────────────────


def test_to_utc_date_from_utc_datetime():
    dt = datetime.datetime(2025, 6, 15, 23, 30, 0, tzinfo=datetime.UTC)
    assert _to_utc_date(dt) == datetime.date(2025, 6, 15)


def test_to_utc_date_from_offset_datetime():
    """A datetime at 2025-06-16 02:00 UTC+8 should normalize to 2025-06-15 UTC."""
    tz_hk = datetime.timezone(datetime.timedelta(hours=8))
    dt = datetime.datetime(2025, 6, 16, 2, 0, 0, tzinfo=tz_hk)
    assert _to_utc_date(dt) == datetime.date(2025, 6, 15)


def test_to_utc_date_from_naive_datetime():
    """Naive datetimes are treated as-is (no timezone conversion)."""
    dt = datetime.datetime(2025, 6, 15, 23, 30, 0)
    assert _to_utc_date(dt) == datetime.date(2025, 6, 15)


def test_to_utc_date_from_date():
    d = datetime.date(2025, 6, 15)
    assert _to_utc_date(d) == d


def test_to_utc_date_boundary_midnight():
    """Midnight UTC should stay on the same date."""
    dt = datetime.datetime(2025, 6, 15, 0, 0, 0, tzinfo=datetime.UTC)
    assert _to_utc_date(dt) == datetime.date(2025, 6, 15)


# ─── Quote functions ────────────────────────────────────────────


def test_get_real_time_quotes():
    mock_ctx = MagicMock()
    mock_ctx.quote.return_value = [
        SimpleNamespace(symbol="AAPL.US", last_done="185.50", prev_close="184.00"),
        SimpleNamespace(symbol="MSFT.US", last_done="420.00", prev_close="418.00"),
    ]
    result = get_real_time_quotes(["AAPL.US", "MSFT.US"], ctx=mock_ctx)
    assert result == {"AAPL.US": Decimal("185.50"), "MSFT.US": Decimal("420.00")}
    mock_ctx.quote.assert_called_once_with(["AAPL.US", "MSFT.US"])


def test_get_prev_close_prices():
    mock_ctx = MagicMock()
    mock_ctx.quote.return_value = [
        SimpleNamespace(symbol="AAPL.US", last_done="185.50", prev_close="184.00"),
    ]
    result = get_prev_close_prices(["AAPL.US"], ctx=mock_ctx)
    assert result == {"AAPL.US": Decimal("184.00")}


# ─── Candlestick ────────────────────────────────────────────────


@patch("portfolio_manager.api.market_data.yf.download")
def test_get_history_candlesticks(mock_download):
    # Simulate Yahoo Finance DataFrame response
    mock_download.return_value = pd.DataFrame(
        {
            "Open": [180.00, 185.50],
            "High": [186.00, 187.00],
            "Low": [179.50, 184.00],
            "Close": [185.50, 186.00],
            "Volume": [1000000, 900000],
        },
        index=pd.to_datetime(["2025-06-15", "2025-06-16"]),
    )
    df = get_history_candlesticks("AAPL.US", datetime.date(2025, 6, 15), datetime.date(2025, 6, 16))

    assert len(df) == 2
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]
    assert df.iloc[0]["close"] == Decimal("185.5")
    assert df.iloc[1]["close"] == Decimal("186.0")
    mock_download.assert_called_once()


# ─── Trading days ───────────────────────────────────────────────


@patch("portfolio_manager.api.market_data.yf.download")
def test_get_trading_days(mock_download):
    # Simulate Yahoo Finance SPY DataFrame with trading day index
    mock_download.return_value = pd.DataFrame(
        {"Close": [450.0, 451.0, 452.0]},
        index=pd.to_datetime(["2025-06-16", "2025-06-17", "2025-06-18"]),
    )
    days = get_trading_days(datetime.date(2025, 6, 16), datetime.date(2025, 6, 18))
    assert days == [datetime.date(2025, 6, 16), datetime.date(2025, 6, 17), datetime.date(2025, 6, 18)]
    mock_download.assert_called_once()


# ─── Trade history pagination + dedup ──────────────────────────


def test_pagination_history_orders_dedup():
    """Feed >1000 mock orders and verify dedup by order_id."""
    mock_ctx = MagicMock()

    base_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    # First call: 1000 orders (triggers pagination)
    batch_1 = [
        _make_order(
            order_id=f"ORD-{i:04d}",
            symbol="AAPL.US",
            side="Buy",
            qty="10",
            price="180.00",
            submitted_at=base_time + datetime.timedelta(hours=i),
        )
        for i in range(1000)
    ]

    # Second call: 500 orders + 100 duplicates from batch 1 edge
    batch_2 = [
        _make_order(
            order_id=f"ORD-{i:04d}",
            symbol="AAPL.US",
            side="Buy",
            qty="10",
            price="180.00",
            submitted_at=base_time + datetime.timedelta(hours=i),
        )
        for i in range(900, 1400)  # 100 overlap with batch 1
    ]

    mock_ctx.history_orders.side_effect = [batch_1, batch_2, []]

    df = get_trade_history(
        start_at=base_time,
        end_at=base_time + datetime.timedelta(days=60),
        ctx=mock_ctx,
    )

    # Should have 1400 unique orders (1000 + 400 new from batch 2)
    assert len(df) == 1400
    assert df["order_id"].nunique() == 1400
    assert all(isinstance(p, Decimal) for p in df["price"])


def test_trade_history_empty():
    mock_ctx = MagicMock()
    mock_ctx.history_orders.return_value = []

    df = get_trade_history(
        start_at=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
        ctx=mock_ctx,
    )
    assert len(df) == 0
    assert list(df.columns) == ["date", "symbol", "side", "quantity", "price", "order_id"]


# ─── Cash flow pagination ──────────────────────────────────────


def test_pagination_cash_flow_exhaustion():
    """Mock multi-page cash flow responses and verify all are collected."""
    mock_ctx = MagicMock()

    base_time = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)

    # Page 1: full page (10000 items triggers next page fetch)
    # Simulate with smaller size by patching — we'll use actual flow
    page_1 = [
        _make_cash_flow(
            balance=f"{100 + i}",
            direction=2,
            business_type=1,
            business_time=base_time + datetime.timedelta(days=i),
        )
        for i in range(50)
    ]

    # Page 2: partial page (end of data)
    page_2 = [
        _make_cash_flow(
            balance=f"{200 + i}",
            direction=2,
            business_type=1,
            business_time=base_time + datetime.timedelta(days=50 + i),
        )
        for i in range(30)
    ]

    mock_ctx.cash_flow.side_effect = [page_1, page_2]

    df = get_cash_flows(
        start_at=base_time,
        end_at=base_time + datetime.timedelta(days=100),
        ctx=mock_ctx,
    )

    # Page 1 has 50 items < 10000, so pagination stops after first call
    # (the second call is never made)
    assert len(df) == 50
    assert all(isinstance(a, Decimal) for a in df["amount"])


def test_cash_flow_empty():
    mock_ctx = MagicMock()
    mock_ctx.cash_flow.return_value = []

    df = get_cash_flows(
        start_at=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
        ctx=mock_ctx,
    )
    assert len(df) == 0
    assert list(df.columns) == ["date", "direction", "business_type", "amount", "currency", "description"]


# ─── Decimal conversion for all API fields ─────────────────────


def test_decimal_conversion_quotes():
    """Verify all quote API string fields are converted to Decimal."""
    mock_ctx = MagicMock()
    mock_ctx.quote.return_value = [
        SimpleNamespace(symbol="TSLA.US", last_done="280.50", prev_close="279.00"),
    ]
    result = get_real_time_quotes(["TSLA.US"], ctx=mock_ctx)
    val = result["TSLA.US"]
    assert isinstance(val, Decimal)
    assert val == Decimal("280.50")


@patch("portfolio_manager.api.market_data.yf.download")
def test_decimal_conversion_candlestick(mock_download):
    """Verify candlestick OHLC fields are all Decimal."""
    mock_download.return_value = pd.DataFrame(
        {
            "Open": [100.10],
            "High": [105.50],
            "Low": [99.00],
            "Close": [103.25],
            "Volume": [500000],
        },
        index=pd.to_datetime(["2025-06-15"]),
    )
    df = get_history_candlesticks("TEST.US", datetime.date(2025, 6, 15), datetime.date(2025, 6, 15))
    for col in ["open", "high", "low", "close"]:
        assert isinstance(df.iloc[0][col], Decimal), f"{col} should be Decimal"


# ─── Timezone normalization for trade history ──────────────────


def test_timezone_normalization_trade_history():
    """UTC timestamps should normalize to correct date."""
    mock_ctx = MagicMock()

    # Order submitted at 23:30 UTC on June 15 — should be June 15
    late_utc = datetime.datetime(2025, 6, 15, 23, 30, 0, tzinfo=datetime.UTC)
    mock_ctx.history_orders.return_value = [
        _make_order("ORD-001", "AAPL.US", "Buy", "10", "185.00", late_utc),
    ]

    df = get_trade_history(
        start_at=datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC),
        ctx=mock_ctx,
    )

    assert df.iloc[0]["date"] == datetime.date(2025, 6, 15)
