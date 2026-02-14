"""Tests for analytics/rebalancing.py — weight-based + ATR-based rebalancing."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pandas as pd

from portfolio_manager.analytics.rebalancing import _calculate_atr, atr_based_rebalance, weight_based_rebalance
from portfolio_manager.core.types import PortfolioConfig, PositionRow

# ─── Helpers ────────────────────────────────────────────────────


def _make_config(**overrides: object) -> PortfolioConfig:
    defaults = {
        "base_currency": "USD",
        "target_allocations": {},
        "rebalance_threshold": Decimal("0.05"),
        "atr_period": 14,
        "atr_multiplier": Decimal("2.0"),
    }
    defaults.update(overrides)
    return PortfolioConfig(**defaults)  # type: ignore[arg-type]


def _make_position(
    symbol: str = "AAPL.US",
    name: str = "Apple",
    quantity: Decimal = Decimal("100"),
    cost_price: Decimal = Decimal("170"),
    last_price: Decimal = Decimal("185"),
    prev_close: Decimal = Decimal("184"),
    market_value: Decimal | None = None,
    weight: Decimal = Decimal("0.15"),
) -> PositionRow:
    mkt_val = market_value if market_value is not None else quantity * last_price
    cost_val = quantity * cost_price
    return PositionRow(
        symbol=symbol,
        name=name,
        quantity=quantity,
        cost_price=cost_price,
        last_price=last_price,
        prev_close=prev_close,
        market_value=mkt_val,
        cost_value=cost_val,
        unrealized_pnl=mkt_val - cost_val,
        unrealized_pnl_pct=((mkt_val - cost_val) / cost_val * 100) if cost_val != 0 else Decimal("0"),
        daily_pnl=quantity * (last_price - prev_close),
        weight=weight,
        currency="USD",
    )


def _make_ohlcv_df(num_days: int, base_price: Decimal = Decimal("100")) -> pd.DataFrame:
    """Generate a simple OHLCV DataFrame with slight variation."""
    rows = []
    for i in range(num_days):
        price = base_price + Decimal(str(i))
        rows.append(
            {
                "date": datetime.date(2025, 1, 1) + datetime.timedelta(days=i),
                "open": price - Decimal("1"),
                "high": price + Decimal("2"),
                "low": price - Decimal("2"),
                "close": price,
                "volume": 1000000,
            }
        )
    return pd.DataFrame(rows)


# ─── Weight-based rebalancing ───────────────────────────────────


def test_buy_within_threshold():
    """No suggestion when drift < threshold."""
    config = _make_config(
        target_allocations={"AAPL.US": Decimal("0.15")},
        rebalance_threshold=Decimal("0.05"),
    )
    pos = _make_position(market_value=Decimal("15000"))
    total_nav = Decimal("100000")

    suggestions = weight_based_rebalance([pos], total_nav, Decimal("20000"), config)

    assert len(suggestions) == 0  # 15% current vs 15% target = 0% drift


def test_buy_suggestion():
    """Should suggest BUY when underweight beyond threshold."""
    config = _make_config(
        target_allocations={"AAPL.US": Decimal("0.20")},
        rebalance_threshold=Decimal("0.05"),
    )
    # Current: 10000 / 100000 = 10%, target 20%, drift = 10%
    pos = _make_position(
        market_value=Decimal("10000"),
        last_price=Decimal("100"),
        quantity=Decimal("100"),
    )
    total_nav = Decimal("100000")

    suggestions = weight_based_rebalance([pos], total_nav, Decimal("50000"), config)

    assert len(suggestions) == 1
    assert suggestions[0].action == "BUY"
    assert suggestions[0].suggested_quantity > 0


def test_buy_capped_by_cash():
    """BUY suggestion should not exceed available cash."""
    config = _make_config(
        target_allocations={"AAPL.US": Decimal("0.50")},
        rebalance_threshold=Decimal("0.05"),
    )
    # Need to buy ~$40000 worth but only have $500 cash
    pos = _make_position(
        market_value=Decimal("10000"),
        last_price=Decimal("200"),
        quantity=Decimal("50"),
    )
    total_nav = Decimal("100000")
    available_cash = Decimal("500")

    suggestions = weight_based_rebalance([pos], total_nav, available_cash, config)

    assert len(suggestions) == 1
    # Max 2 shares at $200 = $400 (floor of 500/200)
    assert suggestions[0].suggested_quantity == 2
    assert suggestions[0].suggested_value <= available_cash


def test_sell_suggestion():
    """Should suggest SELL when overweight beyond threshold."""
    config = _make_config(
        target_allocations={"AAPL.US": Decimal("0.10")},
        rebalance_threshold=Decimal("0.05"),
    )
    # Current: 25000/100000 = 25%, target 10%, drift = 15%
    pos = _make_position(
        market_value=Decimal("25000"),
        last_price=Decimal("250"),
        quantity=Decimal("100"),
    )
    total_nav = Decimal("100000")

    suggestions = weight_based_rebalance([pos], total_nav, Decimal("10000"), config)

    assert len(suggestions) == 1
    assert suggestions[0].action == "SELL"
    assert suggestions[0].suggested_quantity > 0


def test_sell_capped_by_quantity():
    """SELL suggestion should not exceed available quantity."""
    config = _make_config(
        target_allocations={"AAPL.US": Decimal("0.01")},  # tiny target
        rebalance_threshold=Decimal("0.05"),
    )
    # Current: 5000/10000 = 50%, target 1%, should sell almost all
    pos = _make_position(
        market_value=Decimal("5000"),
        last_price=Decimal("100"),
        quantity=Decimal("50"),
    )
    total_nav = Decimal("10000")

    suggestions = weight_based_rebalance([pos], total_nav, Decimal("5000"), config)

    assert len(suggestions) == 1
    assert suggestions[0].suggested_quantity <= 50  # can't sell more than we have


def test_no_target_allocation():
    """Position without target allocation should be skipped."""
    config = _make_config(target_allocations={})
    pos = _make_position()
    total_nav = Decimal("100000")

    suggestions = weight_based_rebalance([pos], total_nav, Decimal("10000"), config)

    assert len(suggestions) == 0


# ─── ATR-based rebalancing ──────────────────────────────────────


def test_atr_in_range():
    """Price within ATR bands should be 'in_range'."""
    config = _make_config(atr_period=14, atr_multiplier=Decimal("2.0"))
    pos = _make_position(
        cost_price=Decimal("100"),
        last_price=Decimal("102"),  # close to cost, well within bands
    )
    df = _make_ohlcv_df(20, base_price=Decimal("100"))

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 1
    assert bands[0].signal == "in_range"
    assert bands[0].atr > 0


def test_atr_breach_upper():
    """Price above upper band should be 'breach_upper'."""
    config = _make_config(atr_period=5, atr_multiplier=Decimal("1.0"))
    # ATR will be ~4 (high-low range is 4 in our synthetic data)
    # Upper band = cost + 1 * ATR = 100 + 4 = 104
    pos = _make_position(
        cost_price=Decimal("100"),
        last_price=Decimal("110"),  # well above upper band
    )
    df = _make_ohlcv_df(10, base_price=Decimal("100"))

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 1
    assert bands[0].signal == "breach_upper"


def test_atr_breach_lower():
    """Price below lower band should be 'breach_lower'."""
    config = _make_config(atr_period=5, atr_multiplier=Decimal("1.0"))
    pos = _make_position(
        cost_price=Decimal("100"),
        last_price=Decimal("90"),  # well below lower band
    )
    df = _make_ohlcv_df(10, base_price=Decimal("100"))

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 1
    assert bands[0].signal == "breach_lower"


def test_atr_insufficient_data():
    """Less than atr_period days should result in no band."""
    config = _make_config(atr_period=14)
    pos = _make_position()
    df = _make_ohlcv_df(10)  # only 10 days, need 14

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 0


def test_atr_no_data():
    """Missing candlestick data should result in no band."""
    config = _make_config(atr_period=14)
    pos = _make_position()

    bands = atr_based_rebalance([pos], {}, config)

    assert len(bands) == 0


def test_atr_band_classification_near_upper():
    """Price near upper band should be 'near_upper'."""
    config = _make_config(atr_period=5, atr_multiplier=Decimal("2.0"))
    # Generate consistent data: each day has range 4 (high-low)
    # ATR ≈ 4, upper = 100 + 2*4 = 108, near_upper threshold = 108 - 0.5*4 = 106
    pos = _make_position(
        cost_price=Decimal("100"),
        last_price=Decimal("107"),  # between near threshold and upper
    )
    df = _make_ohlcv_df(10, base_price=Decimal("100"))

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 1
    assert bands[0].signal == "near_upper"


def test_atr_band_classification_near_lower():
    """Price near lower band should be 'near_lower'."""
    config = _make_config(atr_period=5, atr_multiplier=Decimal("2.0"))
    # lower = 100 - 2*4 = 92, near_lower threshold = 92 + 0.5*4 = 94
    pos = _make_position(
        cost_price=Decimal("100"),
        last_price=Decimal("93"),  # between lower band and near threshold
    )
    df = _make_ohlcv_df(10, base_price=Decimal("100"))

    bands = atr_based_rebalance([pos], {"AAPL.US": df}, config)

    assert len(bands) == 1
    assert bands[0].signal == "near_lower"


def test_atr_with_gap():
    """Seeded ATR correctly captures overnight gap that high-low alone misses."""
    # Seed row: close at 100
    # Next day: gap up, open=105, high=107, low=104, close=106
    # Without seed: TR = 107-104 = 3
    # With seed (prev_close=100): TR = max(3, |107-100|, |104-100|) = 7
    rows = [
        {"date": datetime.date(2025, 1, 1), "open": 99, "high": 101, "low": 99, "close": 100, "volume": 1000},
        {"date": datetime.date(2025, 1, 2), "open": 105, "high": 107, "low": 104, "close": 106, "volume": 1000},
        {"date": datetime.date(2025, 1, 3), "open": 106, "high": 108, "low": 105, "close": 107, "volume": 1000},
        {"date": datetime.date(2025, 1, 6), "open": 107, "high": 109, "low": 106, "close": 108, "volume": 1000},
    ]
    df = pd.DataFrame(rows)

    atr = _calculate_atr(df, period=3)
    assert atr is not None
    # TR1: max(107-104, |107-100|, |104-100|) = max(3, 7, 4) = 7
    # TR2: max(108-105, |108-106|, |105-106|) = max(3, 2, 1) = 3
    # TR3: max(109-106, |109-107|, |106-107|) = max(3, 2, 1) = 3
    # ATR = (7+3+3)/3 ≈ 4.333
    expected = (Decimal("7") + Decimal("3") + Decimal("3")) / Decimal("3")
    assert atr == expected
