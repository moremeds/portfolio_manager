"""Tests for analytics/ledger.py — event ledger, replay, sort order, consistency."""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal

import pandas as pd

from portfolio_manager.analytics.ledger import (
    build_ledger,
    check_position_consistency,
    get_cash_flow_events_in_period,
    get_closed_positions,
    get_nav_at_date,
    replay_to_date,
)
from portfolio_manager.core.types import LedgerEvent, PortfolioState

# ─── build_ledger ───────────────────────────────────────────────


def test_build_ledger_sort_order():
    """Cash events should come before trade events on the same date."""
    trades = pd.DataFrame(
        {
            "date": [datetime.date(2025, 1, 15)],
            "symbol": ["AAPL.US"],
            "side": ["buy"],
            "quantity": [Decimal("10")],
            "price": [Decimal("180.00")],
            "order_id": ["ORD-001"],
        }
    )
    cash_flows = pd.DataFrame(
        {
            "date": [datetime.date(2025, 1, 15)],
            "direction": [2],  # inflow
            "business_type": [1],  # cash deposit
            "amount": [Decimal("5000")],
            "currency": ["USD"],
            "description": ["Deposit"],
        }
    )

    ledger = build_ledger(trades, cash_flows)

    assert len(ledger) == 2
    # Cash in should come first
    assert ledger[0].event_type == "cash_in"
    assert ledger[1].event_type == "buy"


def test_build_ledger_skips_stock_settlement():
    """Cash flows with business_type=2 (stock settlement) should be skipped."""
    trades = pd.DataFrame(columns=["date", "symbol", "side", "quantity", "price", "order_id"])
    cash_flows = pd.DataFrame(
        {
            "date": [datetime.date(2025, 1, 15), datetime.date(2025, 1, 15)],
            "direction": [2, 1],
            "business_type": [2, 1],  # stock settlement, cash deposit
            "amount": [Decimal("1800"), Decimal("5000")],
            "currency": ["USD", "USD"],
            "description": ["Stock purchase", "Wire transfer"],
        }
    )

    ledger = build_ledger(trades, cash_flows)

    # Only the cash deposit should be included
    assert len(ledger) == 1
    assert ledger[0].event_type == "cash_out"  # direction=1 is outflow


def test_build_ledger_date_ascending():
    """Events should be sorted by date ascending."""
    trades = pd.DataFrame(
        {
            "date": [datetime.date(2025, 3, 1), datetime.date(2025, 1, 1), datetime.date(2025, 2, 1)],
            "symbol": ["AAPL.US", "MSFT.US", "NVDA.US"],
            "side": ["buy", "buy", "buy"],
            "quantity": [Decimal("10"), Decimal("5"), Decimal("8")],
            "price": [Decimal("185"), Decimal("420"), Decimal("140")],
            "order_id": ["ORD-003", "ORD-001", "ORD-002"],
        }
    )
    cash_flows = pd.DataFrame(columns=["date", "direction", "business_type", "amount", "currency", "description"])

    ledger = build_ledger(trades, cash_flows)

    dates = [e.date for e in ledger]
    assert dates == sorted(dates)


# ─── replay_to_date ────────────────────────────────────────────


def test_replay_buy_sell():
    """Buy 100, sell 50 -> 50 remaining, cash correct."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("20000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("180"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 2, 1), "sell", "AAPL.US", Decimal("50"), Decimal("190"), "ORD-002"),
    ]

    state = replay_to_date(ledger, datetime.date(2025, 2, 1))

    assert state.positions == {"AAPL.US": Decimal("50")}
    # Cash: 20000 - (100 * 180) + (50 * 190) = 20000 - 18000 + 9500 = 11500
    assert state.cash == Decimal("11500")


def test_replay_full_sell_removes_position():
    """Selling all shares should remove the position from the dict."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("20000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("180"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 2, 1), "sell", "AAPL.US", Decimal("100"), Decimal("190"), "ORD-002"),
    ]

    state = replay_to_date(ledger, datetime.date(2025, 2, 1))

    assert "AAPL.US" not in state.positions
    assert state.cash == Decimal("20000") - Decimal("18000") + Decimal("19000")


def test_replay_to_future_date():
    """Date after last event returns final state."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("50"), Decimal("180"), "ORD-001"),
    ]

    state = replay_to_date(ledger, datetime.date(2026, 12, 31))

    assert state.positions == {"AAPL.US": Decimal("50")}
    assert state.cash == Decimal("10000") - Decimal("9000")


def test_replay_empty_ledger():
    """No events -> zero positions, initial cash."""
    state = replay_to_date([], datetime.date(2025, 1, 1), initial_cash=Decimal("5000"))

    assert state.positions == {}
    assert state.cash == Decimal("5000")


def test_replay_with_initial_cash():
    """Initial cash parameter should be respected."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "buy", "AAPL.US", Decimal("10"), Decimal("100"), "ORD-001"),
    ]

    state = replay_to_date(ledger, datetime.date(2025, 1, 1), initial_cash=Decimal("5000"))

    assert state.positions == {"AAPL.US": Decimal("10")}
    assert state.cash == Decimal("4000")  # 5000 - 10 * 100


def test_replay_dividend():
    """Dividend adds to cash."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 6, 1), "dividend", None, Decimal("50"), Decimal("0"), None),
    ]

    state = replay_to_date(ledger, datetime.date(2025, 6, 1))

    assert state.cash == Decimal("10050")


def test_replay_partial_date():
    """Replay only up to a specific date."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("50"), Decimal("180"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 3, 1), "buy", "MSFT.US", Decimal("10"), Decimal("420"), "ORD-002"),
    ]

    # Replay only up to Jan 2 — should NOT include the March buy
    state = replay_to_date(ledger, datetime.date(2025, 1, 15))

    assert state.positions == {"AAPL.US": Decimal("50")}
    assert "MSFT.US" not in state.positions


# ─── get_nav_at_date ────────────────────────────────────────────


def test_get_nav_at_date():
    state = PortfolioState(
        date=datetime.date(2025, 6, 15),
        positions={"AAPL.US": Decimal("100"), "MSFT.US": Decimal("30")},
        cash=Decimal("5000"),
    )
    prices = {"AAPL.US": Decimal("185"), "MSFT.US": Decimal("420")}

    nav = get_nav_at_date(state, prices)

    # 100 * 185 + 30 * 420 + 5000 = 18500 + 12600 + 5000 = 36100
    assert nav.total_nav == Decimal("36100")
    assert nav.stock_value == Decimal("31100")
    assert nav.cash_value == Decimal("5000")


def test_get_nav_at_date_empty_portfolio():
    state = PortfolioState(
        date=datetime.date(2025, 6, 15),
        positions={},
        cash=Decimal("10000"),
    )

    nav = get_nav_at_date(state, {})

    assert nav.total_nav == Decimal("10000")
    assert nav.stock_value == Decimal("0")


# ─── get_cash_flow_events_in_period ─────────────────────────────


def test_get_cash_flow_events_in_period():
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 1), "cash_in", None, Decimal("10000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("50"), Decimal("180"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 2, 15), "cash_in", None, Decimal("5000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 3, 1), "cash_out", None, Decimal("2000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 4, 1), "buy", "MSFT.US", Decimal("10"), Decimal("420"), "ORD-002"),
    ]

    # Period Feb 1 to Mar 15 — should find the Feb 15 cash_in and Mar 1 cash_out
    flows = get_cash_flow_events_in_period(ledger, datetime.date(2025, 2, 1), datetime.date(2025, 3, 15))

    assert len(flows) == 2
    assert flows[0].event_type == "cash_in"
    assert flows[0].quantity == Decimal("5000")
    assert flows[1].event_type == "cash_out"


def test_get_cash_flow_events_boundary_semantics():
    """Start date is exclusive, end date is inclusive for TWR sub-period splitting."""
    ledger = [
        LedgerEvent(datetime.date(2025, 2, 1), "cash_in", None, Decimal("5000"), Decimal("0"), None),
        LedgerEvent(datetime.date(2025, 3, 1), "cash_in", None, Decimal("3000"), Decimal("0"), None),
    ]

    flows = get_cash_flow_events_in_period(ledger, datetime.date(2025, 2, 1), datetime.date(2025, 3, 1))

    # Start date excluded, end date included
    assert len(flows) == 1
    assert flows[0].date == datetime.date(2025, 3, 1)
    assert flows[0].quantity == Decimal("3000")


# ─── check_position_consistency ─────────────────────────────────


def test_split_detection_warning(caplog):
    """Replayed qty != current API qty -> warning logged."""
    replayed = PortfolioState(
        date=datetime.date(2025, 6, 15),
        positions={"AAPL.US": Decimal("50")},
        cash=Decimal("5000"),
    )
    current = {"AAPL.US": Decimal("200")}  # 4:1 split

    with caplog.at_level(logging.WARNING):
        warnings = check_position_consistency(replayed, current)

    assert len(warnings) == 1
    assert "AAPL.US" in warnings[0]
    assert "replayed=50" in warnings[0]
    assert "current=200" in warnings[0]


def test_position_consistency_no_warnings():
    replayed = PortfolioState(
        date=datetime.date(2025, 6, 15),
        positions={"AAPL.US": Decimal("100"), "MSFT.US": Decimal("30")},
        cash=Decimal("5000"),
    )
    current = {"AAPL.US": Decimal("100"), "MSFT.US": Decimal("30")}

    warnings = check_position_consistency(replayed, current)

    assert len(warnings) == 0


# ─── get_closed_positions ─────────────────────────────────────


def test_closed_position_basic():
    """Buy and fully sell a stock -> closed position with correct P&L."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("150"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 3, 1), "sell", "AAPL.US", Decimal("100"), Decimal("180"), "ORD-002"),
    ]

    closed = get_closed_positions(ledger, current_symbols=set())

    assert len(closed) == 1
    cp = closed[0]
    assert cp.symbol == "AAPL.US"
    assert cp.total_bought_qty == Decimal("100")
    assert cp.avg_buy_price == Decimal("150")
    assert cp.avg_sell_price == Decimal("180")
    # Realized P&L: 100 * 180 - 100 * 150 = 3000
    assert cp.realized_pnl == Decimal("3000")
    # P&L %: (180 - 150) / 150 * 100 = 20%
    assert cp.realized_pnl_pct == Decimal("20")
    assert cp.first_trade_date == datetime.date(2025, 1, 2)
    assert cp.last_trade_date == datetime.date(2025, 3, 1)


def test_closed_position_excludes_current_holdings():
    """Symbols still held should not appear as closed even if buy_qty == sell_qty."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "AAPL.US", Decimal("100"), Decimal("150"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 2, 1), "sell", "AAPL.US", Decimal("100"), Decimal("180"), "ORD-002"),
        # Re-bought
        LedgerEvent(datetime.date(2025, 3, 1), "buy", "AAPL.US", Decimal("50"), Decimal("170"), "ORD-003"),
    ]

    # AAPL.US is currently held (net 50 shares)
    closed = get_closed_positions(ledger, current_symbols={"AAPL.US"})

    assert len(closed) == 0


def test_closed_position_multiple_lots():
    """Multiple buys at different prices -> weighted average buy price."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "TSLA.US", Decimal("50"), Decimal("200"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 1, 15), "buy", "TSLA.US", Decimal("50"), Decimal("220"), "ORD-002"),
        LedgerEvent(datetime.date(2025, 2, 1), "sell", "TSLA.US", Decimal("100"), Decimal("250"), "ORD-003"),
    ]

    closed = get_closed_positions(ledger, current_symbols=set())

    assert len(closed) == 1
    cp = closed[0]
    # Avg buy: (50*200 + 50*220) / 100 = 21000/100 = 210
    assert cp.avg_buy_price == Decimal("210")
    assert cp.avg_sell_price == Decimal("250")
    # P&L: 100*250 - (50*200 + 50*220) = 25000 - 21000 = 4000
    assert cp.realized_pnl == Decimal("4000")


def test_closed_position_open_position_not_included():
    """Partially sold positions should not appear as closed."""
    ledger = [
        LedgerEvent(datetime.date(2025, 1, 2), "buy", "MSFT.US", Decimal("100"), Decimal("400"), "ORD-001"),
        LedgerEvent(datetime.date(2025, 2, 1), "sell", "MSFT.US", Decimal("50"), Decimal("420"), "ORD-002"),
    ]

    closed = get_closed_positions(ledger, current_symbols=set())

    # Net qty is 50, not zero -> not closed
    assert len(closed) == 0


def test_closed_position_empty_ledger():
    """Empty ledger -> no closed positions."""
    closed = get_closed_positions([], current_symbols=set())
    assert len(closed) == 0
