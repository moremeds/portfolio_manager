"""Deterministic event ledger and portfolio state replay.

The ledger is the canonical representation of all portfolio events (trades,
deposits, withdrawals). Portfolio state at any historical date is reconstructed
by replaying events forward from inception.
"""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal

import pandas as pd

from portfolio_manager.core.types import ClosedPosition, LedgerEvent, NavSnapshot, PortfolioState

logger = logging.getLogger(__name__)

# Event type ordering: cash events before trade events on the same date
_EVENT_TYPE_ORDER = {
    "cash_in": 0,
    "cash_out": 1,
    "dividend": 2,
    "buy": 3,
    "sell": 4,
}


def build_ledger(
    trade_history: pd.DataFrame,
    cash_flows: pd.DataFrame,
) -> list[LedgerEvent]:
    """Merge trade history + cash flows into a sorted event timeline.

    Event ordering rules:
    1. Sort by date ascending
    2. Within same date: cash events before trade events
    3. Stable sort preserves original API order within same type

    Args:
        trade_history: DataFrame with columns [date, symbol, side, quantity, price, order_id].
        cash_flows: DataFrame with columns [date, direction, business_type, amount, currency, description].

    Returns:
        Sorted list of LedgerEvent.
    """
    events: list[LedgerEvent] = []

    # Convert trades to ledger events
    for _, row in trade_history.iterrows():
        side = str(row["side"]).lower()
        event_type = "buy" if side == "buy" else "sell"
        events.append(
            LedgerEvent(
                date=row["date"],
                event_type=event_type,
                symbol=str(row["symbol"]),
                quantity=Decimal(str(row["quantity"])),
                price=Decimal(str(row["price"])),
                order_id=str(row["order_id"]),
            )
        )

    # Convert cash flows to ledger events
    # direction: 1=outflow, 2=inflow
    # business_type: 1=cash (deposit/withdrawal), 2=stock (trade settlement), 3=fund
    for _, row in cash_flows.iterrows():
        direction = int(row["direction"])
        business_type = int(row["business_type"])

        # Skip stock-settlement cash flows — they're already reflected in trades
        if business_type == 2:
            continue

        if direction == 2:  # inflow
            event_type = "cash_in"
        else:  # outflow
            event_type = "cash_out"

        # Dividends are inflows with business_type == 3 or description containing "dividend"
        description = str(row.get("description", "")).lower()
        if direction == 2 and ("dividend" in description or "div" in description):
            event_type = "dividend"

        events.append(
            LedgerEvent(
                date=row["date"],
                event_type=event_type,
                symbol=None,
                quantity=Decimal(str(abs(row["amount"]))),
                price=Decimal("0"),
                order_id=None,
            )
        )

    # Sort: by date, then by event type order (cash before trades)
    events.sort(key=lambda e: (e.date, _EVENT_TYPE_ORDER.get(e.event_type, 99)))

    return events


def replay_to_date(
    ledger: list[LedgerEvent],
    target_date: datetime.date,
    initial_cash: Decimal = Decimal("0"),
) -> PortfolioState:
    """Replay events up to and including target_date.

    Returns the portfolio state (positions + cash) at end of target_date.

    Handles:
    - Buy: positions[symbol] += qty, cash -= qty * price
    - Sell: positions[symbol] -= qty, cash += qty * price
    - Cash in: cash += amount
    - Cash out: cash -= amount
    - Dividend: cash += amount
    """
    positions: dict[str, Decimal] = {}
    cash = initial_cash

    for event in ledger:
        if event.date > target_date:
            break

        if event.event_type == "buy":
            assert event.symbol is not None
            positions[event.symbol] = positions.get(event.symbol, Decimal("0")) + event.quantity
            cash -= event.quantity * event.price

        elif event.event_type == "sell":
            assert event.symbol is not None
            positions[event.symbol] = positions.get(event.symbol, Decimal("0")) - event.quantity
            cash += event.quantity * event.price
            # Remove zero positions
            if positions[event.symbol] == 0:
                del positions[event.symbol]

        elif event.event_type == "cash_in":
            cash += event.quantity

        elif event.event_type == "cash_out":
            cash -= event.quantity

        elif event.event_type == "dividend":
            cash += event.quantity

    return PortfolioState(date=target_date, positions=positions, cash=cash)


def get_nav_at_date(
    state: PortfolioState,
    prices: dict[str, Decimal],
) -> NavSnapshot:
    """Calculate NAV from a portfolio state and prices.

    NAV = sum(qty * price for each position) + cash.
    Prices should be from history_candlesticks_by_date with NoAdjust.
    """
    stock_value = Decimal("0")
    for symbol, qty in state.positions.items():
        price = prices.get(symbol, Decimal("0"))
        if price == Decimal("0") and qty > 0:
            logger.warning("No price for %s on %s — using 0", symbol, state.date)
        stock_value += qty * price

    return NavSnapshot(
        date=state.date,
        total_nav=stock_value + state.cash,
        stock_value=stock_value,
        cash_value=state.cash,
    )


def get_cash_flow_events_in_period(
    ledger: list[LedgerEvent],
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[LedgerEvent]:
    """Extract external cash flow events (cash_in/cash_out) within a date range.

    Used for TWR sub-period splitting. Excludes dividends as they are portfolio-generated.
    Includes events strictly after start_date and up to (inclusive) end_date, so that
    cash flows on end_date create proper sub-period boundaries.
    """
    return [
        e
        for e in ledger
        if start_date < e.date <= end_date and e.event_type in ("cash_in", "cash_out")
    ]


def get_closed_positions(
    ledger: list[LedgerEvent],
    current_symbols: set[str],
) -> list[ClosedPosition]:
    """Identify fully closed positions from the ledger.

    A position is "closed" if total bought qty == total sold qty AND
    the symbol is not in current_symbols (no active holding).

    Returns:
        List of ClosedPosition sorted by last_trade_date descending.
    """
    # Aggregate per-symbol trade data
    symbol_data: dict[str, dict[str, object]] = {}

    for event in ledger:
        if event.event_type not in ("buy", "sell") or event.symbol is None:
            continue

        sym = event.symbol
        if sym not in symbol_data:
            symbol_data[sym] = {
                "buy_qty": Decimal("0"),
                "buy_cost": Decimal("0"),  # total cost = sum(qty * price)
                "sell_qty": Decimal("0"),
                "sell_proceeds": Decimal("0"),  # total proceeds = sum(qty * price)
                "first_date": event.date,
                "last_date": event.date,
            }

        d = symbol_data[sym]
        if event.event_type == "buy":
            d["buy_qty"] += event.quantity  # type: ignore[operator]
            d["buy_cost"] += event.quantity * event.price  # type: ignore[operator]
        else:  # sell
            d["sell_qty"] += event.quantity  # type: ignore[operator]
            d["sell_proceeds"] += event.quantity * event.price  # type: ignore[operator]

        if event.date < d["first_date"]:  # type: ignore[operator]
            d["first_date"] = event.date
        if event.date > d["last_date"]:  # type: ignore[operator]
            d["last_date"] = event.date

    closed: list[ClosedPosition] = []
    for sym, d in symbol_data.items():
        buy_qty: Decimal = d["buy_qty"]  # type: ignore[assignment]
        sell_qty: Decimal = d["sell_qty"]  # type: ignore[assignment]
        net_qty = buy_qty - sell_qty

        # Only closed if net zero and not currently held
        if net_qty != 0 or sym in current_symbols:
            continue

        buy_cost: Decimal = d["buy_cost"]  # type: ignore[assignment]
        sell_proceeds: Decimal = d["sell_proceeds"]  # type: ignore[assignment]

        avg_buy = buy_cost / buy_qty if buy_qty != 0 else Decimal("0")
        avg_sell = sell_proceeds / sell_qty if sell_qty != 0 else Decimal("0")
        realized_pnl = sell_proceeds - buy_cost
        realized_pnl_pct = (
            (avg_sell - avg_buy) / avg_buy * Decimal("100") if avg_buy != 0 else Decimal("0")
        )

        closed.append(
            ClosedPosition(
                symbol=sym,
                total_bought_qty=buy_qty,
                avg_buy_price=avg_buy,
                avg_sell_price=avg_sell,
                realized_pnl=realized_pnl,
                realized_pnl_pct=realized_pnl_pct,
                first_trade_date=d["first_date"],  # type: ignore[arg-type]
                last_trade_date=d["last_date"],  # type: ignore[arg-type]
            )
        )

    # Sort by last trade date descending (most recent closures first)
    closed.sort(key=lambda c: c.last_trade_date, reverse=True)
    return closed


def check_position_consistency(
    replayed_state: PortfolioState,
    current_positions: dict[str, Decimal],
) -> list[str]:
    """Compare replayed positions vs current API positions.

    Returns list of warning messages for discrepancies (likely splits/corporate actions).
    """
    warnings: list[str] = []

    all_symbols = set(replayed_state.positions.keys()) | set(current_positions.keys())
    for symbol in sorted(all_symbols):
        replayed_qty = replayed_state.positions.get(symbol, Decimal("0"))
        current_qty = current_positions.get(symbol, Decimal("0"))

        if replayed_qty != current_qty:
            msg = (
                f"Position mismatch for {symbol}: "
                f"replayed={replayed_qty}, current={current_qty} "
                f"(possible split/corporate action)"
            )
            warnings.append(msg)
            logger.warning(msg)

    return warnings
