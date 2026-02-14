"""Performance calculations: TWR (portfolio-level) and price return (per-stock).

TWR (Time-Weighted Return) eliminates the effect of cash flows (deposits/withdrawals)
by compounding sub-period returns. Price return is the simple change in stock price.
"""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal

from portfolio_manager.analytics.ledger import (
    get_cash_flow_events_in_period,
    get_nav_at_date,
    replay_to_date,
)
from portfolio_manager.api.market_data import (
    PriceCache,
    get_history_candlesticks,
)
from portfolio_manager.core.types import (
    LedgerEvent,
    PortfolioPerformance,
    PositionRow,
    StockPerformance,
)

logger = logging.getLogger(__name__)


def _get_prices_at_date(
    symbols: list[str],
    target_date: datetime.date,
    price_cache: PriceCache | None = None,
    today_prices: dict[str, Decimal] | None = None,
) -> dict[str, Decimal]:
    """Get closing prices for symbols at a specific date.

    Uses pre-fetched today_prices for today (avoids creating new API connections).
    Uses PriceCache for historical dates with O(1) dict lookups.
    Falls back to Yahoo Finance candlesticks for uncached dates.
    """
    today = datetime.date.today()
    if target_date >= today:
        if today_prices is not None:
            return {s: today_prices[s] for s in symbols if s in today_prices}
        # Fallback: use cache for latest available date
        if price_cache is not None:
            return price_cache.get_closes(symbols, target_date)
        return {}

    # Fast path: use pre-fetched cache
    if price_cache is not None:
        return price_cache.get_closes(symbols, target_date)

    # Slow path: individual API calls (no cache provided)
    prices: dict[str, Decimal] = {}
    for symbol in symbols:
        df = get_history_candlesticks(symbol, target_date, target_date)
        if not df.empty:
            prices[symbol] = Decimal(str(df.iloc[-1]["close"]))
    return prices


def resolve_anchor_dates(
    as_of: datetime.date,
    inception_date: datetime.date,
    trading_days: list[datetime.date],
) -> dict[str, datetime.date | None]:
    """Resolve all metric anchor dates to actual trading days.

    Returns:
        Dict mapping period name to the nearest trading day on or before
        the target date. Returns None for dates before inception_date.
    """

    def _nearest_trading_day_on_or_before(target: datetime.date) -> datetime.date | None:
        """Find the nearest trading day on or before target."""
        if target < inception_date:
            return None
        # Binary search for nearest trading day <= target
        candidates = [d for d in trading_days if d <= target]
        if not candidates:
            return None
        return candidates[-1]

    # Calculate raw anchor dates
    # WoW: 7 calendar days ago
    wow_raw = as_of - datetime.timedelta(days=7)

    # MTD: last day of previous month
    first_of_month = as_of.replace(day=1)
    mtd_raw = first_of_month - datetime.timedelta(days=1)

    # QTD: last day of previous quarter
    quarter_month = ((as_of.month - 1) // 3) * 3 + 1  # first month of current quarter
    first_of_quarter = as_of.replace(month=quarter_month, day=1)
    qtd_raw = first_of_quarter - datetime.timedelta(days=1)

    # YTD: Dec 31 of previous year
    ytd_raw = datetime.date(as_of.year - 1, 12, 31)

    # Prev year: Dec 31 of year-before-last to Dec 31 of previous year
    prev_year_start_raw = datetime.date(as_of.year - 2, 12, 31)
    prev_year_end_raw = datetime.date(as_of.year - 1, 12, 31)

    return {
        "wow": _nearest_trading_day_on_or_before(wow_raw),
        "mtd": _nearest_trading_day_on_or_before(mtd_raw),
        "qtd": _nearest_trading_day_on_or_before(qtd_raw),
        "ytd": _nearest_trading_day_on_or_before(ytd_raw),
        "prev_year_start": _nearest_trading_day_on_or_before(prev_year_start_raw),
        "prev_year_end": _nearest_trading_day_on_or_before(prev_year_end_raw),
        "inception": inception_date,
    }


def calculate_twr(
    ledger: list[LedgerEvent],
    start_date: datetime.date,
    end_date: datetime.date,
    price_cache: PriceCache | None = None,
    today_prices: dict[str, Decimal] | None = None,
    initial_cash: Decimal = Decimal("0"),
) -> Decimal | None:
    """Time-Weighted Return for the portfolio over [start_date, end_date].

    Args:
        initial_cash: Cash offset to reconcile replayed cash with broker cash.
            When the ledger is missing deposit/withdrawal events, pass the
            difference (broker_cash - replayed_cash) so NAV calculations
            use the correct cash baseline at every replay point.

    Algorithm:
    1. Get all external cash flow events in the period
    2. Define sub-period boundaries: [start, cf1_date, cf2_date, ..., end]
    3. For each sub-period:
       a. Replay ledger to sub-period start -> get state -> get NAV
       b. Replay ledger to sub-period end -> get state -> get NAV
       c. R_i = (NAV_end - NAV_start) / NAV_start
    4. TWR = product(1 + R_i) - 1

    Returns None if start_date is before first ledger event or NAV is zero.
    """
    if not ledger:
        return None

    first_event_date = ledger[0].date
    if start_date < first_event_date:
        return None

    # Get cash flow events in the period (exclusive boundaries)
    cash_flow_events = get_cash_flow_events_in_period(ledger, start_date, end_date)

    # Build sub-period boundaries
    cf_dates = sorted({e.date for e in cash_flow_events})
    boundaries = [start_date, *cf_dates, end_date]

    # Remove duplicates and sort
    boundaries = sorted(set(boundaries))

    # Pre-compute set of cash-flow dates for boundary adjustment
    cf_date_set = set(cf_dates)

    # Calculate sub-period returns
    compound = Decimal("1")
    for i in range(len(boundaries) - 1):
        sub_start = boundaries[i]
        sub_end = boundaries[i + 1]

        # Get portfolio state and NAV at sub-period start
        state_start = replay_to_date(ledger, sub_start, initial_cash)
        symbols_start = list(state_start.positions.keys())
        prices_start = (
            _get_prices_at_date(symbols_start, sub_start, price_cache, today_prices)
            if symbols_start
            else {}
        )
        nav_start = get_nav_at_date(state_start, prices_start)

        # Get portfolio state and NAV at sub-period end
        state_end = replay_to_date(ledger, sub_end, initial_cash)
        symbols_end = list(state_end.positions.keys())
        prices_end = (
            _get_prices_at_date(symbols_end, sub_end, price_cache, today_prices)
            if symbols_end
            else {}
        )
        nav_end = get_nav_at_date(state_end, prices_end)

        # Subtract boundary cash flows to get pre-cash-flow NAV at sub_end.
        # replay_to_date includes events ON sub_end, so deposits inflate NAV_end.
        # Removing them isolates market return from external flows.
        nav_end_value = nav_end.total_nav
        if sub_end in cf_date_set:
            cf_on_boundary = sum(
                e.quantity if e.event_type == "cash_in" else -e.quantity
                for e in cash_flow_events
                if e.date == sub_end
            )
            nav_end_value -= cf_on_boundary

        if nav_start.total_nav == 0:
            # Cannot calculate return from zero NAV
            if nav_end_value == 0:
                continue  # Both zero, skip
            return None  # Started at zero, meaningless

        r_i = (nav_end_value - nav_start.total_nav) / nav_start.total_nav
        compound *= (Decimal("1") + r_i)

    return compound - Decimal("1")


def calculate_stock_price_return(
    symbol: str,
    start_date: datetime.date,
    end_date: datetime.date,
    price_cache: PriceCache | None = None,
    today_prices: dict[str, Decimal] | None = None,
) -> Decimal | None:
    """Simple price return for an individual stock.

    R = (close_end - close_start) / close_start
    Uses Yahoo Finance daily candlestick close prices.
    Returns None if no price data at start_date.
    """
    prices_start = _get_prices_at_date([symbol], start_date, price_cache, today_prices)
    prices_end = _get_prices_at_date([symbol], end_date, price_cache, today_prices)

    price_start = prices_start.get(symbol)
    price_end = prices_end.get(symbol)

    if price_start is None or price_end is None or price_start == 0:
        return None

    return (price_end - price_start) / price_start


def _twr_to_pnl(nav: Decimal, twr: Decimal | None) -> Decimal | None:
    """Derive dollar P&L from TWR and current NAV.

    Formula: P&L = NAV × TWR / (1 + TWR)

    This is exact when there are no mid-period cash flows and a close
    approximation otherwise. Crucially, it always has the same sign as TWR
    and doesn't depend on a potentially-incomplete ledger for absolute NAV.
    """
    if twr is None:
        return None
    denom = Decimal("1") + twr
    if denom == 0:
        return None
    return nav * twr / denom


def _calculate_deposit_roi(
    ledger: list[LedgerEvent],
    current_nav: Decimal,
) -> Decimal | None:
    """ROI on deposits: (current_nav - net_deposits) / net_deposits.

    net_deposits = sum(cash_in) - sum(cash_out).
    """
    net_deposits = Decimal("0")
    for event in ledger:
        if event.event_type == "cash_in":
            net_deposits += event.quantity
        elif event.event_type == "cash_out":
            net_deposits -= event.quantity
    if net_deposits <= 0:
        return None
    return (current_nav - net_deposits) / net_deposits


def calculate_portfolio_performance(
    ledger: list[LedgerEvent],
    positions: list[PositionRow],
    trading_days: list[datetime.date],
    total_nav: Decimal,
    daily_pnl: Decimal,
    price_cache: PriceCache | None = None,
    today_prices: dict[str, Decimal] | None = None,
    initial_cash: Decimal = Decimal("0"),
) -> PortfolioPerformance:
    """Full performance calculation: TWR for portfolio + price return per stock.

    Args:
        ledger: Sorted list of all ledger events.
        positions: Current position rows.
        trading_days: List of US trading days for anchor resolution.
        total_nav: Current total NAV.
        daily_pnl: Today's P&L.
        price_cache: Pre-fetched historical price cache.
        today_prices: Pre-fetched real-time quotes (avoids new API connections).

    Returns:
        PortfolioPerformance with all metrics populated.
    """
    today = datetime.date.today()
    inception_date = ledger[0].date if ledger else today

    # Resolve anchor dates
    anchors = resolve_anchor_dates(today, inception_date, trading_days)

    # Calculate portfolio TWR for each period
    def _twr(start_key: str, end_date: datetime.date = today) -> Decimal | None:
        start = anchors.get(start_key)
        if start is None:
            return None
        return calculate_twr(ledger, start, end_date, price_cache, today_prices, initial_cash)

    wow = _twr("wow")
    mtd = _twr("mtd")
    qtd = _twr("qtd")
    ytd = _twr("ytd")

    # Prev year: from prev_year_start to prev_year_end
    prev_year: Decimal | None = None
    if anchors.get("prev_year_start") is not None and anchors.get("prev_year_end") is not None:
        prev_year = calculate_twr(
            ledger,
            anchors["prev_year_start"],  # type: ignore[arg-type]
            anchors["prev_year_end"],  # type: ignore[arg-type]
            price_cache,
            today_prices,
            initial_cash,
        )

    inception = _twr("inception")

    # Dollar P&L per period — derived from TWR and broker NAV.
    # P&L = NAV × TWR / (1 + TWR)
    # This guarantees the same sign as TWR and avoids incomplete-ledger issues.
    wow_pnl = _twr_to_pnl(total_nav, wow)
    mtd_pnl = _twr_to_pnl(total_nav, mtd)
    qtd_pnl = _twr_to_pnl(total_nav, qtd)
    ytd_pnl = _twr_to_pnl(total_nav, ytd)
    inception_pnl = _twr_to_pnl(total_nav, inception)

    # Prev year: estimate end-of-year NAV by backing out YTD return
    prev_year_pnl: Decimal | None = None
    if prev_year is not None:
        if ytd is not None and ytd != Decimal("-1"):
            nav_eoy = total_nav / (Decimal("1") + ytd)
        else:
            nav_eoy = total_nav
        prev_year_pnl = _twr_to_pnl(nav_eoy, prev_year)

    # Since Inception ROI on deposits
    inception_roi = _calculate_deposit_roi(ledger, total_nav)

    # Calculate per-stock price returns
    def _stock_return(symbol: str, key: str) -> Decimal | None:
        anchor = anchors.get(key)
        if anchor is None:
            return None
        return calculate_stock_price_return(symbol, anchor, today, price_cache, today_prices)

    stock_perf: list[StockPerformance] = []
    for pos in positions:
        s_wow = _stock_return(pos.symbol, "wow")
        s_mtd = _stock_return(pos.symbol, "mtd")
        s_qtd = _stock_return(pos.symbol, "qtd")
        s_ytd = _stock_return(pos.symbol, "ytd")

        s_prev_year: Decimal | None = None
        if anchors.get("prev_year_start") is not None and anchors.get("prev_year_end") is not None:
            s_prev_year = calculate_stock_price_return(
                pos.symbol,
                anchors["prev_year_start"],  # type: ignore[arg-type]
                anchors["prev_year_end"],  # type: ignore[arg-type]
                price_cache,
                today_prices,
            )

        total_return = (
            (pos.last_price - pos.cost_price) / pos.cost_price
            if pos.cost_price != 0
            else Decimal("0")
        )

        stock_perf.append(
            StockPerformance(
                symbol=pos.symbol,
                name=pos.name,
                wow=s_wow,
                mtd=s_mtd,
                qtd=s_qtd,
                ytd=s_ytd,
                prev_year=s_prev_year,
                total_return=total_return,
            )
        )

    prev_nav = total_nav - daily_pnl
    daily_pnl_pct = (daily_pnl / prev_nav * Decimal("100")) if prev_nav != 0 else Decimal("0")

    return PortfolioPerformance(
        as_of_date=today,
        nav=total_nav,
        daily_pnl=daily_pnl,
        daily_pnl_pct=daily_pnl_pct,
        wow=wow,
        mtd=mtd,
        qtd=qtd,
        ytd=ytd,
        prev_year=prev_year,
        inception=inception,
        inception_date=inception_date,
        stock_performance=stock_perf,
        wow_pnl=wow_pnl,
        mtd_pnl=mtd_pnl,
        qtd_pnl=qtd_pnl,
        ytd_pnl=ytd_pnl,
        prev_year_pnl=prev_year_pnl,
        inception_pnl=inception_pnl,
        inception_roi=inception_roi,
    )
