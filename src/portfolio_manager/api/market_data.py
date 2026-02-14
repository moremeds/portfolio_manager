"""Market data API wrappers with pagination, Decimal conversion, and timezone normalization.

Historical OHLCV data and trading days are sourced from Yahoo Finance (yfinance) for
unlimited date range support. Real-time quotes, trade history, and cash flows remain
on the LongPort API (account-specific data).
"""

from __future__ import annotations

import datetime
import logging
from decimal import Decimal

import pandas as pd
import yfinance as yf
from longport.openapi import (
    OrderStatus,
    QuoteContext,
    TradeContext,
)

from portfolio_manager.api.client import get_quote_context, get_trade_context

logger = logging.getLogger(__name__)


def _to_decimal(value: object) -> Decimal:
    """Convert an API value (usually str or float) to Decimal."""
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _to_utc_date(ts: object) -> datetime.date:
    """Normalize a timestamp to a UTC date.

    Handles datetime objects (with or without tzinfo) and date objects.
    """
    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is not None:
            ts = ts.astimezone(datetime.UTC)
        return ts.date()
    if isinstance(ts, datetime.date):
        return ts
    raise TypeError(f"Cannot convert {type(ts)} to date: {ts}")


def get_quotes(
    symbols: list[str],
    ctx: QuoteContext | None = None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    """Fetch real-time last-done and previous close prices in a single API call.

    Returns:
        Tuple of (last_done_prices, prev_close_prices) — each a symbol -> Decimal mapping.
    """
    if ctx is None:
        ctx = get_quote_context()
    quotes = ctx.quote(symbols)
    last_done = {q.symbol: _to_decimal(q.last_done) for q in quotes}
    prev_close = {q.symbol: _to_decimal(q.prev_close) for q in quotes}
    return last_done, prev_close


def get_real_time_quotes(
    symbols: list[str],
    ctx: QuoteContext | None = None,
) -> dict[str, Decimal]:
    """Fetch real-time last-done prices for symbols.

    Returns:
        Mapping of symbol -> last_done price as Decimal.
    """
    last_done, _ = get_quotes(symbols, ctx)
    return last_done


def get_prev_close_prices(
    symbols: list[str],
    ctx: QuoteContext | None = None,
) -> dict[str, Decimal]:
    """Fetch previous close prices for daily P&L calculation.

    Returns:
        Mapping of symbol -> prev_close price as Decimal.
    """
    _, prev_close = get_quotes(symbols, ctx)
    return prev_close


def _to_yahoo_symbol(longport_symbol: str) -> str:
    """Convert LongPort symbol format to Yahoo Finance format.

    LongPort: 'AAPL.US' → Yahoo: 'AAPL'
    """
    return longport_symbol.rsplit(".", 1)[0]


def _yahoo_download(
    tickers: str | list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Single wrapper around yf.download with consistent options."""
    end_exclusive = end_date + datetime.timedelta(days=1)
    return yf.download(
        tickers,
        start=start_date.isoformat(),
        end=end_exclusive.isoformat(),
        auto_adjust=False,
        progress=False,
    )


def _yahoo_df_to_ohlcv(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert a Yahoo Finance DataFrame to our standard OHLCV format with Decimals."""
    if raw.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    rows = []
    for idx, row in raw.iterrows():
        if pd.isna(row.get("Close")):
            continue
        rows.append(
            {
                "date": pd.Timestamp(idx).date(),
                "open": _to_decimal(row["Open"]),
                "high": _to_decimal(row["High"]),
                "low": _to_decimal(row["Low"]),
                "close": _to_decimal(row["Close"]),
                "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
            }
        )

    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    return df.sort_values("date").reset_index(drop=True)


def _extract_ticker_df(raw: pd.DataFrame, yahoo_symbol: str, single: bool) -> pd.DataFrame:
    """Extract a single ticker's data from a yf.download result.

    yfinance ≥1.1.0 returns a MultiIndex ``(PriceType, Ticker)`` for multi-ticker
    downloads.  The ticker lives at level 1, so we cross-section on it.
    """
    if single:
        df = raw.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    try:
        return raw.xs(yahoo_symbol, level=1, axis=1).copy()
    except KeyError:
        return pd.DataFrame()


class PriceCache:
    """Pre-fetched price cache: one yf.download() call, O(1) lookups.

    Usage:
        cache = PriceCache(symbols, start_date, end_date)
        price = cache.get_close("AAPL.US", some_date)
        prices = cache.get_closes(["AAPL.US", "MSFT.US"], some_date)
    """

    def __init__(
        self,
        symbols: list[str],
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> None:
        self._prices: dict[str, dict[datetime.date, Decimal]] = {}
        if not symbols:
            return

        yahoo_symbols = [_to_yahoo_symbol(s) for s in symbols]
        yahoo_to_lp = {_to_yahoo_symbol(s): s for s in symbols}
        single = len(yahoo_symbols) == 1

        raw = _yahoo_download(
            yahoo_symbols if not single else yahoo_symbols[0],
            start_date,
            end_date,
        )
        if raw.empty:
            return

        for ysym in yahoo_symbols:
            lp_sym = yahoo_to_lp[ysym]
            ticker_df = _extract_ticker_df(raw, ysym, single)
            date_prices: dict[datetime.date, Decimal] = {}
            for idx, row in ticker_df.iterrows():
                close_val = row.get("Close")
                if pd.notna(close_val):
                    date_prices[pd.Timestamp(idx).date()] = _to_decimal(close_val)
            self._prices[lp_sym] = date_prices

        logger.info(
            "PriceCache loaded: %d symbols, %s to %s",
            len(symbols), start_date, end_date,
        )

    def get_close(self, symbol: str, target_date: datetime.date) -> Decimal | None:
        """Look up closing price for symbol on target_date.

        Falls back to the nearest earlier date within 5 calendar days
        (handles weekends, holidays) to avoid returning None when the
        exact date has no data.
        """
        date_prices = self._prices.get(symbol)
        if date_prices is None:
            return None

        price = date_prices.get(target_date)
        if price is not None:
            return price

        # Fallback: try up to 5 days back (covers weekends + holidays)
        for offset in range(1, 6):
            fallback_date = target_date - datetime.timedelta(days=offset)
            price = date_prices.get(fallback_date)
            if price is not None:
                return price

        return None

    def get_closes(
        self, symbols: list[str], target_date: datetime.date
    ) -> dict[str, Decimal]:
        """Look up closing prices for multiple symbols on a date."""
        result: dict[str, Decimal] = {}
        for s in symbols:
            p = self.get_close(s, target_date)
            if p is not None:
                result[s] = p
        return result


def get_history_candlesticks(
    symbol: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """Fetch daily OHLCV candlesticks for a single symbol via Yahoo Finance.

    Returns:
        DataFrame with columns: [date, open, high, low, close, volume].
        All price columns are Decimal.
    """
    yahoo_symbol = _to_yahoo_symbol(symbol)
    raw = _yahoo_download(yahoo_symbol, start_date, end_date)

    if raw.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    return _yahoo_df_to_ohlcv(raw)


def get_batch_candlesticks(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> dict[str, pd.DataFrame]:
    """Fetch daily candlesticks for multiple symbols via Yahoo Finance batch download.

    Uses a single yfinance.download() call for all tickers — no rate limiting needed.

    Returns:
        Mapping of LongPort symbol -> OHLCV DataFrame.
    """
    if not symbols:
        return {}

    yahoo_symbols = [_to_yahoo_symbol(s) for s in symbols]
    yahoo_to_lp = {_to_yahoo_symbol(s): s for s in symbols}
    single = len(yahoo_symbols) == 1

    raw = _yahoo_download(
        yahoo_symbols if not single else yahoo_symbols[0],
        start_date,
        end_date,
    )

    result: dict[str, pd.DataFrame] = {}
    for ysym in yahoo_symbols:
        lp_sym = yahoo_to_lp[ysym]
        ticker_df = _extract_ticker_df(raw, ysym, single)
        if ticker_df.empty:
            logger.warning("No Yahoo Finance data for %s", ysym)
            result[lp_sym] = pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume"]
            )
        else:
            result[lp_sym] = _yahoo_df_to_ohlcv(ticker_df)

    return result


def get_trading_days(
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[datetime.date]:
    """Derive US market trading days from Yahoo Finance SPY history.

    SPY (S&P 500 ETF) trades every US market day. We download its history
    and extract dates from the index — no chunking or rate limiting needed.

    Returns:
        Sorted list of trading day dates.
    """
    raw = _yahoo_download("SPY", start_date, end_date)

    if raw.empty:
        return []

    return sorted(pd.Timestamp(idx).date() for idx in raw.index)


def get_trade_history(
    start_at: datetime.datetime,
    end_at: datetime.datetime | None = None,
    ctx: TradeContext | None = None,
) -> pd.DataFrame:
    """Fetch all filled orders with cursor-based pagination and dedup.

    Pagination strategy:
    1. Query with start_at, end_at, status=[Filled]
    2. If has_more: narrow end_at to oldest order's submitted_at, re-query
    3. Deduplicate by order_id (set)
    4. Sort by submitted_at ascending

    Returns:
        DataFrame with columns: [date, symbol, side, quantity, price, order_id].
        All prices are Decimal.
    """
    if ctx is None:
        ctx = get_trade_context()
    if end_at is None:
        end_at = datetime.datetime.now(datetime.UTC)

    seen_order_ids: set[str] = set()
    all_orders: list[dict[str, object]] = []
    current_end = end_at

    while True:
        resp = ctx.history_orders(
            start_at=start_at,
            end_at=current_end,
            status=[OrderStatus.Filled],
        )

        if not resp:
            break

        batch_oldest_time = current_end
        for order in resp:
            oid = str(order.order_id)
            if oid in seen_order_ids:
                continue
            seen_order_ids.add(oid)

            submitted = order.submitted_at
            # Ensure both datetimes are timezone-aware for comparison
            if isinstance(submitted, datetime.datetime) and submitted.tzinfo is None:
                submitted = submitted.replace(tzinfo=datetime.UTC)
            if submitted < batch_oldest_time:
                batch_oldest_time = submitted

            all_orders.append(
                {
                    "date": _to_utc_date(submitted),
                    "symbol": order.symbol,
                    "side": str(order.side).split(".")[-1].lower(),
                    "quantity": _to_decimal(order.executed_quantity),
                    "price": _to_decimal(order.executed_price),
                    "order_id": oid,
                }
            )

        # Check if there are more orders
        has_more = getattr(resp, "has_more", False) if not isinstance(resp, list) else len(resp) >= 1000
        if isinstance(resp, list) and len(resp) >= 1000:
            has_more = True

        if not has_more:
            break

        # Narrow the window to fetch older orders
        prev_end = current_end
        current_end = batch_oldest_time

        # Safety: break if no progress (current_end didn't move)
        if current_end >= prev_end:
            logger.warning(
                "Pagination stalled at %s — %d orders fetched so far",
                current_end,
                len(all_orders),
            )
            break

    if not all_orders:
        return pd.DataFrame(columns=["date", "symbol", "side", "quantity", "price", "order_id"])

    df = pd.DataFrame(all_orders)
    return df.sort_values("date").reset_index(drop=True)


def get_cash_flows(
    start_at: datetime.datetime,
    end_at: datetime.datetime | None = None,
    ctx: TradeContext | None = None,
) -> pd.DataFrame:
    """Fetch all cash flows with page-based pagination.

    Pagination strategy:
    1. Query page=1, size=10000
    2. If len(results) == size: increment page, repeat
    3. Continue until page returns < size results

    Returns:
        DataFrame with columns: [date, direction, business_type, amount, currency, description].
        direction: 1=outflow, 2=inflow.
    """
    if ctx is None:
        ctx = get_trade_context()
    if end_at is None:
        end_at = datetime.datetime.now(datetime.UTC)

    page_size = 10000
    current_page = 1
    all_flows: list[dict[str, object]] = []
    seen: set[tuple[object, str, str, str]] = set()  # (business_time, amount, direction, currency)

    while True:
        flows = ctx.cash_flow(
            start_at=start_at,
            end_at=end_at,
            page=current_page,
            size=page_size,
        )

        if not flows:
            break

        for flow in flows:
            dt = _to_utc_date(flow.business_time)
            amount_str = str(flow.balance)
            direction_str = str(flow.direction)
            # Use full timestamp + currency for collision-resistant dedup
            dedup_key = (flow.business_time, amount_str, direction_str, str(flow.currency))

            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            all_flows.append(
                {
                    "date": dt,
                    "direction": int(flow.direction),
                    "business_type": int(flow.business_type),
                    "amount": _to_decimal(flow.balance),
                    "currency": str(flow.currency),
                    "description": str(flow.description) if flow.description else "",
                }
            )

        if len(flows) < page_size:
            break

        current_page += 1

    if not all_flows:
        return pd.DataFrame(columns=["date", "direction", "business_type", "amount", "currency", "description"])

    df = pd.DataFrame(all_flows)
    return df.sort_values("date").reset_index(drop=True)
