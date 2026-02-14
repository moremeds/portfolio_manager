"""Portfolio analytics orchestrator and CLI entry point."""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
import webbrowser
from decimal import Decimal
from pathlib import Path

from portfolio_manager.analytics.ledger import (
    build_ledger,
    check_position_consistency,
    get_closed_positions,
    replay_to_date,
)
from portfolio_manager.analytics.performance import calculate_portfolio_performance
from portfolio_manager.analytics.rebalancing import atr_based_rebalance, weight_based_rebalance
from portfolio_manager.api.client import get_quote_context, get_trade_context
from portfolio_manager.api.market_data import (
    PriceCache,
    get_batch_candlesticks,
    get_cash_flows,
    get_quotes,
    get_trade_history,
    get_trading_days,
)
from portfolio_manager.api.positions import get_account_balance, get_stock_positions
from portfolio_manager.core.config import load_config
from portfolio_manager.core.types import (
    CashBalance,
    PositionRow,
)
from portfolio_manager.reporting.email_sender import send_report
from portfolio_manager.reporting.renderer import render_report

logger = logging.getLogger(__name__)


def _build_position_rows(
    channels: list[object],
    quotes: dict[str, Decimal],
    prev_closes: dict[str, Decimal],
    total_nav: Decimal,
) -> list[PositionRow]:
    """Build PositionRow list from API responses."""
    rows: list[PositionRow] = []
    for channel in channels:
        for pos in channel.positions:
            symbol = pos.symbol
            qty = Decimal(str(pos.quantity))
            cost = Decimal(str(pos.cost_price))
            last = quotes.get(symbol, Decimal("0"))
            prev = prev_closes.get(symbol, last)

            market_val = qty * last
            cost_val = qty * cost
            unrealized = market_val - cost_val
            unrealized_pct = (unrealized / cost_val * 100) if cost_val != 0 else Decimal("0")
            daily = qty * (last - prev)
            weight = (market_val / total_nav * 100) if total_nav != 0 else Decimal("0")

            rows.append(
                PositionRow(
                    symbol=symbol,
                    name=str(pos.symbol_name),
                    quantity=qty,
                    cost_price=cost,
                    last_price=last,
                    prev_close=prev,
                    market_value=market_val,
                    cost_value=cost_val,
                    unrealized_pnl=unrealized,
                    unrealized_pnl_pct=unrealized_pct,
                    daily_pnl=daily,
                    weight=weight,
                    currency=str(pos.currency),
                )
            )
    return rows


def _build_cash_balances(balances: list[object]) -> list[CashBalance]:
    """Build CashBalance list from API response."""
    result: list[CashBalance] = []
    for bal in balances:
        for ci in bal.cash_infos:
            available = Decimal(str(ci.available_cash))
            frozen = Decimal(str(ci.frozen_cash))
            settling = Decimal(str(ci.settling_cash))
            result.append(
                CashBalance(
                    currency=str(ci.currency),
                    available=available,
                    frozen=frozen,
                    settling=settling,
                    total=available + frozen + settling,
                )
            )
    return result


def run(
    config_path: str | None = None,
    send_email: bool = True,
) -> None:
    """Run the portfolio analytics pipeline.

    Args:
        config_path: Path to YAML config file.
        send_email: If False, save HTML report locally instead of emailing.
    """
    # 1. Load config
    config = load_config(config_path)
    logger.info("Loaded config: base_currency=%s", config.base_currency)

    # 2. Get current positions
    trade_ctx = get_trade_context()
    quote_ctx = get_quote_context()

    channels = get_stock_positions(trade_ctx)
    balances = get_account_balance(trade_ctx)

    # Collect all symbols
    symbols: list[str] = []
    for channel in channels:
        for pos in channel.positions:
            symbols.append(pos.symbol)

    if not symbols:
        logger.warning("No positions found")
        return

    # 3. Get quotes + prev_close (single API call)
    quotes, prev_closes = get_quotes(symbols, quote_ctx)

    # 4. Build position table + cash
    cash_balances = _build_cash_balances(balances)
    total_cash = sum(c.total for c in cash_balances)
    total_stock_value = sum(
        Decimal(str(pos.quantity)) * quotes.get(pos.symbol, Decimal("0"))
        for channel in channels
        for pos in channel.positions
    )
    total_nav = total_stock_value + total_cash

    positions = _build_position_rows(channels, quotes, prev_closes, total_nav)

    # 5. Build ledger for performance analytics
    today = datetime.date.today()
    # Fetch full trade history (LongPort returns only what exists)
    history_start = datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)
    logger.info("Fetching trade history from %s", history_start.date())

    trade_history = get_trade_history(history_start, ctx=trade_ctx)
    cash_flow_data = get_cash_flows(history_start, ctx=trade_ctx)

    ledger = build_ledger(trade_history, cash_flow_data)
    logger.info("Built ledger with %d events", len(ledger))

    # 6. Check position consistency and reconcile cash
    cash_offset = total_cash  # fallback: all cash is untracked deposits
    if ledger:
        replayed = replay_to_date(ledger, today)
        current_positions = {p.symbol: p.quantity for p in positions}
        warnings = check_position_consistency(replayed, current_positions)
        if warnings:
            logger.warning("Position consistency warnings (possible splits/corporate actions):")
            for w in warnings:
                logger.warning("  %s", w)

        # Reconcile replayed cash with broker-reported cash.
        # If deposits/withdrawals are missing from the ledger, replayed cash
        # is wrong (deeply negative). The offset corrects all replay_to_date
        # calls so NAV calculations use the correct cash baseline.
        cash_offset = total_cash - replayed.cash
        if abs(cash_offset) > Decimal("1"):
            logger.info(
                "Cash reconciliation offset: %s (replayed=%s, broker=%s)",
                cash_offset, replayed.cash, total_cash,
            )

    # 7. Derive inception date, then get trading days covering full range
    inception_date = ledger[0].date if ledger else today
    td_start = min(datetime.date(today.year - 2, 1, 1), inception_date)
    trading_days_list = get_trading_days(td_start, today)

    # 7b. Pre-fetch all historical prices in one batch call
    all_symbols = list({e.symbol for e in ledger if e.symbol is not None} | set(symbols))
    price_cache = PriceCache(all_symbols, inception_date, today)

    # 7c. Identify closed positions
    current_symbol_set = {p.symbol for p in positions}
    closed_positions = get_closed_positions(ledger, current_symbol_set)
    logger.info("Found %d closed positions", len(closed_positions))

    # 8. Calculate performance
    total_daily_pnl = sum(p.daily_pnl for p in positions)
    performance = calculate_portfolio_performance(
        ledger=ledger,
        positions=positions,
        trading_days=trading_days_list,
        total_nav=total_nav,
        daily_pnl=total_daily_pnl,
        price_cache=price_cache,
        today_prices=quotes,
        initial_cash=cash_offset,
    )
    logger.info("Performance calculated: inception TWR=%s", performance.inception)

    # 9. Rebalancing — weight-based
    available_cash = sum(c.available for c in cash_balances)
    suggestions = weight_based_rebalance(positions, total_nav, available_cash, config)
    logger.info("Weight rebalancing: %d suggestions", len(suggestions))

    # 10. ATR-based rebalancing
    atr_start = today - datetime.timedelta(days=config.atr_period * 2)
    candlestick_data = get_batch_candlesticks(symbols, atr_start, today)
    atr_bands = atr_based_rebalance(positions, candlestick_data, config)
    logger.info("ATR bands calculated for %d positions", len(atr_bands))

    # 11. Render HTML report
    html_report = render_report(
        positions=positions,
        cash_balances=cash_balances,
        total_nav=total_nav,
        performance=performance,
        suggestions=suggestions,
        atr_bands=atr_bands,
        report_date=today,
        closed_positions=closed_positions,
        atr_period=config.atr_period,
        atr_multiplier=config.atr_multiplier,
    )
    logger.info("HTML report rendered (%d bytes)", len(html_report))

    # 12. Deliver report
    if send_email:
        send_report(html_report, config, report_date=today)
    else:
        report_path = Path(f"report_{today.isoformat()}.html")
        report_path.write_text(html_report, encoding="utf-8")
        print(f"Report saved to {report_path.resolve()}")
        webbrowser.open(report_path.resolve().as_uri())


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Portfolio Analytics & Daily Report")
    parser.add_argument("--config", type=str, default=None, help="Path to portfolio.yml config file")
    parser.add_argument("--no-email", action="store_true", help="Save HTML report locally instead of emailing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        run(
            config_path=args.config,
            send_email=not args.no_email,
        )
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
