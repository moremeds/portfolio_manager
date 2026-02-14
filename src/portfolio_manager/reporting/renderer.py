"""HTML report renderer using Jinja2 templates."""

from __future__ import annotations

import datetime
from decimal import Decimal
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from portfolio_manager.core.types import (
    AtrBand,
    CashBalance,
    ClosedPosition,
    PortfolioPerformance,
    PositionRow,
    RebalanceSuggestion,
)

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _format_currency(val: Decimal) -> str:
    """Format a Decimal as currency string."""
    sign = "" if val >= 0 else "-"
    return f"{sign}${abs(val):,.2f}"


def _format_pct(val: Decimal | None) -> str:
    """Format a Decimal as percentage with sign."""
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val * 100:.2f}%"


def _pnl_class(val: Decimal | None) -> str:
    """Return CSS class name based on positive/negative value."""
    if val is None:
        return ""
    return "positive" if val >= 0 else "negative"


def _fmt_multiplier(val: Decimal | None) -> str:
    """Format multiplier cleanly: '2' not '2.0'."""
    if val is None:
        return ""
    return str(int(val)) if val == int(val) else str(val)


def render_report(
    positions: list[PositionRow],
    cash_balances: list[CashBalance],
    total_nav: Decimal,
    performance: PortfolioPerformance,
    suggestions: list[RebalanceSuggestion],
    atr_bands: list[AtrBand],
    report_date: datetime.date | None = None,
    closed_positions: list[ClosedPosition] | None = None,
    atr_period: int | None = None,
    atr_multiplier: Decimal | None = None,
) -> str:
    """Render the full HTML report.

    Returns:
        Complete HTML string ready for email or file output.
    """
    if report_date is None:
        report_date = datetime.date.today()

    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=True,
    )
    env.filters["currency"] = _format_currency
    env.filters["pct"] = _format_pct
    env.filters["pnl_class"] = _pnl_class

    template = env.get_template("report.html.j2")

    total_cash = sum(c.total for c in cash_balances)
    cash_pct = (total_cash / total_nav * 100) if total_nav != 0 else Decimal("0")
    daily_pnl = performance.daily_pnl
    daily_pnl_pct = performance.daily_pnl_pct

    # Sort positions by market value descending
    sorted_positions = sorted(positions, key=lambda p: p.market_value, reverse=True)

    # Performance periods for the table: (label, return_pct, dollar_pnl)
    perf_periods = [
        ("Week-on-Week", performance.wow, performance.wow_pnl),
        ("Month-to-Date", performance.mtd, performance.mtd_pnl),
        ("Quarter-to-Date", performance.qtd, performance.qtd_pnl),
        ("Year-to-Date", performance.ytd, performance.ytd_pnl),
        ("Prev Year", performance.prev_year, performance.prev_year_pnl),
        ("Since Inception (TWR)", performance.inception, performance.inception_pnl),
        ("Since Inception (ROI)", performance.inception_roi, None),
    ]

    # Pre-build symbol→StockPerformance map to avoid Jinja2 scoping issues
    stock_perf_map = {sp.symbol: sp for sp in performance.stock_performance}

    context = {
        "report_date": report_date,
        "total_nav": total_nav,
        "total_cash": total_cash,
        "cash_pct": cash_pct,
        "daily_pnl": daily_pnl,
        "daily_pnl_pct": daily_pnl_pct,
        "positions": sorted_positions,
        "cash_balances": cash_balances,
        "perf_periods": perf_periods,
        "stock_perf_map": stock_perf_map,
        "suggestions": suggestions,
        "atr_bands": atr_bands,
        "closed_positions": closed_positions or [],
        "atr_period": atr_period,
        "atr_multiplier": _fmt_multiplier(atr_multiplier),
        "generated_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "fmt_currency": _format_currency,
        "fmt_pct": _format_pct,
        "pnl_class": _pnl_class,
    }

    return template.render(**context)
