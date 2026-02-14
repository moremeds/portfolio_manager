"""Tests for reporting/renderer.py — HTML report generation."""

from __future__ import annotations

import datetime
from decimal import Decimal

from portfolio_manager.core.types import (
    AtrBand,
    CashBalance,
    PortfolioPerformance,
    PositionRow,
    RebalanceSuggestion,
    StockPerformance,
)
from portfolio_manager.reporting.renderer import render_report


def _make_position(**overrides: object) -> PositionRow:
    defaults = {
        "symbol": "AAPL.US",
        "name": "Apple Inc",
        "quantity": Decimal("100"),
        "cost_price": Decimal("170"),
        "last_price": Decimal("185"),
        "prev_close": Decimal("184"),
        "market_value": Decimal("18500"),
        "cost_value": Decimal("17000"),
        "unrealized_pnl": Decimal("1500"),
        "unrealized_pnl_pct": Decimal("8.82"),
        "daily_pnl": Decimal("100"),
        "weight": Decimal("14.8"),
        "currency": "USD",
    }
    defaults.update(overrides)
    return PositionRow(**defaults)  # type: ignore[arg-type]


def _make_performance(**overrides: object) -> PortfolioPerformance:
    defaults = {
        "as_of_date": datetime.date(2026, 2, 14),
        "nav": Decimal("125000"),
        "daily_pnl": Decimal("1230"),
        "daily_pnl_pct": Decimal("0.99"),
        "wow": Decimal("0.023"),
        "mtd": Decimal("0.041"),
        "qtd": Decimal("0.087"),
        "ytd": Decimal("0.125"),
        "prev_year": Decimal("0.182"),
        "inception": Decimal("0.25"),
        "inception_date": datetime.date(2024, 6, 1),
        "stock_performance": [
            StockPerformance(
                symbol="AAPL.US",
                name="Apple Inc",
                wow=Decimal("0.012"),
                mtd=Decimal("0.028"),
                qtd=None,
                ytd=Decimal("0.083"),
                prev_year=None,
                total_return=Decimal("0.088"),
            )
        ],
    }
    defaults.update(overrides)
    return PortfolioPerformance(**defaults)  # type: ignore[arg-type]


def test_render_report_basic():
    """Report should render valid HTML with all sections."""
    pos = _make_position()
    cash = CashBalance(
        currency="USD", available=Decimal("15000"), frozen=Decimal("0"),
        settling=Decimal("200"), total=Decimal("15200"),
    )
    perf = _make_performance()

    html = render_report(
        positions=[pos],
        cash_balances=[cash],
        total_nav=Decimal("125000"),
        performance=perf,
        suggestions=[],
        atr_bands=[],
        report_date=datetime.date(2026, 2, 14),
    )

    assert "<!DOCTYPE html>" in html
    assert "Portfolio Report" in html
    assert "AAPL" in html
    assert "$125,000.00" in html or "125,000" in html


def test_render_report_with_suggestions():
    """Report should include rebalancing suggestions."""
    pos = _make_position()
    perf = _make_performance()
    suggestion = RebalanceSuggestion(
        symbol="NVDA.US",
        name="NVIDIA",
        action="SELL",
        reason="weight",
        current_weight=Decimal("0.223"),
        target_weight=Decimal("0.20"),
        current_price=Decimal("140"),
        suggested_quantity=2,
        suggested_value=Decimal("280"),
        detail="SELL 2 shares (~$280)",
    )

    html = render_report(
        positions=[pos],
        cash_balances=[],
        total_nav=Decimal("125000"),
        performance=perf,
        suggestions=[suggestion],
        atr_bands=[],
        report_date=datetime.date(2026, 2, 14),
    )

    assert "Weight Rebalancing" in html
    assert "NVDA" in html
    assert "SELL 2 shares" in html


def test_render_report_with_atr():
    """Report should include ATR bands."""
    pos = _make_position()
    perf = _make_performance()
    band = AtrBand(
        symbol="TSLA.US",
        name="Tesla",
        current_price=Decimal("280"),
        cost_price=Decimal("250"),
        atr=Decimal("15.5"),
        lower_band=Decimal("219"),
        upper_band=Decimal("281"),
        signal="near_upper",
    )

    html = render_report(
        positions=[pos],
        cash_balances=[],
        total_nav=Decimal("125000"),
        performance=perf,
        suggestions=[],
        atr_bands=[band],
        report_date=datetime.date(2026, 2, 14),
    )

    assert "ATR Volatility Bands" in html
    assert "TSLA" in html
    assert "Near Upper" in html


def test_render_report_handles_none_as_na():
    """None values should render as 'N/A'."""
    perf = _make_performance(wow=None, mtd=None, qtd=None, prev_year=None)
    pos = _make_position()

    html = render_report(
        positions=[pos],
        cash_balances=[],
        total_nav=Decimal("125000"),
        performance=perf,
        suggestions=[],
        atr_bands=[],
        report_date=datetime.date(2026, 2, 14),
    )

    assert "N/A" in html


def test_render_report_empty_portfolio():
    """Empty portfolio should still render without errors."""
    perf = _make_performance(stock_performance=[])

    html = render_report(
        positions=[],
        cash_balances=[],
        total_nav=Decimal("10000"),
        performance=perf,
        suggestions=[],
        atr_bands=[],
        report_date=datetime.date(2026, 2, 14),
    )

    assert "<!DOCTYPE html>" in html
    assert "Portfolio Report" in html
