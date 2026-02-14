"""Shared dataclasses for portfolio analytics."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True)
class PositionRow:
    """A single stock position with current pricing and analytics."""

    symbol: str  # e.g. "AAPL.US"
    name: str
    quantity: Decimal
    cost_price: Decimal
    last_price: Decimal
    prev_close: Decimal  # for daily P&L calc
    market_value: Decimal  # quantity * last_price
    cost_value: Decimal  # quantity * cost_price
    unrealized_pnl: Decimal
    unrealized_pnl_pct: Decimal
    daily_pnl: Decimal  # quantity * (last_price - prev_close)
    weight: Decimal  # market_value / total_nav
    currency: str


@dataclass(frozen=True)
class CashBalance:
    """Cash balance for a single currency."""

    currency: str
    available: Decimal
    frozen: Decimal
    settling: Decimal
    total: Decimal


@dataclass(frozen=True)
class NavSnapshot:
    """Net Asset Value at a specific date."""

    date: datetime.date
    total_nav: Decimal
    stock_value: Decimal
    cash_value: Decimal


@dataclass(frozen=True)
class StockPerformance:
    """Per-stock performance metrics (price return)."""

    symbol: str
    name: str
    wow: Decimal | None
    mtd: Decimal | None
    qtd: Decimal | None
    ytd: Decimal | None
    prev_year: Decimal | None
    total_return: Decimal  # (last_price - cost_price) / cost_price


@dataclass(frozen=True)
class PortfolioPerformance:
    """Portfolio-level performance metrics (TWR)."""

    as_of_date: datetime.date
    nav: Decimal
    daily_pnl: Decimal
    daily_pnl_pct: Decimal
    wow: Decimal | None  # TWR
    mtd: Decimal | None  # TWR
    qtd: Decimal | None  # TWR
    ytd: Decimal | None  # TWR
    prev_year: Decimal | None  # TWR
    inception: Decimal | None  # TWR
    inception_date: datetime.date
    stock_performance: list[StockPerformance]
    # Dollar P&L per period (current_NAV - NAV_at_anchor)
    wow_pnl: Decimal | None = None
    mtd_pnl: Decimal | None = None
    qtd_pnl: Decimal | None = None
    ytd_pnl: Decimal | None = None
    prev_year_pnl: Decimal | None = None
    inception_pnl: Decimal | None = None
    # Since Inception ROI on deposits: (NAV - net_deposits) / net_deposits
    inception_roi: Decimal | None = None


@dataclass(frozen=True)
class RebalanceSuggestion:
    """Weight-based rebalancing suggestion."""

    symbol: str
    name: str
    action: str  # "BUY" or "SELL"
    reason: str  # "weight" or "atr"
    current_weight: Decimal
    target_weight: Decimal | None
    current_price: Decimal
    suggested_quantity: int  # floor by cash (BUY), cap by available_quantity (SELL)
    suggested_value: Decimal
    detail: str


@dataclass(frozen=True)
class AtrBand:
    """ATR volatility band for a position."""

    symbol: str
    name: str
    current_price: Decimal
    cost_price: Decimal
    atr: Decimal
    lower_band: Decimal  # cost - multiplier * ATR
    upper_band: Decimal  # cost + multiplier * ATR
    signal: str  # "in_range", "near_upper", "near_lower", "breach_upper", "breach_lower"


@dataclass(frozen=True)
class ClosedPosition:
    """A position that has been fully closed (net zero shares)."""

    symbol: str
    total_bought_qty: Decimal
    avg_buy_price: Decimal
    avg_sell_price: Decimal
    realized_pnl: Decimal
    realized_pnl_pct: Decimal  # (avg_sell - avg_buy) / avg_buy
    first_trade_date: datetime.date
    last_trade_date: datetime.date


@dataclass(frozen=True)
class LedgerEvent:
    """A single event in the portfolio ledger timeline."""

    date: datetime.date  # UTC date
    event_type: str  # "buy", "sell", "cash_in", "cash_out", "dividend"
    symbol: str | None  # None for cash events
    quantity: Decimal  # shares (for trades) or amount (for cash)
    price: Decimal  # execution price (for trades) or Decimal("0")
    order_id: str | None  # for dedup


@dataclass(frozen=True)
class PortfolioState:
    """Portfolio state at a specific date."""

    date: datetime.date
    positions: dict[str, Decimal]  # symbol -> quantity
    cash: Decimal  # available cash


@dataclass
class PortfolioConfig:
    """Portfolio configuration loaded from YAML."""

    base_currency: str
    target_allocations: dict[str, Decimal]
    rebalance_threshold: Decimal
    atr_period: int
    atr_multiplier: Decimal
    # Email fields: None until validated (lazy)
    smtp_host: str | None = None
    smtp_port: int | None = None
    smtp_username: str | None = None
    smtp_password: str | None = None
    email_from: str | None = None
    email_to: list[str] | None = field(default=None)
    email_subject_template: str | None = None

    def validate_email_config(self) -> None:
        """Raise ValueError if any email field is None. Called only when sending."""
        missing = []
        for fname in ("smtp_host", "smtp_port", "smtp_username", "smtp_password", "email_from", "email_to"):
            if getattr(self, fname) is None:
                missing.append(fname)
        if missing:
            raise ValueError(f"Email configuration incomplete — missing: {', '.join(missing)}")
