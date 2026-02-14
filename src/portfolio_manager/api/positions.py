"""Position querying and analysis."""

from longport.openapi import TradeContext

from portfolio_manager.api.client import get_trade_context


def get_stock_positions(ctx: TradeContext | None = None) -> list:
    """Fetch all stock positions from LongPort account."""
    if ctx is None:
        ctx = get_trade_context()
    resp = ctx.stock_positions()
    return resp.channels


def get_fund_positions(ctx: TradeContext | None = None) -> list:
    """Fetch all fund positions from LongPort account."""
    if ctx is None:
        ctx = get_trade_context()
    resp = ctx.fund_positions()
    return resp.channels


def get_account_balance(ctx: TradeContext | None = None) -> list:
    """Fetch account balance across all currencies."""
    if ctx is None:
        ctx = get_trade_context()
    return ctx.account_balance()
