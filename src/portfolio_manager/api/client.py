"""LongPort API client wrapper for portfolio operations."""

from dotenv import load_dotenv
from longport.openapi import Config, QuoteContext, TradeContext

load_dotenv()


def get_config() -> Config:
    """Create LongPort config from environment variables.

    Requires LONGPORT_APP_KEY, LONGPORT_APP_SECRET, LONGPORT_ACCESS_TOKEN.
    """
    return Config.from_env()


def get_trade_context(config: Config | None = None) -> TradeContext:
    """Create a TradeContext for account/position/order operations."""
    if config is None:
        config = get_config()
    return TradeContext(config)


def get_quote_context(config: Config | None = None) -> QuoteContext:
    """Create a QuoteContext for market data operations."""
    if config is None:
        config = get_config()
    return QuoteContext(config)
