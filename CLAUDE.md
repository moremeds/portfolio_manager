# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Portfolio manager connecting to the **LongPort OpenAPI** for position tracking, PnL analysis, portfolio economics, and daily reporting. USD-only (US stocks). No persistence — reconstructs historical state from trade history each run.

- **LongPort Python SDK**: `longport` package — [SDK source](https://github.com/longportapp/openapi/tree/master/python), [Docs](https://open.longbridge.com/docs)
- **Core SDK classes**: `Config`, `TradeContext` (positions/orders/account), `QuoteContext` (market data/quotes)
- **Auth**: Three env vars loaded via `Config.from_env()`: `LONGPORT_APP_KEY`, `LONGPORT_APP_SECRET`, `LONGPORT_ACCESS_TOKEN`

## Commands

```bash
# Install (editable, with dev deps) — required before running tests due to src layout
pip install -e ".[dev]"

# Run all tests
pytest

# Run a single test file / specific test
pytest tests/test_ledger.py
pytest tests/test_performance.py::test_twr_no_cash_flows

# Lint, format, type check
ruff check src/ tests/
ruff format src/ tests/
mypy src/portfolio_manager/

# Run the pipeline
python -m portfolio_manager.main --no-email           # Full pipeline, save HTML locally & open in browser
python -m portfolio_manager.main                      # Full pipeline with email
python -m portfolio_manager.main --config path/to/config.yml --verbose
```

## Project Structure (src layout)

This project uses the [Python src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/). Source code lives under `src/` — you must `pip install -e .` before imports work.

```
src/portfolio_manager/
├── __init__.py
├── __main__.py                    # python -m portfolio_manager
├── main.py                        # Orchestrator + CLI entry point
├── core/
│   ├── __init__.py
│   ├── types.py                   # Shared dataclasses (PositionRow, LedgerEvent, etc.)
│   └── config.py                  # YAML config loader with env var resolution
├── api/
│   ├── __init__.py
│   ├── client.py                  # LongPort API client factory (Config, TradeContext, QuoteContext)
│   ├── positions.py               # Current position + account balance queries
│   └── market_data.py             # Quotes, candlesticks, trade history, cash flows (with pagination)
├── analytics/
│   ├── __init__.py
│   ├── ledger.py                  # Deterministic event ledger + forward portfolio state replay
│   ├── performance.py             # TWR (portfolio) + price return (per-stock)
│   └── rebalancing.py             # Weight-based + ATR-based suggestions
└── reporting/
    ├── __init__.py
    ├── renderer.py                # Jinja2 HTML report rendering
    ├── email_sender.py            # SMTP delivery
    └── templates/
        └── report.html.j2         # Email report template

config/portfolio.yml               # Portfolio configuration (targets, thresholds, email)
.github/workflows/daily_report.yml # GitHub Actions daily cron job
tests/                              # All tests (mocked, no API calls)
```

## Architecture

- **`api/client.py`** is the single entry point for API authentication. All modules use `get_trade_context()` or `get_quote_context()` from here — never construct `Config`/contexts directly elsewhere.
- Functions accept an optional context parameter for dependency injection (testability), falling back to env-var-based creation.
- **Forward ledger replay**: `analytics/ledger.py` builds a chronological event log from trade history + cash flows, then replays it forward to reconstruct portfolio state at any historical date. No database needed.
- **TWR vs Price Return**: Portfolio-level metrics use Time-Weighted Return (eliminates deposit/withdrawal effects). Per-stock metrics use simple price return.
- **NoAdjust prices**: All historical candlestick data uses `AdjustType.NoAdjust` for consistency with actual trade quantities.
- **Lazy email validation**: Email config fields are `None` if env vars are missing. Only `PortfolioConfig.validate_email_config()` raises — called in `email_sender.py`, not at load time. This lets `--no-email` work without SMTP secrets.

## Key Data Flow

```
main.py orchestrates:
  1. load_config()                    → PortfolioConfig
  2. get_stock_positions() + quotes   → list[PositionRow]
  3. get_trade_history() (paginated)  → DataFrame → build_ledger() → list[LedgerEvent]
  4. get_cash_flows() (paginated)     → merged into ledger
  5. replay_to_date() + get_nav()     → NAV verification
  6. calculate_portfolio_performance()→ PortfolioPerformance (TWR + per-stock)
  7. weight_based_rebalance()         → list[RebalanceSuggestion]
  8. atr_based_rebalance()            → list[AtrBand]
  9. render_report()                  → HTML string
  10. send_report()                   → SMTP delivery
```

## LongPort API Patterns

- `TradeContext` methods: `stock_positions()`, `fund_positions()`, `account_balance()`, `cash_flow()`, `history_orders()`, `submit_order()`
- `QuoteContext` methods: `quote(symbols)`, `history_candlesticks_by_date()`, `trading_days()`, `subscribe()`
- Symbol format: `{ticker}.{market}` — e.g., `AAPL.US`, `MSFT.US`, `NVDA.US`
- Rate limits: Quote API max 10 calls/sec (5 concurrent), Trade API max 30 calls/30sec, Candlestick 60 req/30sec
- Pagination: `history_orders` uses cursor-based (narrow end_at + dedup by order_id), `cash_flow` uses page-based
- All timestamps in UTC

## Conventions

- Python 3.11+, type hints required (`mypy --strict`)
- Line length: 120 chars
- Use `Decimal` for all monetary/price values (never `float`)
- Use `pandas` DataFrames only inside API/ledger adapters — analytics functions accept/return typed dataclasses
- Frozen dataclasses (`frozen=True`) for all immutable domain types
- Copy `.env.example` to `.env` for local credentials
- Config values prefixed with `$` resolve from environment variables
