"""Rebalancing engine: weight-based allocation + ATR volatility bands."""

from __future__ import annotations

import logging
import math
from decimal import Decimal

import pandas as pd

from portfolio_manager.core.types import AtrBand, PortfolioConfig, PositionRow, RebalanceSuggestion

logger = logging.getLogger(__name__)


def _calculate_atr(df: pd.DataFrame, period: int) -> Decimal | None:
    """Calculate Average True Range from OHLCV DataFrame.

    ATR = SMA of True Range over `period` days.
    True Range = max(high - low, abs(high - prev_close), abs(low - prev_close))

    Returns None if insufficient data (< period rows).
    """
    if len(df) < period:
        return None

    # Use period+1 rows when available: row 0 is the seed (provides prev_close only).
    # Falls back to current behavior when only `period` rows exist.
    if len(df) >= period + 1:
        data = df.tail(period + 1).copy()
        has_seed = True
    else:
        data = df.tail(period).copy()
        has_seed = False

    true_ranges: list[Decimal] = []
    prev_close: Decimal | None = None

    for i, (_, row) in enumerate(data.iterrows()):
        high = Decimal(str(row["high"]))
        low = Decimal(str(row["low"]))

        if i == 0 and has_seed:
            prev_close = Decimal(str(row["close"]))
            continue  # seed row — don't compute TR

        if prev_close is not None:
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
        else:
            tr = high - low

        true_ranges.append(tr)
        prev_close = Decimal(str(row["close"]))

    if not true_ranges:
        return None

    return sum(true_ranges) / Decimal(str(len(true_ranges)))


def weight_based_rebalance(
    positions: list[PositionRow],
    total_nav: Decimal,
    available_cash: Decimal,
    config: PortfolioConfig,
) -> list[RebalanceSuggestion]:
    """Generate weight-based rebalancing suggestions.

    Only suggests trades when abs(drift) > config.rebalance_threshold.

    Sizing constraints:
    - BUY: suggested_value <= available_cash (floor by cash)
    - SELL: suggested_quantity <= available_quantity (cap by sellable shares)
    - Quantities rounded to whole shares (floor for BUY, ceil for SELL)
    """
    suggestions: list[RebalanceSuggestion] = []

    if total_nav == 0:
        return suggestions

    for pos in positions:
        target_weight = config.target_allocations.get(pos.symbol)
        if target_weight is None:
            continue

        current_weight = pos.market_value / total_nav
        drift = current_weight - target_weight

        if abs(drift) <= config.rebalance_threshold:
            continue

        target_value = target_weight * total_nav
        delta_value = target_value - pos.market_value

        if pos.last_price == 0:
            continue

        delta_shares = delta_value / pos.last_price

        if delta_value > 0:
            # Need to BUY
            # Cap by available cash
            max_buy_value = min(abs(delta_value), available_cash)
            max_buy_shares = int(max_buy_value / pos.last_price)  # floor

            if max_buy_shares <= 0:
                continue

            actual_value = Decimal(str(max_buy_shares)) * pos.last_price

            suggestions.append(
                RebalanceSuggestion(
                    symbol=pos.symbol,
                    name=pos.name,
                    action="BUY",
                    reason="weight",
                    current_weight=current_weight,
                    target_weight=target_weight,
                    current_price=pos.last_price,
                    suggested_quantity=max_buy_shares,
                    suggested_value=actual_value,
                    detail=(
                        f"Underweight by {abs(drift) * 100:.1f}%. "
                        f"BUY {max_buy_shares} shares (~${actual_value:,.0f})"
                    ),
                )
            )
        else:
            # Need to SELL
            sell_shares_needed = int(math.ceil(abs(delta_shares)))
            # Cap by available quantity (can't sell frozen/settling shares)
            # In the PositionRow, quantity is total, but we should respect
            # available_quantity if known. For now, cap by total quantity.
            max_sell = min(sell_shares_needed, int(pos.quantity))

            if max_sell <= 0:
                continue

            actual_value = Decimal(str(max_sell)) * pos.last_price

            suggestions.append(
                RebalanceSuggestion(
                    symbol=pos.symbol,
                    name=pos.name,
                    action="SELL",
                    reason="weight",
                    current_weight=current_weight,
                    target_weight=target_weight,
                    current_price=pos.last_price,
                    suggested_quantity=max_sell,
                    suggested_value=actual_value,
                    detail=(
                        f"Overweight by {abs(drift) * 100:.1f}%. "
                        f"SELL {max_sell} shares (~${actual_value:,.0f})"
                    ),
                )
            )

    return suggestions


def atr_based_rebalance(
    positions: list[PositionRow],
    candlestick_data: dict[str, pd.DataFrame],
    config: PortfolioConfig,
) -> list[AtrBand]:
    """Calculate ATR volatility bands for all positions.

    Signal classification:
    - breach_upper: price > upper_band
    - near_upper: price > upper_band - 0.5 * ATR
    - breach_lower: price < lower_band
    - near_lower: price < lower_band + 0.5 * ATR
    - in_range: otherwise

    Skips positions with < atr_period days of OHLCV data.
    """
    bands: list[AtrBand] = []

    for pos in positions:
        df = candlestick_data.get(pos.symbol)
        if df is None or df.empty:
            logger.debug("No candlestick data for %s, skipping ATR", pos.symbol)
            continue

        atr = _calculate_atr(df, config.atr_period)
        if atr is None:
            logger.debug("Insufficient data for %s ATR (need %d days)", pos.symbol, config.atr_period)
            continue

        lower_band = pos.cost_price - config.atr_multiplier * atr
        upper_band = pos.cost_price + config.atr_multiplier * atr

        # Classify signal
        price = pos.last_price
        half_atr = Decimal("0.5") * atr

        if price > upper_band:
            signal = "breach_upper"
        elif price > upper_band - half_atr:
            signal = "near_upper"
        elif price < lower_band:
            signal = "breach_lower"
        elif price < lower_band + half_atr:
            signal = "near_lower"
        else:
            signal = "in_range"

        bands.append(
            AtrBand(
                symbol=pos.symbol,
                name=pos.name,
                current_price=price,
                cost_price=pos.cost_price,
                atr=atr,
                lower_band=lower_band,
                upper_band=upper_band,
                signal=signal,
            )
        )

    return bands
