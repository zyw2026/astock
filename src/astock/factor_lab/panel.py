from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from astock.connectors.rest_client import AksMcpRestClient
from astock.validation.service import (
    LOOKBACK_CALENDAR_DAYS,
    FUTURE_BUFFER_CALENDAR_DAYS,
    build_feature_frame,
    derive_feature_regime_map,
    fetch_active_symbols,
    fetch_market_regime_map,
    fetch_trade_dates,
)


def _attach_regime(frame: pl.DataFrame, regime_map: dict[date, str]) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    regime_rows = [{"trade_date": trade_date, "regime": regime} for trade_date, regime in regime_map.items()]
    regime_frame = pl.DataFrame(regime_rows).with_columns(pl.col("trade_date").cast(pl.Date))
    return frame.join(regime_frame, on="trade_date", how="left")


def build_discovery_panel(
    client: AksMcpRestClient,
    *,
    start_date: date,
    end_date: date,
    symbol_limit: int,
    chunk_size: int,
) -> tuple[pl.DataFrame, list[str]]:
    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit)
    feature_frame = build_feature_frame(
        client,
        symbols=symbols,
        start_date=start_date - timedelta(days=LOOKBACK_CALENDAR_DAYS),
        end_date=end_date + timedelta(days=FUTURE_BUFFER_CALENDAR_DAYS),
        chunk_size=chunk_size,
    )
    if feature_frame.is_empty():
        return feature_frame, symbols

    trade_dates = fetch_trade_dates(client, start_date=start_date, end_date=end_date)
    date_filter = [item.isoformat() for item in trade_dates]
    panel = feature_frame.filter(pl.col("trade_date").cast(pl.Utf8).is_in(date_filter))
    regime_map = fetch_market_regime_map(client, start_date=start_date, end_date=end_date)
    fallback_map = derive_feature_regime_map(panel, trade_dates=trade_dates)
    for trade_date, regime in fallback_map.items():
        regime_map.setdefault(trade_date, regime)
    panel = _attach_regime(panel, regime_map)
    panel = panel.with_columns(
        [
            pl.when(pl.col("next_3d_max_return") >= 5.0).then(1).otherwise(0).alias("is_big_move_3d"),
            pl.when(pl.col("next_3d_return") > 0).then(1).otherwise(0).alias("is_positive_3d"),
            pl.when(pl.col("max_drawdown_3d") < 0)
            .then(pl.col("next_3d_max_return") / (-pl.col("max_drawdown_3d")))
            .otherwise(pl.col("next_3d_max_return"))
            .alias("profit_drawdown_ratio_3d"),
        ]
    )
    return panel, symbols

