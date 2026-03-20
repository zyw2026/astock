from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from math import fabs
from uuid import uuid4

import polars as pl

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.logic_pool.defaults import build_default_registry
from astock.logic_pool.executor import enrich_feature_frame, execute_logic
from astock.logic_pool.models import LogicSpec
from astock.selection.regime import classify_historical_regime
from astock.storage.duckdb import DuckDbStorage
from astock.validation.models import LogicReliabilitySnapshot, LogicSignalHit, LogicValidationResult


LOOKBACK_CALENDAR_DAYS = 40
FUTURE_BUFFER_CALENDAR_DAYS = 10


def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    return [items[idx : idx + chunk_size] for idx in range(0, len(items), chunk_size)]


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _date_str(value: date) -> str:
    return value.isoformat()


def _effective_chunk_size(*, requested_chunk_size: int, days: int) -> int:
    max_rows = max(settings.api_max_rows_per_request, 1)
    safe_chunk = max(max_rows // max(days, 1), 1)
    return max(1, min(requested_chunk_size, safe_chunk))


def fetch_active_symbols(client: AksMcpRestClient, *, symbol_limit: int | None = None) -> list[str]:
    rows = client.paginate_stock_list(listed_status="active", limit=100)
    symbols = [row["symbol"] for row in rows]
    return symbols[:symbol_limit] if symbol_limit else symbols


def fetch_trade_dates(client: AksMcpRestClient, *, start_date: date, end_date: date) -> list[date]:
    rows = client.paginate_trade_calendar(
        start_date=_date_str(start_date),
        end_date=_date_str(end_date),
        is_open=True,
        limit=200,
    )
    return [_as_date(row["trade_date"]) for row in rows]


def fetch_market_regime_map(client: AksMcpRestClient, *, start_date: date, end_date: date) -> dict[date, str]:
    regime_map: dict[date, str] = {}
    cursor_start = start_date
    while cursor_start <= end_date:
        cursor_end = min(cursor_start + timedelta(days=180), end_date)
        try:
            payload = client.market_fund_flow(
                start_date=_date_str(cursor_start),
                end_date=_date_str(cursor_end),
                limit=settings.api_max_rows_per_request,
            )
        except RuntimeError as exc:
            if "DATA_NOT_READY" in str(exc):
                cursor_start = cursor_end + timedelta(days=1)
                continue
            raise
        for row in payload.get("rows", []):
            trade_date = _as_date(row["trade_date"])
            regime_map[trade_date] = classify_historical_regime(row.get("main_net_inflow"))
        cursor_start = cursor_end + timedelta(days=1)
    return regime_map


def derive_feature_regime_map(frame: pl.DataFrame, *, trade_dates: list[date]) -> dict[date, str]:
    if frame.is_empty():
        return {}
    date_set = {_date_str(item) for item in trade_dates}
    filtered = frame.filter(pl.col("trade_date").cast(pl.Utf8).is_in(list(date_set)))
    if filtered.is_empty():
        return {}
    summary = (
        filtered.group_by("trade_date")
        .agg(
            [
                pl.col("ret_1d").mean().alias("avg_ret_1d"),
                pl.col("ret_5d").mean().alias("avg_ret_5d"),
                (pl.col("ret_1d") >= 2).mean().alias("strong_rate"),
                (pl.col("close") > pl.col("ma5")).mean().alias("above_ma5_rate"),
            ]
        )
        .sort("trade_date")
    )
    regime_map: dict[date, str] = {}
    for row in summary.to_dicts():
        trade_date = _as_date(row["trade_date"])
        avg_ret_1d = float(row.get("avg_ret_1d") or 0.0)
        avg_ret_5d = float(row.get("avg_ret_5d") or 0.0)
        strong_rate = float(row.get("strong_rate") or 0.0)
        above_ma5_rate = float(row.get("above_ma5_rate") or 0.0)
        if avg_ret_1d <= -1.5 and strong_rate < 0.08 and above_ma5_rate < 0.35:
            regime = "panic"
        elif avg_ret_1d <= -0.3 or avg_ret_5d <= -1.5 or above_ma5_rate < 0.5:
            regime = "weak_rotation"
        elif avg_ret_1d >= 0.5 and strong_rate >= 0.18 and above_ma5_rate >= 0.58:
            regime = "trend"
        else:
            regime = "rotation"
        regime_map[trade_date] = regime
    return regime_map


def _fetch_hist_frame(
    client: AksMcpRestClient,
    symbols: list[str],
    *,
    start_date: date,
    end_date: date,
    chunk_size: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    days = max((end_date - start_date).days + 5, 30)
    effective_chunk_size = _effective_chunk_size(requested_chunk_size=chunk_size, days=days)
    for chunk in _chunked(symbols, effective_chunk_size):
        payload = client.stock_hist(
            symbol=chunk,
            start_date=_date_str(start_date),
            end_date=_date_str(end_date),
            fields=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "turnover"],
            limit=len(chunk) * days,
        )
        frame = client.rows_frame(payload)
        if not frame.is_empty():
            frames.append(frame)
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()


def _fetch_indicator_frame(
    client: AksMcpRestClient,
    symbols: list[str],
    *,
    start_date: date,
    end_date: date,
    chunk_size: int,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    days = max((end_date - start_date).days + 5, 30)
    fields = ["symbol", "trade_date", "ma5", "ma10", "macd_dif", "macd_dea", "macd_hist", "boll_mid", "boll_up", "boll_low"]
    effective_chunk_size = _effective_chunk_size(requested_chunk_size=chunk_size, days=days)
    for chunk in _chunked(symbols, effective_chunk_size):
        payload = client.stock_indicators(
            symbol=chunk,
            start_date=_date_str(start_date),
            end_date=_date_str(end_date),
            fields=fields,
            limit=len(chunk) * days,
        )
        frame = client.rows_frame(payload)
        if not frame.is_empty():
            frames.append(frame)
    return pl.concat(frames, how="vertical") if frames else pl.DataFrame()


def build_feature_frame(
    client: AksMcpRestClient,
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
    chunk_size: int,
) -> pl.DataFrame:
    hist = _fetch_hist_frame(client, symbols, start_date=start_date, end_date=end_date, chunk_size=chunk_size)
    if hist.is_empty():
        return hist
    indicators = _fetch_indicator_frame(client, symbols, start_date=start_date, end_date=end_date, chunk_size=chunk_size)
    if indicators.is_empty():
        merged = hist.with_columns(
            [
                pl.lit(None).cast(pl.Float64).alias("ma5"),
                pl.lit(None).cast(pl.Float64).alias("ma10"),
                pl.lit(None).cast(pl.Float64).alias("macd_dif"),
                pl.lit(None).cast(pl.Float64).alias("macd_dea"),
                pl.lit(None).cast(pl.Float64).alias("macd_hist"),
                pl.lit(None).cast(pl.Float64).alias("boll_mid"),
                pl.lit(None).cast(pl.Float64).alias("boll_up"),
                pl.lit(None).cast(pl.Float64).alias("boll_low"),
            ]
        )
    else:
        merged = hist.join(indicators, on=["symbol", "trade_date"], how="left")
    merged = merged.with_columns(pl.col("trade_date").cast(pl.Date))
    return enrich_feature_frame(merged)


def _limit_hits_per_day(frame: pl.DataFrame, *, per_day: int) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    ranked = frame.sort(["trade_date", "trigger_score"], descending=[False, True]).with_columns(
        pl.int_range(0, pl.len()).over("trade_date").alias("_rank_in_day")
    )
    return ranked.filter(pl.col("_rank_in_day") < per_day).drop("_rank_in_day")


def _reliability_score(
    sample_count: int,
    hit_rate_3d: float,
    big_move_rate_3d: float,
    avg_max_return_3d: float,
    profit_drawdown_ratio: float,
    avg_drawdown_3d: float,
) -> float:
    sample_component = min(sample_count / 30.0, 1.0) * 20.0
    hit_component = max(min(hit_rate_3d, 1.0), 0.0) * 20.0
    big_move_component = max(min(big_move_rate_3d / 0.3, 1.0), 0.0) * 25.0
    burst_component = max(min(avg_max_return_3d / 6.0, 1.0), 0.0) * 20.0
    ratio_component = max(min(profit_drawdown_ratio / 1.8, 1.0), 0.0) * 10.0
    drawdown_component = max(min((8.0 + avg_drawdown_3d) / 8.0, 1.0), 0.0) * 5.0
    return round(sample_component + hit_component + big_move_component + burst_component + ratio_component + drawdown_component, 2)


def _aggregate_validation_results(hits: list[LogicSignalHit]) -> tuple[list[LogicValidationResult], list[LogicReliabilitySnapshot]]:
    grouped: dict[tuple[str, str], list[LogicSignalHit]] = defaultdict(list)
    for hit in hits:
        grouped[(hit.logic_id, hit.regime)].append(hit)

    results: list[LogicValidationResult] = []
    snapshots: list[LogicReliabilitySnapshot] = []
    for (logic_id, regime), group in sorted(grouped.items()):
        sample_count = len(group)
        positive_1d = [item for item in group if (item.next_1d_return or 0.0) > 0]
        positive_3d = [item for item in group if (item.next_3d_return or 0.0) > 0]
        big_moves = [item for item in group if (item.next_3d_max_return or 0.0) >= 5.0]
        avg_1d = sum((item.next_1d_return or 0.0) for item in group) / sample_count
        avg_2d = sum((item.next_2d_return or 0.0) for item in group) / sample_count
        avg_3d = sum((item.next_3d_return or 0.0) for item in group) / sample_count
        avg_max_3d = sum((item.next_3d_max_return or 0.0) for item in group) / sample_count
        avg_5d = sum((item.next_5d_return or 0.0) for item in group) / sample_count
        avg_drawdown_3d = sum((item.max_drawdown_3d or 0.0) for item in group) / sample_count
        worst_drawdown = min((item.max_drawdown or 0.0) for item in group)
        hit_rate_1d = len(positive_1d) / sample_count if sample_count else 0.0
        hit_rate_3d = len(positive_3d) / sample_count if sample_count else 0.0
        big_move_rate_3d = len(big_moves) / sample_count if sample_count else 0.0
        profit_drawdown_ratio = (avg_max_3d / fabs(avg_drawdown_3d)) if avg_drawdown_3d < 0 else avg_max_3d
        score = _reliability_score(
            sample_count,
            hit_rate_3d,
            big_move_rate_3d,
            avg_max_3d,
            profit_drawdown_ratio,
            avg_drawdown_3d,
        )
        approved = (
            sample_count >= settings.validation_min_sample_count
            and score >= settings.reliability_threshold
            and big_move_rate_3d >= 0.18
            and avg_max_3d >= 3.0
            and avg_drawdown_3d >= -6.0
        )
        results.append(
            LogicValidationResult(
                logic_id=logic_id,
                regime=regime,
                sample_count=sample_count,
                hit_rate_1d=round(hit_rate_1d, 4),
                hit_rate_3d=round(hit_rate_3d, 4),
                big_move_rate_3d=round(big_move_rate_3d, 4),
                avg_return_1d=round(avg_1d, 4),
                avg_return_2d=round(avg_2d, 4),
                avg_return_3d=round(avg_3d, 4),
                avg_max_return_3d=round(avg_max_3d, 4),
                avg_return_5d=round(avg_5d, 4),
                profit_drawdown_ratio=round(profit_drawdown_ratio, 4),
                max_drawdown_3d=round(avg_drawdown_3d, 4),
                max_drawdown=round(worst_drawdown, 4),
                reliability_score=score,
            )
        )
        snapshots.append(
            LogicReliabilitySnapshot(
                logic_id=logic_id,
                regime=regime,
                reliability_score=score,
                approved=approved,
                sample_count=sample_count,
            )
        )
    return results, snapshots


def run_validation(
    *,
    start_date: date,
    end_date: date,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
) -> dict:
    chunk_size = chunk_size or settings.default_chunk_size
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    registry = build_default_registry()
    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit or settings.default_symbol_limit)
    feature_start = start_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    feature_end = end_date + timedelta(days=FUTURE_BUFFER_CALENDAR_DAYS)
    frame = build_feature_frame(client, symbols=symbols, start_date=feature_start, end_date=feature_end, chunk_size=chunk_size)
    if frame.is_empty():
        return {"signal_hit_count": 0, "validation_result_count": 0, "snapshot_count": 0}

    validation_dates = fetch_trade_dates(client, start_date=start_date, end_date=end_date)
    date_filter = {_date_str(item) for item in validation_dates}
    candidate_frame = frame.filter(pl.col("trade_date").cast(pl.Utf8).is_in(list(date_filter)))
    regime_map = fetch_market_regime_map(client, start_date=start_date, end_date=end_date)
    fallback_regime_map = derive_feature_regime_map(candidate_frame, trade_dates=validation_dates)
    for trade_date, regime in fallback_regime_map.items():
        regime_map.setdefault(trade_date, regime)

    all_hits: list[LogicSignalHit] = []
    for logic in registry.all():
        hits_frame = execute_logic(candidate_frame, logic)
        if hits_frame.is_empty():
            continue
        hits_frame = hits_frame.with_columns(pl.col("trade_date").cast(pl.Utf8))
        hits_frame = _limit_hits_per_day(hits_frame, per_day=logic.max_candidates_per_day)
        for row in hits_frame.to_dicts():
            trade_date = _as_date(row["trade_date"])
            regime = regime_map.get(trade_date, "rotation")
            if regime not in logic.regime_whitelist:
                continue
            all_hits.append(
                LogicSignalHit(
                    logic_id=logic.logic_id,
                    trade_date=_date_str(trade_date),
                    symbol=row["symbol"],
                    regime=regime,
                    trigger_score=float(row["trigger_score"]) if row["trigger_score"] is not None else None,
                    trigger_reason=row["trigger_reason"],
                    entry_price=float(row["entry_price"]) if row["entry_price"] is not None else None,
                    next_1d_return=float(row["next_1d_return"]) if row["next_1d_return"] is not None else None,
                    next_2d_return=float(row["next_2d_return"]) if row["next_2d_return"] is not None else None,
                    next_3d_return=float(row["next_3d_return"]) if row["next_3d_return"] is not None else None,
                    next_3d_max_return=float(row["next_3d_max_return"]) if row["next_3d_max_return"] is not None else None,
                    next_5d_return=float(row["next_5d_return"]) if row["next_5d_return"] is not None else None,
                    max_drawdown_3d=float(row["max_drawdown_3d"]) if row["max_drawdown_3d"] is not None else None,
                    max_drawdown=float(row["max_drawdown"]) if row["max_drawdown"] is not None else None,
                )
            )

    validation_id = uuid4().hex
    snapshot_id = uuid4().hex
    signal_run_id = uuid4().hex
    signal_hit_count = storage.insert_signal_hits(all_hits, run_id=signal_run_id)
    results, snapshots = _aggregate_validation_results(all_hits)
    validation_result_count = storage.insert_validation_results(
        results,
        validation_id=validation_id,
        window_start=_date_str(start_date),
        window_end=_date_str(end_date),
    )
    snapshot_count = storage.replace_reliability_snapshot(snapshots, snapshot_id=snapshot_id)
    return {
        "signal_hit_count": signal_hit_count,
        "validation_result_count": validation_result_count,
        "snapshot_count": snapshot_count,
        "validation_id": validation_id,
        "snapshot_id": snapshot_id,
        "symbol_count": len(symbols),
    }
