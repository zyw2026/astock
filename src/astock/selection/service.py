from __future__ import annotations

from datetime import date, timedelta
from uuid import uuid4

import polars as pl

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.logic_pool.defaults import build_default_registry
from astock.logic_pool.executor import execute_logic
from astock.logic_pool.models import LogicSpec, MarketRegime
from astock.selection.regime import classify_current_regime
from astock.storage.duckdb import DuckDbStorage
from astock.validation.service import (
    _limit_hits_per_day,
    build_feature_frame,
    derive_feature_regime_map,
    fetch_active_symbols,
    fetch_market_regime_map,
    fetch_trade_dates,
)


def active_logics_for_regime(regime: MarketRegime) -> list[LogicSpec]:
    registry = build_default_registry()
    return registry.by_regime(regime)


def approved_logic_cutoff() -> float:
    return settings.reliability_threshold


def _resolve_historical_regime(
    client: AksMcpRestClient,
    *,
    trade_date: date,
    trade_frame: pl.DataFrame,
) -> tuple[MarketRegime, dict]:
    regime_map = fetch_market_regime_map(client, start_date=trade_date, end_date=trade_date)
    has_market_fund_flow = trade_date in regime_map
    if trade_date not in regime_map:
        fallback_map = derive_feature_regime_map(trade_frame, trade_dates=[trade_date])
        regime_map.update(fallback_map)
    regime = regime_map.get(trade_date, "rotation")
    evidence = {"source": "market_fund_flow" if has_market_fund_flow else "fallback_feature_panel"}
    if not trade_frame.is_empty():
        row = (
            trade_frame.select(
                [
                    pl.col("ret_1d").mean().alias("avg_ret_1d"),
                    pl.col("ret_5d").mean().alias("avg_ret_5d"),
                    (pl.col("ret_1d") >= 2).mean().alias("strong_rate"),
                    (pl.col("close") > pl.col("ma5")).mean().alias("above_ma5_rate"),
                ]
            )
            .to_dicts()[0]
        )
        evidence.update(
            {
                "avg_ret_1d": float(row.get("avg_ret_1d") or 0.0),
                "avg_ret_5d": float(row.get("avg_ret_5d") or 0.0),
                "strong_rate": float(row.get("strong_rate") or 0.0),
                "above_ma5_rate": float(row.get("above_ma5_rate") or 0.0),
            }
        )
    return regime, evidence


def _rank_selection_candidates(
    *,
    trade_frame: pl.DataFrame,
    regime: MarketRegime,
    registry,
    approved_score_map: dict[str, float],
    selection_limit: int,
    include_forward_metrics: bool = False,
) -> list[dict]:
    by_symbol: dict[str, dict] = {}
    for logic_id, reliability_score in approved_score_map.items():
        logic = registry.get(logic_id)
        logic_hits = execute_logic(trade_frame, logic)
        if logic_hits.is_empty():
            continue
        logic_hits = _limit_hits_per_day(logic_hits, per_day=logic.max_candidates_per_day)
        logic_hits = logic_hits.sort("trigger_score", descending=True).head(logic.max_candidates_per_day)
        for row in logic_hits.to_dicts():
            candidate = {
                "logic_id": logic.logic_id,
                "logic_name": logic.name,
                "holding_days": logic.holding_days,
                "symbol": row["symbol"],
                "regime": regime,
                "trigger_score": float(row["trigger_score"]) if row["trigger_score"] is not None else None,
                "reliability_score": reliability_score,
                "selection_reason": row["trigger_reason"],
                "invalidation_level": float(abs(row["max_drawdown"])) if row["max_drawdown"] is not None else None,
            }
            if include_forward_metrics:
                candidate.update(
                    {
                        "entry_price": float(row["entry_price"]) if row["entry_price"] is not None else None,
                        "next_1d_return": float(row["next_1d_return"]) if row["next_1d_return"] is not None else None,
                        "next_2d_return": float(row["next_2d_return"]) if row["next_2d_return"] is not None else None,
                        "next_3d_return": float(row["next_3d_return"]) if row["next_3d_return"] is not None else None,
                        "next_1d_max_return": float(row["next_1d_max_return"]) if row["next_1d_max_return"] is not None else None,
                        "next_2d_max_return": float(row["next_2d_max_return"]) if row["next_2d_max_return"] is not None else None,
                        "max_drawdown_1d": float(row["max_drawdown_1d"]) if row["max_drawdown_1d"] is not None else None,
                        "max_drawdown_2d": float(row["max_drawdown_2d"]) if row["max_drawdown_2d"] is not None else None,
                        "next_3d_max_return": float(row["next_3d_max_return"]) if row["next_3d_max_return"] is not None else None,
                        "max_drawdown_3d": float(row["max_drawdown_3d"]) if row["max_drawdown_3d"] is not None else None,
                    }
                )
            current = by_symbol.get(row["symbol"])
            current_total_score = (
                (current.get("trigger_score") or -1e9) + (current.get("reliability_score") or 0.0) * 0.1 if current else -1e9
            )
            candidate_total_score = (candidate["trigger_score"] or -1e9) + reliability_score * 0.1
            if current is None or candidate_total_score > current_total_score:
                by_symbol[row["symbol"]] = candidate

    ranked = sorted(
        by_symbol.values(),
        key=lambda item: ((item.get("trigger_score") or -1e9) + (item.get("reliability_score") or 0.0) * 0.1),
        reverse=True,
    )[:selection_limit]
    for idx, row in enumerate(ranked, start=1):
        row["selection_rank"] = idx
    return ranked


def detect_current_regime(*, trade_date: date | None = None) -> dict:
    client = AksMcpRestClient()
    overview = client.market_overview(top_n=5)
    fund_flow = client.market_fund_flow(limit=5)
    regime, regime_evidence = classify_current_regime(overview, fund_flow)
    anchor_trade_date = (
        trade_date.isoformat()
        if trade_date
        else ((overview.get("rows") or [{}])[0].get("time_context", {}) or {}).get("anchor_trade_date")
    )
    if not anchor_trade_date:
        raise RuntimeError("unable to resolve anchor trade date")
    return {
        "trade_date": anchor_trade_date,
        "regime": regime,
        "regime_evidence": regime_evidence,
    }


def run_daily_selection(
    *,
    trade_date: date | None = None,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    selection_limit: int | None = None,
) -> dict:
    regime_result = detect_current_regime(trade_date=trade_date)
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    regime = regime_result["regime"]
    regime_evidence = regime_result["regime_evidence"]
    resolved_trade_date = date.fromisoformat(regime_result["trade_date"])
    approved = storage.load_latest_reliability_snapshot(regime=regime, approved_only=True)
    if not approved:
        return {
            "trade_date": resolved_trade_date.isoformat(),
            "regime": regime,
            "regime_evidence": regime_evidence,
            "selection_count": 0,
            "warning": "no approved logic for regime; run validation first",
            "rows": [],
        }

    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit or settings.default_symbol_limit)
    frame = build_feature_frame(
        client,
        symbols=symbols,
        start_date=resolved_trade_date - timedelta(days=40),
        end_date=resolved_trade_date + timedelta(days=5),
        chunk_size=chunk_size or settings.default_chunk_size,
    )
    if frame.is_empty():
        return {"regime": regime, "regime_evidence": regime_evidence, "selection_count": 0, "rows": []}
    trade_frame = frame.filter(pl.col("trade_date") == resolved_trade_date)

    registry = build_default_registry()
    allowed_logic_ids = {logic.logic_id for logic in active_logics_for_regime(regime)}
    approved_score_map = {
        row["logic_id"]: float(row["reliability_score"])
        for row in approved
        if row["logic_id"] in allowed_logic_ids
    }
    if not approved_score_map:
        return {
            "trade_date": resolved_trade_date.isoformat(),
            "regime": regime,
            "regime_evidence": regime_evidence,
            "selection_count": 0,
            "warning": "no approved logic matched current regime whitelist",
            "rows": [],
        }
    selected_rows = _rank_selection_candidates(
        trade_frame=trade_frame,
        regime=regime,
        registry=registry,
        approved_score_map=approved_score_map,
        selection_limit=selection_limit or settings.default_selection_limit,
    )

    run_id = uuid4().hex
    selection_count = storage.insert_daily_selection_output(
        selected_rows,
        run_id=run_id,
        run_date=resolved_trade_date.isoformat(),
    )
    return {
        "run_id": run_id,
        "regime": regime,
        "regime_evidence": regime_evidence,
        "selection_count": selection_count,
        "rows": selected_rows,
        "trade_date": resolved_trade_date.isoformat(),
    }


def replay_historical_selection(
    *,
    trade_date: date,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    selection_limit: int | None = None,
    approved_only: bool = True,
) -> dict:
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    resolved_trade_date = trade_date
    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit or settings.default_symbol_limit)
    frame = build_feature_frame(
        client,
        symbols=symbols,
        start_date=resolved_trade_date - timedelta(days=40),
        end_date=resolved_trade_date + timedelta(days=10),
        chunk_size=chunk_size or settings.default_chunk_size,
    )
    if frame.is_empty():
        return {"trade_date": resolved_trade_date.isoformat(), "selection_count": 0, "rows": []}
    trade_frame = frame.filter(pl.col("trade_date") == resolved_trade_date)
    if trade_frame.is_empty():
        return {
            "trade_date": resolved_trade_date.isoformat(),
            "selection_count": 0,
            "warning": "no feature data for trade date",
            "rows": [],
        }

    regime, regime_evidence = _resolve_historical_regime(client, trade_date=resolved_trade_date, trade_frame=trade_frame)
    registry = build_default_registry()
    allowed_logic_ids = {logic.logic_id for logic in active_logics_for_regime(regime)}
    latest_snapshot = {
        row["logic_id"]: float(row["reliability_score"])
        for row in storage.load_latest_reliability_snapshot(regime=regime, approved_only=approved_only)
        if row["logic_id"] in allowed_logic_ids
    }
    if approved_only and not latest_snapshot:
        return {
            "trade_date": resolved_trade_date.isoformat(),
            "regime": regime,
            "regime_evidence": regime_evidence,
            "selection_count": 0,
            "warning": "no approved logic for historical regime; run validation first",
            "rows": [],
        }
    if not approved_only:
        for logic_id in allowed_logic_ids:
            latest_snapshot.setdefault(logic_id, 0.0)

    rows = _rank_selection_candidates(
        trade_frame=trade_frame,
        regime=regime,
        registry=registry,
        approved_score_map=latest_snapshot,
        selection_limit=selection_limit or settings.default_selection_limit,
        include_forward_metrics=True,
    )
    return {
        "trade_date": resolved_trade_date.isoformat(),
        "regime": regime,
        "regime_evidence": regime_evidence,
        "selection_count": len(rows),
        "approved_only": approved_only,
        "rows": rows,
    }


def replay_historical_selection_batch(
    *,
    trade_dates: list[date],
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    selection_limit: int | None = None,
    approved_only: bool = True,
) -> dict:
    if not trade_dates:
        return {"trade_dates": [], "day_results": [], "strategy_stats": [], "rows": []}

    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    sorted_trade_dates = sorted(trade_dates)
    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit or settings.default_symbol_limit)
    frame = build_feature_frame(
        client,
        symbols=symbols,
        start_date=sorted_trade_dates[0] - timedelta(days=40),
        end_date=sorted_trade_dates[-1] + timedelta(days=10),
        chunk_size=chunk_size or settings.default_chunk_size,
    )
    if frame.is_empty():
        return {
            "trade_dates": [item.isoformat() for item in sorted_trade_dates],
            "day_results": [],
            "strategy_stats": [],
            "rows": [],
        }

    regime_map = fetch_market_regime_map(client, start_date=sorted_trade_dates[0], end_date=sorted_trade_dates[-1])
    candidate_frame = frame.filter(pl.col("trade_date").is_in(sorted_trade_dates))
    fallback_regime_map = derive_feature_regime_map(candidate_frame, trade_dates=sorted_trade_dates)
    for trade_date, regime in fallback_regime_map.items():
        regime_map.setdefault(trade_date, regime)

    registry = build_default_registry()
    approved_cache: dict[str, dict[str, float]] = {}
    batch_rows: list[dict] = []
    day_results: list[dict] = []
    for trade_date in sorted_trade_dates:
        trade_frame = frame.filter(pl.col("trade_date") == trade_date)
        if trade_frame.is_empty():
            day_results.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "regime": None,
                    "selection_count": 0,
                    "warning": "no feature data for trade date",
                }
            )
            continue

        regime = regime_map.get(trade_date, "rotation")
        regime_evidence = _resolve_historical_regime(client, trade_date=trade_date, trade_frame=trade_frame)[1]
        allowed_logic_ids = {logic.logic_id for logic in active_logics_for_regime(regime)}
        if regime not in approved_cache:
            approved_cache[regime] = {
                row["logic_id"]: float(row["reliability_score"])
                for row in storage.load_latest_reliability_snapshot(regime=regime, approved_only=approved_only)
                if row["logic_id"] in allowed_logic_ids
            }
            if not approved_only:
                for logic_id in allowed_logic_ids:
                    approved_cache[regime].setdefault(logic_id, 0.0)
        approved_score_map = {logic_id: score for logic_id, score in approved_cache[regime].items() if logic_id in allowed_logic_ids}
        if approved_only and not approved_score_map:
            day_results.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "regime": regime,
                    "selection_count": 0,
                    "warning": "no approved logic for historical regime; run validation first",
                }
            )
            continue

        rows = _rank_selection_candidates(
            trade_frame=trade_frame,
            regime=regime,
            registry=registry,
            approved_score_map=approved_score_map,
            selection_limit=selection_limit or settings.default_selection_limit,
            include_forward_metrics=True,
        )
        day_results.append(
            {
                "trade_date": trade_date.isoformat(),
                "regime": regime,
                "selection_count": len(rows),
                "warning": None,
                "regime_evidence": regime_evidence,
            }
        )
        for row in rows:
            enriched = dict(row)
            enriched["trade_date"] = trade_date.isoformat()
            batch_rows.append(enriched)

    grouped: dict[str, list[dict]] = {}
    for row in batch_rows:
        grouped.setdefault(row["logic_id"], []).append(row)

    strategy_stats: list[dict] = []
    for logic_id, rows in grouped.items():
        sample_count = len(rows)
        hit_1d = sum(1 for row in rows if (row.get("next_1d_return") or 0.0) > 0)
        hit_2d = sum(1 for row in rows if (row.get("next_2d_return") or 0.0) > 0)
        hit_3d = sum(1 for row in rows if (row.get("next_3d_return") or 0.0) > 0)
        big_move_3d = sum(1 for row in rows if (row.get("next_3d_max_return") or 0.0) >= 5.0)
        strategy_stats.append(
            {
                "logic_id": logic_id,
                "logic_name": rows[0].get("logic_name"),
                "sample_count": sample_count,
                "hit_rate_1d": hit_1d / sample_count,
                "hit_rate_2d": hit_2d / sample_count,
                "hit_rate_3d": hit_3d / sample_count,
                "big_move_rate_3d": big_move_3d / sample_count,
                "avg_n1d": sum((row.get("next_1d_return") or 0.0) for row in rows) / sample_count,
                "avg_n2d": sum((row.get("next_2d_return") or 0.0) for row in rows) / sample_count,
                "avg_n3d": sum((row.get("next_3d_return") or 0.0) for row in rows) / sample_count,
                "avg_n3d_max": sum((row.get("next_3d_max_return") or 0.0) for row in rows) / sample_count,
                "avg_n3d_dd": sum((row.get("max_drawdown_3d") or 0.0) for row in rows) / sample_count,
                "avg_trigger_score": sum((row.get("trigger_score") or 0.0) for row in rows) / sample_count,
                "avg_reliability_score": sum((row.get("reliability_score") or 0.0) for row in rows) / sample_count,
            }
        )

    strategy_stats.sort(
        key=lambda item: (
            item["hit_rate_3d"],
            item["big_move_rate_3d"],
            item["avg_n3d_max"],
            item["avg_n3d"],
        ),
        reverse=True,
    )
    return {
        "trade_dates": [item.isoformat() for item in trade_dates],
        "day_results": day_results,
        "strategy_stats": strategy_stats,
        "rows": batch_rows,
    }


def analyze_strategy_with_expanded_signals(
    *,
    logic_id: str,
    min_samples: int = 10,
    lookback_trade_days: int = 40,
    end_date: date | None = None,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    selection_limit: int | None = None,
    approved_only: bool = True,
) -> dict:
    client = AksMcpRestClient()
    resolved_end_date = end_date or date.today()
    calendar_start = resolved_end_date - timedelta(days=max(lookback_trade_days * 2, 90))
    trade_dates = fetch_trade_dates(client, start_date=calendar_start, end_date=resolved_end_date)
    if len(trade_dates) > lookback_trade_days:
        trade_dates = trade_dates[-lookback_trade_days:]

    batch = replay_historical_selection_batch(
        trade_dates=trade_dates,
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        selection_limit=selection_limit,
        approved_only=approved_only,
    )
    target_rows = [row for row in batch["rows"] if row["logic_id"] == logic_id]
    target_day_results: list[dict] = []
    target_trade_dates: list[str] = []
    for day in batch["day_results"]:
        matched = [row for row in target_rows if row["trade_date"] == day["trade_date"]]
        if matched:
            target_day_results.append(
                {
                    "trade_date": day["trade_date"],
                    "regime": day.get("regime"),
                    "selection_count": len(matched),
                    "warning": day.get("warning"),
                }
            )
            target_trade_dates.append(day["trade_date"])

    if not target_rows:
        return {
            "logic_id": logic_id,
            "trade_dates_considered": [item.isoformat() for item in trade_dates],
            "matched_trade_dates": [],
            "sample_count": 0,
            "meets_min_samples": False,
            "strategy_stats": None,
            "day_results": target_day_results,
        }

    sample_count = len(target_rows)
    hit_1d = sum(1 for row in target_rows if (row.get("next_1d_return") or 0.0) > 0)
    hit_2d = sum(1 for row in target_rows if (row.get("next_2d_return") or 0.0) > 0)
    hit_3d = sum(1 for row in target_rows if (row.get("next_3d_return") or 0.0) > 0)
    big_move_3d = sum(1 for row in target_rows if (row.get("next_3d_max_return") or 0.0) >= 5.0)
    strategy_stats = {
        "logic_id": logic_id,
        "logic_name": target_rows[0].get("logic_name"),
        "sample_count": sample_count,
        "hit_rate_1d": hit_1d / sample_count,
        "hit_rate_2d": hit_2d / sample_count,
        "hit_rate_3d": hit_3d / sample_count,
        "big_move_rate_3d": big_move_3d / sample_count,
        "avg_n1d": sum((row.get("next_1d_return") or 0.0) for row in target_rows) / sample_count,
        "avg_n2d": sum((row.get("next_2d_return") or 0.0) for row in target_rows) / sample_count,
        "avg_n3d": sum((row.get("next_3d_return") or 0.0) for row in target_rows) / sample_count,
        "avg_n3d_max": sum((row.get("next_3d_max_return") or 0.0) for row in target_rows) / sample_count,
        "avg_n3d_dd": sum((row.get("max_drawdown_3d") or 0.0) for row in target_rows) / sample_count,
        "avg_trigger_score": sum((row.get("trigger_score") or 0.0) for row in target_rows) / sample_count,
        "avg_reliability_score": sum((row.get("reliability_score") or 0.0) for row in target_rows) / sample_count,
    }
    return {
        "logic_id": logic_id,
        "trade_dates_considered": [item.isoformat() for item in trade_dates],
        "matched_trade_dates": target_trade_dates,
        "sample_count": sample_count,
        "meets_min_samples": sample_count >= min_samples,
        "strategy_stats": strategy_stats,
        "day_results": target_day_results,
    }
