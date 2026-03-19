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
