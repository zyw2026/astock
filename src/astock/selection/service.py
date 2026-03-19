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
from astock.validation.service import build_feature_frame, fetch_active_symbols


def active_logics_for_regime(regime: MarketRegime) -> list[LogicSpec]:
    registry = build_default_registry()
    return registry.by_regime(regime)


def approved_logic_cutoff() -> float:
    return settings.reliability_threshold


def run_daily_selection(
    *,
    trade_date: date | None = None,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    selection_limit: int | None = None,
) -> dict:
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
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
    resolved_trade_date = date.fromisoformat(anchor_trade_date)
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
    selected_rows: list[dict] = []
    by_symbol: dict[str, dict] = {}
    for logic_id, reliability_score in approved_score_map.items():
        logic = registry.get(logic_id)
        logic_hits = execute_logic(trade_frame, logic)
        if logic_hits.is_empty():
            continue
        logic_hits = logic_hits.sort("trigger_score", descending=True).head(logic.max_candidates_per_day)
        for row in logic_hits.to_dicts():
            current = by_symbol.get(row["symbol"])
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
            current_total_score = (current.get("trigger_score") or -1e9) + (current.get("reliability_score") or 0.0) * 0.1 if current else -1e9
            candidate_total_score = (candidate["trigger_score"] or -1e9) + reliability_score * 0.1
            if current is None or candidate_total_score > current_total_score:
                by_symbol[row["symbol"]] = candidate

    ranked = sorted(
        by_symbol.values(),
        key=lambda item: ((item.get("trigger_score") or -1e9) + (item.get("reliability_score") or 0.0) * 0.1),
        reverse=True,
    )
    limit = selection_limit or settings.default_selection_limit
    ranked = ranked[:limit]
    for idx, row in enumerate(ranked, start=1):
        row["selection_rank"] = idx
        selected_rows.append(row)

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
