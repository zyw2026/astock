from __future__ import annotations

from datetime import date
from uuid import uuid4

import polars as pl

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.factor_lab.service import run_discovery
from astock.logic_pool.executor import execute_logic
from astock.logic_pool.models import LogicSpec
from astock.storage.duckdb import DuckDbStorage
from astock.validation.service import (
    _reliability_score,
    build_feature_frame,
    derive_feature_regime_map,
    fetch_active_symbols,
    fetch_market_regime_map,
    fetch_trade_dates,
)


def build_rolling_windows(
    trade_dates: list[date],
    *,
    train_days: int,
    test_days: int,
    follow_days: int,
    step_days: int,
) -> list[dict]:
    windows: list[dict] = []
    total = train_days + test_days + follow_days
    if len(trade_dates) < total:
        return windows
    for start_idx in range(0, len(trade_dates) - total + 1, step_days):
        train = trade_dates[start_idx : start_idx + train_days]
        test = trade_dates[start_idx + train_days : start_idx + train_days + test_days]
        follow = trade_dates[start_idx + train_days + test_days : start_idx + total]
        if not train or not test or not follow:
            continue
        windows.append(
            {
                "window_id": f"w{len(windows)+1:02d}",
                "train_start": train[0],
                "train_end": train[-1],
                "test_start": test[0],
                "test_end": test[-1],
                "follow_start": follow[0],
                "follow_end": follow[-1],
            }
        )
    return windows


def _attach_regime(frame: pl.DataFrame, *, client: AksMcpRestClient, start_date: date, end_date: date) -> pl.DataFrame:
    regime_map = fetch_market_regime_map(client, start_date=start_date, end_date=end_date)
    trade_dates = sorted(frame["trade_date"].unique().to_list()) if not frame.is_empty() else []
    fallback_map = derive_feature_regime_map(frame, trade_dates=trade_dates)
    for trade_date, regime in fallback_map.items():
        regime_map.setdefault(trade_date, regime)
    if not regime_map:
        return frame
    regime_frame = pl.DataFrame(
        [{"trade_date": trade_date, "regime": regime} for trade_date, regime in regime_map.items()]
    ).with_columns(pl.col("trade_date").cast(pl.Date))
    return frame.join(regime_frame, on="trade_date", how="left")


def _topk_metrics(frame: pl.DataFrame, spec: LogicSpec, *, top_k: int) -> dict:
    hits = execute_logic(frame, spec)
    if hits.is_empty():
        return {
            "sample_count": 0,
            "hit_rate_3d": 0.0,
            "big_move_rate_3d": 0.0,
            "avg_n3d": 0.0,
            "avg_n3d_max": 0.0,
            "avg_n3d_dd": 0.0,
            "score": 0.0,
        }
    ranked = (
        hits.sort(["trade_date", "trigger_score"], descending=[False, True])
        .with_columns(pl.int_range(1, pl.len() + 1).over("trade_date").alias("_rank"))
        .filter(pl.col("_rank") <= top_k)
    )
    sample_count = ranked.height
    hit_rate_3d = float((ranked["next_3d_return"] > 0).mean()) if sample_count else 0.0
    big_move_rate_3d = float((ranked["next_3d_max_return"] >= 5.0).mean()) if sample_count else 0.0
    avg_n3d = float(ranked["next_3d_return"].mean()) if sample_count else 0.0
    avg_n3d_max = float(ranked["next_3d_max_return"].mean()) if sample_count else 0.0
    avg_n3d_dd = float(ranked["max_drawdown_3d"].mean()) if sample_count else 0.0
    score = round(
        max(min(hit_rate_3d, 1.0), 0.0) * 30.0
        + max(min(big_move_rate_3d, 1.0), 0.0) * 25.0
        + max(min(avg_n3d, 5.0), -5.0) * 3.0
        + max(min(avg_n3d_max, 8.0), 0.0) * 2.0
        + max(min((4.5 + avg_n3d_dd) / 4.5, 1.0), 0.0) * 10.0,
        2,
    )
    return {
        "sample_count": sample_count,
        "hit_rate_3d": round(hit_rate_3d, 4),
        "big_move_rate_3d": round(big_move_rate_3d, 4),
        "avg_n3d": round(avg_n3d, 4),
        "avg_n3d_max": round(avg_n3d_max, 4),
        "avg_n3d_dd": round(avg_n3d_dd, 4),
        "score": score,
    }


def _follow_validation_metrics(frame: pl.DataFrame, spec: LogicSpec) -> dict:
    hits = execute_logic(frame, spec)
    if hits.is_empty():
        return {"score": 0.0, "approved": False}
    sample_count = hits.height
    hit_rate_3d = float((hits["next_3d_return"] > 0).mean())
    big_move_rate_3d = float((hits["next_3d_max_return"] >= 5.0).mean())
    avg_max_3d = float(hits["next_3d_max_return"].mean())
    avg_drawdown_3d = float(hits["max_drawdown_3d"].mean())
    profit_drawdown_ratio = (avg_max_3d / abs(avg_drawdown_3d)) if avg_drawdown_3d < 0 else avg_max_3d
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
    return {"score": score, "approved": approved}


def _load_candidate_specs(storage: DuckDbStorage, discovery_run_id: str) -> list[dict]:
    rows = storage.list_discovered_candidates(limit=200)
    return [row for row in rows if row["discovery_run_id"] == discovery_run_id]


def run_discovery_stability_eval(
    *,
    start_date: date,
    end_date: date,
    regimes: list[str],
    train_days: int,
    test_days: int,
    follow_days: int,
    step_days: int,
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    candidate_limit: int | None = None,
) -> dict:
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    trade_dates = fetch_trade_dates(client, start_date=start_date, end_date=end_date)
    windows = build_rolling_windows(
        trade_dates,
        train_days=train_days,
        test_days=test_days,
        follow_days=follow_days,
        step_days=step_days,
    )
    eval_run_id = uuid4().hex
    storage.insert_discovery_eval_run(
        eval_run_id=eval_run_id,
        train_days=train_days,
        test_days=test_days,
        follow_days=follow_days,
        step_days=step_days,
        regimes=regimes,
    )

    symbols = fetch_active_symbols(client, symbol_limit=symbol_limit or settings.default_symbol_limit)
    window_rows: list[dict] = []
    candidate_rows: list[dict] = []

    for window in windows:
        discovery_result = run_discovery(
            start_date=window["train_start"],
            end_date=window["train_end"],
            regimes=regimes,
            symbol_limit=symbol_limit,
            chunk_size=chunk_size,
            candidate_limit=candidate_limit,
        )
        candidates = [
            row
            for row in discovery_result["rows"]
            if row.get("approved_for_validation") and row.get("replay_quality_passed")
        ]

        follow_frame = build_feature_frame(
            client,
            symbols=symbols,
            start_date=window["test_start"],
            end_date=window["follow_end"],
            chunk_size=chunk_size or settings.default_chunk_size,
        )
        follow_frame = _attach_regime(
            follow_frame,
            client=client,
            start_date=window["test_start"],
            end_date=window["follow_end"],
        )

        stable_count = 0
        for row in candidates:
            spec = LogicSpec.model_validate_json(row["spec_json"])
            test_frame = follow_frame.filter(
                (pl.col("trade_date") >= window["test_start"])
                & (pl.col("trade_date") <= window["test_end"])
                & (pl.col("regime") == row["regime"])
            )
            follow_only_frame = follow_frame.filter(
                (pl.col("trade_date") >= window["follow_start"])
                & (pl.col("trade_date") <= window["follow_end"])
                & (pl.col("regime") == row["regime"])
            )
            top3 = _topk_metrics(test_frame, spec, top_k=3)
            top5 = _topk_metrics(test_frame, spec, top_k=5)
            follow_validation = _follow_validation_metrics(follow_only_frame, spec)
            stable_passed = (
                top5["hit_rate_3d"] >= 0.45
                and top5["avg_n3d"] > 0
                and top5["avg_n3d_max"] >= 4.0
                and top5["avg_n3d_dd"] >= -4.0
                and follow_validation["approved"]
            )
            if stable_passed:
                stable_count += 1
            candidate_rows.append(
                {
                    "window_id": window["window_id"],
                    "candidate_id": row["candidate_id"],
                    "logic_id": row["logic_id"],
                    "regime": row["regime"],
                    "discovery_run_id": discovery_result["discovery_run_id"],
                    "train_top3_score": row.get("top3_quality_score"),
                    "train_top5_score": row.get("top5_quality_score"),
                    "test_top3_score": top3["score"],
                    "test_top5_score": top5["score"],
                    "test_hit_3d": top5["hit_rate_3d"],
                    "test_big_move_3d": top5["big_move_rate_3d"],
                    "test_avg_n3d": top5["avg_n3d"],
                    "test_avg_n3d_max": top5["avg_n3d_max"],
                    "test_avg_n3d_dd": top5["avg_n3d_dd"],
                    "follow_validation_score": follow_validation["score"],
                    "follow_validation_approved": follow_validation["approved"],
                    "stable_passed": stable_passed,
                    "status": "stable" if stable_passed else "watch",
                }
            )

        window_status = "stable" if stable_count > 0 else ("watch" if candidates else "fail")
        window_rows.append(
            {
                "window_id": window["window_id"],
                "train_start": window["train_start"].isoformat(),
                "train_end": window["train_end"].isoformat(),
                "test_start": window["test_start"].isoformat(),
                "test_end": window["test_end"].isoformat(),
                "follow_start": window["follow_start"].isoformat(),
                "follow_end": window["follow_end"].isoformat(),
                "candidate_count": discovery_result["candidate_count"],
                "dual_pass_count": len(candidates),
                "stable_candidate_count": stable_count,
                "window_status": window_status,
            }
        )

    storage.insert_discovery_eval_window_results(window_rows, eval_run_id=eval_run_id)
    storage.insert_discovery_eval_candidate_results(candidate_rows, eval_run_id=eval_run_id)
    stable_windows = sum(1 for row in window_rows if row["window_status"] == "stable")
    return {
        "eval_run_id": eval_run_id,
        "window_count": len(window_rows),
        "stable_window_count": stable_windows,
        "window_rows": window_rows,
        "candidate_rows": candidate_rows,
    }
