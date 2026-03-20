from __future__ import annotations

import hashlib
import json
from itertools import combinations

import polars as pl

from astock.app.settings import settings
from astock.factor_lab.models import (
    DiscoveredLogicCandidate,
    FactorBucketStat,
    FactorComboResult,
    ReplayQualityResult,
    RuleVariantResult,
)
from astock.logic_pool.models import LogicSpec


FACTOR_SPECS: tuple[dict, ...] = (
    {"field": "ret_1d", "label": "当日涨幅", "weight": 1.6},
    {"field": "ret_3d", "label": "3日涨幅", "weight": 1.1},
    {"field": "ret_5d", "label": "5日涨幅", "weight": 0.8},
    {"field": "ret_10d", "label": "10日涨幅", "weight": 0.6},
    {"field": "prev_ret_1d", "label": "前日涨幅", "weight": -0.2},
    {"field": "close_vs_ma10_pct", "label": "偏离MA10", "weight": -1.0},
    {"field": "close_vs_ma5_pct", "label": "偏离MA5", "weight": -0.8},
    {"field": "ma5_vs_ma10_pct", "label": "MA5相对MA10", "weight": 0.8},
    {"field": "pullback_from_3d_high_pct", "label": "距3日高点回撤", "weight": -0.7},
    {"field": "pullback_from_5d_high_pct", "label": "距5日高点回撤", "weight": -0.5},
    {"field": "intraday_range_pct", "label": "日内振幅", "weight": -0.4},
    {"field": "body_pct", "label": "实体涨跌幅", "weight": 0.4},
)


def _field_weight(field: str) -> float:
    for spec in FACTOR_SPECS:
        if spec["field"] == field:
            return float(spec["weight"])
    return 0.5


def _quantile_bounds(values: list[float], bucket_count: int = 5) -> list[tuple[float, float]]:
    if not values:
        return []
    sorted_values = sorted(values)
    bounds: list[tuple[float, float]] = []
    for idx in range(bucket_count):
        left_q = idx / bucket_count
        right_q = (idx + 1) / bucket_count
        left = sorted_values[min(int(left_q * (len(sorted_values) - 1)), len(sorted_values) - 1)]
        right = sorted_values[min(int(right_q * (len(sorted_values) - 1)), len(sorted_values) - 1)]
        if not bounds or bounds[-1] != (left, right):
            bounds.append((left, right))
    return bounds


def _window_trade_dates(frame: pl.DataFrame, window_size: int | None) -> list:
    trade_dates = sorted(frame["trade_date"].unique().to_list())
    if not window_size or len(trade_dates) <= window_size:
        return trade_dates
    return trade_dates[-window_size:]


def _window_frame(frame: pl.DataFrame, window_size: int | None) -> pl.DataFrame:
    dates = _window_trade_dates(frame, window_size)
    return frame.filter(pl.col("trade_date").is_in(dates))


def _score_bucket(
    *,
    sample_count: int,
    hit_rate_3d: float,
    big_move_rate_3d: float,
    avg_return_3d: float,
    avg_max_return_3d: float,
    max_drawdown_3d: float,
) -> float:
    sample_component = min(sample_count / 20.0, 1.0) * 20.0
    hit_component = max(min(hit_rate_3d, 1.0), 0.0) * 15.0
    big_move_component = max(min(big_move_rate_3d / 0.35, 1.0), 0.0) * 30.0
    return_component = max(min(avg_return_3d / 3.0, 1.0), -1.0) * 10.0
    burst_component = max(min(avg_max_return_3d / 6.0, 1.0), 0.0) * 20.0
    drawdown_component = max(min((8.0 + max_drawdown_3d) / 8.0, 1.0), 0.0) * 5.0
    return round(sample_component + hit_component + big_move_component + return_component + burst_component + drawdown_component, 2)


def _factor_metrics(frame: pl.DataFrame) -> dict | None:
    if frame.is_empty() or frame.height < settings.discovery_min_sample_count:
        return None
    sample_count = frame.height
    hit_rate_3d = float(frame["is_positive_3d"].mean())
    big_move_rate_3d = float(frame["is_big_move_3d"].mean())
    avg_return_3d = float(frame["next_3d_return"].mean())
    avg_max_return_3d = float(frame["next_3d_max_return"].mean())
    max_drawdown_3d = float(frame["max_drawdown_3d"].mean())
    return {
        "sample_count": sample_count,
        "hit_rate_3d": round(hit_rate_3d, 4),
        "big_move_rate_3d": round(big_move_rate_3d, 4),
        "avg_return_3d": round(avg_return_3d, 4),
        "avg_max_return_3d": round(avg_max_return_3d, 4),
        "max_drawdown_3d": round(max_drawdown_3d, 4),
        "discovery_score": _score_bucket(
            sample_count=sample_count,
            hit_rate_3d=hit_rate_3d,
            big_move_rate_3d=big_move_rate_3d,
            avg_return_3d=avg_return_3d,
            avg_max_return_3d=avg_max_return_3d,
            max_drawdown_3d=max_drawdown_3d,
        ),
    }


def _bucket_stats(frame: pl.DataFrame, regime: str, window_size: int, field: str, lower: float, upper: float) -> FactorBucketStat | None:
    filtered = frame.filter((pl.col(field) >= lower) & (pl.col(field) <= upper))
    metrics = _factor_metrics(filtered)
    if metrics is None:
        return None
    return FactorBucketStat(
        regime=regime,
        window_size=window_size,
        field=field,
        min_value=round(lower, 4),
        max_value=round(upper, 4),
        sample_count=metrics["sample_count"],
        hit_rate_3d=metrics["hit_rate_3d"],
        big_move_rate_3d=metrics["big_move_rate_3d"],
        avg_return_3d=metrics["avg_return_3d"],
        avg_max_return_3d=metrics["avg_max_return_3d"],
        max_drawdown_3d=metrics["max_drawdown_3d"],
        discovery_score=metrics["discovery_score"],
    )


def analyze_factors(panel: pl.DataFrame, *, regimes: list[str], lookback_windows: list[int]) -> list[FactorBucketStat]:
    rows: list[FactorBucketStat] = []
    for regime in regimes:
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        if regime_frame.is_empty():
            continue
        for window_size in lookback_windows:
            scoped = _window_frame(regime_frame, window_size)
            if scoped.is_empty():
                continue
            for spec in FACTOR_SPECS:
                field = spec["field"]
                clean = scoped.filter(pl.col(field).is_not_null())
                if clean.height < settings.discovery_min_sample_count:
                    continue
                values = [float(item) for item in clean[field].to_list() if item is not None]
                for lower, upper in _quantile_bounds(values):
                    stat = _bucket_stats(clean, regime, window_size, field, lower, upper)
                    if stat is not None:
                        rows.append(stat)
    rows.sort(key=lambda item: (item.discovery_score, item.sample_count), reverse=True)
    return rows


def _top_factor_buckets(factor_stats: list[FactorBucketStat], regime: str) -> list[FactorBucketStat]:
    rows = [item for item in factor_stats if item.regime == regime]
    rows.sort(key=lambda item: (item.discovery_score, item.sample_count), reverse=True)
    deduped: list[FactorBucketStat] = []
    seen_fields: set[str] = set()
    for item in rows:
        if item.field in seen_fields:
            continue
        seen_fields.add(item.field)
        deduped.append(item)
        if len(deduped) >= settings.discovery_factor_top_n:
            break
    return deduped


def analyze_factor_combos(
    panel: pl.DataFrame,
    *,
    factor_stats: list[FactorBucketStat],
    regimes: list[str],
) -> list[FactorComboResult]:
    results: list[FactorComboResult] = []
    for regime in regimes:
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        top_buckets = _top_factor_buckets(factor_stats, regime)
        singles_by_key = {(item.window_size, item.field): item for item in top_buckets}
        for left, right in combinations(top_buckets, 2):
            if left.field == right.field or left.window_size != right.window_size:
                continue
            scoped = _window_frame(regime_frame, left.window_size)
            filtered = scoped.filter(
                (pl.col(left.field) >= left.min_value)
                & (pl.col(left.field) <= left.max_value)
                & (pl.col(right.field) >= right.min_value)
                & (pl.col(right.field) <= right.max_value)
            )
            metrics = _factor_metrics(filtered)
            if metrics is None:
                continue
            best_single_score = max(
                singles_by_key[(left.window_size, left.field)].discovery_score,
                singles_by_key[(right.window_size, right.field)].discovery_score,
            )
            lift_vs_single = round(metrics["discovery_score"] - best_single_score, 2)
            if lift_vs_single < 0.5:
                continue
            combo_id = hashlib.sha1(
                json.dumps(
                    {
                        "regime": regime,
                        "window_size": left.window_size,
                        "left": left.model_dump(mode="json"),
                        "right": right.model_dump(mode="json"),
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest()[:12]
            results.append(
                FactorComboResult(
                    combo_id=combo_id,
                    regime=regime,
                    window_size=left.window_size,
                    fields=[left.field, right.field],
                    sample_count=metrics["sample_count"],
                    hit_rate_3d=metrics["hit_rate_3d"],
                    big_move_rate_3d=metrics["big_move_rate_3d"],
                    avg_return_3d=metrics["avg_return_3d"],
                    avg_max_return_3d=metrics["avg_max_return_3d"],
                    max_drawdown_3d=metrics["max_drawdown_3d"],
                    discovery_score=metrics["discovery_score"],
                    lift_vs_single=lift_vs_single,
                )
            )
    results.sort(key=lambda item: (item.discovery_score, item.lift_vs_single, item.sample_count), reverse=True)
    return results[: settings.discovery_combo_top_n]


def _adjust_condition(condition: dict, variant_type: str) -> dict:
    adjusted = dict(condition)
    min_value = adjusted.get("min")
    max_value = adjusted.get("max")
    if min_value is None and max_value is None:
        return adjusted
    span = 0.0
    if min_value is not None and max_value is not None:
        span = max(max_value - min_value, 0.01)
    elif min_value is not None:
        span = max(abs(min_value) * 0.15, 0.01)
    elif max_value is not None:
        span = max(abs(max_value) * 0.15, 0.01)
    delta = span * 0.15
    if variant_type == "narrow":
        if min_value is not None:
            adjusted["min"] = round(min_value + delta, 4)
        if max_value is not None:
            adjusted["max"] = round(max_value - delta, 4)
    elif variant_type == "wide":
        if min_value is not None:
            adjusted["min"] = round(min_value - delta, 4)
        if max_value is not None:
            adjusted["max"] = round(max_value + delta, 4)
    if adjusted.get("min") is not None and adjusted.get("max") is not None and adjusted["min"] > adjusted["max"]:
        adjusted["min"], adjusted["max"] = adjusted["max"], adjusted["min"]
    return adjusted


def _build_logic_spec(
    regime: str,
    combo_id: str,
    conditions: list[dict],
    fields: list[str],
    variant_type: str,
    rank: int,
) -> LogicSpec:
    score_weights = [{"field": field, "weight": _field_weight(field)} for field in fields]
    signature = json.dumps(
        {"regime": regime, "combo_id": combo_id, "variant_type": variant_type, "conditions": conditions},
        sort_keys=True,
        ensure_ascii=False,
    )
    suffix = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:8]
    logic_id = f"auto_{regime}_{rank}_{suffix}"
    return LogicSpec(
        logic_id=logic_id,
        name=f"自动反推{regime}候选{rank}",
        description=f"{regime} 环境下自动反推策略，使用 {', '.join(fields)} 组合，变体 {variant_type}。",
        source="factor_lab",
        regime_whitelist=[regime],
        required_datasets=["stock_hist", "stock_indicators"],
        ranking_rule=f"factor_lab {variant_type} combo",
        holding_days=3,
        max_candidates_per_day=8,
        entry_rule={
            "conditions": conditions,
            "score_weights": score_weights,
            "signal_name": f"{logic_id}_signal",
        },
        exit_rule={"take_profit_pct": 6.0, "max_holding_days": 3},
        invalid_rule={"single_day_drawdown_pct": 5.0},
    )


def _compute_replay_quality(
    filtered: pl.DataFrame,
    *,
    logic_id: str,
    logic_name: str,
    trade_days: int,
    top_k: int,
    fields: list[str],
) -> ReplayQualityResult:
    if filtered.is_empty():
        return ReplayQualityResult(
            run_id="",
            logic_id=logic_id,
            logic_name=logic_name,
            trade_days=trade_days,
            top_k=top_k,
            sample_count=0,
            hit_rate_3d=0.0,
            big_move_rate_3d=0.0,
            avg_n3d=0.0,
            avg_n3d_max=0.0,
            avg_n3d_dd=0.0,
            topk_quality_score=0.0,
            passed=False,
        )
    trigger_score = pl.lit(0.0)
    for field in fields:
        trigger_score = trigger_score + pl.col(field) * _field_weight(field)
    ranked = (
        filtered.with_columns(trigger_score.alias("trigger_score"))
        .sort(["trade_date", "trigger_score"], descending=[False, True])
        .with_columns(pl.col("trade_date").cum_count().over("trade_date").alias("_rank"))
        .filter(pl.col("_rank") <= top_k)
    )
    metrics = _factor_metrics(
        ranked.with_columns(
            [
                pl.col("next_3d_return").alias("next_3d_return"),
                pl.col("next_3d_max_return").alias("next_3d_max_return"),
                pl.col("max_drawdown_3d").alias("max_drawdown_3d"),
                pl.col("is_positive_3d").alias("is_positive_3d"),
                pl.col("is_big_move_3d").alias("is_big_move_3d"),
            ]
        )
    )
    if metrics is None:
        return ReplayQualityResult(
            run_id="",
            logic_id=logic_id,
            logic_name=logic_name,
            trade_days=trade_days,
            top_k=top_k,
            sample_count=0,
            hit_rate_3d=0.0,
            big_move_rate_3d=0.0,
            avg_n3d=0.0,
            avg_n3d_max=0.0,
            avg_n3d_dd=0.0,
            topk_quality_score=0.0,
            passed=False,
        )
    topk_quality_score = round(
        metrics["discovery_score"]
        + max(min(metrics["avg_return_3d"], 5.0), -5.0) * 3.0
        + max(min(metrics["avg_max_return_3d"], 8.0), 0.0) * 1.5
        + max(min((4.5 + metrics["max_drawdown_3d"]) / 4.5, 1.0), 0.0) * 8.0,
        2,
    )
    passed = (
        metrics["sample_count"] >= settings.discovery_min_sample_count
        and metrics["avg_return_3d"] >= 0.5
        and metrics["avg_max_return_3d"] >= 3.0
        and metrics["max_drawdown_3d"] >= -4.0
        and topk_quality_score >= settings.discovery_min_score
    )
    return ReplayQualityResult(
        run_id="",
        logic_id=logic_id,
        logic_name=logic_name,
        trade_days=trade_days,
        top_k=top_k,
        sample_count=metrics["sample_count"],
        hit_rate_3d=metrics["hit_rate_3d"],
        big_move_rate_3d=metrics["big_move_rate_3d"],
        avg_n3d=metrics["avg_return_3d"],
        avg_n3d_max=metrics["avg_max_return_3d"],
        avg_n3d_dd=metrics["max_drawdown_3d"],
        topk_quality_score=topk_quality_score,
        passed=passed,
    )


def analyze_rule_variants(
    panel: pl.DataFrame,
    *,
    combos: list[FactorComboResult],
    factor_stats: list[FactorBucketStat],
    top_n_eval: tuple[int, ...] = (3, 5),
) -> tuple[list[RuleVariantResult], list[DiscoveredLogicCandidate], list[ReplayQualityResult]]:
    factor_index = {(item.regime, item.window_size, item.field): item for item in factor_stats}
    variant_results: list[RuleVariantResult] = []
    replay_results: list[ReplayQualityResult] = []
    candidates: list[DiscoveredLogicCandidate] = []

    combos_by_regime: dict[str, list[FactorComboResult]] = {}
    for combo in combos:
        combos_by_regime.setdefault(combo.regime, []).append(combo)

    for regime, regime_combos in combos_by_regime.items():
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        for rank, combo in enumerate(regime_combos[: settings.discovery_candidate_limit], start=1):
            base_conditions = []
            for field in combo.fields:
                factor = factor_index[(regime, combo.window_size, field)]
                base_conditions.append({"field": field, "op": "between", "min": factor.min_value, "max": factor.max_value})
            variant_bundle: list[tuple[RuleVariantResult, DiscoveredLogicCandidate, list[ReplayQualityResult]]] = []
            scoped = _window_frame(regime_frame, combo.window_size)
            for variant_type in ("baseline", "narrow", "wide"):
                conditions = [_adjust_condition(condition, variant_type) for condition in base_conditions]
                filtered = scoped
                for condition in conditions:
                    filtered = filtered.filter(
                        (pl.col(condition["field"]) >= condition.get("min"))
                        & (pl.col(condition["field"]) <= condition.get("max"))
                    )
                metrics = _factor_metrics(filtered)
                if metrics is None:
                    continue
                spec = _build_logic_spec(regime, combo.combo_id, conditions, combo.fields, variant_type, rank)
                quality_rows = [
                    _compute_replay_quality(
                        filtered,
                        logic_id=spec.logic_id,
                        logic_name=spec.name,
                        trade_days=combo.window_size,
                        top_k=top_k,
                        fields=combo.fields,
                    )
                    for top_k in top_n_eval
                ]
                top3 = next((item for item in quality_rows if item.top_k == 3), None)
                top5 = next((item for item in quality_rows if item.top_k == 5), None)
                variant = RuleVariantResult(
                    variant_id=f"{spec.logic_id}_{variant_type}",
                    combo_id=combo.combo_id,
                    regime=regime,
                    logic_id=spec.logic_id,
                    variant_type=variant_type,
                    sample_count=metrics["sample_count"],
                    hit_rate_3d=metrics["hit_rate_3d"],
                    big_move_rate_3d=metrics["big_move_rate_3d"],
                    avg_return_3d=metrics["avg_return_3d"],
                    avg_max_return_3d=metrics["avg_max_return_3d"],
                    max_drawdown_3d=metrics["max_drawdown_3d"],
                    top3_quality_score=top3.topk_quality_score if top3 else 0.0,
                    top5_quality_score=top5.topk_quality_score if top5 else 0.0,
                    discovery_score=metrics["discovery_score"],
                )
                candidate = DiscoveredLogicCandidate(
                    candidate_id=f"{spec.logic_id}_candidate",
                    logic_id=spec.logic_id,
                    logic_name=spec.name,
                    regime=regime,
                    sample_count=metrics["sample_count"],
                    hit_rate_3d=metrics["hit_rate_3d"],
                    big_move_rate_3d=metrics["big_move_rate_3d"],
                    avg_return_3d=metrics["avg_return_3d"],
                    avg_max_return_3d=metrics["avg_max_return_3d"],
                    max_drawdown_3d=metrics["max_drawdown_3d"],
                    discovery_score=metrics["discovery_score"],
                    factor_fields=combo.fields,
                    parent_combo_id=combo.combo_id,
                    variant_type=variant_type,
                    top3_quality_score=variant.top3_quality_score,
                    top5_quality_score=variant.top5_quality_score,
                    replay_quality_passed=all(item.passed for item in quality_rows),
                    spec_json=json.dumps(spec.model_dump(mode="json"), ensure_ascii=False),
                )
                candidate.approved_for_validation = (
                    metrics["sample_count"] >= settings.discovery_min_sample_count
                    and (
                        (
                            metrics["big_move_rate_3d"] >= settings.discovery_min_big_move_rate
                            and metrics["discovery_score"] >= settings.discovery_min_score
                        )
                        or candidate.replay_quality_passed
                    )
                )
                variant_bundle.append((variant, candidate, quality_rows))
            if not variant_bundle:
                continue
            variant_bundle.sort(
                key=lambda item: (
                    item[1].replay_quality_passed,
                    item[0].top3_quality_score,
                    item[0].top5_quality_score,
                    item[0].discovery_score,
                    item[0].sample_count,
                ),
                reverse=True,
            )
            best_variant, best_candidate, best_replay_rows = variant_bundle[0]
            variant_results.extend(item[0] for item in variant_bundle)
            replay_results.extend(best_replay_rows)
            candidates.append(best_candidate)
    candidates.sort(
        key=lambda item: (
            item.replay_quality_passed,
            item.top3_quality_score or 0.0,
            item.top5_quality_score or 0.0,
            item.discovery_score,
            item.sample_count,
        ),
        reverse=True,
    )
    return variant_results, candidates[: settings.discovery_candidate_limit], replay_results
