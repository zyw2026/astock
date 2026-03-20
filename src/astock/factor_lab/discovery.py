from __future__ import annotations

import hashlib
import json
from itertools import combinations

import polars as pl

from astock.app.settings import settings
from astock.factor_lab.models import DiscoveredLogicCandidate, FactorBucketStat
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


def _bucket_stats(frame: pl.DataFrame, regime: str, field: str, lower: float, upper: float) -> FactorBucketStat | None:
    filtered = frame.filter((pl.col(field) >= lower) & (pl.col(field) <= upper))
    if filtered.is_empty() or filtered.height < settings.discovery_min_sample_count:
        return None
    sample_count = filtered.height
    hit_rate_3d = float(filtered["is_positive_3d"].mean())
    big_move_rate_3d = float(filtered["is_big_move_3d"].mean())
    avg_return_3d = float(filtered["next_3d_return"].mean())
    avg_max_return_3d = float(filtered["next_3d_max_return"].mean())
    max_drawdown_3d = float(filtered["max_drawdown_3d"].mean())
    return FactorBucketStat(
        regime=regime,
        field=field,
        min_value=round(lower, 4),
        max_value=round(upper, 4),
        sample_count=sample_count,
        hit_rate_3d=round(hit_rate_3d, 4),
        big_move_rate_3d=round(big_move_rate_3d, 4),
        avg_return_3d=round(avg_return_3d, 4),
        avg_max_return_3d=round(avg_max_return_3d, 4),
        max_drawdown_3d=round(max_drawdown_3d, 4),
        discovery_score=_score_bucket(
            sample_count=sample_count,
            hit_rate_3d=hit_rate_3d,
            big_move_rate_3d=big_move_rate_3d,
            avg_return_3d=avg_return_3d,
            avg_max_return_3d=avg_max_return_3d,
            max_drawdown_3d=max_drawdown_3d,
        ),
    )


def _top_single_buckets(regime_frame: pl.DataFrame, regime: str) -> list[FactorBucketStat]:
    results: list[FactorBucketStat] = []
    for spec in FACTOR_SPECS:
        field = spec["field"]
        clean = regime_frame.filter(pl.col(field).is_not_null())
        if clean.is_empty() or clean.height < settings.discovery_min_sample_count:
            continue
        values = [float(item) for item in clean[field].to_list() if item is not None]
        for lower, upper in _quantile_bounds(values):
            stat = _bucket_stats(clean, regime, field, lower, upper)
            if stat is not None:
                results.append(stat)
    return sorted(results, key=lambda item: (item.discovery_score, item.sample_count), reverse=True)[:6]


def _combine_conditions(regime_frame: pl.DataFrame, left: FactorBucketStat, right: FactorBucketStat) -> dict | None:
    filtered = regime_frame.filter(
        (pl.col(left.field) >= left.min_value)
        & (pl.col(left.field) <= left.max_value)
        & (pl.col(right.field) >= right.min_value)
        & (pl.col(right.field) <= right.max_value)
    )
    if filtered.is_empty() or filtered.height < settings.discovery_min_sample_count:
        return None
    sample_count = filtered.height
    hit_rate_3d = float(filtered["is_positive_3d"].mean())
    big_move_rate_3d = float(filtered["is_big_move_3d"].mean())
    avg_return_3d = float(filtered["next_3d_return"].mean())
    avg_max_return_3d = float(filtered["next_3d_max_return"].mean())
    max_drawdown_3d = float(filtered["max_drawdown_3d"].mean())
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


def _spec_weight(field: str) -> float:
    for spec in FACTOR_SPECS:
        if spec["field"] == field:
            return float(spec["weight"])
    return 0.5


def _build_logic_spec(regime: str, combo: tuple[FactorBucketStat, FactorBucketStat], metrics: dict, rank: int) -> LogicSpec:
    fields = [combo[0].field, combo[1].field]
    condition_rows = [
        {"field": combo[0].field, "op": "between", "min": combo[0].min_value, "max": combo[0].max_value},
        {"field": combo[1].field, "op": "between", "min": combo[1].min_value, "max": combo[1].max_value},
    ]
    score_weights = [{"field": field, "weight": _spec_weight(field)} for field in fields]
    signature = json.dumps({"regime": regime, "conditions": condition_rows}, sort_keys=True, ensure_ascii=False)
    suffix = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:8]
    logic_id = f"auto_{regime}_{rank}_{suffix}"
    return LogicSpec(
        logic_id=logic_id,
        name=f"自动反推{regime}候选{rank}",
        description=f"数据反推候选策略，条件来自 {combo[0].field} 与 {combo[1].field} 的高分组合。",
        source="factor_lab",
        regime_whitelist=[regime],
        required_datasets=["stock_hist", "stock_indicators"],
        ranking_rule=f"prefer {combo[0].field} and {combo[1].field} mix",
        holding_days=3,
        max_candidates_per_day=8,
        entry_rule={
            "conditions": condition_rows,
            "score_weights": score_weights,
            "signal_name": f"{logic_id}_signal",
        },
        exit_rule={"take_profit_pct": 6.0, "max_holding_days": 3},
        invalid_rule={"single_day_drawdown_pct": 5.0},
    )


def discover_logic_candidates(panel: pl.DataFrame, *, regimes: list[str], candidate_limit: int) -> list[DiscoveredLogicCandidate]:
    candidates: list[DiscoveredLogicCandidate] = []
    per_regime_limit = max(1, candidate_limit // max(len(regimes), 1))
    for regime in regimes:
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        if regime_frame.is_empty():
            continue
        top_buckets = _top_single_buckets(regime_frame, regime)
        ranked_combos: list[tuple[tuple[FactorBucketStat, FactorBucketStat], dict]] = []
        for left, right in combinations(top_buckets, 2):
            if left.field == right.field:
                continue
            combo_metrics = _combine_conditions(regime_frame, left, right)
            if combo_metrics is None:
                continue
            ranked_combos.append(((left, right), combo_metrics))
        ranked_combos.sort(key=lambda item: (item[1]["discovery_score"], item[1]["sample_count"]), reverse=True)
        for rank, (combo, metrics) in enumerate(ranked_combos[:per_regime_limit], start=1):
            spec = _build_logic_spec(regime, combo, metrics, rank)
            approved = (
                metrics["sample_count"] >= settings.discovery_min_sample_count
                and metrics["big_move_rate_3d"] >= settings.discovery_min_big_move_rate
                and metrics["discovery_score"] >= settings.discovery_min_score
                and metrics["max_drawdown_3d"] >= -6.0
            )
            candidates.append(
                DiscoveredLogicCandidate(
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
                    approved_for_validation=approved,
                    factor_fields=[combo[0].field, combo[1].field],
                    spec_json=json.dumps(spec.model_dump(mode="json"), ensure_ascii=False),
                )
            )
    candidates.sort(key=lambda item: (item.discovery_score, item.sample_count), reverse=True)
    return candidates[:candidate_limit]
