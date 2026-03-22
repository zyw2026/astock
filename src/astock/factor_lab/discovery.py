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
    FactorIcResult,
    FactorMonotonicityResult,
    FactorWhitelistEntry,
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
    {"field": "gap_pct", "label": "开盘缺口", "weight": 0.8},
    {"field": "breakout_vs_5d_high_pct", "label": "相对5日高点突破", "weight": 1.2},
    {"field": "close_in_day_range_pct", "label": "收盘强度", "weight": 1.0},
    {"field": "volume_ratio_5d", "label": "量比5日", "weight": 1.1},
    {"field": "turnover_ratio_5d", "label": "换手放大", "weight": 0.9},
    {"field": "range_expansion_5d", "label": "振幅扩张", "weight": 0.9},
    {"field": "industry_ret_1d", "label": "行业当日涨幅", "weight": 0.9},
    {"field": "industry_ret_3d", "label": "行业3日涨幅", "weight": 0.9},
    {"field": "industry_ret_5d", "label": "行业5日涨幅", "weight": 0.8},
    {"field": "industry_strong_rate", "label": "行业强势占比", "weight": 1.0},
    {"field": "industry_above_ma5_rate", "label": "行业站上MA5占比", "weight": 0.9},
    {"field": "industry_body_pct", "label": "行业实体强度", "weight": 0.7},
    {"field": "industry_volume_ratio_5d", "label": "行业量比5日", "weight": 0.8},
    {"field": "excess_ret_1d", "label": "相对行业超额1日", "weight": 1.2},
    {"field": "excess_ret_3d", "label": "相对行业超额3日", "weight": 1.1},
    {"field": "excess_ret_5d", "label": "相对行业超额5日", "weight": 1.0},
    {"field": "excess_ret_10d", "label": "相对行业超额10日", "weight": 0.8},
    {"field": "excess_body_pct", "label": "相对行业实体超额", "weight": 0.9},
    {"field": "excess_volume_ratio_5d", "label": "相对行业量比超额", "weight": 1.0},
)

RANKING_TYPES: tuple[str, ...] = (
    "factor_mix",
    "momentum_first",
    "low_extension",
    "reclaim_bias",
    "pullback_absorb",
    "repair_confirm",
)

PRIORITY_FIELDS: frozenset[str] = frozenset(
    {
        "industry_strong_rate",
        "excess_ret_1d",
        "excess_body_pct",
    }
)


def active_factor_specs(factor_fields: list[str] | None = None) -> tuple[dict, ...]:
    if not factor_fields:
        return FACTOR_SPECS
    field_set = set(factor_fields)
    return tuple(spec for spec in FACTOR_SPECS if spec["field"] in field_set)


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


def _build_score_weights(fields: list[str], ranking_type: str) -> list[dict]:
    weights: list[dict] = []
    selected_fields = list(fields)
    if ranking_type == "pullback_absorb" and "pullback_from_5d_high_pct" in fields:
        for extra_field in ("ret_1d", "ret_3d", "body_pct", "ma5_vs_ma10_pct", "close_vs_ma10_pct"):
            if extra_field not in selected_fields:
                selected_fields.append(extra_field)
    for field in selected_fields:
        weight = _field_weight(field)
        if ranking_type == "momentum_first":
            if field in {"ret_3d", "ret_5d", "ret_10d", "body_pct"}:
                weight *= 1.6
            elif field in {"close_vs_ma10_pct", "close_vs_ma5_pct", "pullback_from_3d_high_pct"}:
                weight *= 0.7
        elif ranking_type == "low_extension":
            if field in {"close_vs_ma10_pct", "close_vs_ma5_pct", "pullback_from_3d_high_pct", "pullback_from_5d_high_pct"}:
                weight *= 1.9
            elif field in {"ret_5d", "ret_10d"}:
                weight *= 0.7
        elif ranking_type == "reclaim_bias":
            if field in {"ma5_vs_ma10_pct", "ret_1d", "body_pct"}:
                weight *= 1.7
            elif field in {"prev_ret_1d", "intraday_range_pct"}:
                weight *= 1.25
        elif ranking_type == "pullback_absorb":
            if field in {"pullback_from_5d_high_pct", "close_vs_ma10_pct", "close_vs_ma5_pct"}:
                weight *= 1.35
            elif field in {"intraday_range_pct", "prev_ret_1d"}:
                weight *= 1.05
            elif field in {"ret_1d", "ret_3d", "body_pct", "ma5_vs_ma10_pct"}:
                weight *= 1.95
            elif field in {"ret_5d"}:
                weight *= 1.15
            elif field in {"ret_10d"}:
                weight *= 0.7
        elif ranking_type == "repair_confirm":
            if field in {"excess_body_pct", "body_pct", "ret_1d", "excess_ret_1d", "close_in_day_range_pct"}:
                weight *= 2.0
            elif field in {"pullback_from_5d_high_pct", "close_vs_ma10_pct", "ma5_vs_ma10_pct"}:
                weight *= 1.35
            elif field in {"industry_strong_rate", "industry_ret_1d"}:
                weight *= 1.2
            elif field in {"intraday_range_pct", "prev_ret_1d"}:
                weight *= 0.9
        weights.append({"field": field, "weight": round(weight, 4)})
    return weights


def _soft_condition_score_expr(condition: dict) -> pl.Expr:
    field = condition["field"]
    min_value = condition.get("min")
    max_value = condition.get("max")
    if min_value is None and max_value is None:
        return pl.lit(1.0)
    if min_value is None:
        min_value = max_value
    if max_value is None:
        max_value = min_value
    span = max(float(max_value) - float(min_value), 0.01)
    distance = (
        pl.when(pl.col(field) < float(min_value))
        .then(float(min_value) - pl.col(field))
        .when(pl.col(field) > float(max_value))
        .then(pl.col(field) - float(max_value))
        .otherwise(0.0)
    )
    return (
        pl.when(distance <= 0.0)
        .then(1.0)
        .otherwise((1.0 - (distance / span)).clip(0.0, 1.0))
    )


def _soft_match_expr(conditions: list[dict]) -> pl.Expr:
    if not conditions:
        return pl.lit(1.0)
    expr = pl.lit(0.0)
    for condition in conditions:
        expr = expr + _soft_condition_score_expr(condition)
    return expr / float(len(conditions))


def _apply_hard_conditions(frame: pl.DataFrame, conditions: list[dict]) -> pl.DataFrame:
    filtered = frame
    for condition in conditions:
        field = condition["field"]
        op = condition.get("op", "between")
        if op == "between":
            expr = pl.lit(True)
            if condition.get("min") is not None:
                expr = expr & (pl.col(field) >= float(condition["min"]))
            if condition.get("max") is not None:
                expr = expr & (pl.col(field) <= float(condition["max"]))
            filtered = filtered.filter(expr)
        else:
            raise ValueError(f"unsupported hard condition op in discovery: {op}")
    return filtered


def _confirmation_conditions(
    regime: str,
    regime_detail: str | None,
    fields: list[str],
    variant_type: str,
) -> list[dict]:
    if (
        regime == "weak_rotation"
        and regime_detail == "weak_rotation_drift"
        and {"pullback_from_5d_high_pct", "intraday_range_pct"}.issubset(fields)
        and variant_type.startswith("tighten_lower")
    ):
        return [
            {"field": "ret_1d", "op": "between", "min": 0.2, "max": 3.8},
            {"field": "body_pct", "op": "between", "min": 0.1, "max": 3.2},
            {"field": "ma5_vs_ma10_pct", "op": "between", "min": 0.0, "max": 4.5},
        ]
    if (
        regime == "weak_rotation"
        and regime_detail == "weak_rotation_repair"
        and {"pullback_from_5d_high_pct", "excess_body_pct"}.issubset(fields)
    ):
        return [
            {"field": "ret_1d", "op": "between", "min": 0.5, "max": 4.2},
            {"field": "body_pct", "op": "between", "min": 0.3, "max": 4.5},
            {"field": "excess_ret_1d", "op": "between", "min": 0.0, "max": 5.0},
            {"field": "close_in_day_range_pct", "op": "between", "min": 45.0, "max": 100.0},
        ]
    return []


def _bucket_stats(
    frame: pl.DataFrame,
    regime: str,
    regime_detail: str,
    window_size: int,
    field: str,
    lower: float,
    upper: float,
) -> FactorBucketStat | None:
    filtered = frame.filter((pl.col(field) >= lower) & (pl.col(field) <= upper))
    metrics = _factor_metrics(filtered)
    if metrics is None:
        return None
    return FactorBucketStat(
        regime=regime,
        regime_detail=regime_detail,
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


def analyze_factors(
    panel: pl.DataFrame,
    *,
    regimes: list[str],
    lookback_windows: list[int],
    factor_fields: list[str] | None = None,
) -> list[FactorBucketStat]:
    rows: list[FactorBucketStat] = []
    factor_specs = active_factor_specs(factor_fields)
    for regime in regimes:
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        if regime_frame.is_empty():
            continue
        detail_values = sorted(item for item in regime_frame["regime_detail"].drop_nulls().unique().to_list() if item)
        for regime_detail in detail_values or [regime]:
            detail_frame = regime_frame.filter(pl.col("regime_detail") == regime_detail)
            if detail_frame.is_empty():
                continue
            for window_size in lookback_windows:
                scoped = _window_frame(detail_frame, window_size)
                if scoped.is_empty():
                    continue
                for spec in factor_specs:
                    field = spec["field"]
                    clean = scoped.filter(pl.col(field).is_not_null())
                    if clean.height < settings.discovery_min_sample_count:
                        continue
                    values = [float(item) for item in clean[field].to_list() if item is not None]
                    for lower, upper in _quantile_bounds(values):
                        stat = _bucket_stats(clean, regime, regime_detail, window_size, field, lower, upper)
                        if stat is not None:
                            rows.append(stat)
    rows.sort(
        key=lambda item: (
            item.field in PRIORITY_FIELDS,
            item.discovery_score,
            item.sample_count,
        ),
        reverse=True,
    )
    return rows


def build_factor_whitelist(
    factor_stats: list[FactorBucketStat],
    *,
    ic_results: list[FactorIcResult] | None = None,
    monotonicity_results: list[FactorMonotonicityResult] | None = None,
    regimes: list[str],
) -> list[FactorWhitelistEntry]:
    grouped: dict[tuple[str, str, str | None], list[FactorBucketStat]] = {}
    for row in factor_stats:
        grouped.setdefault((row.regime, row.field, row.regime_detail), []).append(row)
    ic_grouped: dict[tuple[str, str, str | None], list[FactorIcResult]] = {}
    for row in ic_results or []:
        ic_grouped.setdefault((row.regime, row.field, row.regime_detail), []).append(row)
    mono_grouped: dict[tuple[str, str, str | None], list[FactorMonotonicityResult]] = {}
    for row in monotonicity_results or []:
        mono_grouped.setdefault((row.regime, row.field, row.regime_detail), []).append(row)

    whitelist: list[FactorWhitelistEntry] = []
    for regime in regimes:
        for (row_regime, field, regime_detail), rows in grouped.items():
            if row_regime != regime:
                continue
            ic_rows = ic_grouped.get((regime, field, regime_detail), [])
            mono_rows = mono_grouped.get((regime, field, regime_detail), [])
            best_score = max(item.discovery_score for item in rows)
            avg_score = sum(item.discovery_score for item in rows) / len(rows)
            best_big_move = max(item.big_move_rate_3d for item in rows)
            stable_count = sum(1 for item in rows if item.discovery_score >= 68 and item.big_move_rate_3d >= 0.28)
            best_rank_ic_mean = max((abs(item.rank_ic_mean) for item in ic_rows), default=0.0)
            avg_rank_ic_mean = (
                sum(abs(item.rank_ic_mean) for item in ic_rows) / len(ic_rows) if ic_rows else 0.0
            )
            best_rank_ic_ir = max((item.rank_ic_ir for item in ic_rows), default=0.0)
            monotonic_pass_count = sum(1 for item in mono_rows if item.monotonic_passed)
            best_monotonic_score = max((item.eval_score for item in mono_rows), default=0.0)
            priority_bonus = 8.0 if field in PRIORITY_FIELDS else 0.0
            stability_component = min(stable_count / 3.0, 1.0) * 30.0
            discovery_component = max(min(avg_score / 80.0, 1.0), 0.0) * 25.0
            ic_component = max(min(avg_rank_ic_mean / 0.06, 1.0), 0.0) * 20.0
            ir_component = max(min(best_rank_ic_ir / 1.5, 1.0), 0.0) * 10.0
            monotonic_component = max(min(best_monotonic_score / 80.0, 1.0), 0.0) * 15.0
            whitelist_score = round(
                stability_component
                + discovery_component
                + ic_component
                + ir_component
                + monotonic_component,
                2,
            )
            effective_whitelist_score = round(whitelist_score + priority_bonus, 2)
            if (
                stable_count >= 3
                and avg_rank_ic_mean >= 0.08
                and best_rank_ic_ir >= 0.8
                and monotonic_pass_count >= 2
                and effective_whitelist_score >= 82
            ):
                status = "stable"
            elif (
                stable_count >= 2
                and best_score >= 68
                and best_big_move >= 0.25
                and best_rank_ic_mean >= 0.06
                and (monotonic_pass_count >= 1 or best_monotonic_score >= 72)
                and effective_whitelist_score >= 74
            ):
                status = "regime_only"
            else:
                status = "invalid"
            whitelist.append(
                FactorWhitelistEntry(
                    regime=regime,
                    regime_detail=regime_detail,
                    field=field,
                    window_hit_count=len(rows),
                    stable_window_count=stable_count,
                    best_discovery_score=round(best_score, 2),
                    avg_discovery_score=round(avg_score, 2),
                    best_big_move_rate_3d=round(best_big_move, 4),
                    best_rank_ic_mean=round(best_rank_ic_mean, 4),
                    avg_rank_ic_mean=round(avg_rank_ic_mean, 4),
                    best_rank_ic_ir=round(best_rank_ic_ir, 4),
                    monotonic_pass_count=monotonic_pass_count,
                    best_monotonic_score=round(best_monotonic_score, 2),
                    whitelist_score=effective_whitelist_score,
                    status=status,
                    eligible=status in {"stable", "regime_only"},
                )
            )

    grouped_entries: dict[tuple[str, str | None], list[FactorWhitelistEntry]] = {}
    for item in whitelist:
        grouped_entries.setdefault((item.regime, item.regime_detail), []).append(item)
    for _, entries in grouped_entries.items():
        eligible_count = sum(1 for item in entries if item.eligible)
        if eligible_count >= 2:
            continue
        entries.sort(
            key=lambda item: (
                item.whitelist_score,
                item.best_rank_ic_mean,
                item.best_monotonic_score,
                item.best_discovery_score,
            ),
            reverse=True,
        )
        for item in entries:
            if item.eligible:
                continue
            if item.whitelist_score < 78:
                continue
            if item.best_rank_ic_mean < 0.08 and item.monotonic_pass_count < 1:
                continue
            item.status = "regime_only"
            item.eligible = True
            eligible_count += 1
            if eligible_count >= 2:
                break
    whitelist.sort(
        key=lambda item: (
            item.eligible,
            item.status == "stable",
            item.whitelist_score,
            item.best_discovery_score,
            item.avg_discovery_score,
        ),
        reverse=True,
    )
    return whitelist


def _top_factor_buckets(
    factor_stats: list[FactorBucketStat],
    regime: str,
    *,
    regime_detail: str | None = None,
    whitelist: list[FactorWhitelistEntry] | None = None,
) -> list[FactorBucketStat]:
    allowed_fields = None
    if whitelist is not None:
        allowed_fields = {
            item.field
            for item in whitelist
            if item.regime == regime and item.regime_detail == regime_detail and item.eligible
        }
    rows = [
        item
        for item in factor_stats
        if item.regime == regime
        and item.regime_detail == regime_detail
        and (allowed_fields is None or item.field in allowed_fields)
    ]
    rows.sort(key=lambda item: (item.discovery_score, item.sample_count), reverse=True)
    deduped: list[FactorBucketStat] = []
    field_counts: dict[str, int] = {}
    for item in rows:
        if field_counts.get(item.field, 0) >= 2:
            continue
        field_counts[item.field] = field_counts.get(item.field, 0) + 1
        deduped.append(item)
        if len(deduped) >= settings.discovery_factor_top_n:
            break
    return deduped


def _core_whitelist_fields(
    whitelist: list[FactorWhitelistEntry] | None,
    regime: str,
    *,
    regime_detail: str | None = None,
    limit: int = 2,
) -> set[str]:
    if whitelist is None:
        return set()
    rows = [
        item
        for item in whitelist
        if item.regime == regime
        and item.regime_detail == regime_detail
        and item.eligible
        and (
            item.status == "stable"
            or (
                item.best_rank_ic_mean >= 0.08
                and (item.monotonic_pass_count >= 1 or item.best_monotonic_score >= 72)
            )
        )
    ]
    rows.sort(
        key=lambda item: (
            item.field in PRIORITY_FIELDS,
            item.status == "stable",
            item.whitelist_score,
            item.best_rank_ic_mean,
            item.best_monotonic_score,
            item.best_discovery_score,
        ),
        reverse=True,
    )
    return {item.field for item in rows[:limit]}


def analyze_factor_combos(
    panel: pl.DataFrame,
    *,
    factor_stats: list[FactorBucketStat],
    regimes: list[str],
    whitelist: list[FactorWhitelistEntry] | None = None,
) -> list[FactorComboResult]:
    best_results: dict[tuple[str, str | None, tuple[str, ...]], FactorComboResult] = {}
    for regime in regimes:
        regime_frame = panel.filter((pl.col("regime") == regime) & pl.col("next_3d_max_return").is_not_null())
        detail_values = sorted(item for item in regime_frame["regime_detail"].drop_nulls().unique().to_list() if item)
        for regime_detail in detail_values or [regime]:
            detail_frame = regime_frame.filter(pl.col("regime_detail") == regime_detail)
            top_buckets = _top_factor_buckets(factor_stats, regime, regime_detail=regime_detail, whitelist=whitelist)
            core_fields = _core_whitelist_fields(whitelist, regime, regime_detail=regime_detail, limit=2)
            filtered_top_buckets: list[FactorBucketStat] = []
            seen_fields: set[str] = set()
            for item in top_buckets:
                if item.field in seen_fields:
                    continue
                seen_fields.add(item.field)
                filtered_top_buckets.append(item)
            top_buckets = filtered_top_buckets
            if len(top_buckets) < 2:
                continue
            singles_by_key = {(item.window_size, item.field): item for item in top_buckets}
            for left, right in combinations(top_buckets, 2):
                if left.field == right.field:
                    continue
                if core_fields and left.field not in core_fields and right.field not in core_fields:
                    continue
                combo_window = min(left.window_size, right.window_size)
                scoped = _window_frame(detail_frame, combo_window)
                combo_conditions = [
                    {"field": left.field, "min": left.min_value, "max": left.max_value},
                    {"field": right.field, "min": right.min_value, "max": right.max_value},
                ]
                scored = scoped.with_columns(_soft_match_expr(combo_conditions).alias("_combo_match"))
                threshold = max(float(scored["_combo_match"].quantile(0.55) or 0.0), 0.25)
                filtered = scored.filter(pl.col("_combo_match") >= threshold)
                metrics = _factor_metrics(filtered)
                if metrics is None:
                    continue
                best_single_score = max(
                    singles_by_key[(left.window_size, left.field)].discovery_score,
                    singles_by_key[(right.window_size, right.field)].discovery_score,
                )
                best_single_big_move = max(
                    singles_by_key[(left.window_size, left.field)].big_move_rate_3d,
                    singles_by_key[(right.window_size, right.field)].big_move_rate_3d,
                )
                lift_vs_single = round(metrics["discovery_score"] - best_single_score, 2)
                coarse_pass = metrics["sample_count"] >= settings.discovery_min_sample_count
                quality_guard = (
                    metrics["big_move_rate_3d"] >= max(best_single_big_move - 0.03, settings.discovery_min_big_move_rate)
                    and metrics["avg_max_return_3d"] >= 3.0
                )
                strict_pass = lift_vs_single >= 0.25 or quality_guard
                if not coarse_pass:
                    continue
                raw_quality_bonus = 0.0
                if metrics["avg_max_return_3d"] >= 2.5:
                    raw_quality_bonus += 1.0
                if metrics["big_move_rate_3d"] >= 0.15:
                    raw_quality_bonus += 1.0
                combo_id = hashlib.sha1(
                    json.dumps(
                        {
                            "regime": regime,
                            "regime_detail": regime_detail,
                            "window_size": combo_window,
                            "left": left.model_dump(mode="json"),
                            "right": right.model_dump(mode="json"),
                        },
                        sort_keys=True,
                        ensure_ascii=False,
                    ).encode("utf-8")
                ).hexdigest()[:12]
                combo_result = FactorComboResult(
                    combo_id=combo_id,
                    regime=regime,
                    regime_detail=regime_detail,
                    window_size=combo_window,
                    fields=[left.field, right.field],
                    match_threshold=round(threshold, 4),
                    sample_count=metrics["sample_count"],
                    hit_rate_3d=metrics["hit_rate_3d"],
                    big_move_rate_3d=metrics["big_move_rate_3d"],
                    avg_return_3d=metrics["avg_return_3d"],
                    avg_max_return_3d=metrics["avg_max_return_3d"],
                    max_drawdown_3d=metrics["max_drawdown_3d"],
                    discovery_score=round(
                        metrics["discovery_score"] + raw_quality_bonus + (3.0 if strict_pass else 0.0),
                        2,
                    ),
                    lift_vs_single=lift_vs_single,
                )
                pair_key = (regime, regime_detail, tuple(sorted(combo_result.fields)))
                previous = best_results.get(pair_key)
                if previous is None or (
                    combo_result.discovery_score,
                    combo_result.lift_vs_single,
                    combo_result.sample_count,
                ) > (
                    previous.discovery_score,
                    previous.lift_vs_single,
                    previous.sample_count,
                ):
                    best_results[pair_key] = combo_result
    results = list(best_results.values())
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
    tight_delta = span * 0.3
    soft_tight_delta = span * 0.2
    if variant_type == "narrow":
        if min_value is not None:
            adjusted["min"] = round(min_value + delta, 4)
        if max_value is not None:
            adjusted["max"] = round(max_value - delta, 4)
    elif variant_type == "tight":
        if min_value is not None:
            adjusted["min"] = round(min_value + tight_delta, 4)
        if max_value is not None:
            adjusted["max"] = round(max_value - tight_delta, 4)
    elif variant_type == "tighten_lower":
        if min_value is not None:
            adjusted["min"] = round(min_value + tight_delta, 4)
    elif variant_type == "tighten_upper":
        if max_value is not None:
            adjusted["max"] = round(max_value - tight_delta, 4)
    elif variant_type == "tighten_lower_soft":
        if min_value is not None:
            adjusted["min"] = round(min_value + soft_tight_delta, 4)
    elif variant_type == "tighten_upper_soft":
        if max_value is not None:
            adjusted["max"] = round(max_value - soft_tight_delta, 4)
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
    regime_detail: str | None,
    combo_id: str,
    hard_conditions: list[dict],
    conditions: list[dict],
    fields: list[str],
    variant_type: str,
    ranking_type: str,
    match_threshold: float,
    rank: int,
) -> LogicSpec:
    signature = json.dumps(
        {
            "regime": regime,
            "regime_detail": regime_detail,
            "combo_id": combo_id,
            "variant_type": variant_type,
            "ranking_type": ranking_type,
            "conditions": conditions,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    suffix = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:8]
    logic_id = f"auto_{regime_detail or regime}_{rank}_{suffix}"
    return LogicSpec(
        logic_id=logic_id,
        name=f"自动反推{regime_detail or regime}候选{rank}",
        description=f"{regime_detail or regime} 环境下自动反推策略，使用 {', '.join(fields)} 组合，变体 {variant_type}，排序 {ranking_type}。",
        source="factor_lab",
        regime_whitelist=[regime],
        required_datasets=["stock_hist", "stock_indicators"],
        ranking_rule=f"factor_lab {variant_type} {ranking_type}",
        holding_days=3,
        max_candidates_per_day=8,
        entry_rule={
            "conditions": hard_conditions,
            "soft_conditions": conditions,
            "match_threshold": round(match_threshold, 4),
            "score_weights": _build_score_weights(fields, ranking_type),
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
    ranking_type: str,
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
    for weight in _build_score_weights(fields, ranking_type):
        trigger_score = trigger_score + pl.col(weight["field"]) * float(weight["weight"])
    ranked = (
        filtered.with_columns(trigger_score.alias("trigger_score"))
        .sort(["trade_date", "trigger_score"], descending=[False, True])
        .with_columns(pl.col("trade_date").cum_count().over("trade_date").alias("_rank"))
        .filter(pl.col("_rank") <= top_k)
    )
    metrics = _factor_metrics(ranked)
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
    factor_index = {
        (item.regime, item.regime_detail, item.window_size, item.field): item
        for item in factor_stats
    }
    variant_results: list[RuleVariantResult] = []
    replay_results: list[ReplayQualityResult] = []
    candidates: list[DiscoveredLogicCandidate] = []

    combos_by_bucket: dict[tuple[str, str | None], list[FactorComboResult]] = {}
    for combo in combos:
        combos_by_bucket.setdefault((combo.regime, combo.regime_detail), []).append(combo)

    for (regime, regime_detail), regime_combos in combos_by_bucket.items():
        regime_frame = panel.filter(
            (pl.col("regime") == regime)
            & (pl.col("regime_detail") == regime_detail)
            & pl.col("next_3d_max_return").is_not_null()
        )
        if regime_frame.is_empty():
            continue
        for rank, combo in enumerate(regime_combos[: settings.discovery_candidate_limit], start=1):
            base_conditions = []
            for field in combo.fields:
                factor = factor_index[(regime, regime_detail, combo.window_size, field)]
                base_conditions.append({"field": field, "op": "between", "min": factor.min_value, "max": factor.max_value})
            variant_bundle: list[tuple[RuleVariantResult, DiscoveredLogicCandidate, list[ReplayQualityResult]]] = []
            scoped = _window_frame(regime_frame, combo.window_size)
            is_repair_combo = (
                regime == "weak_rotation"
                and regime_detail == "weak_rotation_repair"
                and {"pullback_from_5d_high_pct", "excess_body_pct"}.issubset(combo.fields)
            )
            if is_repair_combo:
                variant_configs = (
                    ("baseline", 0.0),
                    ("narrow", 0.0),
                    ("wide", 0.0),
                )
                ranking_types = ("repair_confirm", "pullback_absorb", "factor_mix")
            elif regime == "weak_rotation":
                variant_configs = (
                    ("baseline", 0.0),
                    ("narrow", 0.0),
                    ("tight", 0.0),
                    ("tighten_lower_soft", 0.0),
                    ("tighten_lower_soft", 0.03),
                    ("tighten_upper_soft", 0.0),
                    ("tighten_lower", 0.0),
                    ("tighten_lower", 0.03),
                    ("tighten_upper", 0.0),
                    ("wide", 0.0),
                )
                ranking_types = RANKING_TYPES
            else:
                variant_configs = (("baseline", 0.0), ("narrow", 0.0), ("wide", 0.0))
                ranking_types = RANKING_TYPES
            for variant_type, extra_threshold_delta in variant_configs:
                variant_name = variant_type if extra_threshold_delta <= 0 else f"{variant_type}_hi"
                conditions = [_adjust_condition(condition, variant_type) for condition in base_conditions]
                hard_conditions = _confirmation_conditions(regime, regime_detail, combo.fields, variant_name)
                base_threshold = combo.match_threshold or 0.35
                threshold_delta = 0.0
                if variant_type == "narrow":
                    threshold_delta = 0.05
                elif variant_type in {"tight", "tighten_lower", "tighten_upper"}:
                    threshold_delta = 0.1
                elif variant_type in {"tighten_lower_soft", "tighten_upper_soft"}:
                    threshold_delta = 0.07
                elif variant_type == "wide":
                    threshold_delta = -0.05
                threshold_delta += extra_threshold_delta
                match_threshold = min(max(base_threshold + threshold_delta, 0.25), 0.95)
                filtered = _apply_hard_conditions(scoped, hard_conditions)
                filtered = filtered.with_columns(_soft_match_expr(conditions).alias("_match_score")).filter(
                    pl.col("_match_score") >= match_threshold
                )
                metrics = _factor_metrics(filtered)
                if metrics is None:
                    continue
                ranking_bundle: list[tuple[RuleVariantResult, DiscoveredLogicCandidate, list[ReplayQualityResult]]] = []
                for ranking_type in ranking_types:
                    spec = _build_logic_spec(
                        regime,
                        regime_detail,
                        combo.combo_id,
                        hard_conditions,
                        conditions,
                        combo.fields,
                        variant_name,
                        ranking_type,
                        match_threshold,
                        rank,
                    )
                    quality_rows = [
                        _compute_replay_quality(
                            filtered,
                            logic_id=spec.logic_id,
                            logic_name=spec.name,
                            trade_days=combo.window_size,
                            top_k=top_k,
                            fields=combo.fields,
                            ranking_type=ranking_type,
                        )
                        for top_k in top_n_eval
                    ]
                    top3 = next((item for item in quality_rows if item.top_k == 3), None)
                    top5 = next((item for item in quality_rows if item.top_k == 5), None)
                    variant = RuleVariantResult(
                        variant_id=f"{spec.logic_id}_{variant_type}_{ranking_type}",
                        combo_id=combo.combo_id,
                        regime=regime,
                        regime_detail=regime_detail,
                        logic_id=spec.logic_id,
                        variant_type=variant_name,
                        ranking_type=ranking_type,
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
                        regime_detail=regime_detail,
                        sample_count=metrics["sample_count"],
                        hit_rate_3d=metrics["hit_rate_3d"],
                        big_move_rate_3d=metrics["big_move_rate_3d"],
                        avg_return_3d=metrics["avg_return_3d"],
                        avg_max_return_3d=metrics["avg_max_return_3d"],
                        max_drawdown_3d=metrics["max_drawdown_3d"],
                        discovery_score=metrics["discovery_score"],
                        factor_fields=combo.fields,
                        parent_combo_id=combo.combo_id,
                        variant_type=variant_name,
                        ranking_type=ranking_type,
                        lifecycle_state="candidate",
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
                    ranking_bundle.append((variant, candidate, quality_rows))
                if not ranking_bundle:
                    continue
                if regime == "weak_rotation":
                    ranking_bundle.sort(
                        key=lambda item: (
                            item[1].replay_quality_passed,
                            item[0].top5_quality_score,
                            item[0].big_move_rate_3d,
                            item[0].avg_max_return_3d,
                            item[0].avg_return_3d,
                            item[0].top3_quality_score,
                            item[0].discovery_score,
                            item[0].sample_count,
                        ),
                        reverse=True,
                    )
                else:
                    ranking_bundle.sort(
                        key=lambda item: (
                            item[1].replay_quality_passed,
                            item[0].top3_quality_score,
                            item[0].top5_quality_score,
                            item[0].discovery_score,
                            item[0].sample_count,
                        ),
                        reverse=True,
                    )
                variant_bundle.append(ranking_bundle[0])
            if not variant_bundle:
                continue
            if regime == "weak_rotation":
                variant_bundle.sort(
                    key=lambda item: (
                        item[1].replay_quality_passed,
                        item[0].top5_quality_score,
                        item[0].big_move_rate_3d,
                        item[0].avg_max_return_3d,
                        item[0].avg_return_3d,
                        item[0].top3_quality_score,
                        item[0].discovery_score,
                        item[0].sample_count,
                    ),
                    reverse=True,
                )
            else:
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
