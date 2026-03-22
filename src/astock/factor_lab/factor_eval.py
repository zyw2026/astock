from __future__ import annotations

import math

import polars as pl

from astock.app.settings import settings
from astock.factor_lab.discovery import active_factor_specs
from astock.factor_lab.ic import cross_sectional_ic
from astock.factor_lab.models import FactorIcResult, FactorMonotonicityResult


def _window_trade_dates(frame: pl.DataFrame, window_size: int | None) -> list:
    trade_dates = sorted(frame["trade_date"].unique().to_list())
    if not window_size or len(trade_dates) <= window_size:
        return trade_dates
    return trade_dates[-window_size:]


def _window_frame(frame: pl.DataFrame, window_size: int | None) -> pl.DataFrame:
    dates = _window_trade_dates(frame, window_size)
    return frame.filter(pl.col("trade_date").is_in(dates))


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float], mean_value: float) -> float:
    if len(values) < 2:
        return 0.0
    return math.sqrt(sum((value - mean_value) ** 2 for value in values) / (len(values) - 1))


def _ic_ir(mean_value: float, std_value: float) -> float:
    if std_value <= 1e-9:
        return 0.0
    return mean_value / std_value


def _quantile_bounds(values: list[float], quantiles: int) -> list[tuple[float, float]]:
    if not values:
        return []
    sorted_values = sorted(values)
    bounds: list[tuple[float, float]] = []
    for idx in range(quantiles):
        left_q = idx / quantiles
        right_q = (idx + 1) / quantiles
        left = sorted_values[min(int(left_q * (len(sorted_values) - 1)), len(sorted_values) - 1)]
        right = sorted_values[min(int(right_q * (len(sorted_values) - 1)), len(sorted_values) - 1)]
        if not bounds or bounds[-1] != (left, right):
            bounds.append((left, right))
    return bounds


def _eval_monotonicity(
    frame: pl.DataFrame,
    *,
    regime: str,
    regime_detail: str,
    window_size: int,
    field: str,
    quantiles: int,
) -> FactorMonotonicityResult | None:
    clean = frame.select([field, "next_3d_max_return"]).drop_nulls()
    if clean.height < settings.discovery_min_sample_count:
        return None
    values = [float(v) for v in clean[field].to_list()]
    bounds = _quantile_bounds(values, quantiles)
    if len(bounds) < 3:
        return None
    bucket_returns: list[float] = []
    for lower, upper in bounds:
        bucket = clean.filter((pl.col(field) >= lower) & (pl.col(field) <= upper))
        if bucket.is_empty():
            bucket_returns.append(0.0)
            continue
        bucket_returns.append(round(float(bucket["next_3d_max_return"].mean()), 4))
    increasing = all(bucket_returns[idx] <= bucket_returns[idx + 1] for idx in range(len(bucket_returns) - 1))
    decreasing = all(bucket_returns[idx] >= bucket_returns[idx + 1] for idx in range(len(bucket_returns) - 1))
    top_bottom_spread = round(bucket_returns[-1] - bucket_returns[0], 4)
    monotonic_direction = "flat"
    if increasing:
        monotonic_direction = "up"
    elif decreasing:
        monotonic_direction = "down"
    passed = increasing or decreasing
    eval_score = round(
        max(min(abs(top_bottom_spread) / 4.0, 1.0), 0.0) * 60.0
        + (20.0 if passed else 0.0)
        + min(clean.height / 60.0, 1.0) * 20.0,
        2,
    )
    return FactorMonotonicityResult(
        regime=regime,
        regime_detail=regime_detail,
        window_size=window_size,
        field=field,
        quantiles=quantiles,
        sample_count=clean.height,
        bucket_returns=bucket_returns,
        top_bottom_spread=top_bottom_spread,
        monotonic_direction=monotonic_direction,
        monotonic_passed=passed,
        eval_score=eval_score,
    )


def evaluate_factors(
    panel: pl.DataFrame,
    *,
    regimes: list[str],
    lookback_windows: list[int],
    quantiles: int = 5,
    factor_fields: list[str] | None = None,
) -> tuple[list[FactorIcResult], list[FactorMonotonicityResult]]:
    ic_rows: list[FactorIcResult] = []
    mono_rows: list[FactorMonotonicityResult] = []
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
                dates = sorted(scoped["trade_date"].unique().to_list())
                for spec in factor_specs:
                    field = spec["field"]
                    ic_values: list[float] = []
                    rank_ic_values: list[float] = []
                    clean = scoped.filter(pl.col(field).is_not_null())
                    if clean.height < settings.discovery_min_sample_count:
                        continue
                    for trade_date in dates:
                        day_frame = clean.filter(pl.col("trade_date") == trade_date)
                        ic_value, rank_ic_value = cross_sectional_ic(day_frame, field=field, target_field="next_3d_max_return")
                        if ic_value is not None:
                            ic_values.append(float(ic_value))
                        if rank_ic_value is not None:
                            rank_ic_values.append(float(rank_ic_value))
                    if ic_values:
                        ic_mean = _mean(ic_values)
                        ic_std = _std(ic_values, ic_mean)
                        rank_ic_mean = _mean(rank_ic_values)
                        rank_ic_std = _std(rank_ic_values, rank_ic_mean)
                        ic_rows.append(
                            FactorIcResult(
                                regime=regime,
                                regime_detail=regime_detail,
                                window_size=window_size,
                                field=field,
                                date_count=len(ic_values),
                                sample_count=clean.height,
                                ic_mean=round(ic_mean, 4),
                                ic_std=round(ic_std, 4),
                                rank_ic_mean=round(rank_ic_mean, 4),
                                rank_ic_std=round(rank_ic_std, 4),
                                ic_ir=round(_ic_ir(ic_mean, ic_std), 4),
                                rank_ic_ir=round(_ic_ir(rank_ic_mean, rank_ic_std), 4),
                            )
                        )
                    mono = _eval_monotonicity(
                        clean,
                        regime=regime,
                        regime_detail=regime_detail,
                        window_size=window_size,
                        field=field,
                        quantiles=quantiles,
                    )
                    if mono is not None:
                        mono_rows.append(mono)

    ic_rows.sort(key=lambda item: (abs(item.rank_ic_mean), item.rank_ic_ir, item.sample_count), reverse=True)
    mono_rows.sort(key=lambda item: (item.eval_score, abs(item.top_bottom_spread), item.sample_count), reverse=True)
    return ic_rows, mono_rows
