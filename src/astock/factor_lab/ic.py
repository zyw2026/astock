from __future__ import annotations

import math

import polars as pl


def _safe_corr(series_a: list[float], series_b: list[float]) -> float | None:
    if len(series_a) < 3 or len(series_b) < 3 or len(series_a) != len(series_b):
        return None
    n = len(series_a)
    mean_a = sum(series_a) / n
    mean_b = sum(series_b) / n
    cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(series_a, series_b))
    var_a = sum((a - mean_a) ** 2 for a in series_a)
    var_b = sum((b - mean_b) ** 2 for b in series_b)
    if var_a <= 0 or var_b <= 0:
        return None
    return cov / math.sqrt(var_a * var_b)


def _rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def cross_sectional_ic(frame: pl.DataFrame, *, field: str, target_field: str) -> tuple[float | None, float | None]:
    clean = frame.select([field, target_field]).drop_nulls()
    if clean.height < 3:
        return None, None
    xs = [float(v) for v in clean[field].to_list()]
    ys = [float(v) for v in clean[target_field].to_list()]
    ic = _safe_corr(xs, ys)
    rank_ic = _safe_corr(_rank(xs), _rank(ys))
    return ic, rank_ic

