from __future__ import annotations

import polars as pl

from astock.logic_pool.models import LogicSpec


def enrich_feature_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return (
        frame.sort(["symbol", "trade_date"])
        .with_columns(
            [
                pl.col("close").shift(1).over("symbol").alias("prev_close"),
                pl.col("open").shift(1).over("symbol").alias("prev_open"),
                pl.col("close").shift(3).over("symbol").alias("close_3d_ago"),
                pl.col("close").shift(5).over("symbol").alias("close_5d_ago"),
                pl.col("close").shift(10).over("symbol").alias("close_10d_ago"),
                pl.col("high").rolling_max(window_size=3).shift(1).over("symbol").alias("recent_3d_high"),
                pl.col("low").rolling_min(window_size=3).shift(1).over("symbol").alias("recent_3d_low"),
                pl.col("high").rolling_max(window_size=5).shift(1).over("symbol").alias("recent_5d_high"),
                pl.col("low").rolling_min(window_size=5).shift(1).over("symbol").alias("recent_5d_low"),
                pl.col("volume").rolling_mean(window_size=5).shift(1).over("symbol").alias("avg_volume_5d"),
                pl.col("turnover").rolling_mean(window_size=5).shift(1).over("symbol").alias("avg_turnover_5d"),
                ((((pl.col("high") - pl.col("low")) / pl.col("close")) * 100))
                .rolling_mean(window_size=5)
                .shift(1)
                .over("symbol")
                .alias("avg_intraday_range_5d"),
                pl.col("close").rolling_mean(window_size=20).over("symbol").alias("ma20"),
            ]
        )
        .with_columns(
            [
                (((pl.col("close") / pl.col("prev_close")) - 1) * 100).alias("ret_1d"),
                (((pl.col("close") / pl.col("close_3d_ago")) - 1) * 100).alias("ret_3d"),
                (((pl.col("close") / pl.col("close_5d_ago")) - 1) * 100).alias("ret_5d"),
                (((pl.col("close") / pl.col("close_10d_ago")) - 1) * 100).alias("ret_10d"),
            ]
        )
        .with_columns(
            [
                pl.col("ret_1d").shift(1).over("symbol").alias("prev_ret_1d"),
                (((pl.col("recent_3d_high") - pl.col("close")) / pl.col("recent_3d_high")) * 100).alias("pullback_from_3d_high_pct"),
                (((pl.col("recent_5d_high") - pl.col("close")) / pl.col("recent_5d_high")) * 100).alias("pullback_from_5d_high_pct"),
                (((pl.col("close") / pl.col("ma5")) - 1) * 100).alias("close_vs_ma5_pct"),
                (((pl.col("close") / pl.col("ma10")) - 1) * 100).alias("close_vs_ma10_pct"),
                (((pl.col("ma5") / pl.col("ma10")) - 1) * 100).alias("ma5_vs_ma10_pct"),
                ((((pl.col("high") - pl.col("low")) / pl.col("close")) * 100)).alias("intraday_range_pct"),
                ((((pl.col("close") - pl.col("open")) / pl.col("open")) * 100)).alias("body_pct"),
                (((pl.col("open") / pl.col("prev_close")) - 1) * 100).alias("gap_pct"),
                (((pl.col("close") / pl.col("recent_5d_high")) - 1) * 100).alias("breakout_vs_5d_high_pct"),
                (((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + 1e-6)) * 100).alias("close_in_day_range_pct"),
                (pl.col("volume") / (pl.col("avg_volume_5d") + 1e-6)).alias("volume_ratio_5d"),
                (pl.col("turnover") / (pl.col("avg_turnover_5d") + 1e-6)).alias("turnover_ratio_5d"),
                ((((pl.col("high") - pl.col("low")) / pl.col("close")) * 100) / (pl.col("avg_intraday_range_5d") + 1e-6)).alias("range_expansion_5d"),
                pl.col("high").shift(-1).over("symbol").alias("future_high_1d"),
                pl.col("low").shift(-1).over("symbol").alias("future_low_1d"),
                pl.max_horizontal(
                    [pl.col("high").shift(-i).over("symbol") for i in range(1, 3)]
                ).alias("future_max_high_2d"),
                pl.min_horizontal(
                    [pl.col("low").shift(-i).over("symbol") for i in range(1, 3)]
                ).alias("future_min_low_2d"),
                pl.min_horizontal(
                    [pl.col("low").shift(-i).over("symbol") for i in range(1, 4)]
                ).alias("future_min_low_3d"),
                pl.max_horizontal(
                    [pl.col("high").shift(-i).over("symbol") for i in range(1, 4)]
                ).alias("future_max_high_3d"),
                pl.min_horizontal(
                    [pl.col("low").shift(-i).over("symbol") for i in range(1, 6)]
                ).alias("future_min_low_5d"),
                pl.col("close").shift(-1).over("symbol").alias("future_close_1d"),
                pl.col("close").shift(-2).over("symbol").alias("future_close_2d"),
                pl.col("close").shift(-3).over("symbol").alias("future_close_3d"),
                pl.col("close").shift(-5).over("symbol").alias("future_close_5d"),
            ]
        )
        .with_columns(
            [
                (((pl.col("future_close_1d") / pl.col("close")) - 1) * 100).alias("next_1d_return"),
                (((pl.col("future_close_2d") / pl.col("close")) - 1) * 100).alias("next_2d_return"),
                (((pl.col("future_close_3d") / pl.col("close")) - 1) * 100).alias("next_3d_return"),
                (((pl.col("future_high_1d") / pl.col("close")) - 1) * 100).alias("next_1d_max_return"),
                (((pl.col("future_max_high_2d") / pl.col("close")) - 1) * 100).alias("next_2d_max_return"),
                (((pl.col("future_low_1d") / pl.col("close")) - 1) * 100).alias("max_drawdown_1d"),
                (((pl.col("future_min_low_2d") / pl.col("close")) - 1) * 100).alias("max_drawdown_2d"),
                (((pl.col("future_max_high_3d") / pl.col("close")) - 1) * 100).alias("next_3d_max_return"),
                (((pl.col("future_min_low_3d") / pl.col("close")) - 1) * 100).alias("max_drawdown_3d"),
                (((pl.col("future_close_5d") / pl.col("close")) - 1) * 100).alias("next_5d_return"),
                (((pl.col("future_min_low_5d") / pl.col("close")) - 1) * 100).alias("max_drawdown"),
            ]
        )
    )


def _base_output(frame: pl.DataFrame, reason: str, score_expr: pl.Expr) -> pl.DataFrame:
    return frame.select(
        [
            "trade_date",
            "symbol",
            pl.col("close").alias("entry_price"),
            score_expr.alias("trigger_score"),
            pl.lit(reason).alias("trigger_reason"),
            "next_1d_return",
            "next_2d_return",
            "next_3d_return",
            "next_1d_max_return",
            "next_2d_max_return",
            "max_drawdown_1d",
            "max_drawdown_2d",
            "next_3d_max_return",
            "next_5d_return",
            "max_drawdown_3d",
            "max_drawdown",
        ]
    )


def _apply_generic_conditions(frame: pl.DataFrame, conditions: list[dict]) -> pl.DataFrame:
    filtered = frame
    for condition in conditions:
        field = condition["field"]
        op = condition.get("op", "between")
        if op == "between":
            min_value = condition.get("min")
            max_value = condition.get("max")
            expr = pl.lit(True)
            if min_value is not None:
                expr = expr & (pl.col(field) >= min_value)
            if max_value is not None:
                expr = expr & (pl.col(field) <= max_value)
            filtered = filtered.filter(expr)
        elif op == "gte":
            filtered = filtered.filter(pl.col(field) >= condition["value"])
        elif op == "lte":
            filtered = filtered.filter(pl.col(field) <= condition["value"])
        elif op == "gt":
            filtered = filtered.filter(pl.col(field) > condition["value"])
        elif op == "lt":
            filtered = filtered.filter(pl.col(field) < condition["value"])
        else:
            raise ValueError(f"unsupported generic condition op: {op}")
    return filtered


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


def _generic_score_expr(score_weights: list[dict]) -> pl.Expr:
    if not score_weights:
        return pl.lit(0.0)
    expr = pl.lit(0.0)
    for item in score_weights:
        expr = expr + pl.col(item["field"]) * float(item.get("weight", 1.0))
    return expr


def _execute_generic_logic(frame: pl.DataFrame, logic: LogicSpec) -> pl.DataFrame:
    conditions = logic.entry_rule.get("conditions", [])
    soft_conditions = logic.entry_rule.get("soft_conditions", [])
    match_threshold = logic.entry_rule.get("match_threshold")
    score_weights = logic.entry_rule.get("score_weights", [])
    signal_name = logic.entry_rule.get("signal_name", f"{logic.logic_id}_signal")
    filtered = _apply_generic_conditions(frame, conditions)
    if soft_conditions:
        filtered = filtered.with_columns(_soft_match_expr(soft_conditions).alias("_match_score"))
        if match_threshold is not None:
            filtered = filtered.filter(pl.col("_match_score") >= float(match_threshold))
    if filtered.is_empty():
        return pl.DataFrame()
    return _base_output(filtered, signal_name, _generic_score_expr(score_weights))


def execute_logic(frame: pl.DataFrame, logic: LogicSpec) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame()
    if logic.source == "factor_lab" or logic.entry_rule.get("conditions"):
        return _execute_generic_logic(frame, logic)
    if logic.logic_id == "trend_pullback":
        filtered = frame.filter(
            (pl.col("ret_5d") >= 10)
            & (pl.col("close") >= pl.col("ma5"))
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("pullback_from_3d_high_pct") >= 0)
            & (pl.col("pullback_from_3d_high_pct") <= 5.5)
            & (pl.col("ret_1d").abs() <= 3.5)
        )
        return _base_output(filtered, "trend_pullback_signal", pl.col("ret_5d") - pl.col("pullback_from_3d_high_pct"))
    if logic.logic_id == "leader_first_pullback":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= 5.0)
            & (pl.col("prev_ret_1d") <= 9.9)
            & (pl.col("ret_1d") >= -2.5)
            & (pl.col("ret_1d") <= 2.0)
            & (pl.col("ret_10d") >= 8.0)
            & (pl.col("ret_10d") <= 20.0)
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("pullback_from_3d_high_pct") >= 1.0)
            & (pl.col("pullback_from_3d_high_pct") <= 6.0)
        )
        return _base_output(
            filtered,
            "leader_first_pullback_signal",
            pl.col("prev_ret_1d") * 0.8 - pl.col("ret_1d").abs() * 0.8 - pl.col("pullback_from_3d_high_pct") * 0.3,
        )
    if logic.logic_id == "rotation_catchup":
        filtered = frame.filter(
            (pl.col("ret_10d") >= 4)
            & (pl.col("ret_10d") <= 14)
            & (pl.col("ret_5d") >= 0)
            & (pl.col("ret_5d") <= 7)
            & (pl.col("prev_ret_1d") <= 2.5)
            & (pl.col("ret_1d") >= 0.8)
            & (pl.col("ret_1d") <= 3.5)
            & (pl.col("close") > pl.col("ma5"))
            & (pl.col("ma5") >= pl.col("ma10"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.05)
            & (pl.col("pullback_from_3d_high_pct") >= 0)
            & (pl.col("pullback_from_3d_high_pct") <= 3.0)
        )
        return _base_output(
            filtered,
            "rotation_catchup_signal",
            pl.col("ret_10d") * 0.6
            + pl.col("ret_5d") * 0.8
            + pl.col("ret_1d") * 1.2
            - (pl.col("close") / pl.col("ma10") - 1) * 150
            - pl.col("pullback_from_3d_high_pct") * 0.7,
        )
    if logic.logic_id == "rotation_base_breakout":
        filtered = frame.filter(
            (pl.col("ret_10d") >= 3)
            & (pl.col("ret_10d") <= 13)
            & (pl.col("ret_5d") >= 1)
            & (pl.col("ret_5d") <= 6)
            & (pl.col("prev_ret_1d") <= 2.5)
            & (pl.col("ret_1d") >= 1)
            & (pl.col("ret_1d") <= 3.5)
            & (pl.col("close") > pl.col("ma5"))
            & (pl.col("ma5") >= pl.col("ma10"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.04)
            & (pl.col("pullback_from_3d_high_pct") >= 0)
            & (pl.col("pullback_from_3d_high_pct") <= 2.0)
        )
        return _base_output(
            filtered,
            "rotation_base_breakout_signal",
            pl.col("ret_10d") * 0.6
            + pl.col("ret_5d") * 0.5
            + pl.col("ret_1d") * 1.8
            - (pl.col("close") / pl.col("ma10") - 1) * 120
            - pl.col("pullback_from_3d_high_pct") * 0.6,
        )
    if logic.logic_id == "ma10_reclaim":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= -4.5)
            & (pl.col("prev_ret_1d") <= -1.5)
            & (pl.col("ret_1d") >= 1.2)
            & (pl.col("ret_1d") <= 3.8)
            & (pl.col("ret_5d") >= -3.0)
            & (pl.col("ret_5d") <= 5.0)
            & (pl.col("ret_10d") >= -2)
            & (pl.col("ret_10d") <= 8)
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("close") >= pl.col("ma5") * 0.985)
            & ((pl.col("close") / pl.col("ma10")) <= 1.035)
            & (pl.col("pullback_from_3d_high_pct") >= 0.5)
            & (pl.col("pullback_from_3d_high_pct") <= 4.0)
        )
        return _base_output(
            filtered,
            "ma10_reclaim_signal",
            pl.col("ret_1d") * 1.4
            + pl.col("prev_ret_1d").abs() * 0.6
            - (pl.col("close") / pl.col("ma10") - 1) * 100
            - pl.col("pullback_from_3d_high_pct") * 0.5,
        )
    if logic.logic_id == "weak_rotation_dip_absorb":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= -5.5)
            & (pl.col("prev_ret_1d") <= -1.5)
            & (pl.col("ret_1d") >= 0.5)
            & (pl.col("ret_1d") <= 3.5)
            & (pl.col("ret_5d") >= -6)
            & (pl.col("ret_5d") <= 4)
            & (pl.col("pullback_from_3d_high_pct") >= 1.5)
            & (pl.col("pullback_from_3d_high_pct") <= 7)
            & (pl.col("close") >= pl.col("ma10") * 0.99)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.02)
        )
        return _base_output(
            filtered,
            "weak_rotation_dip_absorb_signal",
            pl.col("prev_ret_1d").abs() * 1.2 + pl.col("ret_1d") * 1.3 - pl.col("pullback_from_3d_high_pct") * 0.6,
        )
    if logic.logic_id == "weak_rotation_failed_break_reclaim":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= -6.0)
            & (pl.col("prev_ret_1d") <= -2.0)
            & (pl.col("ret_1d") >= 1.0)
            & (pl.col("ret_1d") <= 4.0)
            & (pl.col("ret_5d") >= -6)
            & (pl.col("ret_5d") <= 2)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.02)
            & (pl.col("close") >= pl.col("ma10") * 0.995)
            & (pl.col("pullback_from_3d_high_pct") >= 1.5)
            & (pl.col("pullback_from_3d_high_pct") <= 7)
        )
        return _base_output(
            filtered,
            "weak_rotation_failed_break_reclaim_signal",
            pl.col("ret_1d") * 1.5 + pl.col("prev_ret_1d").abs() * 0.8 - pl.col("pullback_from_3d_high_pct") * 0.4,
        )
    if logic.logic_id == "fund_flow_reversal":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") <= -3)
            & (pl.col("ret_1d") >= 2)
            & (pl.col("close") > pl.col("ma5"))
            & (pl.col("ret_5d") >= -8)
            & (pl.col("ret_5d") <= 8)
        )
        return _base_output(filtered, "fund_flow_reversal_proxy_signal", pl.col("ret_1d") - pl.col("prev_ret_1d").abs() * 0.3)
    if logic.logic_id == "weak_rotation_flat_reclaim":
        filtered = frame.filter(
            (pl.col("ret_10d") >= -2)
            & (pl.col("ret_10d") <= 10)
            & (pl.col("ret_5d") >= -3)
            & (pl.col("ret_5d") <= 4)
            & (pl.col("ret_1d") >= 1)
            & (pl.col("ret_1d") <= 4)
            & (pl.col("prev_ret_1d") <= -0.5)
            & (pl.col("close") >= pl.col("ma10"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.04)
            & (pl.col("pullback_from_3d_high_pct") >= 1)
            & (pl.col("pullback_from_3d_high_pct") <= 5)
        )
        return _base_output(
            filtered,
            "weak_rotation_flat_reclaim_signal",
            pl.col("ret_1d") * 1.4 + pl.col("ret_10d") * 0.4 - (pl.col("close") / pl.col("ma10") - 1) * 100,
        )
    if logic.logic_id == "oversold_rebound":
        filtered = frame.filter(
            (pl.col("ret_5d") >= -10)
            & (pl.col("ret_5d") <= -3)
            & (pl.col("ret_10d") >= -18)
            & (pl.col("ret_10d") <= 2)
            & (pl.col("prev_ret_1d") <= -2.5)
            & (pl.col("ret_1d") >= 1.5)
            & (pl.col("ret_1d") <= 5)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.015)
            & (pl.col("close") >= pl.col("ma5") * 0.965)
        )
        return _base_output(
            filtered,
            "oversold_rebound_signal",
            pl.col("ret_1d") * 1.5 - pl.col("ret_5d").abs() * 0.2 + ((pl.col("close") / pl.col("recent_3d_low")) - 1) * 100,
        )
    if logic.logic_id == "limit_up_repair":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= 7.5)
            & (pl.col("prev_ret_1d") <= 11.0)
            & (pl.col("ret_1d") >= -3.5)
            & (pl.col("ret_1d") <= 1.5)
            & (pl.col("close") >= pl.col("prev_close") * 0.96)
            & (pl.col("close") >= pl.col("ma10"))
        )
        return _base_output(
            filtered,
            "limit_up_repair_signal",
            pl.col("prev_ret_1d") * 0.8 - pl.col("ret_1d").abs() - (1 - pl.col("close") / pl.col("prev_close")) * 100,
        )
    return pl.DataFrame()
