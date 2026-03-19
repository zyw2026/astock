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
                pl.col("close").shift(5).over("symbol").alias("close_5d_ago"),
                pl.col("close").shift(10).over("symbol").alias("close_10d_ago"),
                pl.col("high").rolling_max(window_size=3).shift(1).over("symbol").alias("recent_3d_high"),
                pl.col("low").rolling_min(window_size=3).shift(1).over("symbol").alias("recent_3d_low"),
            ]
        )
        .with_columns(
            [
                (((pl.col("close") / pl.col("prev_close")) - 1) * 100).alias("ret_1d"),
                (((pl.col("close") / pl.col("close_5d_ago")) - 1) * 100).alias("ret_5d"),
                (((pl.col("close") / pl.col("close_10d_ago")) - 1) * 100).alias("ret_10d"),
            ]
        )
        .with_columns(
            [
                pl.col("ret_1d").shift(1).over("symbol").alias("prev_ret_1d"),
                (((pl.col("recent_3d_high") - pl.col("close")) / pl.col("recent_3d_high")) * 100).alias("pullback_from_3d_high_pct"),
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


def execute_logic(frame: pl.DataFrame, logic: LogicSpec) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame()
    if logic.logic_id == "trend_pullback":
        filtered = frame.filter(
            (pl.col("ret_5d") >= 12)
            & (pl.col("close") >= pl.col("ma5"))
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("pullback_from_3d_high_pct") >= 0)
            & (pl.col("pullback_from_3d_high_pct") <= 5)
            & (pl.col("ret_1d").abs() <= 3)
        )
        return _base_output(filtered, "trend_pullback_signal", pl.col("ret_5d") - pl.col("pullback_from_3d_high_pct"))
    if logic.logic_id == "leader_first_pullback":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= 7)
            & (pl.col("ret_1d") <= 1)
            & (pl.col("ret_1d") >= -4)
            & (pl.col("ret_5d") >= 15)
            & (pl.col("close") >= pl.col("ma5"))
        )
        return _base_output(filtered, "leader_first_pullback_signal", pl.col("prev_ret_1d") - pl.col("ret_1d").abs())
    if logic.logic_id == "rotation_catchup":
        filtered = frame.filter(
            (pl.col("ret_10d") >= 5)
            & (pl.col("ret_10d") <= 20)
            & (pl.col("ret_1d") >= 0.5)
            & (pl.col("ret_1d") <= 5)
            & (pl.col("close") > pl.col("ma5"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.08)
        )
        return _base_output(filtered, "rotation_catchup_signal", pl.col("ret_10d") - (pl.col("close") / pl.col("ma10") - 1) * 100)
    if logic.logic_id == "rotation_base_breakout":
        filtered = frame.filter(
            (pl.col("ret_10d") >= 2)
            & (pl.col("ret_10d") <= 15)
            & (pl.col("ret_5d") >= 1)
            & (pl.col("ret_5d") <= 8)
            & (pl.col("ret_1d") >= 1)
            & (pl.col("ret_1d") <= 4)
            & (pl.col("close") > pl.col("ma5"))
            & (pl.col("ma5") >= pl.col("ma10"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.05)
            & (pl.col("pullback_from_3d_high_pct") >= 0)
            & (pl.col("pullback_from_3d_high_pct") <= 2.5)
        )
        return _base_output(
            filtered,
            "rotation_base_breakout_signal",
            pl.col("ret_10d") * 0.6 + pl.col("ret_1d") * 2 - (pl.col("close") / pl.col("ma10") - 1) * 100,
        )
    if logic.logic_id == "ma10_reclaim":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") <= -2.5)
            & (pl.col("ret_1d") >= 1.5)
            & (pl.col("ret_1d") <= 5)
            & (pl.col("ret_10d") >= -5)
            & (pl.col("ret_10d") <= 12)
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("close") >= pl.col("ma5") * 0.98)
            & (pl.col("pullback_from_3d_high_pct") <= 6)
        )
        return _base_output(
            filtered,
            "ma10_reclaim_signal",
            pl.col("ret_1d") * 1.5 + pl.col("prev_ret_1d").abs() * 0.8 - pl.col("pullback_from_3d_high_pct"),
        )
    if logic.logic_id == "weak_rotation_dip_absorb":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= -5.5)
            & (pl.col("prev_ret_1d") <= -1.5)
            & (pl.col("ret_1d") >= 0.5)
            & (pl.col("ret_1d") <= 3.2)
            & (pl.col("ret_5d") >= -6)
            & (pl.col("ret_5d") <= 4)
            & (pl.col("pullback_from_3d_high_pct") >= 2)
            & (pl.col("pullback_from_3d_high_pct") <= 7)
            & (pl.col("close") >= pl.col("ma10") * 0.99)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.03)
        )
        return _base_output(
            filtered,
            "weak_rotation_dip_absorb_signal",
            pl.col("prev_ret_1d").abs() * 1.2 + pl.col("ret_1d") * 1.3 - pl.col("pullback_from_3d_high_pct") * 0.6,
        )
    if logic.logic_id == "weak_rotation_failed_break_reclaim":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= -6.5)
            & (pl.col("prev_ret_1d") <= -2.0)
            & (pl.col("ret_1d") >= 1.5)
            & (pl.col("ret_1d") <= 4.5)
            & (pl.col("ret_5d") >= -8)
            & (pl.col("ret_5d") <= 3)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.04)
            & (pl.col("close") >= pl.col("ma10"))
            & (pl.col("pullback_from_3d_high_pct") >= 2)
            & (pl.col("pullback_from_3d_high_pct") <= 8)
        )
        return _base_output(
            filtered,
            "weak_rotation_failed_break_reclaim_signal",
            pl.col("ret_1d") * 1.7 + pl.col("prev_ret_1d").abs() - pl.col("pullback_from_3d_high_pct") * 0.4,
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
    if logic.logic_id == "weak_rotation_tight_range_pop":
        filtered = frame.filter(
            (pl.col("ret_10d") >= -1)
            & (pl.col("ret_10d") <= 8)
            & (pl.col("ret_5d") >= -2.5)
            & (pl.col("ret_5d") <= 3.5)
            & (pl.col("ret_1d") >= 0.8)
            & (pl.col("ret_1d") <= 3.5)
            & (pl.col("prev_ret_1d").abs() <= 2.5)
            & (pl.col("close") >= pl.col("ma5"))
            & (pl.col("close") >= pl.col("ma10"))
            & ((pl.col("close") / pl.col("ma10")) <= 1.03)
            & (pl.col("pullback_from_3d_high_pct") >= 0.5)
            & (pl.col("pullback_from_3d_high_pct") <= 3.5)
        )
        return _base_output(
            filtered,
            "weak_rotation_tight_range_pop_signal",
            pl.col("ret_1d") * 1.3 + pl.col("ret_10d") * 0.5 - pl.col("pullback_from_3d_high_pct") * 0.5,
        )
    if logic.logic_id == "oversold_rebound":
        filtered = frame.filter(
            (pl.col("ret_5d") >= -12)
            & (pl.col("ret_5d") <= -2)
            & (pl.col("ret_10d") >= -20)
            & (pl.col("ret_10d") <= 5)
            & (pl.col("prev_ret_1d") <= -3.5)
            & (pl.col("ret_1d") >= 2)
            & (pl.col("ret_1d") <= 6)
            & (pl.col("close") >= pl.col("recent_3d_low") * 1.02)
            & (pl.col("close") >= pl.col("ma5") * 0.97)
        )
        return _base_output(
            filtered,
            "oversold_rebound_signal",
            pl.col("ret_1d") * 1.5 - pl.col("ret_5d").abs() * 0.2 + ((pl.col("close") / pl.col("recent_3d_low")) - 1) * 100,
        )
    if logic.logic_id == "limit_up_repair":
        filtered = frame.filter(
            (pl.col("prev_ret_1d") >= 9.5)
            & (pl.col("ret_1d") <= 2)
            & (pl.col("ret_1d") >= -4)
            & (pl.col("close") >= pl.col("prev_close") * 0.96)
            & (pl.col("close") > pl.col("ma10"))
        )
        return _base_output(filtered, "limit_up_repair_signal", pl.col("prev_ret_1d") - pl.col("ret_1d").abs())
    return pl.DataFrame()
