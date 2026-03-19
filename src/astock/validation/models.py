from __future__ import annotations

from pydantic import BaseModel


class LogicSignalHit(BaseModel):
    logic_id: str
    trade_date: str
    symbol: str
    regime: str
    trigger_score: float | None = None
    trigger_reason: str | None = None
    entry_price: float | None = None
    next_1d_return: float | None = None
    next_2d_return: float | None = None
    next_3d_return: float | None = None
    next_3d_max_return: float | None = None
    next_5d_return: float | None = None
    max_drawdown_3d: float | None = None
    max_drawdown: float | None = None


class LogicValidationResult(BaseModel):
    logic_id: str
    regime: str
    sample_count: int
    hit_rate_1d: float | None = None
    hit_rate_3d: float | None = None
    big_move_rate_3d: float | None = None
    avg_return_1d: float | None = None
    avg_return_2d: float | None = None
    avg_return_3d: float | None = None
    avg_max_return_3d: float | None = None
    avg_return_5d: float | None = None
    profit_drawdown_ratio: float | None = None
    max_drawdown_3d: float | None = None
    max_drawdown: float | None = None
    reliability_score: float | None = None


class LogicReliabilitySnapshot(BaseModel):
    logic_id: str
    regime: str
    reliability_score: float
    approved: bool
    sample_count: int
