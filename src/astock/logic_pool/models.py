from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


MarketRegime = Literal["trend", "rotation", "weak_rotation", "panic"]


class LogicSpec(BaseModel):
    logic_id: str
    name: str
    description: str
    regime_whitelist: list[MarketRegime] = Field(default_factory=list)
    required_datasets: list[str] = Field(default_factory=list)
    ranking_rule: str | None = None
    holding_days: int = 3
    max_candidates_per_day: int = 10
    entry_rule: dict = Field(default_factory=dict)
    exit_rule: dict = Field(default_factory=dict)
    invalid_rule: dict = Field(default_factory=dict)
