from __future__ import annotations

from pydantic import BaseModel, Field


class FactorBucketStat(BaseModel):
    regime: str
    field: str
    min_value: float | None = None
    max_value: float | None = None
    sample_count: int
    hit_rate_3d: float
    big_move_rate_3d: float
    avg_return_3d: float
    avg_max_return_3d: float
    max_drawdown_3d: float
    discovery_score: float


class DiscoveredLogicCandidate(BaseModel):
    candidate_id: str
    logic_id: str
    logic_name: str
    regime: str
    sample_count: int
    hit_rate_3d: float
    big_move_rate_3d: float
    avg_return_3d: float
    avg_max_return_3d: float
    max_drawdown_3d: float
    discovery_score: float
    source: str = "factor_lab"
    approved_for_validation: bool = False
    promoted_to_runtime: bool = False
    factor_fields: list[str] = Field(default_factory=list)
    spec_json: str

