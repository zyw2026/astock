from __future__ import annotations

from pydantic import BaseModel, Field


class FactorBucketStat(BaseModel):
    regime: str
    regime_detail: str | None = None
    window_size: int
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


class FactorWhitelistEntry(BaseModel):
    regime: str
    regime_detail: str | None = None
    field: str
    window_hit_count: int
    stable_window_count: int
    best_discovery_score: float
    avg_discovery_score: float
    best_big_move_rate_3d: float
    best_rank_ic_mean: float = 0.0
    avg_rank_ic_mean: float = 0.0
    best_rank_ic_ir: float = 0.0
    monotonic_pass_count: int = 0
    best_monotonic_score: float = 0.0
    whitelist_score: float = 0.0
    status: str
    eligible: bool


class FactorIcResult(BaseModel):
    run_id: str = ""
    regime: str
    regime_detail: str | None = None
    window_size: int
    field: str
    date_count: int
    sample_count: int
    ic_mean: float
    ic_std: float
    rank_ic_mean: float
    rank_ic_std: float
    ic_ir: float
    rank_ic_ir: float


class FactorMonotonicityResult(BaseModel):
    run_id: str = ""
    regime: str
    regime_detail: str | None = None
    window_size: int
    field: str
    quantiles: int
    sample_count: int
    bucket_returns: list[float] = Field(default_factory=list)
    top_bottom_spread: float
    monotonic_direction: str
    monotonic_passed: bool
    eval_score: float


class FactorComboResult(BaseModel):
    combo_id: str
    regime: str
    regime_detail: str | None = None
    window_size: int
    fields: list[str] = Field(default_factory=list)
    match_threshold: float = 0.0
    sample_count: int
    hit_rate_3d: float
    big_move_rate_3d: float
    avg_return_3d: float
    avg_max_return_3d: float
    max_drawdown_3d: float
    discovery_score: float
    lift_vs_single: float


class RuleVariantResult(BaseModel):
    variant_id: str
    combo_id: str
    regime: str
    regime_detail: str | None = None
    logic_id: str
    variant_type: str
    ranking_type: str = "factor_mix"
    sample_count: int
    hit_rate_3d: float
    big_move_rate_3d: float
    avg_return_3d: float
    avg_max_return_3d: float
    max_drawdown_3d: float
    top3_quality_score: float
    top5_quality_score: float
    discovery_score: float


class ReplayQualityResult(BaseModel):
    run_id: str
    logic_id: str
    logic_name: str
    trade_days: int
    top_k: int
    sample_count: int
    hit_rate_3d: float
    big_move_rate_3d: float
    avg_n3d: float
    avg_n3d_max: float
    avg_n3d_dd: float
    topk_quality_score: float
    passed: bool


class DiscoveredLogicCandidate(BaseModel):
    candidate_id: str
    logic_id: str
    logic_name: str
    regime: str
    regime_detail: str | None = None
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
    parent_combo_id: str | None = None
    variant_type: str = "baseline"
    ranking_type: str = "factor_mix"
    lifecycle_state: str = "candidate"
    top3_quality_score: float | None = None
    top5_quality_score: float | None = None
    replay_quality_passed: bool = False
    spec_json: str


class DiscoveryEvalWindowResult(BaseModel):
    eval_run_id: str
    window_id: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    follow_start: str
    follow_end: str
    candidate_count: int
    dual_pass_count: int
    stable_candidate_count: int
    window_status: str


class DiscoveryEvalCandidateResult(BaseModel):
    eval_run_id: str
    window_id: str
    candidate_id: str
    logic_id: str
    regime: str
    discovery_run_id: str
    train_top3_score: float | None = None
    train_top5_score: float | None = None
    test_top3_score: float | None = None
    test_top5_score: float | None = None
    test_hit_3d: float | None = None
    test_big_move_3d: float | None = None
    test_avg_n3d: float | None = None
    test_avg_n3d_max: float | None = None
    test_avg_n3d_dd: float | None = None
    follow_validation_score: float | None = None
    follow_validation_approved: bool = False
    stable_passed: bool = False
    status: str
