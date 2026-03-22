from __future__ import annotations

from datetime import date
from uuid import uuid4

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.factor_lab.discovery import (
    analyze_factor_combos,
    analyze_factors,
    analyze_rule_variants,
    build_factor_whitelist,
)
from astock.factor_lab.factor_eval import evaluate_factors
from astock.factor_lab.panel import build_discovery_panel
from astock.storage.duckdb import DuckDbStorage


def run_factor_evaluation(
    *,
    start_date: date,
    end_date: date,
    regimes: list[str],
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    lookback_windows: list[int] | None = None,
    quantiles: int = 5,
    factor_fields: list[str] | None = None,
) -> dict:
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    panel, symbols = build_discovery_panel(
        client,
        start_date=start_date,
        end_date=end_date,
        symbol_limit=symbol_limit or settings.default_symbol_limit,
        chunk_size=chunk_size or settings.default_chunk_size,
    )
    windows = lookback_windows or [20, 40, 60]
    ic_rows, monotonicity_rows = evaluate_factors(
        panel,
        regimes=regimes,
        lookback_windows=windows,
        quantiles=quantiles,
        factor_fields=factor_fields,
    )
    run_id = uuid4().hex
    storage.insert_factor_ic_results([item.model_dump(mode="json") for item in ic_rows], run_id=run_id)
    storage.insert_factor_monotonicity_results([item.model_dump(mode="json") for item in monotonicity_rows], run_id=run_id)
    return {
        "run_id": run_id,
        "symbol_count": len(symbols),
        "ic_count": len(ic_rows),
        "monotonicity_count": len(monotonicity_rows),
        "ic_rows": [item.model_dump(mode="json") for item in ic_rows],
        "monotonicity_rows": [item.model_dump(mode="json") for item in monotonicity_rows],
    }


def run_discovery(
    *,
    start_date: date,
    end_date: date,
    regimes: list[str],
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    candidate_limit: int | None = None,
    lookback_windows: list[int] | None = None,
    factor_fields: list[str] | None = None,
) -> dict:
    client = AksMcpRestClient()
    storage = DuckDbStorage()
    storage.initialize()
    panel, symbols = build_discovery_panel(
        client,
        start_date=start_date,
        end_date=end_date,
        symbol_limit=symbol_limit or settings.default_symbol_limit,
        chunk_size=chunk_size or settings.default_chunk_size,
    )
    windows = lookback_windows or [20, 40, 60]
    factor_profiles = analyze_factors(panel, regimes=regimes, lookback_windows=windows, factor_fields=factor_fields)
    ic_rows, monotonicity_rows = evaluate_factors(
        panel,
        regimes=regimes,
        lookback_windows=windows,
        factor_fields=factor_fields,
    )
    whitelist = build_factor_whitelist(
        factor_profiles,
        ic_results=ic_rows,
        monotonicity_results=monotonicity_rows,
        regimes=regimes,
    )
    combo_results = analyze_factor_combos(panel, factor_stats=factor_profiles, regimes=regimes, whitelist=whitelist)
    variant_results, candidates, replay_quality = analyze_rule_variants(panel, combos=combo_results, factor_stats=factor_profiles)
    candidates = candidates[: candidate_limit or settings.discovery_candidate_limit]

    discovery_run_id = uuid4().hex
    storage.insert_discovery_run(
        discovery_run_id=discovery_run_id,
        window_start=start_date.isoformat(),
        window_end=end_date.isoformat(),
        regimes=regimes,
        symbol_count=len(symbols),
        candidate_count=len(candidates),
        notes="factor_lab discovery",
    )
    storage.insert_factor_profiles([item.model_dump(mode="json") for item in factor_profiles], run_id=discovery_run_id)
    storage.insert_factor_ic_results([item.model_dump(mode="json") for item in ic_rows], run_id=discovery_run_id)
    storage.insert_factor_monotonicity_results([item.model_dump(mode="json") for item in monotonicity_rows], run_id=discovery_run_id)
    storage.insert_factor_whitelist([item.model_dump(mode="json") for item in whitelist], run_id=discovery_run_id)
    storage.insert_factor_combo_results([item.model_dump(mode="json") for item in combo_results], run_id=discovery_run_id)
    storage.insert_rule_variant_results([item.model_dump(mode="json") for item in variant_results], run_id=discovery_run_id)
    storage.insert_replay_quality_results([item.model_dump(mode="json") for item in replay_quality], run_id=discovery_run_id)
    inserted_count = storage.insert_discovered_candidates(
        [item.model_dump(mode="json") for item in candidates],
        discovery_run_id=discovery_run_id,
    )
    return {
        "discovery_run_id": discovery_run_id,
        "symbol_count": len(symbols),
        "factor_profile_count": len(factor_profiles),
        "factor_ic_count": len(ic_rows),
        "factor_monotonicity_count": len(monotonicity_rows),
        "whitelist_count": len(whitelist),
        "combo_count": len(combo_results),
        "variant_count": len(variant_results),
        "replay_quality_count": len(replay_quality),
        "candidate_count": inserted_count,
        "factor_fields": factor_fields or [],
        "whitelist_rows": [item.model_dump(mode="json") for item in whitelist],
        "combo_rows": [item.model_dump(mode="json") for item in combo_results],
        "variant_rows": [item.model_dump(mode="json") for item in variant_results],
        "replay_quality_rows": [item.model_dump(mode="json") for item in replay_quality],
        "rows": [item.model_dump(mode="json") for item in candidates],
    }
