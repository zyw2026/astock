from __future__ import annotations

from datetime import date
from uuid import uuid4

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.factor_lab.discovery import (
    analyze_factor_combos,
    analyze_factors,
    analyze_rule_variants,
)
from astock.factor_lab.panel import build_discovery_panel
from astock.storage.duckdb import DuckDbStorage


def run_discovery(
    *,
    start_date: date,
    end_date: date,
    regimes: list[str],
    symbol_limit: int | None = None,
    chunk_size: int | None = None,
    candidate_limit: int | None = None,
    lookback_windows: list[int] | None = None,
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
    factor_profiles = analyze_factors(panel, regimes=regimes, lookback_windows=windows)
    combo_results = analyze_factor_combos(panel, factor_stats=factor_profiles, regimes=regimes)
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
        "combo_count": len(combo_results),
        "variant_count": len(variant_results),
        "replay_quality_count": len(replay_quality),
        "candidate_count": inserted_count,
        "rows": [item.model_dump(mode="json") for item in candidates],
    }
