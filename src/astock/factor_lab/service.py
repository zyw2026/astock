from __future__ import annotations

from datetime import date
from uuid import uuid4

from astock.app.settings import settings
from astock.connectors.rest_client import AksMcpRestClient
from astock.factor_lab.discovery import discover_logic_candidates
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
    candidates = discover_logic_candidates(
        panel,
        regimes=regimes,
        candidate_limit=candidate_limit or settings.discovery_candidate_limit,
    )
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
    inserted_count = storage.insert_discovered_candidates(
        [item.model_dump() for item in candidates],
        discovery_run_id=discovery_run_id,
    )
    return {
        "discovery_run_id": discovery_run_id,
        "symbol_count": len(symbols),
        "candidate_count": inserted_count,
        "rows": [item.model_dump() for item in candidates],
    }
