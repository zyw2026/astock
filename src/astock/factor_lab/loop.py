from __future__ import annotations

from datetime import date
from uuid import uuid4

from astock.factor_lab.discovery import FACTOR_SPECS
from astock.factor_lab.service import run_discovery
from astock.storage.duckdb import DuckDbStorage


CORE_LOOP_FIELDS: tuple[str, ...] = (
    "pullback_from_5d_high_pct",
    "intraday_range_pct",
    "industry_strong_rate",
    "excess_ret_1d",
    "excess_body_pct",
)


def _factor_category(field: str) -> str:
    if "industry" in field or "excess" in field:
        return "industry_relative"
    if "volume" in field or "turnover" in field or "range_expansion" in field:
        return "volume_price"
    if "breakout" in field or "gap" in field:
        return "breakout"
    return "price"


def seed_factor_pool() -> dict:
    storage = DuckDbStorage()
    storage.initialize()
    rows = []
    for idx, spec in enumerate(FACTOR_SPECS, start=1):
        rows.append(
            {
                "factor_id": f"factor_{spec['field']}",
                "field": spec["field"],
                "label": spec["label"],
                "category": _factor_category(spec["field"]),
                "source": "manual",
                "status": "pending",
                "priority": max(100 - idx, 1),
                "notes": None,
            }
        )
    inserted = storage.seed_factor_candidate_pool(rows)
    return {
        "seed_count": len(rows),
        "inserted_count": inserted,
    }


def build_factor_batch_plan(*, batch_size: int) -> list[str]:
    storage = DuckDbStorage()
    rows = storage.list_factor_candidate_pool(statuses=["pending", "testing"], limit=batch_size)
    return [row["field"] for row in rows if row["field"] not in CORE_LOOP_FIELDS]


def _factor_result_rows(
    *,
    loop_run_id: str,
    iteration_no: int,
    factor_batch: list[str],
    whitelist_rows: list[dict],
) -> list[dict]:
    best_by_field: dict[str, dict] = {}
    for row in whitelist_rows:
        field = row["field"]
        if field not in factor_batch:
            continue
        previous = best_by_field.get(field)
        key = (
            1 if row.get("eligible") else 0,
            row.get("whitelist_score") or 0,
            row.get("best_rank_ic_mean") or 0,
            row.get("best_monotonic_score") or 0,
            row.get("best_discovery_score") or 0,
        )
        if previous is None or key > previous["_key"]:
            best_by_field[field] = {
                "_key": key,
                "field": field,
                "whitelist_status": row["status"],
                "best_rank_ic_mean": row.get("best_rank_ic_mean"),
                "best_rank_ic_ir": row.get("best_rank_ic_ir"),
                "best_monotonic_score": row.get("best_monotonic_score"),
                "best_discovery_score": row.get("best_discovery_score"),
                "result_status": "approved" if row.get("eligible") else "rejected",
            }
    results = []
    for field in factor_batch:
        row = best_by_field.get(
            field,
            {
                "field": field,
                "whitelist_status": "invalid",
                "best_rank_ic_mean": 0.0,
                "best_rank_ic_ir": 0.0,
                "best_monotonic_score": 0.0,
                "best_discovery_score": 0.0,
                "result_status": "rejected",
            },
        )
        results.append(
            {
                "loop_run_id": loop_run_id,
                "iteration_no": iteration_no,
                **{k: v for k, v in row.items() if not k.startswith("_")},
            }
        )
    return results


def _factor_status_updates(factor_rows: list[dict]) -> list[dict]:
    updates = []
    for row in factor_rows:
        if row["result_status"] == "approved":
            status = "approved"
            notes = "eligible in whitelist"
        elif (row.get("best_rank_ic_mean") or 0) >= 0.06 or (row.get("best_monotonic_score") or 0) >= 70:
            status = "testing"
            notes = "partial evidence only"
        else:
            status = "rejected"
            notes = "insufficient factor evidence"
        updates.append({"field": row["field"], "status": status, "notes": notes})
    return updates


def run_discovery_loop_iteration(
    *,
    loop_run_id: str,
    iteration_no: int,
    start_date: date,
    end_date: date,
    regimes: list[str],
    symbol_limit: int,
    chunk_size: int,
    candidate_limit: int,
    factor_batch: list[str],
) -> dict:
    storage = DuckDbStorage()
    active_fields = list(dict.fromkeys([*CORE_LOOP_FIELDS, *factor_batch]))
    result = run_discovery(
        start_date=start_date,
        end_date=end_date,
        regimes=regimes,
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        candidate_limit=candidate_limit,
        factor_fields=active_fields,
    )
    whitelist_rows = result.get("whitelist_rows", [])
    factor_rows = _factor_result_rows(
        loop_run_id=loop_run_id,
        iteration_no=iteration_no,
        factor_batch=factor_batch,
        whitelist_rows=whitelist_rows,
    )
    storage.insert_discovery_loop_factor_results(factor_rows)
    storage.update_factor_candidate_statuses(_factor_status_updates(factor_rows))
    replay_pass_count = sum(1 for row in result.get("rows", []) if row.get("replay_quality_passed"))
    runtime_promoted_count = storage.promote_discovered_candidates(latest_approved=True, limit=candidate_limit)
    lifecycle = storage.apply_candidate_lifecycle_for_run(discovery_run_id=result["discovery_run_id"])
    watch_count = lifecycle["watch_count"]
    retired_count = lifecycle["retired_count"]
    whitelist_pass_count = sum(1 for row in factor_rows if row["result_status"] == "approved")
    status = "runtime" if runtime_promoted_count > 0 else "watch" if watch_count > 0 else "fail"
    storage.insert_discovery_loop_iteration(
        {
            "loop_run_id": loop_run_id,
            "iteration_no": iteration_no,
            "factor_batch": factor_batch,
            "candidate_factor_count": len(factor_batch),
            "whitelist_pass_count": whitelist_pass_count,
            "combo_count": result["combo_count"],
            "variant_count": result["variant_count"],
            "replay_pass_count": replay_pass_count,
            "runtime_promoted_count": runtime_promoted_count,
            "watch_count": watch_count,
            "retired_count": retired_count,
            "status": status,
        }
    )
    return {
        "iteration_no": iteration_no,
        "factor_batch": factor_batch,
        "candidate_factor_count": len(factor_batch),
        "whitelist_pass_count": whitelist_pass_count,
        "combo_count": result["combo_count"],
        "variant_count": result["variant_count"],
        "replay_pass_count": replay_pass_count,
        "runtime_promoted_count": runtime_promoted_count,
        "watch_count": watch_count,
        "retired_count": retired_count,
        "status": status,
        "discovery_run_id": result["discovery_run_id"],
    }


def run_auto_discovery_loop(
    *,
    start_date: date,
    end_date: date,
    regimes: list[str],
    symbol_limit: int,
    chunk_size: int,
    candidate_limit: int,
    batch_size: int,
    max_iterations: int,
    target_runtime_candidates: int,
    max_stagnation_iterations: int,
) -> dict:
    storage = DuckDbStorage()
    storage.initialize()
    if not storage.list_factor_candidate_pool(statuses=["pending", "testing", "approved", "rejected"], limit=1):
        seed_factor_pool()
    loop_run_id = uuid4().hex
    storage.insert_discovery_loop_run(
        loop_run_id=loop_run_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        regimes=regimes,
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        status="running",
    )
    iteration_rows: list[dict] = []
    total_runtime = 0
    stagnation = 0
    final_status = "completed"
    for iteration_no in range(1, max_iterations + 1):
        factor_batch = build_factor_batch_plan(batch_size=batch_size)
        if not factor_batch:
            final_status = "depleted"
            break
        row = run_discovery_loop_iteration(
            loop_run_id=loop_run_id,
            iteration_no=iteration_no,
            start_date=start_date,
            end_date=end_date,
            regimes=regimes,
            symbol_limit=symbol_limit,
            chunk_size=chunk_size,
            candidate_limit=candidate_limit,
            factor_batch=factor_batch,
        )
        iteration_rows.append(row)
        total_runtime += row["runtime_promoted_count"]
        stagnation = stagnation + 1 if row["runtime_promoted_count"] == 0 else 0
        if total_runtime >= target_runtime_candidates:
            final_status = "target_reached"
            break
        if stagnation >= max_stagnation_iterations:
            final_status = "stagnated"
            break
    storage.complete_discovery_loop_run(loop_run_id=loop_run_id, status=final_status)
    return {
        "loop_run_id": loop_run_id,
        "status": final_status,
        "iteration_count": len(iteration_rows),
        "runtime_promoted_total": total_runtime,
        "rows": iteration_rows,
    }
