from __future__ import annotations

from datetime import date, timedelta

import typer
from rich.console import Console
from rich.table import Table

from astock.app.settings import settings

app = typer.Typer(help="astock research and stock-selection engine")
console = Console()


def _parse_date_arg(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _parse_date_list_arg(value: str) -> list[date]:
    return [date.fromisoformat(item.strip()) for item in value.split(",") if item.strip()]


@app.command()
def info() -> None:
    """Show project intent and current phase."""
    console.print("astock: independent logic-research and stock-selection project")
    console.print("phase: executable project scaffold")
    console.print(f"aks-mcp base url: {settings.aks_mcp_base_url}")
    console.print(f"local db path: {settings.local_db_path}")


@app.command("init-db")
def init_db() -> None:
    """Initialize local DuckDB research storage."""
    from astock.storage.duckdb import DuckDbStorage

    path = DuckDbStorage().initialize()
    console.print(f"initialized local storage: {path}")


@app.command("list-logics")
def list_logics() -> None:
    """List built-in logic candidates."""
    from astock.logic_pool.defaults import build_default_registry

    registry = build_default_registry()
    table = Table(title="Built-in Logic Candidates")
    table.add_column("logic_id")
    table.add_column("name")
    table.add_column("source")
    table.add_column("regimes")
    table.add_column("holding_days", justify="right")
    for spec in registry.all():
        table.add_row(spec.logic_id, spec.name, spec.source, ",".join(spec.regime_whitelist), str(spec.holding_days))
    console.print(table)


@app.command("ping-source")
def ping_source() -> None:
    """Probe the deployed aks-mcp service."""
    from astock.connectors.rest_client import AksMcpRestClient

    client = AksMcpRestClient()
    ok = client.readyz()
    console.print(f"aks-mcp readyz: {ok}")


@app.command("show-market")
def show_market(top_n: int = 3) -> None:
    """Fetch a compact market overview from aks-mcp."""
    from astock.connectors.rest_client import AksMcpRestClient

    client = AksMcpRestClient()
    payload = client.market_overview(top_n=top_n)
    rows = payload.get("rows", [])
    if not rows:
        console.print("no market overview rows returned")
        raise typer.Exit(code=1)
    row = rows[0]
    console.print(f"anchor trade date: {row.get('time_context', {}).get('anchor_trade_date')}")
    console.print(f"warning: {payload.get('warning')}")
    console.print(f"indices: {len(row.get('latest_index_snapshots', []))}")
    console.print(f"industry leaders: {len(row.get('industry_fund_flow_leaders', []))}")


@app.command("show-regime")
def show_regime(trade_date: str | None = None) -> None:
    """Show current market regime and evidence."""
    from astock.selection.service import detect_current_regime

    result = detect_current_regime(trade_date=_parse_date_arg(trade_date))
    console.print(f"trade_date: {result['trade_date']}")
    console.print(f"regime: {result['regime']}")
    console.print(f"regime_evidence: {result['regime_evidence']}")


@app.command("validate-logics")
def validate_logics(
    start_date: str | None = None,
    end_date: str | None = None,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
) -> None:
    """Run historical validation and write logic hits and validation results."""
    from astock.validation.service import run_validation

    resolved_end_date = _parse_date_arg(end_date) or (date.today() - timedelta(days=1))
    resolved_start_date = _parse_date_arg(start_date) or (
        resolved_end_date - timedelta(days=settings.default_validation_window_days)
    )
    result = run_validation(
        start_date=resolved_start_date,
        end_date=resolved_end_date,
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
    )
    console.print(f"validation window: {resolved_start_date} -> {resolved_end_date}")
    console.print(f"symbols scanned: {result['symbol_count']}")
    console.print(f"logic_signal_hit rows inserted: {result['signal_hit_count']}")
    console.print(f"logic_validation_result rows inserted: {result['validation_result_count']}")
    console.print(f"logic_reliability_snapshot rows inserted: {result['snapshot_count']}")


@app.command("discover-logics")
def discover_logics(
    start_date: str,
    end_date: str,
    regimes: str = "rotation,weak_rotation",
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    candidate_limit: int = settings.discovery_candidate_limit,
) -> None:
    """Discover explainable candidate strategies from factor panel statistics."""
    from astock.factor_lab.service import run_discovery

    result = run_discovery(
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        regimes=[item.strip() for item in regimes.split(",") if item.strip()],
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        candidate_limit=candidate_limit,
    )
    console.print(f"discovery_run_id: {result['discovery_run_id']}")
    console.print(f"symbols scanned: {result['symbol_count']}")
    console.print(f"factor_profiles: {result['factor_profile_count']}")
    console.print(f"factor_combos: {result['combo_count']}")
    console.print(f"rule_variants: {result['variant_count']}")
    console.print(f"replay_quality_rows: {result['replay_quality_count']}")
    console.print(f"candidates inserted: {result['candidate_count']}")
    table = Table(title="Discovered Logic Candidates")
    table.add_column("candidate_id")
    table.add_column("logic_id")
    table.add_column("regime")
    table.add_column("variant")
    table.add_column("samples", justify="right")
    table.add_column("top3", justify="right")
    table.add_column("top5", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("max_3d", justify="right")
    table.add_column("dd_3d", justify="right")
    table.add_column("score", justify="right")
    table.add_column("approved")
    table.add_column("replay")
    for row in result["rows"]:
        table.add_row(
            row["candidate_id"],
            row["logic_id"],
            row["regime"],
            row.get("variant_type") or "baseline",
            str(row["sample_count"]),
            f"{(row.get('top3_quality_score') or 0):.2f}",
            f"{(row.get('top5_quality_score') or 0):.2f}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['avg_max_return_3d'] or 0):.2f}",
            f"{(row['max_drawdown_3d'] or 0):.2f}",
            f"{(row['discovery_score'] or 0):.2f}",
            "yes" if row["approved_for_validation"] else "no",
            "yes" if row.get("replay_quality_passed") else "no",
        )
    console.print(table)


@app.command("analyze-factors")
def analyze_factors(
    regime: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest factor bucket profiles from factor discovery."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_latest_factor_profiles(regime=regime, limit=limit)
    if not rows:
        console.print("no factor profile found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Factor Profiles")
    table.add_column("regime")
    table.add_column("window", justify="right")
    table.add_column("field")
    table.add_column("range")
    table.add_column("samples", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("score", justify="right")
    for row in rows:
        table.add_row(
            row["regime"],
            str(row["window_size"]),
            row["field"],
            f"{row['min_value']:.2f} ~ {row['max_value']:.2f}",
            str(row["sample_count"]),
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['discovery_score'] or 0):.2f}",
        )
    console.print(table)


@app.command("analyze-factor-combos")
def analyze_factor_combos(
    regime: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest factor combo results and lift over single factor."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_latest_factor_combo_results(regime=regime, limit=limit)
    if not rows:
        console.print("no factor combo found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Factor Combos")
    table.add_column("combo_id")
    table.add_column("regime")
    table.add_column("window", justify="right")
    table.add_column("fields")
    table.add_column("samples", justify="right")
    table.add_column("lift", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("score", justify="right")
    for row in rows:
        table.add_row(
            row["combo_id"],
            row["regime"],
            str(row["window_size"]),
            ",".join(row["fields"]),
            str(row["sample_count"]),
            f"{(row['lift_vs_single'] or 0):.2f}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['discovery_score'] or 0):.2f}",
        )
    console.print(table)


@app.command("analyze-rule-variants")
def analyze_rule_variants(
    regime: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest baseline/narrow/wide rule experiment results."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_latest_rule_variant_results(regime=regime, limit=limit)
    if not rows:
        console.print("no rule variant result found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Rule Variants")
    table.add_column("logic_id")
    table.add_column("regime")
    table.add_column("variant")
    table.add_column("samples", justify="right")
    table.add_column("top3", justify="right")
    table.add_column("top5", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("score", justify="right")
    for row in rows:
        table.add_row(
            row["logic_id"],
            row["regime"],
            row["variant_type"],
            str(row["sample_count"]),
            f"{(row['top3_quality_score'] or 0):.2f}",
            f"{(row['top5_quality_score'] or 0):.2f}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['discovery_score'] or 0):.2f}",
        )
    console.print(table)


@app.command("show-discovered-logics")
def show_discovered_logics(
    regime: str | None = None,
    approved_only: bool = False,
    promoted_only: bool = False,
    limit: int = 20,
) -> None:
    """Show discovered candidate strategies from local storage."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_discovered_candidates(
        regime=regime,
        approved_only=approved_only,
        promoted_only=promoted_only,
        limit=limit,
    )
    if not rows:
        console.print("no discovered logic found")
        raise typer.Exit(code=1)
    table = Table(title="Discovered Logic Candidates")
    table.add_column("candidate_id")
    table.add_column("logic_id")
    table.add_column("regime")
    table.add_column("variant")
    table.add_column("samples", justify="right")
    table.add_column("top3", justify="right")
    table.add_column("top5", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("score", justify="right")
    table.add_column("approved")
    table.add_column("replay")
    table.add_column("runtime")
    for row in rows:
        table.add_row(
            row["candidate_id"],
            row["logic_id"],
            row["regime"],
            row.get("variant_type") or "baseline",
            str(row["sample_count"]),
            f"{(row.get('top3_quality_score') or 0):.2f}",
            f"{(row.get('top5_quality_score') or 0):.2f}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['discovery_score'] or 0):.2f}",
            "yes" if row["approved_for_validation"] else "no",
            "yes" if row.get("replay_quality_passed") else "no",
            "yes" if row["promoted_to_runtime"] else "no",
        )
    console.print(table)


@app.command("promote-discovered-logics")
def promote_discovered_logics(
    candidate_ids: str | None = None,
    latest_approved: bool = True,
    limit: int = settings.discovery_candidate_limit,
) -> None:
    """Promote discovered candidates into runtime registry."""
    from astock.storage.duckdb import DuckDbStorage

    ids = [item.strip() for item in candidate_ids.split(",") if item.strip()] if candidate_ids else None
    count = DuckDbStorage().promote_discovered_candidates(
        candidate_ids=ids,
        latest_approved=latest_approved if ids is None else False,
        limit=limit,
    )
    console.print(f"promoted_count: {count}")


@app.command("cleanup-discovered-logics")
def cleanup_discovered_logics(
    require_replay_passed: bool = True,
) -> None:
    """Remove promoted discovered strategies that no longer meet the current gate."""
    from astock.storage.duckdb import DuckDbStorage

    removed = DuckDbStorage().cleanup_runtime_discovered_logics(require_replay_passed=require_replay_passed)
    console.print(f"removed_count: {removed}")


@app.command("show-replay-quality")
def show_replay_quality(
    logic_id: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest stored replay quality for discovered strategies."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_latest_replay_quality_results(logic_id=logic_id, limit=limit)
    if not rows:
        console.print("no replay quality found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Replay Quality")
    table.add_column("logic_id")
    table.add_column("logic_name")
    table.add_column("days", justify="right")
    table.add_column("top_k", justify="right")
    table.add_column("samples", justify="right")
    table.add_column("hit_3d", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("avg_n3d", justify="right")
    table.add_column("avg_max", justify="right")
    table.add_column("avg_dd", justify="right")
    table.add_column("score", justify="right")
    table.add_column("passed")
    for row in rows:
        table.add_row(
            row["logic_id"],
            row.get("logic_name") or "",
            str(row["trade_days"]),
            str(row["top_k"]),
            str(row["sample_count"]),
            f"{(row['hit_rate_3d'] or 0):.2%}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['avg_n3d'] or 0):.2f}",
            f"{(row['avg_n3d_max'] or 0):.2f}",
            f"{(row['avg_n3d_dd'] or 0):.2f}",
            f"{(row['topk_quality_score'] or 0):.2f}",
            "yes" if row["passed"] else "no",
        )
    console.print(table)


@app.command("run-selection")
def run_selection(
    trade_date: str | None = None,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    selection_limit: int = settings.default_selection_limit,
) -> None:
    """Run market-regime detection, reliability filtering, and daily selection."""
    from astock.selection.service import run_daily_selection

    result = run_daily_selection(
        trade_date=_parse_date_arg(trade_date),
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        selection_limit=selection_limit,
    )
    console.print(f"trade_date: {result.get('trade_date')}")
    console.print(f"regime: {result['regime']}")
    console.print(f"regime_evidence: {result['regime_evidence']}")
    if result.get("warning"):
        console.print(f"warning: {result['warning']}")
        raise typer.Exit(code=1)
    table = Table(title="Daily Selection")
    table.add_column("rank", justify="right")
    table.add_column("symbol")
    table.add_column("logic_id")
    table.add_column("score", justify="right")
    table.add_column("reason")
    for row in result["rows"]:
        table.add_row(
            str(row["selection_rank"]),
            row["symbol"],
            row["logic_id"],
            f"{(row.get('trigger_score') or 0):.2f}",
            row.get("selection_reason") or "",
        )
    console.print(table)


@app.command("replay-selection")
def replay_selection(
    trade_date: str,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    selection_limit: int = settings.default_selection_limit,
    approved_only: bool = True,
) -> None:
    """Replay a historical trading day using historical regime and forward 1-3d outcomes."""
    from astock.selection.service import replay_historical_selection

    result = replay_historical_selection(
        trade_date=date.fromisoformat(trade_date),
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        selection_limit=selection_limit,
        approved_only=approved_only,
    )
    console.print(f"trade_date: {result.get('trade_date')}")
    console.print(f"regime: {result.get('regime')}")
    console.print(f"regime_evidence: {result.get('regime_evidence')}")
    if result.get("warning"):
        console.print(f"warning: {result['warning']}")
        raise typer.Exit(code=1)
    table = Table(title="Historical Replay Selection")
    table.add_column("rank", justify="right")
    table.add_column("symbol")
    table.add_column("logic_id")
    table.add_column("score", justify="right")
    table.add_column("n1d", justify="right")
    table.add_column("n1d_max", justify="right")
    table.add_column("n1d_dd", justify="right")
    table.add_column("n2d", justify="right")
    table.add_column("n2d_max", justify="right")
    table.add_column("n2d_dd", justify="right")
    table.add_column("n3d", justify="right")
    table.add_column("n3d_max", justify="right")
    table.add_column("n3d_dd", justify="right")
    for row in result["rows"]:
        table.add_row(
            str(row["selection_rank"]),
            row["symbol"],
            row["logic_id"],
            f"{(row.get('trigger_score') or 0):.2f}",
            f"{(row.get('next_1d_return') or 0):.2f}",
            f"{(row.get('next_1d_max_return') or 0):.2f}",
            f"{(row.get('max_drawdown_1d') or 0):.2f}",
            f"{(row.get('next_2d_return') or 0):.2f}",
            f"{(row.get('next_2d_max_return') or 0):.2f}",
            f"{(row.get('max_drawdown_2d') or 0):.2f}",
            f"{(row.get('next_3d_return') or 0):.2f}",
            f"{(row.get('next_3d_max_return') or 0):.2f}",
            f"{(row.get('max_drawdown_3d') or 0):.2f}",
        )
    console.print(table)


@app.command("replay-batch")
def replay_batch(
    trade_dates: str,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    selection_limit: int = settings.default_selection_limit,
    approved_only: bool = True,
) -> None:
    """Replay multiple historical trade dates and aggregate strategy quality."""
    from astock.selection.service import replay_historical_selection_batch

    result = replay_historical_selection_batch(
        trade_dates=_parse_date_list_arg(trade_dates),
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        selection_limit=selection_limit,
        approved_only=approved_only,
    )
    console.print(f"trade_dates: {', '.join(result['trade_dates'])}")
    day_table = Table(title="Replay Days")
    day_table.add_column("trade_date")
    day_table.add_column("regime")
    day_table.add_column("count", justify="right")
    day_table.add_column("warning")
    for row in result["day_results"]:
        day_table.add_row(
            row["trade_date"],
            row.get("regime") or "",
            str(row.get("selection_count") or 0),
            row.get("warning") or "",
        )
    console.print(day_table)

    if not result["strategy_stats"]:
        console.print("no replay rows found")
        raise typer.Exit(code=1)

    table = Table(title="Replay Strategy Quality")
    table.add_column("logic_id")
    table.add_column("logic_name")
    table.add_column("samples", justify="right")
    table.add_column("hit_1d", justify="right")
    table.add_column("hit_2d", justify="right")
    table.add_column("hit_3d", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("avg_n1d", justify="right")
    table.add_column("avg_n2d", justify="right")
    table.add_column("avg_n3d", justify="right")
    table.add_column("avg_n3d_max", justify="right")
    table.add_column("avg_n3d_dd", justify="right")
    for row in result["strategy_stats"]:
        table.add_row(
            row["logic_id"],
            row.get("logic_name") or "",
            str(row["sample_count"]),
            f"{row['hit_rate_1d']:.2%}",
            f"{row['hit_rate_2d']:.2%}",
            f"{row['hit_rate_3d']:.2%}",
            f"{row['big_move_rate_3d']:.2%}",
            f"{row['avg_n1d']:.2f}",
            f"{row['avg_n2d']:.2f}",
            f"{row['avg_n3d']:.2f}",
            f"{row['avg_n3d_max']:.2f}",
            f"{row['avg_n3d_dd']:.2f}",
        )
    console.print(table)


@app.command("strategy-sample")
def strategy_sample(
    logic_id: str,
    min_samples: int = 10,
    lookback_trade_days: int = 40,
    end_date: str | None = None,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    selection_limit: int = settings.default_selection_limit,
    approved_only: bool = True,
) -> None:
    """Expand signal days for one strategy and report whether sample size is usable."""
    from astock.selection.service import analyze_strategy_with_expanded_signals

    result = analyze_strategy_with_expanded_signals(
        logic_id=logic_id,
        min_samples=min_samples,
        lookback_trade_days=lookback_trade_days,
        end_date=_parse_date_arg(end_date),
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        selection_limit=selection_limit,
        approved_only=approved_only,
    )
    console.print(f"logic_id: {result['logic_id']}")
    console.print(f"sample_count: {result['sample_count']}")
    console.print(f"meets_min_samples: {result['meets_min_samples']}")
    console.print(f"matched_trade_dates: {', '.join(result['matched_trade_dates']) if result['matched_trade_dates'] else 'none'}")
    stats = result.get("strategy_stats")
    if not stats:
        console.print("no matched rows found")
        raise typer.Exit(code=1)
    table = Table(title="Expanded Strategy Sample")
    table.add_column("logic_name")
    table.add_column("samples", justify="right")
    table.add_column("hit_1d", justify="right")
    table.add_column("hit_2d", justify="right")
    table.add_column("hit_3d", justify="right")
    table.add_column("big_3d", justify="right")
    table.add_column("avg_n1d", justify="right")
    table.add_column("avg_n2d", justify="right")
    table.add_column("avg_n3d", justify="right")
    table.add_column("avg_n3d_max", justify="right")
    table.add_column("avg_n3d_dd", justify="right")
    table.add_row(
        stats.get("logic_name") or "",
        str(stats["sample_count"]),
        f"{stats['hit_rate_1d']:.2%}",
        f"{stats['hit_rate_2d']:.2%}",
        f"{stats['hit_rate_3d']:.2%}",
        f"{stats['big_move_rate_3d']:.2%}",
        f"{stats['avg_n1d']:.2f}",
        f"{stats['avg_n2d']:.2f}",
        f"{stats['avg_n3d']:.2f}",
        f"{stats['avg_n3d_max']:.2f}",
        f"{stats['avg_n3d_dd']:.2f}",
    )
    console.print(table)


@app.command("rolling-discovery-eval")
def rolling_discovery_eval(
    start_date: str,
    end_date: str,
    regimes: str = "rotation,weak_rotation",
    train_days: int = 80,
    test_days: int = 20,
    follow_days: int = 10,
    step_days: int = 10,
    symbol_limit: int = settings.default_symbol_limit,
    chunk_size: int = settings.default_chunk_size,
    candidate_limit: int = settings.discovery_candidate_limit,
) -> None:
    """Run rolling discovery stability evaluation."""
    from astock.factor_lab.stability import run_discovery_stability_eval

    result = run_discovery_stability_eval(
        start_date=date.fromisoformat(start_date),
        end_date=date.fromisoformat(end_date),
        regimes=[item.strip() for item in regimes.split(",") if item.strip()],
        train_days=train_days,
        test_days=test_days,
        follow_days=follow_days,
        step_days=step_days,
        symbol_limit=symbol_limit,
        chunk_size=chunk_size,
        candidate_limit=candidate_limit,
    )
    console.print(f"eval_run_id: {result['eval_run_id']}")
    console.print(f"window_count: {result['window_count']}")
    console.print(f"stable_window_count: {result['stable_window_count']}")
    table = Table(title="Rolling Discovery Windows")
    table.add_column("window_id")
    table.add_column("train")
    table.add_column("test")
    table.add_column("follow")
    table.add_column("candidates", justify="right")
    table.add_column("dual_pass", justify="right")
    table.add_column("stable", justify="right")
    table.add_column("status")
    for row in result["window_rows"]:
        table.add_row(
            row["window_id"],
            f"{row['train_start']} ~ {row['train_end']}",
            f"{row['test_start']} ~ {row['test_end']}",
            f"{row['follow_start']} ~ {row['follow_end']}",
            str(row["candidate_count"]),
            str(row["dual_pass_count"]),
            str(row["stable_candidate_count"]),
            row["window_status"],
        )
    console.print(table)


@app.command("show-discovery-stability")
def show_discovery_stability(
    logic_id: str | None = None,
    stable_only: bool = False,
    limit: int = 20,
) -> None:
    """Show latest rolling discovery stability results."""
    from astock.storage.duckdb import DuckDbStorage

    storage = DuckDbStorage()
    windows = storage.list_latest_discovery_eval_windows(limit=limit)
    candidates = storage.list_latest_discovery_eval_candidates(logic_id=logic_id, stable_only=stable_only, limit=limit)
    if not windows:
        console.print("no discovery stability result found")
        raise typer.Exit(code=1)
    win_table = Table(title="Latest Discovery Stability Windows")
    win_table.add_column("window_id")
    win_table.add_column("train")
    win_table.add_column("test")
    win_table.add_column("follow")
    win_table.add_column("cand", justify="right")
    win_table.add_column("dual", justify="right")
    win_table.add_column("stable", justify="right")
    win_table.add_column("status")
    for row in windows:
        win_table.add_row(
            row["window_id"],
            f"{row['train_start']} ~ {row['train_end']}",
            f"{row['test_start']} ~ {row['test_end']}",
            f"{row['follow_start']} ~ {row['follow_end']}",
            str(row["candidate_count"]),
            str(row["dual_pass_count"]),
            str(row["stable_candidate_count"]),
            row["window_status"],
        )
    console.print(win_table)
    if not candidates:
        return
    cand_table = Table(title="Latest Discovery Stability Candidates")
    cand_table.add_column("window_id")
    cand_table.add_column("logic_id")
    cand_table.add_column("regime")
    cand_table.add_column("train_top5", justify="right")
    cand_table.add_column("test_top5", justify="right")
    cand_table.add_column("test_hit", justify="right")
    cand_table.add_column("test_big", justify="right")
    cand_table.add_column("follow_score", justify="right")
    cand_table.add_column("follow_ok")
    cand_table.add_column("stable")
    for row in candidates:
        cand_table.add_row(
            row["window_id"],
            row["logic_id"],
            row["regime"],
            f"{(row['train_top5_score'] or 0):.2f}",
            f"{(row['test_top5_score'] or 0):.2f}",
            f"{(row['test_hit_3d'] or 0):.2%}",
            f"{(row['test_big_move_3d'] or 0):.2%}",
            f"{(row['follow_validation_score'] or 0):.2f}",
            "yes" if row["follow_validation_approved"] else "no",
            "yes" if row["stable_passed"] else "no",
        )
    console.print(cand_table)


@app.command("show-validation")
def show_validation(
    regime: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest validation result snapshot from local storage."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_latest_validation_results(regime=regime, limit=limit)
    if not rows:
        console.print("no validation result found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Validation Results")
    table.add_column("logic_id")
    table.add_column("regime")
    table.add_column("samples", justify="right")
    table.add_column("hit_3d", justify="right")
    table.add_column("big_mv", justify="right")
    table.add_column("max_3d", justify="right")
    table.add_column("dd_3d", justify="right")
    table.add_column("p/dd", justify="right")
    table.add_column("score", justify="right")
    for row in rows:
        table.add_row(
            row["logic_id"],
            row["regime"],
            str(row["sample_count"]),
            f"{(row['hit_rate_3d'] or 0):.2%}",
            f"{(row['big_move_rate_3d'] or 0):.2%}",
            f"{(row['avg_max_return_3d'] or 0):.2f}",
            f"{(row['max_drawdown_3d'] or 0):.2f}",
            f"{(row['profit_drawdown_ratio'] or 0):.2f}",
            f"{(row['reliability_score'] or 0):.2f}",
        )
    console.print(
        f"validation window: {rows[0]['window_start']} -> {rows[0]['window_end']}  validation_id: {rows[0]['validation_id']}"
    )
    console.print(table)


@app.command("show-snapshot")
def show_snapshot(
    regime: str | None = None,
    approved_only: bool = False,
) -> None:
    """Show latest logic reliability snapshot from local storage."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().load_latest_reliability_snapshot(regime=regime, approved_only=approved_only)
    if not rows:
        console.print("no reliability snapshot found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Reliability Snapshot")
    table.add_column("logic_id")
    table.add_column("regime")
    table.add_column("score", justify="right")
    table.add_column("approved")
    table.add_column("samples", justify="right")
    for row in rows:
        table.add_row(
            row["logic_id"],
            row["regime"],
            f"{(row['reliability_score'] or 0):.2f}",
            "yes" if row["approved"] else "no",
            str(row["sample_count"]),
        )
    console.print(f"snapshot_id: {rows[0]['snapshot_id']}")
    console.print(table)


@app.command("show-selection")
def show_selection(
    trade_date: str | None = None,
    limit: int = 20,
) -> None:
    """Show latest daily selection result from local storage."""
    from astock.storage.duckdb import DuckDbStorage

    rows = DuckDbStorage().list_recent_selection_output(run_date=trade_date, limit=limit)
    if not rows:
        console.print("no selection output found")
        raise typer.Exit(code=1)
    table = Table(title="Latest Selection Output")
    table.add_column("rank", justify="right")
    table.add_column("symbol")
    table.add_column("logic_id")
    table.add_column("logic_name")
    table.add_column("hold", justify="right")
    table.add_column("regime")
    table.add_column("score", justify="right")
    table.add_column("reliability", justify="right")
    table.add_column("invalidation", justify="right")
    table.add_column("reason")
    for row in rows:
        table.add_row(
            str(row["selection_rank"]),
            row["symbol"],
            row["logic_id"],
            row.get("logic_name") or "",
            str(row.get("holding_days") or ""),
            row["regime"],
            f"{(row['trigger_score'] or 0):.2f}",
            f"{(row['reliability_score'] or 0):.2f}",
            f"{(row['invalidation_level'] or 0):.2f}",
            row["selection_reason"] or "",
        )
    console.print(f"run_date: {rows[0]['run_date']}  run_id: {rows[0]['run_id']}")
    console.print(table)


if __name__ == "__main__":
    app()
