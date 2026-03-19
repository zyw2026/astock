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
    table.add_column("regimes")
    table.add_column("holding_days", justify="right")
    for spec in registry.all():
        table.add_row(spec.logic_id, spec.name, ",".join(spec.regime_whitelist), str(spec.holding_days))
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
