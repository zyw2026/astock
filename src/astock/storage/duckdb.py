from __future__ import annotations

import json
from pathlib import Path

import duckdb

from astock.app.settings import settings
from astock.logic_pool.models import LogicSpec
from astock.validation.models import LogicReliabilitySnapshot, LogicSignalHit, LogicValidationResult


SCHEMA_SQL = """
create table if not exists logic_signal_hit (
    run_id varchar,
    logic_id varchar not null,
    trade_date date not null,
    symbol varchar not null,
    regime varchar not null,
    trigger_score double,
    trigger_reason varchar,
    entry_price double,
    next_1d_return double,
    next_2d_return double,
    next_3d_return double,
    next_3d_max_return double,
    next_5d_return double,
    max_drawdown_3d double,
    max_drawdown double,
    created_at timestamp default current_timestamp
);

create table if not exists logic_validation_result (
    validation_id varchar,
    logic_id varchar not null,
    regime varchar not null,
    window_start date,
    window_end date,
    sample_count bigint not null,
    hit_rate_1d double,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_return_1d double,
    avg_return_2d double,
    avg_return_3d double,
    avg_max_return_3d double,
    avg_return_5d double,
    profit_drawdown_ratio double,
    max_drawdown_3d double,
    max_drawdown double,
    reliability_score double,
    created_at timestamp default current_timestamp
);

create table if not exists logic_reliability_snapshot (
    snapshot_id varchar,
    logic_id varchar not null,
    regime varchar not null,
    reliability_score double not null,
    approved boolean not null,
    sample_count bigint not null,
    created_at timestamp default current_timestamp
);

create table if not exists daily_selection_output (
    run_id varchar,
    run_date date not null,
    logic_id varchar not null,
    logic_name varchar,
    holding_days bigint,
    symbol varchar not null,
    regime varchar not null,
    selection_rank bigint,
    trigger_score double,
    reliability_score double,
    selection_reason varchar,
    invalidation_level double,
    created_at timestamp default current_timestamp
);

create table if not exists discovery_run_result (
    discovery_run_id varchar,
    window_start date,
    window_end date,
    regimes varchar,
    symbol_count bigint,
    candidate_count bigint,
    notes varchar,
    created_at timestamp default current_timestamp
);

create table if not exists discovered_logic_candidate (
    candidate_id varchar,
    discovery_run_id varchar,
    source varchar not null,
    logic_id varchar not null,
    logic_name varchar not null,
    regime varchar not null,
    sample_count bigint not null,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_return_3d double,
    avg_max_return_3d double,
    max_drawdown_3d double,
    discovery_score double,
    spec_json varchar not null,
    approved_for_validation boolean not null,
    promoted_to_runtime boolean not null default false,
    created_at timestamp default current_timestamp
);

create table if not exists runtime_discovered_logic (
    candidate_id varchar not null,
    promoted_at timestamp default current_timestamp
);
"""

MIGRATION_SQL = (
    "alter table logic_signal_hit add column if not exists next_2d_return double;",
    "alter table logic_signal_hit add column if not exists next_3d_max_return double;",
    "alter table logic_signal_hit add column if not exists max_drawdown_3d double;",
    "alter table logic_validation_result add column if not exists hit_rate_1d double;",
    "alter table logic_validation_result add column if not exists hit_rate_3d double;",
    "alter table logic_validation_result add column if not exists big_move_rate_3d double;",
    "alter table logic_validation_result add column if not exists avg_return_2d double;",
    "alter table logic_validation_result add column if not exists avg_max_return_3d double;",
    "alter table logic_validation_result add column if not exists profit_drawdown_ratio double;",
    "alter table logic_validation_result add column if not exists max_drawdown_3d double;",
    "alter table daily_selection_output add column if not exists reliability_score double;",
    "alter table daily_selection_output add column if not exists logic_name varchar;",
    "alter table daily_selection_output add column if not exists holding_days bigint;",
    "alter table discovered_logic_candidate add column if not exists approved_for_validation boolean default false;",
    "alter table discovered_logic_candidate add column if not exists promoted_to_runtime boolean default false;",
)


class DuckDbStorage:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or settings.local_db_path

    def connect(self, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path), read_only=read_only)

    def initialize(self) -> Path:
        with self.connect() as conn:
            conn.execute(SCHEMA_SQL)
            for statement in MIGRATION_SQL:
                conn.execute(statement)
            conn.commit()
        return self.db_path

    def insert_signal_hits(self, hits: list[LogicSignalHit], *, run_id: str) -> int:
        if not hits:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into logic_signal_hit (
                    run_id, logic_id, trade_date, symbol, regime, trigger_score, trigger_reason,
                    entry_price, next_1d_return, next_2d_return, next_3d_return, next_3d_max_return,
                    next_5d_return, max_drawdown_3d, max_drawdown
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        hit.logic_id,
                        hit.trade_date,
                        hit.symbol,
                        hit.regime,
                        hit.trigger_score,
                        hit.trigger_reason,
                        hit.entry_price,
                        hit.next_1d_return,
                        hit.next_2d_return,
                        hit.next_3d_return,
                        hit.next_3d_max_return,
                        hit.next_5d_return,
                        hit.max_drawdown_3d,
                        hit.max_drawdown,
                    )
                    for hit in hits
                ],
            )
            conn.commit()
        return len(hits)

    def insert_validation_results(
        self,
        results: list[LogicValidationResult],
        *,
        validation_id: str,
        window_start: str,
        window_end: str,
    ) -> int:
        if not results:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into logic_validation_result (
                    validation_id, logic_id, regime, window_start, window_end, sample_count, hit_rate_1d,
                    hit_rate_3d, big_move_rate_3d, avg_return_1d, avg_return_2d, avg_return_3d,
                    avg_max_return_3d, avg_return_5d, profit_drawdown_ratio, max_drawdown_3d,
                    max_drawdown, reliability_score
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        validation_id,
                        item.logic_id,
                        item.regime,
                        window_start,
                        window_end,
                        item.sample_count,
                        item.hit_rate_1d,
                        item.hit_rate_3d,
                        item.big_move_rate_3d,
                        item.avg_return_1d,
                        item.avg_return_2d,
                        item.avg_return_3d,
                        item.avg_max_return_3d,
                        item.avg_return_5d,
                        item.profit_drawdown_ratio,
                        item.max_drawdown_3d,
                        item.max_drawdown,
                        item.reliability_score,
                    )
                    for item in results
                ],
            )
            conn.commit()
        return len(results)

    def replace_reliability_snapshot(self, rows: list[LogicReliabilitySnapshot], *, snapshot_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into logic_reliability_snapshot (
                    snapshot_id, logic_id, regime, reliability_score, approved, sample_count
                ) values (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id,
                        row.logic_id,
                        row.regime,
                        row.reliability_score,
                        row.approved,
                        row.sample_count,
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def load_latest_reliability_snapshot(self, *, regime: str | None = None, approved_only: bool = False) -> list[dict]:
        query = """
            with latest as (
                select snapshot_id
                from logic_reliability_snapshot
                order by created_at desc, snapshot_id desc
                limit 1
            )
            select
                logic_reliability_snapshot.logic_id,
                logic_reliability_snapshot.regime,
                logic_reliability_snapshot.reliability_score,
                logic_reliability_snapshot.approved,
                logic_reliability_snapshot.sample_count,
                logic_reliability_snapshot.created_at,
                logic_reliability_snapshot.snapshot_id
            from logic_reliability_snapshot, latest
            where logic_reliability_snapshot.snapshot_id = latest.snapshot_id
        """
        params: list = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        if approved_only:
            query += " and approved = true"
        query += " order by reliability_score desc, sample_count desc, logic_id"
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "logic_id": row[0],
                "regime": row[1],
                "reliability_score": row[2],
                "approved": row[3],
                "sample_count": row[4],
                "created_at": row[5],
                "snapshot_id": row[6],
            }
            for row in rows
        ]

    def insert_daily_selection_output(self, rows: list[dict], *, run_id: str, run_date: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into daily_selection_output (
                    run_id, run_date, logic_id, logic_name, holding_days, symbol, regime, selection_rank, trigger_score,
                    reliability_score, selection_reason, invalidation_level
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        run_date,
                        row["logic_id"],
                        row.get("logic_name"),
                        row.get("holding_days"),
                        row["symbol"],
                        row["regime"],
                        row["selection_rank"],
                        row.get("trigger_score"),
                        row.get("reliability_score"),
                        row.get("selection_reason"),
                        row.get("invalidation_level"),
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def list_latest_validation_results(self, *, regime: str | None = None, limit: int = 20) -> list[dict]:
        query = """
            with latest as (
                select validation_id
                from logic_validation_result
                order by created_at desc, validation_id desc
                limit 1
            )
            select
                logic_validation_result.logic_id,
                logic_validation_result.regime,
                logic_validation_result.sample_count,
                logic_validation_result.hit_rate_1d,
                logic_validation_result.hit_rate_3d,
                logic_validation_result.big_move_rate_3d,
                logic_validation_result.avg_return_1d,
                logic_validation_result.avg_return_2d,
                logic_validation_result.avg_return_3d,
                logic_validation_result.avg_max_return_3d,
                logic_validation_result.avg_return_5d,
                logic_validation_result.profit_drawdown_ratio,
                logic_validation_result.max_drawdown_3d,
                logic_validation_result.max_drawdown,
                logic_validation_result.reliability_score,
                logic_validation_result.window_start,
                logic_validation_result.window_end,
                logic_validation_result.created_at,
                logic_validation_result.validation_id
            from logic_validation_result, latest
            where logic_validation_result.validation_id = latest.validation_id
        """
        params: list[object] = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        query += " order by reliability_score desc, sample_count desc, logic_id limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "logic_id": row[0],
                "regime": row[1],
                "sample_count": row[2],
                "hit_rate_1d": row[3],
                "hit_rate_3d": row[4],
                "big_move_rate_3d": row[5],
                "avg_return_1d": row[6],
                "avg_return_2d": row[7],
                "avg_return_3d": row[8],
                "avg_max_return_3d": row[9],
                "avg_return_5d": row[10],
                "profit_drawdown_ratio": row[11],
                "max_drawdown_3d": row[12],
                "max_drawdown": row[13],
                "reliability_score": row[14],
                "window_start": row[15],
                "window_end": row[16],
                "created_at": row[17],
                "validation_id": row[18],
            }
            for row in rows
        ]

    def list_recent_selection_output(self, *, run_date: str | None = None, limit: int = 20) -> list[dict]:
        query = """
            with latest as (
                select run_id
                from daily_selection_output
        """
        params: list[object] = []
        if run_date is not None:
            query += " where run_date = ?"
            params.append(run_date)
        query += """
                order by created_at desc, run_id desc
                limit 1
            )
            select
                daily_selection_output.run_date,
                daily_selection_output.logic_id,
                daily_selection_output.logic_name,
                daily_selection_output.holding_days,
                daily_selection_output.symbol,
                daily_selection_output.regime,
                daily_selection_output.selection_rank,
                daily_selection_output.trigger_score,
                daily_selection_output.reliability_score,
                daily_selection_output.selection_reason,
                daily_selection_output.invalidation_level,
                daily_selection_output.created_at,
                daily_selection_output.run_id
            from daily_selection_output, latest
            where daily_selection_output.run_id = latest.run_id
            order by selection_rank asc, trigger_score desc, symbol
            limit ?
        """
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "run_date": row[0],
                "logic_id": row[1],
                "logic_name": row[2],
                "holding_days": row[3],
                "symbol": row[4],
                "regime": row[5],
                "selection_rank": row[6],
                "trigger_score": row[7],
                "reliability_score": row[8],
                "selection_reason": row[9],
                "invalidation_level": row[10],
                "created_at": row[11],
                "run_id": row[12],
            }
            for row in rows
        ]

    def insert_discovery_run(
        self,
        *,
        discovery_run_id: str,
        window_start: str,
        window_end: str,
        regimes: list[str],
        symbol_count: int,
        candidate_count: int,
        notes: str | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                insert into discovery_run_result (
                    discovery_run_id, window_start, window_end, regimes, symbol_count, candidate_count, notes
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    discovery_run_id,
                    window_start,
                    window_end,
                    ",".join(regimes),
                    symbol_count,
                    candidate_count,
                    notes,
                ),
            )
            conn.commit()

    def insert_discovered_candidates(self, rows: list[dict], *, discovery_run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into discovered_logic_candidate (
                    candidate_id, discovery_run_id, source, logic_id, logic_name, regime,
                    sample_count, hit_rate_3d, big_move_rate_3d, avg_return_3d, avg_max_return_3d,
                    max_drawdown_3d, discovery_score, spec_json, approved_for_validation, promoted_to_runtime
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["candidate_id"],
                        discovery_run_id,
                        row.get("source", "factor_lab"),
                        row["logic_id"],
                        row["logic_name"],
                        row["regime"],
                        row["sample_count"],
                        row.get("hit_rate_3d"),
                        row.get("big_move_rate_3d"),
                        row.get("avg_return_3d"),
                        row.get("avg_max_return_3d"),
                        row.get("max_drawdown_3d"),
                        row.get("discovery_score"),
                        row["spec_json"],
                        1 if row.get("approved_for_validation", False) else 0,
                        1 if row.get("promoted_to_runtime", False) else 0,
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def list_discovered_candidates(
        self,
        *,
        regime: str | None = None,
        approved_only: bool = False,
        promoted_only: bool = False,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            select
                candidate_id,
                discovery_run_id,
                source,
                logic_id,
                logic_name,
                regime,
                sample_count,
                hit_rate_3d,
                big_move_rate_3d,
                avg_return_3d,
                avg_max_return_3d,
                max_drawdown_3d,
                discovery_score,
                approved_for_validation,
                exists(
                    select 1
                    from runtime_discovered_logic
                    where runtime_discovered_logic.candidate_id = discovered_logic_candidate.candidate_id
                ) as promoted_to_runtime,
                created_at
            from discovered_logic_candidate
            where 1 = 1
        """
        params: list[object] = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        if approved_only:
            query += " and approved_for_validation = true"
        if promoted_only:
            query += """
             and exists (
                 select 1
                 from runtime_discovered_logic
                 where runtime_discovered_logic.candidate_id = discovered_logic_candidate.candidate_id
             )
            """
        query += " order by created_at desc, discovery_score desc, sample_count desc, logic_id limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "candidate_id": row[0],
                "discovery_run_id": row[1],
                "source": row[2],
                "logic_id": row[3],
                "logic_name": row[4],
                "regime": row[5],
                "sample_count": row[6],
                "hit_rate_3d": row[7],
                "big_move_rate_3d": row[8],
                "avg_return_3d": row[9],
                "avg_max_return_3d": row[10],
                "max_drawdown_3d": row[11],
                "discovery_score": row[12],
                "approved_for_validation": row[13],
                "promoted_to_runtime": row[14],
                "created_at": row[15],
            }
            for row in rows
        ]

    def promote_discovered_candidates(
        self,
        *,
        candidate_ids: list[str] | None = None,
        latest_approved: bool = False,
        limit: int = 5,
    ) -> int:
        with self.connect() as conn:
            if candidate_ids:
                for candidate_id in candidate_ids:
                    conn.execute(
                        """
                        insert into runtime_discovered_logic (candidate_id)
                        select ?
                        where not exists (
                            select 1 from runtime_discovered_logic where candidate_id = ?
                        )
                        """,
                        (candidate_id, candidate_id),
                    )
                conn.commit()
                return len(candidate_ids)
            if latest_approved:
                rows = conn.execute(
                    """
                    with latest as (
                        select discovery_run_id
                        from discovery_run_result
                        order by created_at desc, discovery_run_id desc
                        limit 1
                    )
                    select candidate_id
                    from discovered_logic_candidate, latest
                    where discovered_logic_candidate.discovery_run_id = latest.discovery_run_id
                      and approved_for_validation = true
                    order by discovery_score desc, sample_count desc, logic_id
                    limit ?
                    """,
                    (limit,),
                ).fetchall()
                ids = [row[0] for row in rows]
                if not ids:
                    return 0
                for candidate_id in ids:
                    conn.execute(
                        """
                        insert into runtime_discovered_logic (candidate_id)
                        select ?
                        where not exists (
                            select 1 from runtime_discovered_logic where candidate_id = ?
                        )
                        """,
                        (candidate_id, candidate_id),
                    )
                conn.commit()
                return len(ids)
        return 0

    def load_promoted_logic_specs(self) -> list[LogicSpec]:
        if not self.db_path.exists():
            return []
        with self.connect(read_only=True) as conn:
            rows = conn.execute(
                """
                select discovered_logic_candidate.logic_id, discovered_logic_candidate.spec_json
                from discovered_logic_candidate
                join runtime_discovered_logic
                  on runtime_discovered_logic.candidate_id = discovered_logic_candidate.candidate_id
                order by runtime_discovered_logic.promoted_at desc,
                         discovered_logic_candidate.discovery_score desc,
                         discovered_logic_candidate.logic_id
                """
            ).fetchall()
        specs: list[LogicSpec] = []
        seen: set[str] = set()
        for logic_id, spec_json in rows:
            if logic_id in seen:
                continue
            seen.add(logic_id)
            specs.append(LogicSpec.model_validate(json.loads(spec_json)))
        return specs
