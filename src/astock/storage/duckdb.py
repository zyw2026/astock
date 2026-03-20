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
    discovery_run_id varchar,
    promoted_at timestamp default current_timestamp
);

create table if not exists factor_signal_profile (
    run_id varchar,
    regime varchar not null,
    window_size bigint not null,
    field varchar not null,
    min_value double,
    max_value double,
    sample_count bigint not null,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_return_3d double,
    avg_max_return_3d double,
    max_drawdown_3d double,
    discovery_score double,
    created_at timestamp default current_timestamp
);

create table if not exists factor_combo_result (
    run_id varchar,
    combo_id varchar not null,
    regime varchar not null,
    window_size bigint not null,
    fields_json varchar not null,
    sample_count bigint not null,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_return_3d double,
    avg_max_return_3d double,
    max_drawdown_3d double,
    discovery_score double,
    lift_vs_single double,
    created_at timestamp default current_timestamp
);

create table if not exists rule_variant_result (
    run_id varchar,
    variant_id varchar not null,
    combo_id varchar not null,
    regime varchar not null,
    logic_id varchar not null,
    variant_type varchar not null,
    sample_count bigint not null,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_return_3d double,
    avg_max_return_3d double,
    max_drawdown_3d double,
    top3_quality_score double,
    top5_quality_score double,
    discovery_score double,
    created_at timestamp default current_timestamp
);

create table if not exists replay_quality_result (
    run_id varchar,
    logic_id varchar not null,
    logic_name varchar,
    trade_days bigint not null,
    top_k bigint not null,
    sample_count bigint not null,
    hit_rate_3d double,
    big_move_rate_3d double,
    avg_n3d double,
    avg_n3d_max double,
    avg_n3d_dd double,
    topk_quality_score double,
    passed boolean not null,
    created_at timestamp default current_timestamp
);

create table if not exists discovery_eval_run (
    eval_run_id varchar,
    train_days bigint not null,
    test_days bigint not null,
    follow_days bigint not null,
    step_days bigint not null,
    regimes varchar,
    created_at timestamp default current_timestamp
);

create table if not exists discovery_eval_window_result (
    eval_run_id varchar,
    window_id varchar not null,
    train_start date,
    train_end date,
    test_start date,
    test_end date,
    follow_start date,
    follow_end date,
    candidate_count bigint not null,
    dual_pass_count bigint not null,
    stable_candidate_count bigint not null,
    window_status varchar not null,
    created_at timestamp default current_timestamp
);

create table if not exists discovery_eval_candidate_result (
    eval_run_id varchar,
    window_id varchar not null,
    candidate_id varchar not null,
    logic_id varchar not null,
    regime varchar not null,
    discovery_run_id varchar,
    train_top3_score double,
    train_top5_score double,
    test_top3_score double,
    test_top5_score double,
    test_hit_3d double,
    test_big_move_3d double,
    test_avg_n3d double,
    test_avg_n3d_max double,
    test_avg_n3d_dd double,
    follow_validation_score double,
    follow_validation_approved boolean not null,
    stable_passed boolean not null,
    status varchar not null,
    created_at timestamp default current_timestamp
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
    "alter table discovered_logic_candidate add column if not exists parent_combo_id varchar;",
    "alter table discovered_logic_candidate add column if not exists variant_type varchar default 'baseline';",
    "alter table discovered_logic_candidate add column if not exists top3_quality_score double;",
    "alter table discovered_logic_candidate add column if not exists top5_quality_score double;",
    "alter table discovered_logic_candidate add column if not exists replay_quality_passed boolean default false;",
    "alter table runtime_discovered_logic add column if not exists discovery_run_id varchar;",
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
            conn.execute(
                """
                update runtime_discovered_logic
                set discovery_run_id = sub.discovery_run_id
                from (
                    select candidate_id, discovery_run_id
                    from discovered_logic_candidate
                    qualify row_number() over (partition by candidate_id order by created_at desc) = 1
                ) as sub
                where runtime_discovered_logic.discovery_run_id is null
                  and runtime_discovered_logic.candidate_id = sub.candidate_id
                """
            )
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
                    max_drawdown_3d, discovery_score, spec_json, approved_for_validation, promoted_to_runtime,
                    parent_combo_id, variant_type, top3_quality_score, top5_quality_score, replay_quality_passed
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        row.get("parent_combo_id"),
                        row.get("variant_type", "baseline"),
                        row.get("top3_quality_score"),
                        row.get("top5_quality_score"),
                        1 if row.get("replay_quality_passed", False) else 0,
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
                parent_combo_id,
                variant_type,
                top3_quality_score,
                top5_quality_score,
                replay_quality_passed,
                exists(
                    select 1
                    from runtime_discovered_logic
                    where runtime_discovered_logic.candidate_id = discovered_logic_candidate.candidate_id
                      and runtime_discovered_logic.discovery_run_id = discovered_logic_candidate.discovery_run_id
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
                   and runtime_discovered_logic.discovery_run_id = discovered_logic_candidate.discovery_run_id
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
                "parent_combo_id": row[14],
                "variant_type": row[15],
                "top3_quality_score": row[16],
                "top5_quality_score": row[17],
                "replay_quality_passed": row[18],
                "promoted_to_runtime": row[19],
                "created_at": row[20],
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
                        insert into runtime_discovered_logic (candidate_id, discovery_run_id)
                        select candidate_id, discovery_run_id
                        from discovered_logic_candidate
                        where candidate_id = ?
                          and not exists (
                              select 1
                              from runtime_discovered_logic
                              where runtime_discovered_logic.candidate_id = discovered_logic_candidate.candidate_id
                                and runtime_discovered_logic.discovery_run_id = discovered_logic_candidate.discovery_run_id
                          )
                        order by created_at desc
                        limit 1
                        """,
                        (candidate_id,),
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
                    select candidate_id, discovery_run_id
                    from discovered_logic_candidate, latest
                    where discovered_logic_candidate.discovery_run_id = latest.discovery_run_id
                      and approved_for_validation = true
                      and replay_quality_passed = true
                    order by discovery_score desc, sample_count desc, logic_id
                    limit ?
                    """,
                    (limit,),
                ).fetchall()
                ids = [(row[0], row[1]) for row in rows]
                if not ids:
                    return 0
                for candidate_id, discovery_run_id in ids:
                    conn.execute(
                        """
                        insert into runtime_discovered_logic (candidate_id, discovery_run_id)
                        select ?, ?
                        where not exists (
                            select 1
                            from runtime_discovered_logic
                            where candidate_id = ?
                              and discovery_run_id = ?
                        )
                        """,
                        (candidate_id, discovery_run_id, candidate_id, discovery_run_id),
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
                 and runtime_discovered_logic.discovery_run_id = discovered_logic_candidate.discovery_run_id
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

    def cleanup_runtime_discovered_logics(self, *, require_replay_passed: bool = True) -> int:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select count(*)
                from runtime_discovered_logic r
                join discovered_logic_candidate d
                  on d.candidate_id = r.candidate_id
                 and d.discovery_run_id = r.discovery_run_id
                where d.approved_for_validation = false
                   or (? and d.replay_quality_passed = false)
                """,
                (require_replay_passed,),
            ).fetchone()
            removed = int(rows[0] or 0)
            conn.execute(
                """
                delete from runtime_discovered_logic
                where exists (
                    select 1
                    from discovered_logic_candidate d
                    where d.candidate_id = runtime_discovered_logic.candidate_id
                      and d.discovery_run_id = runtime_discovered_logic.discovery_run_id
                      and (
                          d.approved_for_validation = false
                          or (? and d.replay_quality_passed = false)
                      )
                )
                """,
                (require_replay_passed,),
            )
            conn.commit()
        return removed

    def insert_factor_profiles(self, rows: list[dict], *, run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into factor_signal_profile (
                    run_id, regime, window_size, field, min_value, max_value, sample_count,
                    hit_rate_3d, big_move_rate_3d, avg_return_3d, avg_max_return_3d, max_drawdown_3d, discovery_score
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row["regime"],
                        row["window_size"],
                        row["field"],
                        row.get("min_value"),
                        row.get("max_value"),
                        row["sample_count"],
                        row["hit_rate_3d"],
                        row["big_move_rate_3d"],
                        row["avg_return_3d"],
                        row["avg_max_return_3d"],
                        row["max_drawdown_3d"],
                        row["discovery_score"],
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def insert_factor_combo_results(self, rows: list[dict], *, run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into factor_combo_result (
                    run_id, combo_id, regime, window_size, fields_json, sample_count, hit_rate_3d,
                    big_move_rate_3d, avg_return_3d, avg_max_return_3d, max_drawdown_3d, discovery_score, lift_vs_single
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row["combo_id"],
                        row["regime"],
                        row["window_size"],
                        json.dumps(row["fields"], ensure_ascii=False),
                        row["sample_count"],
                        row["hit_rate_3d"],
                        row["big_move_rate_3d"],
                        row["avg_return_3d"],
                        row["avg_max_return_3d"],
                        row["max_drawdown_3d"],
                        row["discovery_score"],
                        row["lift_vs_single"],
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def insert_rule_variant_results(self, rows: list[dict], *, run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into rule_variant_result (
                    run_id, variant_id, combo_id, regime, logic_id, variant_type, sample_count,
                    hit_rate_3d, big_move_rate_3d, avg_return_3d, avg_max_return_3d,
                    max_drawdown_3d, top3_quality_score, top5_quality_score, discovery_score
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row["variant_id"],
                        row["combo_id"],
                        row["regime"],
                        row["logic_id"],
                        row["variant_type"],
                        row["sample_count"],
                        row["hit_rate_3d"],
                        row["big_move_rate_3d"],
                        row["avg_return_3d"],
                        row["avg_max_return_3d"],
                        row["max_drawdown_3d"],
                        row["top3_quality_score"],
                        row["top5_quality_score"],
                        row["discovery_score"],
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def insert_replay_quality_results(self, rows: list[dict], *, run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into replay_quality_result (
                    run_id, logic_id, logic_name, trade_days, top_k, sample_count, hit_rate_3d,
                    big_move_rate_3d, avg_n3d, avg_n3d_max, avg_n3d_dd, topk_quality_score, passed
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        row["logic_id"],
                        row.get("logic_name"),
                        row["trade_days"],
                        row["top_k"],
                        row["sample_count"],
                        row["hit_rate_3d"],
                        row["big_move_rate_3d"],
                        row["avg_n3d"],
                        row["avg_n3d_max"],
                        row["avg_n3d_dd"],
                        row["topk_quality_score"],
                        1 if row.get("passed", False) else 0,
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def list_latest_factor_profiles(
        self,
        *,
        regime: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            with latest as (
                select run_id
                from factor_signal_profile
                order by created_at desc, run_id desc
                limit 1
            )
            select factor_signal_profile.run_id, factor_signal_profile.regime, factor_signal_profile.window_size,
                   factor_signal_profile.field, factor_signal_profile.min_value, factor_signal_profile.max_value,
                   factor_signal_profile.sample_count, factor_signal_profile.hit_rate_3d,
                   factor_signal_profile.big_move_rate_3d, factor_signal_profile.avg_return_3d,
                   factor_signal_profile.avg_max_return_3d, factor_signal_profile.max_drawdown_3d,
                   factor_signal_profile.discovery_score
            from factor_signal_profile, latest
            where factor_signal_profile.run_id = latest.run_id
        """
        params: list[object] = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        query += " order by discovery_score desc, sample_count desc, field limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "run_id": row[0],
                "regime": row[1],
                "window_size": row[2],
                "field": row[3],
                "min_value": row[4],
                "max_value": row[5],
                "sample_count": row[6],
                "hit_rate_3d": row[7],
                "big_move_rate_3d": row[8],
                "avg_return_3d": row[9],
                "avg_max_return_3d": row[10],
                "max_drawdown_3d": row[11],
                "discovery_score": row[12],
            }
            for row in rows
        ]

    def list_latest_factor_combo_results(
        self,
        *,
        regime: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            with latest as (
                select run_id
                from factor_combo_result
                order by created_at desc, run_id desc
                limit 1
            )
            select factor_combo_result.run_id, factor_combo_result.combo_id, factor_combo_result.regime,
                   factor_combo_result.window_size, factor_combo_result.fields_json,
                   factor_combo_result.sample_count, factor_combo_result.hit_rate_3d,
                   factor_combo_result.big_move_rate_3d, factor_combo_result.avg_return_3d,
                   factor_combo_result.avg_max_return_3d, factor_combo_result.max_drawdown_3d,
                   factor_combo_result.discovery_score, factor_combo_result.lift_vs_single
            from factor_combo_result, latest
            where factor_combo_result.run_id = latest.run_id
        """
        params: list[object] = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        query += " order by discovery_score desc, lift_vs_single desc, sample_count desc limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "run_id": row[0],
                "combo_id": row[1],
                "regime": row[2],
                "window_size": row[3],
                "fields": json.loads(row[4]),
                "sample_count": row[5],
                "hit_rate_3d": row[6],
                "big_move_rate_3d": row[7],
                "avg_return_3d": row[8],
                "avg_max_return_3d": row[9],
                "max_drawdown_3d": row[10],
                "discovery_score": row[11],
                "lift_vs_single": row[12],
            }
            for row in rows
        ]

    def list_latest_rule_variant_results(
        self,
        *,
        regime: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            with latest as (
                select run_id
                from rule_variant_result
                order by created_at desc, run_id desc
                limit 1
            )
            select rule_variant_result.run_id, rule_variant_result.variant_id, rule_variant_result.combo_id,
                   rule_variant_result.regime, rule_variant_result.logic_id, rule_variant_result.variant_type,
                   rule_variant_result.sample_count, rule_variant_result.hit_rate_3d,
                   rule_variant_result.big_move_rate_3d, rule_variant_result.avg_return_3d,
                   rule_variant_result.avg_max_return_3d, rule_variant_result.max_drawdown_3d,
                   rule_variant_result.top3_quality_score, rule_variant_result.top5_quality_score,
                   rule_variant_result.discovery_score
            from rule_variant_result, latest
            where rule_variant_result.run_id = latest.run_id
        """
        params: list[object] = []
        if regime is not None:
            query += " and regime = ?"
            params.append(regime)
        query += " order by top3_quality_score desc, top5_quality_score desc, discovery_score desc limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "run_id": row[0],
                "variant_id": row[1],
                "combo_id": row[2],
                "regime": row[3],
                "logic_id": row[4],
                "variant_type": row[5],
                "sample_count": row[6],
                "hit_rate_3d": row[7],
                "big_move_rate_3d": row[8],
                "avg_return_3d": row[9],
                "avg_max_return_3d": row[10],
                "max_drawdown_3d": row[11],
                "top3_quality_score": row[12],
                "top5_quality_score": row[13],
                "discovery_score": row[14],
            }
            for row in rows
        ]

    def list_latest_replay_quality_results(
        self,
        *,
        logic_id: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            with latest as (
                select run_id
                from replay_quality_result
                order by created_at desc, run_id desc
                limit 1
            )
            select replay_quality_result.run_id, replay_quality_result.logic_id, replay_quality_result.logic_name,
                   replay_quality_result.trade_days, replay_quality_result.top_k,
                   replay_quality_result.sample_count, replay_quality_result.hit_rate_3d,
                   replay_quality_result.big_move_rate_3d, replay_quality_result.avg_n3d,
                   replay_quality_result.avg_n3d_max, replay_quality_result.avg_n3d_dd,
                   replay_quality_result.topk_quality_score, replay_quality_result.passed
            from replay_quality_result, latest
            where replay_quality_result.run_id = latest.run_id
        """
        params: list[object] = []
        if logic_id is not None:
            query += " and logic_id = ?"
            params.append(logic_id)
        query += " order by topk_quality_score desc, sample_count desc, logic_id limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "run_id": row[0],
                "logic_id": row[1],
                "logic_name": row[2],
                "trade_days": row[3],
                "top_k": row[4],
                "sample_count": row[5],
                "hit_rate_3d": row[6],
                "big_move_rate_3d": row[7],
                "avg_n3d": row[8],
                "avg_n3d_max": row[9],
                "avg_n3d_dd": row[10],
                "topk_quality_score": row[11],
                "passed": row[12],
            }
            for row in rows
        ]

    def insert_discovery_eval_run(
        self,
        *,
        eval_run_id: str,
        train_days: int,
        test_days: int,
        follow_days: int,
        step_days: int,
        regimes: list[str],
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                insert into discovery_eval_run (
                    eval_run_id, train_days, test_days, follow_days, step_days, regimes
                ) values (?, ?, ?, ?, ?, ?)
                """,
                (eval_run_id, train_days, test_days, follow_days, step_days, ",".join(regimes)),
            )
            conn.commit()

    def insert_discovery_eval_window_results(self, rows: list[dict], *, eval_run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into discovery_eval_window_result (
                    eval_run_id, window_id, train_start, train_end, test_start, test_end,
                    follow_start, follow_end, candidate_count, dual_pass_count,
                    stable_candidate_count, window_status
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        eval_run_id,
                        row["window_id"],
                        row["train_start"],
                        row["train_end"],
                        row["test_start"],
                        row["test_end"],
                        row["follow_start"],
                        row["follow_end"],
                        row["candidate_count"],
                        row["dual_pass_count"],
                        row["stable_candidate_count"],
                        row["window_status"],
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def insert_discovery_eval_candidate_results(self, rows: list[dict], *, eval_run_id: str) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into discovery_eval_candidate_result (
                    eval_run_id, window_id, candidate_id, logic_id, regime, discovery_run_id,
                    train_top3_score, train_top5_score, test_top3_score, test_top5_score,
                    test_hit_3d, test_big_move_3d, test_avg_n3d, test_avg_n3d_max, test_avg_n3d_dd,
                    follow_validation_score, follow_validation_approved, stable_passed, status
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        eval_run_id,
                        row["window_id"],
                        row["candidate_id"],
                        row["logic_id"],
                        row["regime"],
                        row["discovery_run_id"],
                        row.get("train_top3_score"),
                        row.get("train_top5_score"),
                        row.get("test_top3_score"),
                        row.get("test_top5_score"),
                        row.get("test_hit_3d"),
                        row.get("test_big_move_3d"),
                        row.get("test_avg_n3d"),
                        row.get("test_avg_n3d_max"),
                        row.get("test_avg_n3d_dd"),
                        row.get("follow_validation_score"),
                        1 if row.get("follow_validation_approved", False) else 0,
                        1 if row.get("stable_passed", False) else 0,
                        row["status"],
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    def list_latest_discovery_eval_windows(self, *, limit: int = 20) -> list[dict]:
        query = """
            with latest as (
                select eval_run_id
                from discovery_eval_run
                order by created_at desc, eval_run_id desc
                limit 1
            )
            select discovery_eval_window_result.eval_run_id, discovery_eval_window_result.window_id,
                   discovery_eval_window_result.train_start, discovery_eval_window_result.train_end,
                   discovery_eval_window_result.test_start, discovery_eval_window_result.test_end,
                   discovery_eval_window_result.follow_start, discovery_eval_window_result.follow_end,
                   discovery_eval_window_result.candidate_count, discovery_eval_window_result.dual_pass_count,
                   discovery_eval_window_result.stable_candidate_count, discovery_eval_window_result.window_status
            from discovery_eval_window_result, latest
            where discovery_eval_window_result.eval_run_id = latest.eval_run_id
            order by train_start
            limit ?
        """
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, (limit,)).fetchall()
        return [
            {
                "eval_run_id": row[0],
                "window_id": row[1],
                "train_start": row[2],
                "train_end": row[3],
                "test_start": row[4],
                "test_end": row[5],
                "follow_start": row[6],
                "follow_end": row[7],
                "candidate_count": row[8],
                "dual_pass_count": row[9],
                "stable_candidate_count": row[10],
                "window_status": row[11],
            }
            for row in rows
        ]

    def list_latest_discovery_eval_candidates(
        self,
        *,
        logic_id: str | None = None,
        stable_only: bool = False,
        limit: int = 20,
    ) -> list[dict]:
        query = """
            with latest as (
                select eval_run_id
                from discovery_eval_run
                order by created_at desc, eval_run_id desc
                limit 1
            )
            select discovery_eval_candidate_result.eval_run_id, discovery_eval_candidate_result.window_id,
                   discovery_eval_candidate_result.candidate_id, discovery_eval_candidate_result.logic_id,
                   discovery_eval_candidate_result.regime, discovery_eval_candidate_result.discovery_run_id,
                   discovery_eval_candidate_result.train_top3_score, discovery_eval_candidate_result.train_top5_score,
                   discovery_eval_candidate_result.test_top3_score, discovery_eval_candidate_result.test_top5_score,
                   discovery_eval_candidate_result.test_hit_3d, discovery_eval_candidate_result.test_big_move_3d,
                   discovery_eval_candidate_result.test_avg_n3d, discovery_eval_candidate_result.test_avg_n3d_max,
                   discovery_eval_candidate_result.test_avg_n3d_dd,
                   discovery_eval_candidate_result.follow_validation_score,
                   discovery_eval_candidate_result.follow_validation_approved,
                   discovery_eval_candidate_result.stable_passed,
                   discovery_eval_candidate_result.status
            from discovery_eval_candidate_result, latest
            where discovery_eval_candidate_result.eval_run_id = latest.eval_run_id
        """
        params: list[object] = []
        if logic_id is not None:
            query += " and logic_id = ?"
            params.append(logic_id)
        if stable_only:
            query += " and stable_passed = true"
        query += " order by stable_passed desc, test_top5_score desc, follow_validation_score desc nulls last limit ?"
        params.append(limit)
        with self.connect(read_only=True) as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "eval_run_id": row[0],
                "window_id": row[1],
                "candidate_id": row[2],
                "logic_id": row[3],
                "regime": row[4],
                "discovery_run_id": row[5],
                "train_top3_score": row[6],
                "train_top5_score": row[7],
                "test_top3_score": row[8],
                "test_top5_score": row[9],
                "test_hit_3d": row[10],
                "test_big_move_3d": row[11],
                "test_avg_n3d": row[12],
                "test_avg_n3d_max": row[13],
                "test_avg_n3d_dd": row[14],
                "follow_validation_score": row[15],
                "follow_validation_approved": row[16],
                "stable_passed": row[17],
                "status": row[18],
            }
            for row in rows
        ]
