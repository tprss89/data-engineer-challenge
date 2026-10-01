"""Conexão com o DuckLake e tabelas de controle do pipeline."""
from __future__ import annotations

import duckdb

from settlement_pipeline.config import Settings

SCHEMAS = ("bronze", "ops", "gold")

# Uma linha por arquivo processado, inclusive os ignorados e os que falharam.
# É o primeiro lugar a olhar quando um dado "sumiu".
INGESTION_LOG_DDL = """
create table if not exists ops.ingestion_log (
    batch_id        varchar,
    source          varchar,
    source_file     varchar,
    file_hash       varchar,
    file_size_bytes bigint,
    reference_date  date,
    status          varchar,   -- loaded | skipped | failed
    rows_read       bigint,
    rows_inserted   bigint,
    schema_drift    varchar,   -- colunas novas que apareceram na fonte
    error           varchar,
    started_at      timestamptz,
    finished_at     timestamptz
)
"""

# Uma linha por execução da CLI. Gravada no início com status running e atualizada no fim:
# uma execução que morreu no meio fica como running, o que já é um sinal.
PIPELINE_RUNS_DDL = """
create table if not exists ops.pipeline_runs (
    run_id             varchar,
    command            varchar,
    reference_date     date,
    status             varchar,   -- running | succeeded | succeeded_with_warnings | blocked | failed
    exit_code          integer,
    started_at         timestamptz,
    finished_at        timestamptz,
    duration_seconds   double,
    files_loaded       integer,
    files_skipped      integer,
    files_failed       integer,
    rows_ingested      bigint,
    dbt_counts         json,
    warnings           json,
    failures           json,
    published_snapshot bigint,
    error              varchar
)
"""

# Resultado de cada modelo e teste do dbt em cada execução: série histórica da qualidade.
DQ_RESULTS_DDL = """
create table if not exists ops.dq_results (
    run_id        varchar,
    node          varchar,
    resource_type varchar,   -- model | test
    status        varchar,   -- success | pass | warn | fail | error | skipped
    failures      bigint,
    message       varchar,
    recorded_at   timestamptz
)
"""

# Cada publicação da gold: qual snapshot os consumidores passaram a ver, e quando.
PUBLICATIONS_DDL = """
create table if not exists ops.publications (
    run_id       varchar,
    snapshot_id  bigint,
    tables       varchar[],
    published_at timestamptz
)
"""


class LakeLockedError(RuntimeError):
    """O catálogo está aberto por outro processo."""


class LakeNotFoundError(RuntimeError):
    """Ainda não existe lake no caminho configurado."""


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(settings: Settings, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if read_only and not settings.catalog_file.exists():
        raise LakeNotFoundError(
            f"lake não encontrado em {settings.lake_path}; rode `pipeline run` primeiro"
        )
    if not read_only:
        settings.lake_data_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("load ducklake")
    options = f"data_path {_sql_literal(settings.lake_data_path.as_posix() + '/')}"
    if read_only:
        options += ", read_only"
    try:
        con.execute(
            f"attach {_sql_literal('ducklake:' + settings.catalog_file.as_posix())} "
            f"as lake ({options})"
        )
    except duckdb.IOException as exc:
        if "lock" not in str(exc).lower() and "permission denied" not in str(exc).lower():
            raise
        # O catálogo é um arquivo DuckDB: só um processo pode tê-lo aberto para escrita.
        raise LakeLockedError(
            f"não consegui abrir {settings.catalog_file} para escrita; outro processo está com "
            "o catálogo aberto (DBeaver, por exemplo). Feche a conexão e rode de novo."
        ) from exc
    con.execute("use lake")
    if not read_only:
        for schema in SCHEMAS:
            con.execute(f"create schema if not exists {schema}")
        for ddl in (INGESTION_LOG_DDL, PIPELINE_RUNS_DDL, DQ_RESULTS_DDL, PUBLICATIONS_DDL):
            con.execute(ddl)
    return con
