"""Ingestão no bronze: cópia fiel das fontes, só append, com metadados de ingestão.

Regras desta camada:

- Nenhuma transformação de valor. O CSV entra como texto, inclusive "R$ 32.245,91".
- Idempotência em dois níveis:
    1. arquivo: um arquivo com o mesmo hash já carregado é ignorado;
    2. evento (só CDC): as extrações CDC são acumuladas, então uma extração nova traz
       eventos que já estão no bronze. Cada evento tem um hash do conteúdo e só entra
       uma vez. Para CSV não existe dedupe por linha: duplicatas do processador são
       mantidas aqui para auditoria e tratadas no silver.
- Cada arquivo é carregado numa transação junto com a sua linha em ops.ingestion_log.
  Se algo falha no meio, nada do arquivo fica no lake.
- Colunas novas na fonte (schema drift) entram na tabela e geram alerta. Coluna
  obrigatória ausente ou tipo diferente do contrato bloqueia o arquivo.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from settlement_pipeline.contracts import Contract
from settlement_pipeline.logs import log_event

logger = logging.getLogger(__name__)

METADATA_COLUMNS = {
    "_batch_id": "varchar",
    "_source_file": "varchar",
    "_file_hash": "varchar",
    "_source_row": "bigint",
    "_ingested_at": "timestamptz",
}
CDC_METADATA_COLUMNS = {"_event_hash": "varchar"}
FILE_METADATA_COLUMNS = {"_reference_date": "date"}


class ContractViolation(Exception):
    """O arquivo não respeita o contrato da fonte. Bloqueia o arquivo."""


@dataclass
class IngestResult:
    source: str
    source_file: str
    status: str  # loaded | skipped | failed
    rows_read: int = 0
    rows_inserted: int = 0
    schema_drift: list[str] = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(contract: Contract, data_path: Path) -> list[Path]:
    """Arquivos da fonte, na ordem dos padrões do contrato e depois por nome."""
    seen: dict[Path, None] = {}
    for pattern in contract.paths:
        for path in sorted(data_path.glob(pattern)):
            seen.setdefault(path.resolve(), None)
    return list(seen)


def _reader_sql(contract: Contract, path: Path) -> str:
    if contract.format == "parquet":
        return f"read_parquet({_literal(path.as_posix())}, file_row_number = true)"
    # Formato definido no glossário: UTF-8, vírgula, aspas duplas, header na primeira linha.
    return (
        f"read_csv({_literal(path.as_posix())}, header = true, delim = ',', quote = '\"', "
        "encoding = 'utf-8', all_varchar = true)"
    )


def _source_schema(con: duckdb.DuckDBPyConnection, reader: str) -> dict[str, str]:
    rows = con.execute(f"describe select * from {reader}").fetchall()
    return {name: dtype.upper() for name, dtype, *_ in rows if name != "file_row_number"}


def validate_schema(contract: Contract, source_schema: dict[str, str]) -> list[str]:
    """Valida o schema do arquivo contra o contrato e devolve as colunas novas (drift)."""
    missing = [c for c in contract.required_columns if c not in source_schema]
    if missing:
        raise ContractViolation(f"colunas obrigatórias ausentes: {', '.join(missing)}")

    wrong_types = [
        f"{c.name} (esperado {c.type}, veio {source_schema[c.name]})"
        for c in contract.columns
        if c.name in source_schema and source_schema[c.name] != c.type.upper()
    ]
    if wrong_types:
        raise ContractViolation(f"tipos diferentes do contrato: {'; '.join(wrong_types)}")

    return [name for name in source_schema if contract.column(name) is None]


def _resolve_reference_date(contract: Contract, path: Path, cli_date: str | None) -> date:
    from_name = contract.reference_date_from_filename(path.name)
    value = from_name or cli_date
    if value is None:
        raise ContractViolation(
            f"{path.name} não tem data no nome; informe --reference-date para este arquivo"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ContractViolation(f"data de referência inválida: {value}") from exc


def _metadata_columns(contract: Contract) -> dict[str, str]:
    extra = CDC_METADATA_COLUMNS if contract.kind == "cdc" else FILE_METADATA_COLUMNS
    return {**METADATA_COLUMNS, **extra}


def _ensure_table(con, contract: Contract, source_schema: dict[str, str]) -> None:
    columns = [f"{_quote(c.name)} {c.type}" for c in contract.columns]
    columns += [f"{name} {dtype}" for name, dtype in _metadata_columns(contract).items()]
    con.execute(f"create table if not exists {contract.table} ({', '.join(columns)})")

    existing = {row[0] for row in con.execute(f"describe {contract.table}").fetchall()}
    for name, dtype in source_schema.items():
        if name not in existing:
            con.execute(f"alter table {contract.table} add column {_quote(name)} {dtype}")


def _already_loaded(con, source: str, file_hash: str) -> str | None:
    row = con.execute(
        "select source_file from ops.ingestion_log "
        "where source = ? and file_hash = ? and status = 'loaded' limit 1",
        [source, file_hash],
    ).fetchone()
    return row[0] if row else None


def _write_log(con, batch_id: str, contract: Contract, path: Path, file_hash: str | None,
               reference_date: date | None, result: IngestResult, started_at: datetime) -> None:
    con.execute(
        "insert into ops.ingestion_log values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            batch_id, contract.name, path.as_posix(), file_hash,
            path.stat().st_size if path.exists() else None, reference_date, result.status,
            result.rows_read, result.rows_inserted,
            ", ".join(result.schema_drift) or None, result.error,
            started_at, datetime.now(UTC),
        ],
    )


def _insert_sql(contract: Contract, reader: str, source_schema: dict[str, str]) -> str:
    source_cols = ", ".join(_quote(c) for c in source_schema)
    common = (
        "? as _batch_id, ? as _source_file, ? as _file_hash, "
        "current_timestamp as _ingested_at"
    )
    if contract.kind == "cdc":
        # Hash do evento sobre todas as colunas da fonte, em ordem fixa.
        packed = ", ".join(f"{_quote(c)} := {_quote(c)}" for c in sorted(source_schema))
        return f"""
            insert into {contract.table} by name
            with src as (
                select {source_cols},
                       file_row_number + 1 as _source_row,
                       md5(to_json(struct_pack({packed}))) as _event_hash
                from {reader}
            ),
            new_events as (
                select * from src
                where _event_hash not in (select _event_hash from {contract.table})
                qualify row_number() over (partition by _event_hash order by _source_row) = 1
            )
            select *, {common} from new_events
        """
    return f"""
        insert into {contract.table} by name
        select {source_cols},
               row_number() over () as _source_row,
               ? as _reference_date,
               {common}
        from {reader}
    """


# ---------------------------------------------------------------------------
# Ingestão
# ---------------------------------------------------------------------------

def ingest_file(con: duckdb.DuckDBPyConnection, contract: Contract, path: Path,
                reference_date: str | None = None) -> IngestResult:
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)
    result = IngestResult(source=contract.name, source_file=path.as_posix(), status="failed")
    file_hash = None
    ref_date = None
    fields = {"source": contract.name, "file": path.name, "batch_id": batch_id}

    try:
        file_hash = file_sha256(path)
        loaded_from = _already_loaded(con, contract.name, file_hash)
        if loaded_from:
            result.status = "skipped"
            result.error = f"mesmo conteúdo já carregado de {loaded_from}"
            _write_log(con, batch_id, contract, path, file_hash, None, result, started_at)
            log_event(logger, "ingest.skipped", **fields, reason=result.error)
            return result

        reader = _reader_sql(contract, path)
        source_schema = _source_schema(con, reader)
        result.schema_drift = validate_schema(contract, source_schema)
        if contract.kind == "file":
            ref_date = _resolve_reference_date(contract, path, reference_date)
        result.rows_read = con.execute(f"select count(*) from {reader}").fetchone()[0]

        params = [batch_id, path.as_posix(), file_hash]
        if contract.kind == "file":
            params = [ref_date, *params]

        con.execute("begin transaction")
        try:
            _ensure_table(con, contract, source_schema)
            inserted = con.execute(_insert_sql(contract, reader, source_schema), params)
            result.rows_inserted = inserted.fetchone()[0]
            result.status = "loaded"
            _write_log(con, batch_id, contract, path, file_hash, ref_date, result, started_at)
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise

    # Qualquer erro num arquivo vira falha registrada; as outras fontes seguem.
    except Exception as exc:  # noqa: BLE001
        result.status = "failed"
        result.rows_inserted = 0
        result.error = f"{type(exc).__name__}: {exc}"
        _write_log(con, batch_id, contract, path, file_hash, ref_date, result, started_at)
        log_event(logger, "ingest.failed", logging.ERROR, **fields, error=result.error)
        return result

    if result.schema_drift:
        log_event(logger, "ingest.schema_drift", logging.WARNING, **fields,
                  new_columns=result.schema_drift)
    log_event(logger, "ingest.loaded", **fields, rows_read=result.rows_read,
              rows_inserted=result.rows_inserted,
              duration_ms=int((datetime.now(UTC) - started_at).total_seconds() * 1000))
    return result


def ingest(con: duckdb.DuckDBPyConnection, contracts: list[Contract], data_path: Path,
           reference_date: str | None = None) -> list[IngestResult]:
    results = []
    for contract in contracts:
        files = discover_files(contract, data_path)
        if not files:
            log_event(logger, "ingest.no_files", logging.WARNING, source=contract.name,
                      data_path=data_path.as_posix(), patterns=contract.paths)
        for path in files:
            results.append(ingest_file(con, contract, path, reference_date))
    return results
