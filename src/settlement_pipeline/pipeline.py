"""Orquestra uma execução: ingestão, transformação, auditoria e publicação.

Toda execução fica registrada em ops.pipeline_runs. A linha é gravada no início (running)
e atualizada no fim, inclusive quando a execução quebra no meio.

Status:
    succeeded                tudo certo, gold publicada
    succeeded_with_warnings  publicada, com alertas de qualidade
    blocked                  dado bloqueado (contrato ou check com severidade error);
                             a gold publicada continua a anterior
    failed                   erro técnico; a gold publicada continua a anterior
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from settlement_pipeline import bronze, lake, transform
from settlement_pipeline.config import Settings
from settlement_pipeline.contracts import load_contracts
from settlement_pipeline.logs import log_event
from settlement_pipeline.publish import publish

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_DATA_BLOCKED = 1
EXIT_ERROR = 2


@dataclass
class PipelineOutcome:
    run_id: str
    status: str = "running"
    exit_code: int = EXIT_ERROR
    ingest: list[bronze.IngestResult] = field(default_factory=list)
    transform: transform.TransformResult | None = None
    published_snapshot: int | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Registro em ops.pipeline_runs
# ---------------------------------------------------------------------------

def _start(settings: Settings, run_id: str, command: str, reference_date: str | None) -> None:
    con = lake.connect(settings)
    try:
        con.execute(
            "insert into ops.pipeline_runs (run_id, command, reference_date, status, started_at) "
            "values (?, ?, ?, 'running', ?)",
            [run_id, command, reference_date, datetime.now(UTC)],
        )
    finally:
        con.close()


def _finish(settings: Settings, outcome: PipelineOutcome) -> None:
    ingest = outcome.ingest
    result = outcome.transform
    con = lake.connect(settings)
    try:
        con.execute("begin transaction")
        con.execute("""
            update ops.pipeline_runs set
                status = ?, exit_code = ?, finished_at = ?,
                duration_seconds = date_diff('millisecond', started_at, ?) / 1000,
                files_loaded = ?, files_skipped = ?, files_failed = ?, rows_ingested = ?,
                dbt_counts = ?, warnings = ?, failures = ?, published_snapshot = ?, error = ?
            where run_id = ?
        """, [
            outcome.status, outcome.exit_code, datetime.now(UTC), datetime.now(UTC),
            sum(r.status == "loaded" for r in ingest),
            sum(r.status == "skipped" for r in ingest),
            sum(r.status == "failed" for r in ingest),
            sum(r.rows_inserted for r in ingest),
            json.dumps(dict(result.counts)) if result else None,
            json.dumps(result.warnings) if result else None,
            json.dumps(result.failures) if result else None,
            outcome.published_snapshot, outcome.error, outcome.run_id,
        ])
        if result and result.nodes:
            now = datetime.now(UTC)
            con.executemany(
                "insert into ops.dq_results values (?, ?, ?, ?, ?, ?, ?)",
                [[outcome.run_id, n["node"], n["resource_type"], n["status"], n["failures"],
                  n["message"], now] for n in result.nodes],
            )
        con.execute("commit")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def run_pipeline(settings: Settings, command: str = "run", reference_date: str | None = None,
                 ingest: bool = True, transform_data: bool = True,
                 sources: list[str] | None = None, select: list[str] | None = None,
                 full_refresh: bool = False) -> PipelineOutcome:
    outcome = PipelineOutcome(run_id=str(uuid.uuid4()))
    _start(settings, outcome.run_id, command, reference_date)
    log_event(logger, "pipeline.started", run_id=outcome.run_id, command=command,
              reference_date=reference_date)
    try:
        if ingest:
            outcome.ingest = _ingest(settings, reference_date, sources)
            if any(r.status == "failed" for r in outcome.ingest):
                # Publicar com uma fonte desatualizada é pior do que não publicar.
                outcome.status, outcome.exit_code = "blocked", EXIT_DATA_BLOCKED
                return outcome

        if transform_data:
            outcome.transform = transform.run_dbt(settings, select, reference_date, full_refresh)
            if not outcome.transform.success:
                blocked = outcome.transform.blocked_by_quality
                outcome.status = "blocked" if blocked else "failed"
                outcome.exit_code = EXIT_DATA_BLOCKED if blocked else EXIT_ERROR
                return outcome
            # Build parcial (--select) não publica: a gold publicada tem que vir de um
            # build completo e testado.
            if not select:
                outcome.published_snapshot = publish(settings, outcome.run_id)

        warned = outcome.transform is not None and bool(outcome.transform.warnings)
        outcome.status = "succeeded_with_warnings" if warned else "succeeded"
        outcome.exit_code = EXIT_OK
        return outcome

    except Exception as exc:
        outcome.status, outcome.exit_code = "failed", EXIT_ERROR
        outcome.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        _finish(settings, outcome)
        log_event(logger, "pipeline.finished",
                  logging.INFO if outcome.exit_code == EXIT_OK else logging.ERROR,
                  run_id=outcome.run_id, status=outcome.status, exit_code=outcome.exit_code,
                  published_snapshot=outcome.published_snapshot)


def _ingest(settings: Settings, reference_date: str | None,
            sources: list[str] | None) -> list[bronze.IngestResult]:
    contracts = load_contracts(settings.contracts_path)
    if sources:
        unknown = set(sources) - {c.name for c in contracts}
        if unknown:
            raise ValueError(f"fonte desconhecida: {', '.join(sorted(unknown))}")
        contracts = [c for c in contracts if c.name in sources]

    con = lake.connect(settings)
    try:
        results = bronze.ingest(con, contracts, settings.data_path, reference_date)
    finally:
        con.close()

    for r in results:
        line = f"{r.status:<8} {r.source:<24} {Path(r.source_file).name:<40} +{r.rows_inserted}"
        if r.schema_drift:
            line += f"  drift: {', '.join(r.schema_drift)}"
        if r.status == "failed":
            line += f"  erro: {r.error}"
        print(line)
    return results
