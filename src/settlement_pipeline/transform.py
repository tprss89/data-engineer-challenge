"""Roda o dbt (silver e gold) e traduz o resultado em algo que o pipeline entende.

O dbt roda num subprocesso, como um orquestrador rodaria. Dentro do mesmo processo, o
dbt-duckdb mantém o catálogo aberto depois de terminar e o estado global do dbt vaza entre
execuções. O resultado de cada modelo e teste vem do run_results.json.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from settlement_pipeline.config import REPO_ROOT, Settings
from settlement_pipeline.logs import log_event

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_EXECUTABLE = Path(sys.executable).parent / "dbt"


@dataclass
class TransformResult:
    exit_code: int
    counts: Counter = field(default_factory=Counter)  # success, pass, warn, fail, error, skipped
    warnings: dict[str, int] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.exit_code == 0

    @property
    def blocked_by_quality(self) -> bool:
        """Falhou por check de dado (teste com severidade error), não por erro técnico."""
        return self.counts["fail"] > 0 and self.counts["error"] == 0


def _node_name(unique_id: str) -> str:
    # model.settlement.transactions -> transactions
    return unique_id.split(".")[2]


def run_dbt(settings: Settings, select: list[str] | None = None,
            reference_date: str | None = None, full_refresh: bool = False) -> TransformResult:
    target_path = settings.lake_path.parent / "dbt_target"
    run_results = target_path / "run_results.json"
    run_results.unlink(missing_ok=True)

    cmd = [str(DBT_EXECUTABLE), "build", "--project-dir", str(DBT_PROJECT_DIR),
           "--profiles-dir", str(DBT_PROJECT_DIR), "--target-path", str(target_path),
           "--log-path", str(target_path / "logs"), "--no-use-colors"]
    if select:
        cmd += ["--select", *select]
    if reference_date:
        # O fato da gold é incremental: com a data, só ela é apagada e regravada.
        cmd += ["--vars", json.dumps({"reference_date": reference_date})]
    if full_refresh:
        cmd += ["--full-refresh"]
    env = {**os.environ, "LAKE_PATH": settings.lake_path.as_posix()}

    log_event(logger, "transform.started", select=select, reference_date=reference_date,
              full_refresh=full_refresh)
    completed = subprocess.run(cmd, env=env, check=False)
    result = TransformResult(exit_code=completed.returncode)

    if run_results.exists():
        for node in json.loads(run_results.read_text(encoding="utf-8"))["results"]:
            name, status = _node_name(node["unique_id"]), node["status"]
            result.counts[status] += 1
            if status == "warn":
                result.warnings[name] = node.get("failures") or 0
            elif status in ("fail", "error"):
                result.failures[name] = node.get("message") or status
    elif not result.success:
        result.counts["error"] += 1
        result.failures["dbt"] = f"dbt saiu com código {completed.returncode} sem run_results"

    log_event(logger, "transform.finished",
              logging.INFO if result.success else logging.ERROR,
              exit_code=result.exit_code, counts=dict(result.counts),
              warnings=result.warnings, failures=result.failures)
    return result
