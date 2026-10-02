#!/usr/bin/env python3
"""Mede o pipeline em escala com dados do gerador. Roda dentro do container.

    python scripts/benchmark.py --rows 5000000 --days 30

Etapas medidas, cada uma num lake temporário próprio:
    1. geração dos dados
    2. ingestão no bronze
    3. dbt build completo + publicação
    4. segunda execução sem dados novos (idempotência: deveria ser barata)
    5. reprocessamento de uma única data de referência (incremental)

Memória: pico de RSS do processo (ingestão) e dos subprocessos (dbt).
"""
from __future__ import annotations

import argparse
import json
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from settlement_pipeline import lake
from settlement_pipeline.config import REPO_ROOT, load_settings
from settlement_pipeline.pipeline import run_pipeline


def peak_mb(who: int) -> int:
    return resource.getrusage(who).ru_maxrss // 1024


def dir_mb(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) // (1024 * 1024)


def timed(label: str, results: list, fn):
    start = time.perf_counter()
    value = fn()
    elapsed = time.perf_counter() - start
    results.append((label, elapsed))
    print(f"[benchmark] {label}: {elapsed:.1f}s", file=sys.stderr, flush=True)
    # Uma etapa que falhou invalida o resto da medição.
    if getattr(value, "exit_code", 0) != 0:
        raise SystemExit(f"[benchmark] {label} terminou com status {value.status}: "
                         f"{getattr(value.transform, 'failures', None) or value.error}")
    return value


def slowest_models(target: Path, top: int = 6) -> list[tuple[str, float]]:
    run_results = json.loads((target / "run_results.json").read_text(encoding="utf-8"))
    models = [(n["unique_id"].split(".")[-1], n["execution_time"])
              for n in run_results["results"] if n["unique_id"].startswith("model.")]
    return sorted(models, key=lambda m: m[1], reverse=True)[:top]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--merchants", type=int, default=500)
    parser.add_argument("--workdir", type=Path, default=None,
                        help="onde gravar dados e lake (default: diretório temporário)")
    parser.add_argument("--keep", action="store_true", help="não apaga dados e lake no fim")
    parser.add_argument("--reuse-sources", action="store_true",
                        help="reaproveita os dados já gerados no workdir (pula a geração)")
    args = parser.parse_args()

    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="benchmark-"))
    data, lake_path = workdir / "sources", workdir / "lake"
    reuse = args.reuse_sources and data.exists()
    shutil.rmtree(lake_path, ignore_errors=True)
    shutil.rmtree(workdir / "dbt_target", ignore_errors=True)
    if not reuse:
        shutil.rmtree(workdir, ignore_errors=True)
    settings = load_settings(data_path=data, lake_path=lake_path)
    steps: list[tuple[str, float]] = []

    if reuse:
        print(f"[benchmark] reaproveitando fontes em {data}", file=sys.stderr)
    else:
        timed("1. geração", steps, lambda: subprocess.run([
        sys.executable, str(REPO_ROOT / "scripts" / "generate_sample_data.py"),
            "--rows", str(args.rows), "--days", str(args.days),
            "--merchants", str(args.merchants), "--out", str(data),
        ], check=True, stdout=subprocess.DEVNULL))

    ingest = timed("2. ingestão (bronze)", steps,
                   lambda: run_pipeline(settings, "ingest", transform_data=False))
    ingest_peak = peak_mb(resource.RUSAGE_SELF)

    full = timed("3. dbt build completo + publicação", steps,
                 lambda: run_pipeline(settings, "transform", ingest=False))
    target = lake_path.parent / "dbt_target"
    models = slowest_models(target)
    dbt_peak = peak_mb(resource.RUSAGE_CHILDREN)

    timed("4. segunda execução, sem dados novos", steps, lambda: run_pipeline(settings, "run"))

    con = lake.connect(settings, read_only=True)
    one_date = con.execute(
        "select max(reference_date)::varchar from silver.reconciliation_runs").fetchone()[0]
    rows = dict(con.execute("""
        select 'bronze eventos/linhas', (select count(*) from bronze.transactions)
             + (select count(*) from bronze.reconciliation_results)
             + (select count(*) from bronze.paysettler_settlements)
        union all select 'silver.transactions', count(*) from silver.transactions
        union all select 'silver.reconciliation_results', count(*) from silver.reconciliation_results
        union all select 'gold.fct_reconciliation_result', count(*) from gold.fct_reconciliation_result
        union all select 'gold.audit_transaction_trail', count(*) from gold.audit_transaction_trail
        union all select 'silver.quarantine', count(*) from silver.quarantine
    """).fetchall())
    con.close()

    timed(f"5. reprocessar uma data ({one_date})", steps,
          lambda: run_pipeline(settings, "transform", one_date, ingest=False))

    print(f"\n## Benchmark: {args.rows:,} transações, {args.days} dias\n")
    print("| Etapa | Tempo |\n|---|---|")
    for label, elapsed in steps:
        print(f"| {label} | {elapsed:.1f} s |")
    print(f"\nStatus: ingestão `{ingest.status}`, build `{full.status}`.")
    print(f"Pico de memória: ingestão {ingest_peak} MB, dbt {dbt_peak} MB.")
    print(f"Tamanho: fontes {dir_mb(data)} MB, lake {dir_mb(lake_path)} MB.\n")
    print("| Tabela | Linhas |\n|---|---|")
    for name, count in rows.items():
        print(f"| {name} | {count:,} |")
    print("\n| Modelo mais lento no build completo | Tempo |\n|---|---|")
    for name, seconds in models:
        print(f"| {name} | {seconds:.1f} s |")

    if not args.keep:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
