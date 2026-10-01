"""CLI do pipeline. É a unidade que um orquestrador chamaria em produção.

Códigos de saída:
    0  tudo certo (pode haver alertas)
    1  dado bloqueado: arquivo fora do contrato ou check de qualidade com severidade error
    2  erro inesperado (configuração, infraestrutura, bug)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import duckdb

from settlement_pipeline import lake
from settlement_pipeline.config import load_settings
from settlement_pipeline.logs import log_event, setup_logging
from settlement_pipeline.pipeline import EXIT_DATA_BLOCKED, EXIT_ERROR, EXIT_OK, run_pipeline

logger = logging.getLogger("settlement_pipeline")

__all__ = ["EXIT_DATA_BLOCKED", "EXIT_ERROR", "EXIT_OK", "main"]


def _iso_date(value: str) -> str:
    date.fromisoformat(value)  # valida o formato YYYY-MM-DD
    return value


def cmd_ingest(args: argparse.Namespace) -> int:
    settings = load_settings(data_path=args.data_path)
    return run_pipeline(settings, "ingest", args.reference_date, transform_data=False,
                        sources=args.source).exit_code


def cmd_transform(args: argparse.Namespace) -> int:
    return run_pipeline(load_settings(), "transform", args.reference_date, ingest=False,
                        select=args.select, full_refresh=args.full_refresh).exit_code


def cmd_run(args: argparse.Namespace) -> int:
    settings = load_settings(data_path=args.data_path)
    return run_pipeline(settings, "run", args.reference_date).exit_code


def cmd_query(args: argparse.Namespace) -> int:
    """Roda SQL (texto ou arquivo .sql) contra o lake, em modo leitura."""
    sql = Path(args.sql).read_text(encoding="utf-8") if args.sql.endswith(".sql") else args.sql
    con = lake.connect(load_settings(), read_only=True)
    try:
        # O parser do DuckDB separa as instruções sem tropeçar em ";" dentro de comentários.
        for statement in duckdb.extract_statements(sql):
            relation = con.sql(statement.query)
            if relation is not None:  # set variable e afins não devolvem tabela
                relation.show(max_width=args.max_width, max_rows=args.max_rows)
    finally:
        con.close()
    return EXIT_OK


STATUS_QUERIES = {
    "Últimas execuções": """
        select started_at::timestamp(0) as inicio, command as comando, reference_date,
               status, round(duration_seconds, 1) as duracao_s, files_loaded as carregados,
               files_failed as bloqueados, len(json_keys(warnings)) as alertas,
               published_snapshot as snapshot_publicado, left(error, 60) as erro
        from ops.pipeline_runs order by started_at desc limit {limit}
    """,
    "Execuções que não terminaram (morreram no meio?)": """
        select run_id, command, started_at::timestamp(0) as inicio,
               date_diff('minute', started_at, now()) as minutos_desde_o_inicio
        from ops.pipeline_runs where status = 'running' order by started_at
    """,
    "Frescor por fonte (bronze)": """
        select source as fonte, max(finished_at)::timestamp(0) as ultima_carga,
               max(reference_date) as ultima_data_referencia,
               count(*) filter (where status = 'failed') as falhas_total
        from ops.ingestion_log where status in ('loaded', 'failed')
        group by 1 order by 1
    """,
    "O que os consumidores estão vendo (gold publicada)": """
        select p.snapshot_id as snapshot, p.published_at::timestamp(0) as publicado_em,
               (select max(reference_date) from gold.agg_run_health) as ultimo_run_publicado,
               (select max(reference_date) from silver.reconciliation_runs) as ultimo_run_na_silver
        from ops.publications as p order by p.published_at desc limit 1
    """,
    "Alertas e falhas da última execução com dbt": """
        select node as check, status, failures as linhas, left(message, 80) as mensagem
        from ops.dq_results
        where run_id = (select run_id from ops.dq_results order by recorded_at desc limit 1)
          and status in ('warn', 'fail', 'error')
        order by status, node
    """,
}


def cmd_status(args: argparse.Namespace) -> int:
    """Painel de diagnóstico: o primeiro comando a rodar quando algo parece errado."""
    con = lake.connect(load_settings(), read_only=True)
    try:
        for title, sql in STATUS_QUERIES.items():
            print(f"\n== {title}")
            try:
                relation = con.sql(sql.format(limit=int(args.limit)))
                if relation.fetchone() is None:
                    print("(nada)")
                else:
                    relation.show(max_width=200)
            except duckdb.CatalogException:
                print("(ainda não existe; rode `pipeline run`)")
    finally:
        con.close()
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="carrega as fontes no bronze")
    p_ingest.add_argument("--data-path", type=Path,
                          help="diretório das fontes (default: $DATA_PATH)")
    p_ingest.add_argument("--reference-date", type=_iso_date,
                          help="data de referência para CSVs sem data no nome (YYYY-MM-DD)")
    p_ingest.add_argument("--source", action="append",
                          help="carrega só esta fonte (pode repetir)")
    p_ingest.set_defaults(func=cmd_ingest)

    p_transform = sub.add_parser("transform", help="dbt build e publicação da gold")
    p_transform.add_argument("--select", nargs="+", help="seleção do dbt, ex.: silver")
    p_transform.add_argument("--reference-date", type=_iso_date,
                             help="reprocessa só esta data no fato incremental da gold")
    p_transform.add_argument("--full-refresh", action="store_true",
                             help="reconstrói as tabelas incrementais do zero")
    p_transform.set_defaults(func=cmd_transform)

    p_run = sub.add_parser("run", help="ingest + transform + publicação da gold")
    p_run.add_argument("--data-path", type=Path,
                       help="diretório das fontes (default: $DATA_PATH)")
    p_run.add_argument("--reference-date", type=_iso_date,
                       help="data de referência para CSVs sem data no nome (YYYY-MM-DD)")
    p_run.set_defaults(func=cmd_run, source=None)

    p_query = sub.add_parser("query", help="roda SQL contra o lake (somente leitura)")
    p_query.add_argument("sql", help="texto SQL ou caminho de um arquivo .sql")
    p_query.add_argument("--max-rows", type=int, default=50)
    p_query.add_argument("--max-width", type=int, default=200)
    p_query.set_defaults(func=cmd_query)

    p_status = sub.add_parser("status", help="painel: execuções, frescor e alertas")
    p_status.add_argument("--limit", type=int, default=10)
    p_status.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(load_settings().log_level)
    try:
        return args.func(args)
    except (lake.LakeLockedError, lake.LakeNotFoundError) as exc:
        log_event(logger, "pipeline.lake_unavailable", logging.ERROR, error=str(exc))
        print(f"erro: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except Exception:
        logger.exception("pipeline.error")
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
