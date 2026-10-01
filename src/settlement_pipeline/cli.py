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

from settlement_pipeline import bronze, lake, transform
from settlement_pipeline.config import load_settings
from settlement_pipeline.contracts import load_contracts
from settlement_pipeline.logs import log_event, setup_logging

logger = logging.getLogger("settlement_pipeline")

EXIT_OK = 0
EXIT_DATA_BLOCKED = 1
EXIT_ERROR = 2


def _iso_date(value: str) -> str:
    date.fromisoformat(value)  # valida o formato YYYY-MM-DD
    return value


def cmd_ingest(args: argparse.Namespace) -> int:
    settings = load_settings(data_path=args.data_path)
    contracts = load_contracts(settings.contracts_path)
    if args.source:
        unknown = set(args.source) - {c.name for c in contracts}
        if unknown:
            raise SystemExit(f"fonte desconhecida: {', '.join(sorted(unknown))}")
        contracts = [c for c in contracts if c.name in args.source]

    con = lake.connect(settings)
    try:
        results = bronze.ingest(con, contracts, settings.data_path, args.reference_date)
    finally:
        con.close()

    summary = {status: sum(r.status == status for r in results)
               for status in ("loaded", "skipped", "failed")}
    log_event(logger, "ingest.summary", **summary,
              rows_inserted=sum(r.rows_inserted for r in results))
    for r in results:
        line = f"{r.status:<8} {r.source:<24} {Path(r.source_file).name:<40} +{r.rows_inserted}"
        if r.schema_drift:
            line += f"  drift: {', '.join(r.schema_drift)}"
        if r.status == "failed":
            line += f"  erro: {r.error}"
        print(line)
    return EXIT_DATA_BLOCKED if summary["failed"] else EXIT_OK


def cmd_transform(args: argparse.Namespace) -> int:
    result = transform.run_dbt(load_settings(), args.select, args.reference_date,
                               args.full_refresh)
    if result.success:
        return EXIT_OK
    return EXIT_DATA_BLOCKED if result.blocked_by_quality else EXIT_ERROR


def cmd_run(args: argparse.Namespace) -> int:
    """Ingestão + transformação. Se alguma fonte violar o contrato, não transforma:
    publicar silver/gold com uma fonte desatualizada é pior do que não publicar."""
    ingest_code = cmd_ingest(args)
    if ingest_code != EXIT_OK:
        log_event(logger, "run.stopped_before_transform", logging.ERROR,
                  reason="ingestão com arquivos bloqueados", exit_code=ingest_code)
        return ingest_code
    args.select, args.full_refresh = None, False
    return cmd_transform(args)


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


def cmd_status(args: argparse.Namespace) -> int:
    con = lake.connect(load_settings(), read_only=True)
    try:
        con.sql(f"""
            select started_at::timestamp(0) as started_at, source, status,
                   regexp_extract(source_file, '[^/]+$') as file, reference_date,
                   rows_read, rows_inserted, schema_drift, left(error, 60) as error
            from ops.ingestion_log
            order by started_at desc
            limit {int(args.limit)}
        """).show(max_width=200)
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

    p_transform = sub.add_parser("transform", help="roda o dbt build (silver e gold)")
    p_transform.add_argument("--select", nargs="+", help="seleção do dbt, ex.: silver")
    p_transform.add_argument("--reference-date", type=_iso_date,
                             help="reprocessa só esta data no fato incremental da gold")
    p_transform.add_argument("--full-refresh", action="store_true",
                             help="reconstrói as tabelas incrementais do zero")
    p_transform.set_defaults(func=cmd_transform)

    p_run = sub.add_parser("run", help="ingest + transform")
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

    p_status = sub.add_parser("status", help="mostra as últimas ingestões")
    p_status.add_argument("--limit", type=int, default=20)
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
