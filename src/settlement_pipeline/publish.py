"""Publicação da gold (o "publish" do write-audit-publish).

O dbt grava e testa tudo em gold_build. Só depois que o build inteiro passa, o pipeline
aponta as views de gold para o snapshot do DuckLake que acabou de ser testado, numa única
transação. Consequências:

- consumidores nunca veem uma publicação pela metade;
- se um check bloqueante falha, as views continuam no snapshot anterior e os consumidores
  seguem vendo a última versão boa;
- não há cópia de dados: a view lê a tabela como ela estava naquele snapshot;
- voltar uma publicação é apontar as views para um snapshot anterior.

Cuidado em produção: expirar snapshots antigos não pode apagar o snapshot publicado.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from settlement_pipeline import lake
from settlement_pipeline.config import Settings
from settlement_pipeline.logs import log_event

logger = logging.getLogger(__name__)

BUILD_SCHEMA = "gold_build"
PUBLISHED_SCHEMA = "gold"


def publish(settings: Settings, run_id: str) -> int:
    con = lake.connect(settings)
    try:
        snapshot = con.execute("select max(snapshot_id) from lake.snapshots()").fetchone()[0]
        tables = [row[0] for row in con.execute(f"""
            select table_name from information_schema.tables
            where table_schema = '{BUILD_SCHEMA}' and table_type = 'BASE TABLE'
              and table_name not like '%__dbt_tmp' and table_name not like '%__dbt_backup'
            order by table_name
        """).fetchall()]
        if not tables:
            raise RuntimeError(f"nada para publicar: {BUILD_SCHEMA} está vazio")

        con.execute("begin transaction")
        for table in tables:
            con.execute(
                f"create or replace view {PUBLISHED_SCHEMA}.{table} as "
                f"select * from {BUILD_SCHEMA}.{table} at (version => {snapshot})"
            )
        con.execute("insert into ops.publications values (?, ?, ?, ?)",
                    [run_id, snapshot, tables, datetime.now(UTC)])
        con.execute("commit")
    finally:
        con.close()

    log_event(logger, "publish.done", run_id=run_id, snapshot_id=snapshot, tables=tables)
    return snapshot
