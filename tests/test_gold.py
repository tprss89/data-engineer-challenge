"""Produtos de dados da gold: o que cada consumidor recebe, verificado sobre o fixture."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
from conftest import FIXTURE_REFERENCE_DATE, SAMPLE_DATA
from test_silver import build_lake, query, scalar

from settlement_pipeline import lake, transform
from settlement_pipeline.config import REPO_ROOT, load_settings
from settlement_pipeline.pipeline import run_pipeline

EXAMPLE_QUERIES = sorted((REPO_ROOT / "docs" / "queries").glob("*.sql"))


@pytest.fixture(scope="module")
def gold(tmp_path_factory):
    settings, result = build_lake(tmp_path_factory.mktemp("gold"))
    assert result.success, result.failures
    return settings


# ---------------------------------------------------------------------------
# Operações
# ---------------------------------------------------------------------------

def test_run_health_flags_the_failed_run(gold):
    rows = dict(query(gold, "select run_id, health_status from gold.agg_run_health"))
    assert rows[7] == "FAILED"
    assert {status for run_id, status in rows.items() if run_id != 7} == {"OK"}


def test_run_health_compares_declared_and_received_results(gold):
    declared, received = query(gold, """
        select declared_results, results_received from gold.agg_run_health where run_id = 1
    """)[0]
    assert declared == received == 119


def test_backlog_separates_open_from_resolved(gold):
    open_count, resolved_count, never_discrepant = query(gold, """
        select count(*) filter (where is_open),
               count(*) filter (where was_resolved),
               count(*) filter (where runs_with_discrepancy = 0)
        from gold.fct_transaction_reconciliation_status
    """)[0]
    assert (open_count, resolved_count) == (73, 29)
    # Toda transação está em exatamente um dos três grupos.
    assert open_count + resolved_count + never_discrepant == scalar(
        gold, "select count(*) from gold.fct_transaction_reconciliation_status")


def test_spike_in_discrepancy_rate_is_flagged(tmp_path):
    data = tmp_path / "sources"
    shutil.copytree(SAMPLE_DATA, data)
    # No último run, 20 MATCHED viram MISMATCHED de verdade (valores e categoria coerentes).
    path = data / "reconciliation_results.parquet"
    duckdb.execute(f"""
        copy (
            with ranked as (
                select *, row_number() over (order by id) as n
                from read_parquet('{path.as_posix()}')
                where run_id = 10 and category = 'MATCHED'
            )
            select * replace (
                case when id in (select id from ranked where n <= 20)
                     then 'MISMATCHED' else category end as category,
                case when id in (select id from ranked where n <= 20)
                     then internal_amount + 100 else processor_amount end as processor_amount,
                case when id in (select id from ranked where n <= 20)
                     then 100.0 else difference end as difference
            )
            from read_parquet('{path.as_posix()}')
        ) to '{(data / 'tmp.parquet').as_posix()}'
    """)
    (data / "tmp.parquet").replace(path)

    settings, result = build_lake(tmp_path, data=data)

    assert result.success
    assert scalar(settings, """
        select health_status from gold.agg_run_health where run_id = 10
    """) == "DISCREPANCY_SPIKE"


# ---------------------------------------------------------------------------
# CFO
# ---------------------------------------------------------------------------

def test_daily_volume_matches_current_transactions(gold):
    volume, transactions = query(gold, """
        select (select sum(transacted_amount) from gold.fct_daily_volume),
               (select sum(amount) from silver.transactions)
    """)[0]
    assert volume == transactions


def test_daily_volume_nets_out_reversals(gold):
    settled, reversed_, net = query(gold, """
        select sum(settled_amount), sum(reversed_amount), sum(net_settled_amount)
        from gold.fct_daily_volume where volume_date = '2025-03-16'
    """)[0]
    assert reversed_ > 0
    assert net == settled - reversed_


def test_amounts_in_other_currencies_are_never_added_as_brl(tmp_path):
    data = tmp_path / "generated"
    subprocess.run([
        sys.executable, str(REPO_ROOT / "scripts" / "generate_sample_data.py"),
        "--rows", "20000", "--days", "10", "--merchants", "50", "--seed", "11",
        "--out", str(data),
    ], check=True, capture_output=True)
    settings, result = build_lake(tmp_path, data=data, reference_date=None)
    assert result.success, result.failures

    by_source = dict(query(settings, """
        select currency_source, count(*) from gold.fct_reconciliation_result group by 1
    """))
    # O gerador é consistente: toda moeda vem de uma fonte, nenhuma é suposta.
    assert set(by_source) <= {"internal_transaction", "processor_file"}
    assert scalar(settings, """
        select count(distinct currency) from gold.fct_reconciliation_result
    """) == 3
    # A saúde dos runs soma só BRL e conta à parte o que está em outra moeda.
    brl, other = query(settings, """
        select sum(amount_at_risk_brl), sum(discrepancies_in_other_currencies)
        from gold.agg_run_health
    """)[0]
    assert brl == scalar(settings, """
        select sum(amount_at_risk) from gold.fct_reconciliation_result where currency = 'BRL'
    """)
    assert other > 0


# ---------------------------------------------------------------------------
# Compliance
# ---------------------------------------------------------------------------

def test_audit_trail_follows_cdc_order(gold):
    events = [r[0] for r in query(gold, """
        select event_type from gold.audit_transaction_trail
        where transaction_id like 'acc62b77%' order by event_seq
    """)]
    internal = [e for e in events if e.startswith("internal_record")]
    assert internal == [
        "internal_record_created", "internal_record_updated", "internal_record_deleted",
    ]


def test_every_audit_event_points_to_a_hashed_source_file(gold):
    assert scalar(gold, """
        select count(*) from gold.audit_transaction_trail
        where source_file is null or source_row is null or length(source_file_sha256) <> 64
    """) == 0


def test_facts_keep_the_merchant_as_it_was_at_the_time(gold):
    # MERCH_004 virou S.A. em 07/03; o run de 04/03 tem que mostrar Ltda.
    names = dict(query(gold, """
        select fact.reference_date::varchar, merchant.legal_name
        from gold.fct_reconciliation_result as fact
        join gold.dim_merchant as merchant using (merchant_key)
        where fact.merchant_id = 'MERCH_004'
          and fact.reference_date in ('2025-03-04', '2025-03-31')
        group by all
    """))
    assert names == {"2025-03-04": "Empresa MERCH_004 Ltda",
                     "2025-03-31": "Empresa MERCH_004 S.A."}


# ---------------------------------------------------------------------------
# Incremental por data
# ---------------------------------------------------------------------------

def test_reprocessing_a_date_rewrites_only_that_date(gold):
    con = lake.connect(gold, read_only=True)
    before = con.execute("select max(snapshot_id) from lake.snapshots()").fetchone()[0]
    con.close()

    result = transform.run_dbt(gold, reference_date=FIXTURE_REFERENCE_DATE)
    assert result.success

    con = lake.connect(gold, read_only=True)
    after = con.execute("select max(snapshot_id) from lake.snapshots()").fetchone()[0]
    changes = con.execute(f"""
        select change_type, reference_date::varchar, count(*)
        from ducklake_table_changes('lake', 'gold_build', 'fct_reconciliation_result',
                                    {before + 1}, {after})
        group by all order by 1
    """).fetchall()
    con.close()
    assert changes == [("delete", FIXTURE_REFERENCE_DATE, 93), ("insert", FIXTURE_REFERENCE_DATE, 93)]


# ---------------------------------------------------------------------------
# Contrato da saída
# ---------------------------------------------------------------------------

def test_gold_contract_blocks_a_model_that_changes_its_columns(tmp_path, gold):
    """Renomear uma coluna da gold sem mudar o contrato quebra o build e não publica."""
    project = tmp_path / "dbt"
    shutil.copytree(REPO_ROOT / "dbt", project,
                    ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
    model = project / "models" / "gold" / "agg_run_health.sql"
    model.write_text(model.read_text(encoding="utf-8").replace(
        "as health_status", "as status_de_saude"), encoding="utf-8")
    settings = load_settings(data_path=gold.data_path, lake_path=gold.lake_path,
                             dbt_project_path=project)
    published_before = scalar(gold, "select max(snapshot_id) from ops.publications")

    outcome = run_pipeline(settings, "transform", ingest=False)

    assert outcome.status == "failed"
    assert "agg_run_health" in outcome.transform.failures
    assert "contract" in outcome.transform.failures["agg_run_health"].lower()
    assert scalar(gold, "select max(snapshot_id) from ops.publications") == published_before


# ---------------------------------------------------------------------------
# Queries de exemplo da documentação
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", EXAMPLE_QUERIES, ids=lambda p: Path(p).stem)
def test_example_query_runs_and_returns_rows(gold, path):
    con = lake.connect(gold, read_only=True)
    try:
        last = None
        for statement in duckdb.extract_statements(path.read_text(encoding="utf-8")):
            relation = con.sql(statement.query)
            if relation is not None:
                last = relation.fetchall()
    finally:
        con.close()
    assert last, f"{path.name} não devolveu linhas"
