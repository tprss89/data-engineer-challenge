"""Regras da silver, verificadas sobre o fixture e sobre dados do gerador.

Cada teste roda o caminho real: bronze.ingest + dbt build num lake temporário.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import duckdb
import pytest
from conftest import FIXTURE_REFERENCE_DATE, SAMPLE_DATA

from settlement_pipeline import bronze, cli, lake, transform
from settlement_pipeline.config import REPO_ROOT, load_settings
from settlement_pipeline.contracts import load_contracts
from settlement_pipeline.pipeline import run_pipeline


def build_lake(root: Path, data: Path | None = None, reference_date=FIXTURE_REFERENCE_DATE):
    """Roda o pipeline completo (ingest, dbt, publicação) num lake temporário."""
    if data is None:
        data = root / "sources"
        shutil.copytree(SAMPLE_DATA, data)
    settings = load_settings(data_path=data, lake_path=root / "lake")
    outcome = run_pipeline(settings, reference_date=reference_date)
    return settings, outcome.transform


def query(settings, sql: str):
    con = lake.connect(settings, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def scalar(settings, sql: str):
    return query(settings, sql)[0][0]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    return build_lake(tmp_path_factory.mktemp("silver"))


@pytest.fixture(scope="module")
def silver(built):
    return built[0]


# ---------------------------------------------------------------------------
# Build e checks de qualidade
# ---------------------------------------------------------------------------

def test_build_passes_with_only_the_expected_warnings(built):
    _, result = built
    assert result.success
    assert result.failures == {}
    assert result.warnings == {
        "not_null_reconciliation_results_merchant_id": 16,
        "quarantine_is_empty": 15,
        # No fixture, resultados, transações e arquivo do processador não batem entre si.
        "assert_results_consistent_with_internal_records": 763,
        "assert_results_consistent_with_processor_files": 91,
    }


def test_quarantine_has_each_rejection_with_its_reason(silver):
    rows = query(silver, """
        select source, reasons::varchar, count(*) from silver.quarantine
        group by all order by 1
    """)
    assert rows == [
        ("paysettler_settlements", "[duplicate_row]", 10),
        ("reconciliation_results",
         "[run_not_found, category_contradicts_amounts, difference_contradicts_amounts]", 5),
    ]


# ---------------------------------------------------------------------------
# CDC
# ---------------------------------------------------------------------------

def test_cdc_tie_is_broken_by_operation(silver):
    # acc62b77 tem U e D no mesmo _timestamp: D vence, a transação está apagada.
    assert scalar(silver, """
        select count(*) from silver.transactions where transaction_id like 'acc62b77%'
    """) == 0
    assert scalar(silver, """
        select is_deleted from silver.transactions_history
        where transaction_id like 'acc62b77%' and is_current
    """) is True


def test_cdc_reinsert_after_delete_keeps_transaction(silver):
    # 7a3d40bb: I, D e depois I de novo. O estado final é ativo.
    assert scalar(silver, """
        select count(*) from silver.transactions where transaction_id like '7a3d40bb%'
    """) == 1


def test_current_transactions_exclude_deletes(silver):
    assert scalar(silver, "select count(*) from silver.transactions") == 995


def test_merchant_history_is_scd2(silver):
    versions = query(silver, """
        select legal_name, valid_from, valid_to, is_current
        from silver.merchants_history where merchant_id = 'MERCH_004' order by version
    """)
    assert [v[0] for v in versions] == ["Empresa MERCH_004 Ltda"] + ["Empresa MERCH_004 S.A."] * 3
    # Cada versão termina exatamente quando a próxima começa; só a última está aberta.
    for current, following in pairwise(versions):
        assert current[2] == following[1]
    assert versions[-1][2] is None and versions[-1][3] is True


# ---------------------------------------------------------------------------
# Resultados da reconciliação
# ---------------------------------------------------------------------------

def test_tolerance_is_checked_in_decimal_not_double(silver):
    # Em double, dezenas de MATCHED parecem passar de R$ 0,01. Em decimal, nenhum passa.
    assert scalar(silver, """
        select count(*) from silver.reconciliation_results
        where category = 'MATCHED' and abs(internal_amount - processor_amount) > 0.01
    """) == 0
    float_trap = scalar(silver, """
        select count(*) from silver.reconciliation_results
        where category = 'MATCHED'
          and abs(internal_amount::double - processor_amount::double) > 0.01
    """)
    assert float_trap > 100


def test_only_latest_result_per_transaction_and_run_is_effective(silver):
    total, effective = query(silver, """
        select count(*), count(*) filter (where is_effective) from silver.reconciliation_results
    """)[0]
    assert (total, effective) == (832, 788)


# ---------------------------------------------------------------------------
# PaySettler
# ---------------------------------------------------------------------------

def test_brl_formatted_amounts_are_parsed(silver):
    assert scalar(silver, """
        select amount from silver.paysettler_settlements where amount_raw = 'R$ 32.245,91'
    """) == Decimal("32245.91")
    assert scalar(silver, """
        select count(*) from silver.paysettler_settlements where amount_was_brl_formatted
    """) == 22


def test_timestamps_without_timezone_are_utc(silver):
    raw, parsed = query(silver, """
        select settled_at_raw, strftime(settled_at at time zone 'UTC', '%Y-%m-%dT%H:%M:%S')
        from silver.paysettler_settlements where not settled_at_had_timezone limit 1
    """)[0]
    assert parsed == raw


def test_reprocessing_is_idempotent(built):
    settings, _ = built
    before = query(settings, "select count(*) from silver.paysettler_settlements")
    assert transform.run_dbt(settings).success
    assert query(settings, "select count(*) from silver.paysettler_settlements") == before


def test_resent_file_replaces_previous_version_for_that_date(tmp_path):
    settings, _ = build_lake(tmp_path)
    data = settings.data_path

    # O processador reenvia o arquivo de 16/03, agora só com as 100 primeiras linhas.
    (data / "paysettler").mkdir()
    lines = (data / "settlement_paysettler.csv").read_text(encoding="utf-8").splitlines()
    (data / "paysettler" / "settlement_2025-03-16.csv").write_text(
        "\n".join(lines[:101]) + "\n", encoding="utf-8")
    distinct_rows = len(set(lines[1:101]))  # duplicata exata vai para quarentena
    con = lake.connect(settings)
    bronze.ingest(con, load_contracts(settings.contracts_path), data, FIXTURE_REFERENCE_DATE)
    con.close()
    assert transform.run_dbt(settings).success

    assert scalar(settings, "select count(*) from silver.paysettler_settlements") == distinct_rows
    # As duas versões continuam no bronze para auditoria.
    assert scalar(settings, "select count(*) from bronze.paysettler_settlements") == 610


# ---------------------------------------------------------------------------
# Bloqueio de publicação
# ---------------------------------------------------------------------------

def test_quality_gate_blocks_when_too_many_rows_are_rejected(tmp_path, monkeypatch):
    data = tmp_path / "sources"
    shutil.copytree(SAMPLE_DATA, data)
    # Simula um bug no serviço: todo MISMATCHED gravado como MATCHED (62 + 5 = 8% rejeitado).
    path = data / "reconciliation_results.parquet"
    duckdb.execute(f"""
        copy (
            select * replace (
                case when category = 'MISMATCHED' then 'MATCHED' else category end as category)
            from read_parquet('{path.as_posix()}')
        ) to '{(data / 'tmp.parquet').as_posix()}'
    """)
    (data / "tmp.parquet").replace(path)

    monkeypatch.setenv("DATA_PATH", str(data))
    monkeypatch.setenv("LAKE_PATH", str(tmp_path / "lake"))
    exit_code = cli.main(["run", "--reference-date", FIXTURE_REFERENCE_DATE])

    assert exit_code == cli.EXIT_DATA_BLOCKED


# ---------------------------------------------------------------------------
# Dados do gerador (mais tipos de sujeira que o fixture)
# ---------------------------------------------------------------------------

def test_generated_dirty_data_builds_end_to_end(tmp_path):
    data = tmp_path / "generated"
    subprocess.run([
        sys.executable, str(REPO_ROOT / "scripts" / "generate_sample_data.py"),
        "--rows", "5000", "--days", "15", "--merchants", "50", "--seed", "7",
        "--out", str(data),
    ], check=True, capture_output=True)

    settings, result = build_lake(tmp_path, data=data)

    assert result.success, result.failures
    # Lote 2 tem payment_method; replay de CDC some da tabela atual.
    assert scalar(settings, """
        select count(*) from silver.transactions group by transaction_id order by 1 desc limit 1
    """) == 1
    assert scalar(settings, """
        select count(*) from silver.paysettler_settlements where merchant_id <> upper(merchant_id)
    """) == 0
