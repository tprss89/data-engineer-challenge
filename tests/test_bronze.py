"""Garantias do bronze: cópia fiel, idempotente, atômica e protegida pelo contrato."""
from __future__ import annotations

import duckdb
import pytest
from conftest import FIXTURE_REFERENCE_DATE, SAMPLE_DATA

from settlement_pipeline import bronze, cli
from settlement_pipeline.contracts import load_contracts


def count(con, table: str) -> int:
    return con.execute(f"select count(*) from {table}").fetchone()[0]


def by_name(contracts, name):
    return next(c for c in contracts if c.name == name)


def test_contracts_cover_every_sample_file(contracts):
    assert {c.name for c in contracts} == {
        "transactions", "reconciliation_runs", "reconciliation_results",
        "enterprise_company", "paysettler_settlements",
    }
    for contract in contracts:
        assert bronze.discover_files(contract, SAMPLE_DATA), f"{contract.name} sem arquivo"


def test_loads_every_source_row(con, contracts, data_path):
    results = bronze.ingest(con, contracts, data_path, FIXTURE_REFERENCE_DATE)

    assert {r.status for r in results} == {"loaded"}
    assert count(con, "bronze.transactions") == 1060  # 742 + 318 eventos CDC
    assert count(con, "bronze.reconciliation_runs") == 19
    assert count(con, "bronze.reconciliation_results") == 837
    assert count(con, "bronze.enterprise_company") == 70
    assert count(con, "bronze.paysettler_settlements") == 510  # duplicatas incluídas


def test_second_run_changes_nothing(con, contracts, data_path):
    bronze.ingest(con, contracts, data_path, FIXTURE_REFERENCE_DATE)
    before = {c.name: count(con, c.table) for c in contracts}

    rerun = bronze.ingest(con, contracts, data_path, FIXTURE_REFERENCE_DATE)

    assert {r.status for r in rerun} == {"skipped"}
    assert {c.name: count(con, c.table) for c in contracts} == before


def test_csv_is_kept_verbatim(con, contracts, data_path):
    bronze.ingest(con, contracts, data_path, FIXTURE_REFERENCE_DATE)

    raw_amounts = {r[0] for r in con.execute(
        "select amount from bronze.paysettler_settlements").fetchall()}
    assert "R$ 32.245,91" in raw_amounts
    ref_dates = con.execute(
        "select distinct _reference_date::varchar from bronze.paysettler_settlements").fetchall()
    assert ref_dates == [(FIXTURE_REFERENCE_DATE,)]


def test_optional_column_absent_in_older_batch(con, contracts, data_path):
    results = bronze.ingest(con, [by_name(contracts, "transactions")], data_path)

    drift = {r.source_file.rsplit("/", 1)[-1]: r.schema_drift for r in results}
    assert drift == {"transactions_batch_1.parquet": [], "transactions_batch_2.parquet": []}
    # payment_method é opcional no contrato: lote 1 fica com null, lote 2 com valor.
    filled = con.execute("""
        select _source_file like '%batch_2%' as lote_2, count(payment_method) > 0
        from bronze.transactions group by 1 order by 1
    """).fetchall()
    assert filled == [(False, False), (True, True)]


def test_undeclared_column_is_drift_not_failure(con, contracts, data_path):
    path = data_path / "enterprise_company.parquet"
    duckdb.execute(f"""
        copy (select *, 'SP' as state from read_parquet('{path.as_posix()}'))
        to '{(data_path / 'tmp.parquet').as_posix()}'
    """)
    (data_path / "tmp.parquet").replace(path)

    [result] = bronze.ingest(con, [by_name(contracts, "enterprise_company")], data_path)

    assert result.status == "loaded"
    assert result.schema_drift == ["state"]
    assert "state" in {r[0] for r in con.execute("describe bronze.enterprise_company").fetchall()}


def test_cumulative_cdc_extract_only_adds_new_events(con, contracts, data_path):
    runs = by_name(contracts, "reconciliation_runs")
    path = data_path / "reconciliation_runs.parquet"
    bronze.ingest(con, [runs], data_path)

    # Nova extração: tudo que já existia mais um evento novo.
    duckdb.execute(f"""
        copy (
            select * from read_parquet('{path.as_posix()}')
            union all
            select * replace ('U' as Op, 'COMPLETED' as status,
                              '2025-03-23T09:00:00+00:00' as _timestamp)
            from read_parquet('{path.as_posix()}') where id = 7
        ) to '{(data_path / 'tmp.parquet').as_posix()}'
    """)
    (data_path / "tmp.parquet").replace(path)

    [result] = bronze.ingest(con, [runs], data_path)

    assert result.status == "loaded"
    assert (result.rows_read, result.rows_inserted) == (20, 1)
    assert count(con, "bronze.reconciliation_runs") == 20


def test_cdc_event_identity_survives_schema_drift(con, contracts, data_path):
    # Extração nova e acumulada: os mesmos eventos do lote 1, agora com a coluna que surgiu
    # no lote 2 (vazia). São os mesmos eventos e não podem entrar de novo.
    transactions = by_name(contracts, "transactions")
    bronze.ingest(con, [transactions], data_path)
    source = (data_path / "transactions_batch_1.parquet").as_posix()
    duckdb.execute(f"""
        copy (select *, null::varchar as payment_method from read_parquet('{source}'))
        to '{(data_path / 'transactions_batch_3.parquet').as_posix()}'
    """)

    results = bronze.ingest(con, [transactions], data_path)

    [new_extract] = [r for r in results if r.source_file.endswith("batch_3.parquet")]
    assert (new_extract.status, new_extract.rows_read, new_extract.rows_inserted) == \
        ("loaded", 742, 0)
    assert count(con, "bronze.transactions") == 1060


def test_csv_without_date_needs_reference_date(con, contracts, data_path):
    [result] = bronze.ingest(con, [by_name(contracts, "paysettler_settlements")], data_path)

    assert result.status == "failed"
    assert "--reference-date" in result.error
    assert con.execute(
        "select status from ops.ingestion_log where source = 'paysettler_settlements'"
    ).fetchall() == [("failed",)]


@pytest.mark.parametrize("change, expected_error", [
    ("select * exclude (category) from", "colunas obrigatórias ausentes: category"),
    ("select * replace (cast(internal_amount as varchar) as internal_amount) from",
     "internal_amount (esperado DOUBLE, veio VARCHAR)"),
])
def test_contract_violation_blocks_file(con, contracts, data_path, change, expected_error):
    path = data_path / "reconciliation_results.parquet"
    duckdb.execute(f"""
        copy ({change} read_parquet('{path.as_posix()}'))
        to '{(data_path / 'tmp.parquet').as_posix()}'
    """)
    (data_path / "tmp.parquet").replace(path)

    results = bronze.ingest(con, contracts, data_path, FIXTURE_REFERENCE_DATE)

    failed = [r for r in results if r.status == "failed"]
    assert [r.source for r in failed] == ["reconciliation_results"]
    assert expected_error in failed[0].error
    assert count(con, "bronze.transactions") == 1060  # as outras fontes seguem


def test_failure_mid_load_leaves_nothing_behind(con, contracts, data_path, monkeypatch):
    real_write_log = bronze._write_log

    def write_log_failing_on_success(con, batch_id, contract, path, file_hash, ref_date,
                                     result, started_at):
        # A carga dos dados já rodou dentro da transação; falha logo antes do commit.
        if result.status == "loaded":
            raise RuntimeError("disco cheio no meio da carga")
        return real_write_log(con, batch_id, contract, path, file_hash, ref_date,
                              result, started_at)

    monkeypatch.setattr(bronze, "_write_log", write_log_failing_on_success)
    [result] = bronze.ingest(con, [by_name(contracts, "enterprise_company")], data_path)

    assert result.status == "failed"
    # O rollback desfaz até o create table, que rodou na mesma transação.
    tables = con.execute(
        "select table_name from information_schema.tables where table_schema = 'bronze'"
    ).fetchall()
    assert tables == []
    monkeypatch.undo()

    # Como nada foi marcado como carregado, a próxima execução carrega o arquivo inteiro.
    [retry] = bronze.ingest(con, [by_name(contracts, "enterprise_company")], data_path)
    assert (retry.status, retry.rows_inserted) == ("loaded", 70)


def test_cli_exit_codes(settings, monkeypatch, capsys):
    monkeypatch.setenv("DATA_PATH", str(settings.data_path))
    monkeypatch.setenv("LAKE_PATH", str(settings.lake_path))

    assert cli.main(["ingest"]) == cli.EXIT_DATA_BLOCKED  # CSV sem data no nome
    assert cli.main(["ingest", "--reference-date", FIXTURE_REFERENCE_DATE]) == cli.EXIT_OK
    assert cli.main(["status"]) == cli.EXIT_OK
    assert "paysettler_settlements" in capsys.readouterr().out


def test_contract_files_are_valid(tmp_path):
    (tmp_path / "bad.yml").write_text("name: x\nkind: cdc\nformat: parquet\npaths: []\n"
                                      "columns: [{name: id, type: BIGINT}]\n")
    with pytest.raises(Exception, match="Op e _timestamp"):
        load_contracts(tmp_path)
