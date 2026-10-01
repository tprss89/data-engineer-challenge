"""Execução ponta a ponta: registro em ops.pipeline_runs e publicação segura da gold."""
from __future__ import annotations

import shutil

import duckdb
import pytest
from conftest import FIXTURE_REFERENCE_DATE, SAMPLE_DATA
from test_silver import query, scalar

from settlement_pipeline import cli, pipeline, transform
from settlement_pipeline.config import load_settings


@pytest.fixture
def settings(tmp_path):
    data = tmp_path / "sources"
    shutil.copytree(SAMPLE_DATA, data)
    return load_settings(data_path=data, lake_path=tmp_path / "lake")


def simulate_service_bug(settings) -> None:
    """Nova extração CDC: o serviço regrava todo MISMATCHED como MATCHED (8% dos resultados)."""
    path = settings.data_path / "reconciliation_results.parquet"
    tmp = settings.data_path / "tmp.parquet"
    duckdb.execute(f"""
        copy (
            select * from read_parquet('{path.as_posix()}')
            union all
            select * replace (
                'MATCHED' as category, 'U' as Op,
                strftime(_timestamp::timestamptz + interval 1 day, '%Y-%m-%dT%H:%M:%S+00:00')
                    as _timestamp)
            from read_parquet('{path.as_posix()}')
            where category = 'MISMATCHED'
        ) to '{tmp.as_posix()}'
    """)
    tmp.replace(path)


def test_successful_run_is_recorded_and_published(settings):
    outcome = pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)

    assert (outcome.status, outcome.exit_code) == ("succeeded_with_warnings", 0)
    row = query(settings, """
        select status, files_loaded, files_failed, published_snapshot, finished_at is not null
        from ops.pipeline_runs
    """)
    assert row == [("succeeded_with_warnings", 6, 0, outcome.published_snapshot, True)]
    # O resultado de cada modelo e teste fica guardado, inclusive os alertas.
    assert scalar(settings, "select count(*) from ops.dq_results where status = 'warn'") == 2
    published = scalar(settings, "select tables from ops.publications")
    assert "agg_run_health" in published and "fct_reconciliation_result" in published


def test_blocked_run_keeps_previous_publication(settings):
    first = pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)
    health_before = query(settings, "select * from gold.agg_run_health order by run_id")

    simulate_service_bug(settings)
    second = pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)

    assert (second.status, second.exit_code) == ("blocked", cli.EXIT_DATA_BLOCKED)
    assert second.published_snapshot is None
    assert "rejected_ratio_below" in "".join(second.transform.failures)
    # O build novo existe em gold_build, mas os consumidores continuam vendo o anterior.
    assert scalar(settings, "select count(*) from ops.publications") == 1
    assert query(settings, "select * from gold.agg_run_health order by run_id") == health_before
    assert scalar(settings, "select max(snapshot_id) from lake.snapshots()") > \
        first.published_snapshot


def test_contract_violation_stops_before_transform(settings):
    (settings.data_path / "settlement_paysettler.csv").write_text("so,isso\n1,2\n")

    outcome = pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)

    assert (outcome.status, outcome.transform) == ("blocked", None)
    assert scalar(settings, "select files_failed from ops.pipeline_runs") == 1


def test_crash_is_recorded_as_failed(settings, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("dbt sumiu do container")

    monkeypatch.setattr(transform, "run_dbt", broken)
    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)

    status, error = query(settings, "select status, error from ops.pipeline_runs")[0]
    assert status == "failed"
    assert "dbt sumiu do container" in error


def test_status_panel_shows_runs_and_interrupted_executions(settings, monkeypatch, capsys):
    pipeline.run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)
    # Uma execução que começou e nunca terminou (processo morto, máquina reiniciada).
    pipeline._start(settings, "execucao-interrompida", "run", FIXTURE_REFERENCE_DATE)
    monkeypatch.setenv("LAKE_PATH", str(settings.lake_path))
    capsys.readouterr()

    assert cli.main(["status"]) == cli.EXIT_OK

    out = capsys.readouterr().out
    assert "succeeded_with_warnings" in out
    assert "execucao-interrompida" in out
    assert "quarantine_is_empty" in out
