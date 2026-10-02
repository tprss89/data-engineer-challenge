"""Plugar um segundo processador de liquidação, com schema diferente, sem tocar no código.

O teste faz numa cópia do projeto exatamente o que o README descreve na seção
Extensibilidade, e verifica que o processador novo aparece da silver até a gold.

O "FastPay" é fictício e foi escolhido para ser parecido mas não idêntico ao PaySettler:
colunas em português, valor em centavos, data no formato brasileiro e outro vocabulário
de status.
"""
from __future__ import annotations

import re
import shutil
from decimal import Decimal

from conftest import FIXTURE_REFERENCE_DATE, SAMPLE_DATA
from test_silver import query, scalar

from settlement_pipeline.config import REPO_ROOT, load_settings
from settlement_pipeline.pipeline import run_pipeline

# 1. Contrato da fonte (arquivo novo).
CONTRACT = r"""
name: fastpay_settlements
description: Arquivo diário de liquidações do FastPay.
owner: FastPay (externo)
kind: file
format: csv
paths:
  - fastpay/fastpay_*.csv
reference_date_pattern: 'fastpay_(\d{4}-\d{2}-\d{2})\.csv$'
columns:
  - {name: id_transacao, type: VARCHAR, required: true}
  - {name: codigo_loja, type: VARCHAR, required: true}
  - {name: valor_centavos, type: VARCHAR, required: true}
  - {name: moeda, type: VARCHAR, required: true}
  - {name: liquidado_em, type: VARCHAR, required: true}
  - {name: tipo, type: VARCHAR, required: true}
"""

# 2. Tradução para o formato comum (arquivo novo).
CHECKED_MODEL = """
{{ config(materialized='table') }}
with typed as (
    select
        _reference_date as reference_date,
        try_cast(trim(id_transacao) as uuid)::varchar as transaction_id,
        upper(trim(codigo_loja)) as merchant_id,
        (try_cast(valor_centavos as bigint) / 100)::decimal(18, 2) as amount,
        upper(trim(moeda)) as currency,
        timezone('UTC', try_strptime(liquidado_em, '%d/%m/%Y %H:%M')) as settled_at,
        case upper(trim(tipo)) when 'CREDITO' then 'SETTLED' when 'ESTORNO' then 'REVERSED' end
            as status,
        id_transacao as processor_reference,
        valor_centavos as amount_raw,
        _source_row, _batch_id, _source_file
    from {{ source('bronze', 'fastpay_settlements') }}
)
select *, {{ dq_errors({
    'invalid_transaction_id': 'transaction_id is null',
    'invalid_amount': 'amount is null',
    'invalid_settled_at': 'settled_at is null',
    'invalid_status': 'status is null',
}) }} as _dq_errors
from typed
"""

# 3. Linhas aceitas (arquivo novo).
ACCEPTED_MODEL = """
select * exclude (_dq_errors)
from {{ ref('fastpay_settlements_checked') }}
where len(_dq_errors) = 0
"""

CSV = """id_transacao,codigo_loja,valor_centavos,moeda,liquidado_em,tipo
6f1d2c3b-0000-4000-8000-000000000001,merch_001,1500050,BRL,16/03/2025 10:15,CREDITO
6f1d2c3b-0000-4000-8000-000000000002,MERCH_002,250000,BRL,16/03/2025 11:40,CREDITO
6f1d2c3b-0000-4000-8000-000000000003,MERCH_002,50000,BRL,16/03/2025 18:05,ESTORNO
"""


def plug_fastpay(project, contracts, data) -> None:
    (contracts / "fastpay_settlements.yml").write_text(CONTRACT, encoding="utf-8")
    silver = project / "models" / "silver"
    (silver / "fastpay_settlements_checked.sql").write_text(CHECKED_MODEL, encoding="utf-8")
    (silver / "fastpay_settlements.sql").write_text(ACCEPTED_MODEL, encoding="utf-8")

    # 4. Uma linha em cada um de dois arquivos existentes.
    sources = project / "models" / "sources.yml"
    sources.write_text(sources.read_text(encoding="utf-8").replace(
        "      - name: paysettler_settlements\n",
        "      - name: paysettler_settlements\n      - name: fastpay_settlements\n"),
        encoding="utf-8")
    dbt_project = project / "dbt_project.yml"
    dbt_project.write_text(re.sub(
        r"settlement_processors: \[paysettler\]", "settlement_processors: [paysettler, fastpay]",
        dbt_project.read_text(encoding="utf-8")), encoding="utf-8")

    (data / "fastpay").mkdir()
    (data / "fastpay" / "fastpay_2025-03-16.csv").write_text(CSV, encoding="utf-8")


def test_second_processor_flows_from_bronze_to_gold(tmp_path):
    project, contracts, data = tmp_path / "dbt", tmp_path / "contracts", tmp_path / "sources"
    shutil.copytree(REPO_ROOT / "dbt", project,
                    ignore=shutil.ignore_patterns("target", "logs", ".user.yml"))
    shutil.copytree(REPO_ROOT / "contracts", contracts)
    shutil.copytree(SAMPLE_DATA, data)
    plug_fastpay(project, contracts, data)

    settings = load_settings(data_path=data, lake_path=tmp_path / "lake",
                             contracts_path=contracts, dbt_project_path=project)
    outcome = run_pipeline(settings, reference_date=FIXTURE_REFERENCE_DATE)

    assert outcome.exit_code == 0, outcome.transform and outcome.transform.failures
    assert dict(query(settings, """
        select processor, count(*) from silver.settlements group by 1
    """)) == {"paysettler": 500, "fastpay": 3}
    # Merchant normalizado, centavos convertidos, estorno reconhecido.
    assert query(settings, """
        select merchant_id, amount, status from silver.settlements
        where processor = 'fastpay' order by processor_reference
    """) == [("MERCH_001", Decimal("15000.50"), "SETTLED"),
             ("MERCH_002", Decimal("2500.00"), "SETTLED"),
             ("MERCH_002", Decimal("500.00"), "REVERSED")]
    # O volume do CFO e a trilha de Compliance passam a incluir o FastPay sem mudança na gold.
    assert scalar(settings, """
        select net_settled_amount from gold.fct_daily_volume
        where volume_date = '2025-03-16' and merchant_id = 'MERCH_001'
    """) >= Decimal("15000.50")
    assert scalar(settings, """
        select count(*) from gold.audit_transaction_trail where source_system = 'fastpay.csv'
    """) == 3
