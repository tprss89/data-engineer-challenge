-- Linha do tempo de cada transação, para auditoria. Junta, num formato só, tudo o que
-- os sistemas registraram sobre ela:
--   - cada versão do registro interno (insert, update, delete no settlement_db)
--   - cada resultado de reconciliação, inclusive os regravados e os barrados na quarentena
--   - cada liquidação ou estorno informado pelo PaySettler
--
-- Cada evento aponta para o arquivo, a linha e o hash do arquivo de onde veio. O hash
-- prova que o arquivo é o mesmo que recebemos.
with events as (
    select
        transaction_id,
        valid_from as event_at,
        'internal_record_' || case cdc_operation
            when 'I' then 'created' when 'U' then 'updated' when 'D' then 'deleted' end as event_type,
        'settlement_db.transactions' as source_system,
        json_object(
            'status', status, 'amount', amount, 'currency', currency,
            'merchant_id', merchant_id, 'version', version
        ) as details,
        version as _sort_hint,
        _batch_id, _source_file, _source_row
    from {{ ref('transactions_history') }}

    union all

    select
        results.transaction_id,
        results.created_at,
        'reconciliation_result',
        'settlement_db.reconciliation_results',
        json_object(
            'run_id', results.run_id, 'reference_date', runs.reference_date,
            'category', results.category, 'internal_amount', results.internal_amount,
            'processor_amount', results.processor_amount, 'difference', results.difference,
            'is_effective', results.is_effective
        ),
        null,
        results._batch_id, results._source_file, results._source_row
    from {{ ref('reconciliation_results') }} as results
    left join {{ ref('reconciliation_runs') }} as runs on runs.run_id = results.run_id

    union all

    select
        json_extract_string(payload, '$.transaction_id'),
        json_extract_string(payload, '$.created_at')::timestamptz,
        'reconciliation_result_quarantined',
        'settlement_db.reconciliation_results',
        json_object('reasons', reasons, 'record', payload),
        null,
        _batch_id, _source_file, json_extract(payload, '$._source_row')::bigint
    from {{ ref('quarantine') }}
    where source = 'reconciliation_results'

    union all

    select
        transaction_id,
        settled_at,
        'settlement_' || lower(status),
        'paysettler.csv',
        json_object(
            'reference_date', reference_date, 'amount', amount, 'amount_raw', amount_raw,
            'currency', currency, 'processor_reference', processor_reference
        ),
        null,
        _batch_id, _source_file, _source_row
    from {{ ref('paysettler_settlements') }}
)

select
    events.transaction_id,
    row_number() over (
        partition by events.transaction_id
        -- No mesmo instante, eventos do registro interno seguem a ordem de versão do CDC
        -- (que já aplica o desempate D > U > I).
        order by events.event_at, events._sort_hint nulls last, events.event_type,
                 events._source_row
    ) as event_seq,
    events.event_at,
    events.event_type,
    events.source_system,
    events.details,
    events._source_file as source_file,
    events._source_row as source_row,
    log.file_hash as source_file_sha256,
    log.finished_at as ingested_at,
    events._batch_id as ingestion_batch_id
from events
left join {{ source('ops', 'ingestion_log') }} as log on log.batch_id = events._batch_id
where events.transaction_id is not null
