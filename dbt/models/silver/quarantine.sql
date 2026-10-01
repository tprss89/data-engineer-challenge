-- Tudo que foi barrado na silver, com o motivo e a linha original em JSON.
-- É o que Operações consulta para cobrar a correção na origem.
with rejected as (
    select
        'transactions' as source,
        coalesce(transaction_id, source_id::varchar) as record_key,
        null::date as reference_date,
        _dq_errors as reasons,
        _batch_id,
        _source_file,
        to_json(t) as payload
    from {{ ref('transactions_checked') }} as t
    where len(_dq_errors) > 0

    union all

    select
        'reconciliation_results',
        result_id::varchar,
        null::date,
        _dq_errors,
        _batch_id,
        _source_file,
        to_json(r)
    from {{ ref('reconciliation_results_checked') }} as r
    where len(_dq_errors) > 0

    union all

    select
        'paysettler_settlements',
        coalesce(transaction_id, amount_raw),
        reference_date,
        _dq_errors,
        _batch_id,
        _source_file,
        to_json(s)
    from {{ ref('paysettler_settlements_checked') }} as s
    where len(_dq_errors) > 0
)

select * from rejected
