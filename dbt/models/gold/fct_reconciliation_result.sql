{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='reference_date',
    on_schema_change='fail'
  )
}}

-- Uma linha por resultado efetivo de reconciliação (run x transação).
--
-- Incremental por data de referência: com --reference-date, só aquela data é apagada e
-- regravada. Sem a data (ou na primeira execução), processa tudo.
--
-- amount_at_risk é o dinheiro que a discrepância envolve: a diferença, no MISMATCHED; o
-- valor inteiro do lado que existe, nos UNRECONCILED. Os resultados não trazem moeda; pelo
-- README os valores internos são em BRL.
with results as (
    select results.*, runs.reference_date
    from {{ ref('reconciliation_results') }} as results
    join {{ ref('reconciliation_runs') }} as runs on runs.run_id = results.run_id
    where results.is_effective
    {% if is_incremental() and var('reference_date', none) %}
      and runs.reference_date = '{{ var("reference_date") }}'::date
    {% endif %}
)

select
    results.result_id,
    results.run_id,
    results.reference_date,
    results.transaction_id,
    results.merchant_id,
    coalesce(merchant.merchant_key, '-1') as merchant_key,
    results.category,
    results.category <> 'MATCHED' as is_discrepancy,
    results.internal_amount,
    results.processor_amount,
    results.difference,
    case results.category
        when 'MATCHED' then 0
        when 'MISMATCHED' then results.difference
        when 'UNRECONCILED_PROCESSOR' then results.processor_amount
        when 'UNRECONCILED_INTERNAL' then results.internal_amount
    end::decimal(18, 2) as amount_at_risk,
    results.created_at as reconciled_at,
    results._batch_id,
    results._source_file
from results
left join {{ ref('dim_merchant') }} as merchant
    on merchant.merchant_id = results.merchant_id
   and results.created_at >= merchant.valid_from
   and results.created_at < merchant.valid_to
