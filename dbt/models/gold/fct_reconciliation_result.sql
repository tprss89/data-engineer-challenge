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
-- valor inteiro do lado que existe, nos UNRECONCILED.
--
-- Os resultados não trazem moeda, e o gerador produz transações em USD e EUR. A moeda vem
-- da transação interna; quando ela não existe (UNRECONCILED_PROCESSOR), do arquivo do
-- processador; só em último caso assume BRL, como o README diz. currency_source registra
-- de onde veio. Valores de moedas diferentes nunca são somados.
with internal_currency as (
    -- Inclui transações apagadas: o resultado pode ser anterior ao delete.
    select transaction_id, currency
    from {{ ref('transactions_history') }}
    where is_current
    qualify row_number() over (partition by transaction_id order by source_id) = 1
),

processor_currency as (
    select transaction_id, arg_max(currency, settled_at) as currency
    from {{ ref('settlements') }}
    group by transaction_id
),

results as (
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
    coalesce(internal_currency.currency, processor_currency.currency, 'BRL') as currency,
    case
        when internal_currency.currency is not null then 'internal_transaction'
        when processor_currency.currency is not null then 'processor_file'
        else 'assumed_brl'
    end as currency_source,
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
left join internal_currency on internal_currency.transaction_id = results.transaction_id
left join processor_currency on processor_currency.transaction_id = results.transaction_id
left join {{ ref('dim_merchant') }} as merchant
    on merchant.merchant_id = results.merchant_id
   and results.created_at >= merchant.valid_from
   and results.created_at < merchant.valid_to
