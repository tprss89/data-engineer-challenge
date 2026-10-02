{{ config(severity='warn') }}

-- Check cruzado: o lado interno de cada resultado efetivo bate com o sistema interno?
-- Não bloqueia: o resultado da reconciliação é a fonte de verdade das categorias, e uma
-- divergência aqui é sinal para investigar o serviço, não motivo para esconder números.
-- No fixture a divergência é alta de propósito; nos dados do gerador é zero.
with internal as (
    select transaction_id, merchant_id, amount
    from {{ ref('transactions_history') }}
    where is_current
    qualify row_number() over (partition by transaction_id order by source_id) = 1
)

select
    results.run_id,
    results.transaction_id,
    case
        when internal.transaction_id is null then 'transaction_not_in_internal_records'
        when results.internal_amount <> internal.amount then 'internal_amount_differs'
        else 'merchant_differs'
    end as divergence,
    results.internal_amount,
    internal.amount as internal_record_amount,
    results.merchant_id,
    internal.merchant_id as internal_record_merchant_id
from {{ ref('reconciliation_results') }} as results
left join internal on internal.transaction_id = results.transaction_id
where results.is_effective
  and results.internal_amount is not null
  and (
      internal.transaction_id is null
      or results.internal_amount <> internal.amount
      or (results.merchant_id is not null and results.merchant_id <> internal.merchant_id)
  )
