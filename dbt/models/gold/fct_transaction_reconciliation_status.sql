-- Situação de reconciliação de cada transação, considerando todos os runs em que apareceu.
--
-- As janelas de 7 dias se sobrepõem, então a mesma transação passa por vários runs. Uma
-- discrepância num run pode ter se resolvido no seguinte. Esta tabela separa o que ainda
-- está aberto do que já se resolveu, e mede há quantos dias está aberto.
--
-- "Hoje" é a data do run mais recente nos dados, não a data do relógio: assim o resultado
-- é reproduzível e não muda só porque o tempo passou.
with results as (
    select * from {{ ref('fct_reconciliation_result') }}
),

as_of as (
    select max(reference_date) as as_of_date from results
),

per_transaction as (
    select
        transaction_id,
        arg_max(merchant_id, (reference_date, reconciled_at)) as merchant_id,
        arg_max(merchant_key, (reference_date, reconciled_at)) as merchant_key,
        arg_max(category, (reference_date, reconciled_at)) as latest_category,
        arg_max(amount_at_risk, (reference_date, reconciled_at)) as latest_amount_at_risk,
        arg_max(currency, (reference_date, reconciled_at)) as currency,
        min(reference_date) as first_reference_date,
        max(reference_date) as last_reference_date,
        count(*) as runs_count,
        count(*) filter (where is_discrepancy) as runs_with_discrepancy,
        min(reference_date) filter (where is_discrepancy) as first_discrepancy_date,
        max(reference_date) filter (where not is_discrepancy) as last_matched_date
    from results
    group by transaction_id
),

current_streak as (
    -- Início da discrepância atual: o primeiro run discrepante depois do último MATCHED.
    select results.transaction_id, min(results.reference_date) as open_since
    from results
    join per_transaction using (transaction_id)
    where results.is_discrepancy
      and results.reference_date > coalesce(per_transaction.last_matched_date, '-infinity'::date)
    group by results.transaction_id
)

select
    transaction_id,
    merchant_id,
    merchant_key,
    latest_category,
    latest_category <> 'MATCHED' as is_open,
    runs_with_discrepancy > 0 and latest_category = 'MATCHED' as was_resolved,
    case when latest_category <> 'MATCHED' then open_since end as open_since,
    case when latest_category <> 'MATCHED' then as_of.as_of_date - open_since end as days_open,
    case when latest_category <> 'MATCHED' then latest_amount_at_risk else 0 end
        ::decimal(18, 2) as open_amount_at_risk,
    currency,
    first_reference_date,
    last_reference_date,
    runs_count,
    runs_with_discrepancy,
    first_discrepancy_date,
    as_of.as_of_date
from per_transaction
left join current_streak using (transaction_id)
cross join as_of
