-- Volume financeiro por dia, merchant e moeda. Junta duas visões do mesmo dinheiro:
--   transacionado: transações internas, pela data de criação e pelo status atual
--   liquidado:     o que os processadores liquidaram, pela data de liquidação
--
-- Datas em UTC, como o resto do pipeline. Moedas nunca são somadas entre si: não temos
-- taxa de câmbio, então cada moeda é uma linha.
with transacted as (
    select
        created_at::date as volume_date,
        merchant_id,
        currency,
        count(*) as transactions_count,
        sum(amount) as transacted_amount,
        sum(amount) filter (where status = 'COMPLETED') as completed_amount,
        sum(amount) filter (where status = 'PENDING') as pending_amount,
        sum(amount) filter (where status = 'FAILED') as failed_amount
    from {{ ref('transactions') }}
    group by all
),

settled as (
    select
        settled_at::date as volume_date,
        merchant_id,
        currency,
        count(*) as settlements_count,
        sum(amount) filter (where status = 'SETTLED') as settled_amount,
        sum(amount) filter (where status = 'REVERSED') as reversed_amount
    from {{ ref('settlements') }}
    group by all
),

combined as (
    select
        volume_date,
        merchant_id,
        currency,
        coalesce(sum(transactions_count), 0) as transactions_count,
        coalesce(sum(transacted_amount), 0) as transacted_amount,
        coalesce(sum(completed_amount), 0) as completed_amount,
        coalesce(sum(pending_amount), 0) as pending_amount,
        coalesce(sum(failed_amount), 0) as failed_amount,
        coalesce(sum(settlements_count), 0) as settlements_count,
        coalesce(sum(settled_amount), 0) as settled_amount,
        coalesce(sum(reversed_amount), 0) as reversed_amount
    from (select * from transacted union all by name select * from settled)
    group by all
)

select
    combined.volume_date,
    combined.merchant_id,
    coalesce(merchant.merchant_key, '-1') as merchant_key,
    combined.currency,
    combined.transactions_count::bigint as transactions_count,
    combined.transacted_amount::decimal(18, 2) as transacted_amount,
    combined.completed_amount::decimal(18, 2) as completed_amount,
    combined.pending_amount::decimal(18, 2) as pending_amount,
    combined.failed_amount::decimal(18, 2) as failed_amount,
    combined.settlements_count::bigint as settlements_count,
    combined.settled_amount::decimal(18, 2) as settled_amount,
    combined.reversed_amount::decimal(18, 2) as reversed_amount,
    (combined.settled_amount - combined.reversed_amount)::decimal(18, 2) as net_settled_amount
from combined
left join {{ ref('dim_merchant') }} as merchant
    on merchant.merchant_id = combined.merchant_id
   and combined.volume_date::timestamptz >= merchant.valid_from
   and combined.volume_date::timestamptz < merchant.valid_to
