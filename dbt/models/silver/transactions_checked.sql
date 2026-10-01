{{ config(materialized='ephemeral') }}

-- Estado atual de cada transação, ainda com os registros problemáticos marcados.
with current_rows as (
    select * from {{ ref('transactions_history') }}
    where is_current and not is_deleted
),

ranked as (
    select
        *,
        -- Replay de CDC: a mesma transaction_id com outro id. Fica a primeira (menor id).
        row_number() over (partition by transaction_id order by source_id) as _dup_rank
    from current_rows
)

select
    * exclude (_dup_rank),
    {{ dq_errors({
        'transaction_id_null': 'transaction_id is null',
        'duplicate_transaction_id': '_dup_rank > 1',
    }) }} as _dq_errors
from ranked
