-- Todas as versões de cada transação, como o settlement_db as viu ao longo do tempo.
-- Serve para auditoria ("qual era o status desta transação no dia X?").
with versions as (
    {{ cdc_versions(source('bronze', 'transactions'), 'id') }}
)

select
    id as source_id,
    transaction_id,
    upper(trim(merchant_id)) as merchant_id,
    amount::decimal(18, 2) as amount,
    upper(trim(currency)) as currency,
    status,
    description,
    payment_method,
    created_at::timestamptz as created_at,
    updated_at::timestamptz as updated_at,
    Op as cdc_operation,
    _changed_at as valid_from,
    _valid_to as valid_to,
    _version as version,
    _is_current as is_current,
    _is_deleted as is_deleted,
    _batch_id,
    _source_file,
    _source_row
from versions
