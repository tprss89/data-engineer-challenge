-- Tabela (e não ephemeral) porque o modelo final e a quarentena leem daqui: como
-- ephemeral, a resolução de CDC rodaria duas vezes.
{{ config(materialized='table') }}

-- Liquidações do PaySettler tipadas, com os problemas de qualidade marcados.
-- Se o processador reenviar o arquivo de uma data, vale a versão carregada por último;
-- as anteriores continuam no bronze.
with latest_file as (
    select
        _reference_date as reference_date,
        transaction_id as transaction_id_raw,
        merchant_id as merchant_id_raw,
        amount as amount_raw,
        currency as currency_raw,
        settled_at as settled_at_raw,
        processor_reference as processor_reference_raw,
        status as status_raw,
        _source_row,
        _batch_id,
        _source_file
    from {{ source('bronze', 'paysettler_settlements') }}
    qualify dense_rank() over (
        partition by _reference_date order by _ingested_at desc, _batch_id desc
    ) = 1
),

typed as (
    select
        reference_date,
        try_cast(trim(transaction_id_raw) as uuid)::varchar as transaction_id,
        upper(trim(merchant_id_raw)) as merchant_id,
        {{ parse_amount('amount_raw') }} as amount,
        upper(trim(currency_raw)) as currency,
        {{ parse_utc_timestamp('settled_at_raw') }} as settled_at,
        upper(trim(status_raw)) as status,
        trim(processor_reference_raw) as processor_reference,
        amount_raw,
        settled_at_raw,
        amount_raw like 'R$%' as amount_was_brl_formatted,
        regexp_matches(trim(settled_at_raw), '(Z|[+-]\d{2}:?\d{2})$') as settled_at_had_timezone,
        merchant_id_raw is distinct from upper(trim(merchant_id_raw)) as merchant_id_was_normalized,
        -- Linha repetida idêntica no arquivo: a primeira fica, as cópias vão para quarentena.
        row_number() over (
            partition by transaction_id_raw, merchant_id_raw, amount_raw, currency_raw,
                         settled_at_raw, processor_reference_raw, status_raw
            order by _source_row
        ) as _copy_number,
        _source_row,
        _batch_id,
        _source_file
    from latest_file
),

with_conflicts as (
    select
        *,
        -- Mesma transação mais de uma vez no arquivo com conteúdo diferente: não dá para
        -- escolher uma, então nenhuma passa.
        count(*) filter (where _copy_number = 1)
            over (partition by reference_date, transaction_id) as _distinct_versions
    from typed
)

select
    * exclude (_distinct_versions),
    {{ dq_errors({
        'invalid_transaction_id': 'transaction_id is null',
        'invalid_amount': 'amount is null',
        'invalid_settled_at': 'settled_at is null',
        'invalid_status': "status is null or status not in ('SETTLED', 'REVERSED')",
        'invalid_currency': "currency is null or not regexp_matches(currency, '^[A-Z]{3}$')",
        'duplicate_row': '_copy_number > 1',
        'conflicting_rows_for_transaction': '_distinct_versions > 1',
    }) }} as _dq_errors
from with_conflicts
