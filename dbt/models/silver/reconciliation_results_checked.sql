{{ config(materialized='ephemeral') }}

-- Resultado atual de cada linha de reconciliação, com os problemas de qualidade marcados.
-- Valores sempre em decimal(18,2): em double, 0,01 de diferença vira 0,0100000000002 e
-- dezenas de MATCHED pareceriam fora da tolerância.
with versions as (
    {{ cdc_versions(source('bronze', 'reconciliation_results'), 'id') }}
),

typed as (
    select
        id as result_id,
        run_id,
        transaction_id,
        upper(trim(merchant_id)) as merchant_id,
        category,
        internal_amount::decimal(18, 2) as internal_amount,
        processor_amount::decimal(18, 2) as processor_amount,
        difference::decimal(18, 2) as difference,
        created_at::timestamptz as created_at,
        _batch_id,
        _source_file,
        _source_row
    from versions
    where _is_current and not _is_deleted
),

with_expectation as (
    select
        typed.*,
        runs.run_id is not null as run_exists,
        -- Categoria que os próprios valores indicam, pela regra do glossário (tolerância R$ 0,01).
        case
            when internal_amount is null and processor_amount is not null then 'UNRECONCILED_PROCESSOR'
            when processor_amount is null and internal_amount is not null then 'UNRECONCILED_INTERNAL'
            when abs(internal_amount - processor_amount) <= 0.01 then 'MATCHED'
            when abs(internal_amount - processor_amount) > 0.01 then 'MISMATCHED'
        end as expected_category
    from typed
    left join {{ ref('reconciliation_runs') }} as runs on runs.run_id = typed.run_id
)

select
    *,
    {{ dq_errors({
        'transaction_id_null': 'transaction_id is null',
        'run_not_found': 'not run_exists',
        'invalid_category': "category is null or category not in ('MATCHED', 'MISMATCHED', 'UNRECONCILED_PROCESSOR', 'UNRECONCILED_INTERNAL')",
        'category_contradicts_amounts': 'category is distinct from expected_category',
        'difference_contradicts_amounts': 'internal_amount is not null and processor_amount is not null and difference is distinct from abs(internal_amount - processor_amount)',
    }) }} as _dq_errors
from with_expectation
