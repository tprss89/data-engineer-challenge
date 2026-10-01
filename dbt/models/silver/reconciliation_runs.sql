-- Estado atual de cada execução do serviço de reconciliação.
with versions as (
    {{ cdc_versions(source('bronze', 'reconciliation_runs'), 'id') }}
)

select
    id as run_id,
    reference_date::date as reference_date,
    file_name,
    status,
    total_transactions,
    started_at::timestamptz as started_at,
    completed_at::timestamptz as completed_at,
    created_at::timestamptz as created_at,
    date_diff('second', started_at::timestamptz, completed_at::timestamptz) as duration_seconds,
    status in ('COMPLETED', 'FAILED') as is_finished,
    _changed_at as last_changed_at,
    _batch_id,
    _source_file
from versions
where _is_current and not _is_deleted
