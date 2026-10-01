-- Saúde de cada execução da reconciliação. É a tabela que o dashboard de Operações lê.
--
-- health_status resume a linha numa palavra, nesta ordem de gravidade:
--   FAILED / IN_PROGRESS  o run não terminou bem
--   INCOMPLETE            chegaram menos resultados do que o run declarou
--   DISCREPANCY_SPIKE     taxa de discrepância 2 p.p. acima da média dos 3 runs anteriores
--   OK
with received as (
    -- Tudo que chegou do run, aceito ou não, para comparar com o total declarado.
    select run_id, count(*) as results_received
    from {{ ref('reconciliation_results') }}
    group by run_id
),

quarantined as (
    select json_extract(payload, '$.run_id')::bigint as run_id, count(*) as results_quarantined
    from {{ ref('quarantine') }}
    where source = 'reconciliation_results'
    group by 1
),

effective as (
    select
        run_id,
        count(*) as effective_results,
        count(*) filter (where category = 'MATCHED') as matched,
        count(*) filter (where category = 'MISMATCHED') as mismatched,
        count(*) filter (where category = 'UNRECONCILED_PROCESSOR') as unreconciled_processor,
        count(*) filter (where category = 'UNRECONCILED_INTERNAL') as unreconciled_internal,
        sum(amount_at_risk) as amount_at_risk,
        sum(internal_amount) filter (where category in ('MATCHED', 'MISMATCHED')) as amount_compared
    from {{ ref('fct_reconciliation_result') }}
    group by run_id
),

runs as (
    select
        runs.run_id,
        runs.reference_date,
        runs.status as run_status,
        runs.file_name,
        runs.started_at,
        runs.completed_at,
        runs.duration_seconds,
        runs.total_transactions as declared_results,
        coalesce(received.results_received, 0)
            + coalesce(quarantined.results_quarantined, 0) as results_received,
        coalesce(quarantined.results_quarantined, 0) as results_quarantined,
        coalesce(effective.effective_results, 0) as effective_results,
        coalesce(effective.matched, 0) as matched,
        coalesce(effective.mismatched, 0) as mismatched,
        coalesce(effective.unreconciled_processor, 0) as unreconciled_processor,
        coalesce(effective.unreconciled_internal, 0) as unreconciled_internal,
        coalesce(effective.amount_at_risk, 0)::decimal(18, 2) as amount_at_risk,
        round((effective.effective_results - effective.matched) / effective.effective_results, 4)
            as discrepancy_rate,
        runs.reference_date - lag(runs.reference_date) over (order by runs.reference_date)
            as days_since_previous_run
    from {{ ref('reconciliation_runs') }} as runs
    left join received on received.run_id = runs.run_id
    left join quarantined on quarantined.run_id = runs.run_id
    left join effective on effective.run_id = runs.run_id
),

with_trend as (
    select
        *,
        lag(discrepancy_rate) over by_date as previous_discrepancy_rate,
        round(avg(discrepancy_rate) over (
            order by reference_date rows between 3 preceding and 1 preceding), 4)
            as trailing_avg_discrepancy_rate
    from runs
    window by_date as (order by reference_date)
)

select
    *,
    case
        when run_status = 'FAILED' then 'FAILED'
        when run_status <> 'COMPLETED' then 'IN_PROGRESS'
        when results_received < declared_results then 'INCOMPLETE'
        when discrepancy_rate > trailing_avg_discrepancy_rate + 0.02 then 'DISCREPANCY_SPIKE'
        else 'OK'
    end as health_status
from with_trend
