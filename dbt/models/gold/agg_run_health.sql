-- Saúde de cada execução da reconciliação. É a tabela que o dashboard de Operações lê.
--
-- health_status resume a linha numa palavra, nesta ordem de gravidade:
--   FAILED / IN_PROGRESS  o run não terminou bem
--   INCOMPLETE            chegaram menos resultados do que o run declarou
--   DISCREPANCY_SPIKE     taxa de discrepância estatisticamente acima da linha de base
--   OK
--
-- Pico: a linha de base é a taxa agregada dos até 7 runs anteriores. O run é marcado quando
-- a taxa dele fica mais de 3 desvios padrão acima dela (teste z para proporção). O desvio
-- depende do número de resultados do run: com 90 resultados, 2 p.p. a mais é ruído; com
-- 5 milhões, 0,5 p.p. já é uma mudança real. Um limiar fixo em p.p. erraria nos dois casos.
--
-- Valor em risco: só BRL é somado aqui. Discrepâncias em outras moedas são contadas à parte
-- e detalhadas por moeda no fato.
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
        sum(amount_at_risk) filter (where currency = 'BRL') as amount_at_risk_brl,
        count(*) filter (where is_discrepancy and currency <> 'BRL')
            as discrepancies_in_other_currencies
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
        coalesce(effective.amount_at_risk_brl, 0)::decimal(18, 2) as amount_at_risk_brl,
        coalesce(effective.discrepancies_in_other_currencies, 0)
            as discrepancies_in_other_currencies,
        effective.effective_results - effective.matched as discrepancies,
        round((effective.effective_results - effective.matched) / effective.effective_results, 4)
            as discrepancy_rate,
        runs.reference_date - lag(runs.reference_date) over (order by runs.reference_date)
            as days_since_previous_run
    from {{ ref('reconciliation_runs') }} as runs
    left join received on received.run_id = runs.run_id
    left join quarantined on quarantined.run_id = runs.run_id
    left join effective on effective.run_id = runs.run_id
),

with_baseline as (
    select
        *,
        lag(discrepancy_rate) over (order by reference_date) as previous_discrepancy_rate,
        -- Taxa agregada (não a média das taxas) dos até 7 runs anteriores com resultados.
        sum(discrepancies) over previous_runs
            / nullif(sum(effective_results) over previous_runs, 0) as baseline_rate
    from runs
    window previous_runs as (order by reference_date rows between 7 preceding and 1 preceding)
),

with_z as (
    select
        *,
        (discrepancy_rate - baseline_rate)
            / sqrt(baseline_rate * (1 - baseline_rate) / nullif(effective_results, 0))
            as discrepancy_z_score
    from with_baseline
)

select
    * exclude (baseline_rate, discrepancy_z_score),
    round(baseline_rate, 4) as baseline_discrepancy_rate,
    round(discrepancy_z_score, 2) as discrepancy_z_score,
    case
        when run_status = 'FAILED' then 'FAILED'
        when run_status <> 'COMPLETED' then 'IN_PROGRESS'
        when results_received < declared_results then 'INCOMPLETE'
        when discrepancy_z_score > 3 then 'DISCREPANCY_SPIKE'
        else 'OK'
    end as health_status
from with_z
