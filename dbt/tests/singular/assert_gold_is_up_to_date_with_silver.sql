{{ config(severity='warn') }}

-- O fato é incremental por data. Se a silver mudou numa data que não foi reprocessada, a
-- gold fica atrás. Isso não bloqueia (pode ser intencional), mas avisa para rodar sem
-- --reference-date ou com a data em questão.
with silver_by_date as (
    select runs.reference_date, count(*) as n
    from {{ ref('reconciliation_results') }} as results
    join {{ ref('reconciliation_runs') }} as runs using (run_id)
    where results.is_effective
    group by 1
),
gold_by_date as (
    select reference_date, count(*) as n
    from {{ ref('fct_reconciliation_result') }}
    group by 1
)
select coalesce(s.reference_date, g.reference_date) as reference_date,
       s.n as silver_rows, g.n as gold_rows
from silver_by_date as s
full outer join gold_by_date as g using (reference_date)
where s.n is distinct from g.n
