-- As contagens por categoria em agg_run_health têm que fechar com o fato, run a run.
with from_fact as (
    select run_id, count(*) as n, sum(amount_at_risk) as risk
    from {{ ref('fct_reconciliation_result') }}
    group by run_id
)
select health.run_id, health.effective_results, from_fact.n
from {{ ref('agg_run_health') }} as health
left join from_fact using (run_id)
where health.effective_results <> coalesce(from_fact.n, 0)
   or health.matched + health.mismatched + health.unreconciled_processor
      + health.unreconciled_internal <> health.effective_results
   or health.amount_at_risk <> coalesce(from_fact.risk, 0)
