-- As contagens por categoria e o valor em risco em BRL de agg_run_health têm que fechar
-- com o fato, run a run.
with from_fact as (
    select
        run_id,
        count(*) as n,
        coalesce(sum(amount_at_risk) filter (where currency = 'BRL'), 0) as risk_brl
    from {{ ref('fct_reconciliation_result') }}
    group by run_id
)
select health.run_id, health.effective_results, from_fact.n
from {{ ref('agg_run_health') }} as health
left join from_fact using (run_id)
where health.effective_results <> coalesce(from_fact.n, 0)
   or health.matched + health.mismatched + health.unreconciled_processor
      + health.unreconciled_internal <> health.effective_results
   or health.amount_at_risk_brl <> coalesce(from_fact.risk_brl, 0)
