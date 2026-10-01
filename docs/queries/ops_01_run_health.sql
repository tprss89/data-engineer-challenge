-- Operações: os runs estão saudáveis? A taxa de discrepância está subindo?
--
-- Uma linha por run, do mais recente para o mais antigo. health_status já resume o que
-- precisa de atenção; variacao_pp compara com a média dos 3 runs anteriores.
select
    reference_date,
    health_status,
    run_status,
    effective_results as resultados,
    round(discrepancy_rate * 100, 1) as taxa_discrepancia_pct,
    round(trailing_avg_discrepancy_rate * 100, 1) as media_3_runs_pct,
    round((discrepancy_rate - trailing_avg_discrepancy_rate) * 100, 1) as variacao_pp,
    amount_at_risk as valor_em_risco,
    declared_results - results_received as resultados_faltando,
    days_since_previous_run as dias_desde_run_anterior,
    round(duration_seconds / 60, 0) as duracao_min
from gold.agg_run_health
order by reference_date desc
