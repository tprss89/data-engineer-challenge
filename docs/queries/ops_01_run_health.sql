-- Operações: os runs estão saudáveis? A taxa de discrepância está subindo?
--
-- Uma linha por run, do mais recente para o mais antigo. health_status já resume o que
-- precisa de atenção. A linha de base é a taxa dos até 7 runs anteriores; z_score diz
-- quantos desvios padrão o run está acima dela, já considerando o tamanho do run
-- (acima de 3 vira DISCREPANCY_SPIKE).
select
    reference_date,
    health_status,
    run_status,
    effective_results as resultados,
    round(discrepancy_rate * 100, 1) as taxa_discrepancia_pct,
    round(baseline_discrepancy_rate * 100, 1) as linha_de_base_pct,
    discrepancy_z_score as z_score,
    amount_at_risk_brl as valor_em_risco_brl,
    discrepancies_in_other_currencies as discrepancias_outras_moedas,
    declared_results - results_received as resultados_faltando,
    days_since_previous_run as dias_desde_run_anterior,
    round(duration_seconds / 60, 0) as duracao_min
from gold.agg_run_health
order by reference_date desc
