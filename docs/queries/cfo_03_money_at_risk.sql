-- CFO: quanto dinheiro está em discrepância, e quanto continua em aberto?
--
-- Por run: o valor envolvido em discrepâncias. A última linha mostra o que continua aberto
-- na data do run mais recente, já descontado o que se resolveu em runs seguintes.
select
    reference_date::varchar as referencia,
    amount_at_risk as valor_em_discrepancia,
    mismatched + unreconciled_processor + unreconciled_internal as discrepancias,
    effective_results as resultados
from gold.agg_run_health
where run_status = 'COMPLETED'

union all

select
    'em aberto em ' || max(as_of_date)::varchar,
    sum(open_amount_at_risk),
    count(*) filter (where is_open),
    count(*)
from gold.fct_transaction_reconciliation_status

order by referencia
