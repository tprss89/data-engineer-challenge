-- CFO: quanto dinheiro está em discrepância, e quanto continua em aberto?
--
-- Por data de referência e moeda: o valor envolvido em discrepâncias naquele run. A última
-- parte mostra o que continua aberto na data do run mais recente, já descontado o que se
-- resolveu em runs seguintes. Moedas diferentes nunca são somadas.
select
    reference_date::varchar as referencia,
    currency as moeda,
    sum(amount_at_risk) as valor_em_discrepancia,
    count(*) filter (where is_discrepancy) as discrepancias,
    count(*) as resultados
from gold.fct_reconciliation_result
group by all

union all

select
    'em aberto em ' || max(as_of_date)::varchar,
    currency,
    sum(open_amount_at_risk),
    count(*) filter (where is_open),
    count(*)
from gold.fct_transaction_reconciliation_status
group by currency

order by referencia, moeda
