-- Operações: o que está pendente de verdade, e há quanto tempo?
--
-- Uma transação passa por vários runs (as janelas de 7 dias se sobrepõem). Aqui só entra o
-- que continua com discrepância no run mais recente em que apareceu. O que se resolveu
-- depois não aparece como pendência.
select
    latest_category as categoria,
    case
        when days_open <= 3 then '0-3 dias'
        when days_open <= 7 then '4-7 dias'
        when days_open <= 14 then '8-14 dias'
        else 'mais de 14 dias'
    end as idade,
    count(*) as transacoes,
    sum(open_amount_at_risk) as valor_em_risco
from gold.fct_transaction_reconciliation_status
where is_open
group by all
order by categoria, min(days_open)
