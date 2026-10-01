-- Operações: quais merchants concentram as pendências abertas?
--
-- Ajuda a priorizar: poucos merchants costumam concentrar boa parte do valor.
-- O nome vem da versão atual do cadastro.
select
    status.merchant_id,
    merchant.trade_name as nome_fantasia,
    count(*) as pendencias_abertas,
    sum(status.open_amount_at_risk) as valor_em_risco,
    round(100 * sum(status.open_amount_at_risk)
          / sum(sum(status.open_amount_at_risk)) over (), 1) as pct_do_total,
    max(status.days_open) as pendencia_mais_antiga_dias
from gold.fct_transaction_reconciliation_status as status
left join gold.dim_merchant as merchant
    on merchant.merchant_id = status.merchant_id and merchant.is_current
where status.is_open
group by all
order by valor_em_risco desc
limit 10
