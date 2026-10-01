-- Compliance: com qual razão social o merchant estava cadastrado quando a transação foi
-- reconciliada? E qual é a de hoje?
--
-- O fato aponta para a versão do cadastro válida na data do evento (SCD2). Sem isso, um
-- relatório antigo mostraria o nome atual e não o da época.
select
    fact.reference_date,
    fact.merchant_id,
    at_the_time.legal_name as razao_social_na_epoca,
    today.legal_name as razao_social_hoje,
    count(*) as resultados
from gold.fct_reconciliation_result as fact
join gold.dim_merchant as at_the_time on at_the_time.merchant_key = fact.merchant_key
join gold.dim_merchant as today
    on today.merchant_id = fact.merchant_id and today.is_current
where at_the_time.legal_name <> today.legal_name
group by all
order by fact.merchant_id, fact.reference_date
limit 20
