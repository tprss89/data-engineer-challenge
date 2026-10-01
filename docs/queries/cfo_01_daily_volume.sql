-- CFO: quanto dinheiro passou por dia?
--
-- Transacionado é o que o sistema interno registrou (pela data de criação); liquidado é o
-- que o PaySettler confirmou (pela data de liquidação), já descontados os estornos.
-- Cada moeda é uma linha: sem taxa de câmbio, não somamos moedas diferentes.
select
    volume_date as dia,
    currency as moeda,
    sum(transactions_count) as transacoes,
    sum(transacted_amount) as transacionado,
    sum(completed_amount) as concluido,
    sum(pending_amount) as pendente,
    sum(failed_amount) as falhou,
    sum(settled_amount) as liquidado,
    sum(reversed_amount) as estornado,
    sum(net_settled_amount) as liquido
from gold.fct_daily_volume
group by all
order by dia, moeda
