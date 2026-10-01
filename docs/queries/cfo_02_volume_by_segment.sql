-- CFO: de quais segmentos vem o volume?
--
-- Segmento é a divisão do CNAE (dois primeiros dígitos) do merchant na data da transação.
-- Período: tudo o que está disponível nos dados.
select
    coalesce(merchant.cnae_division, 'sem cadastro') as divisao_cnae,
    volume.currency as moeda,
    count(distinct volume.merchant_id) as merchants,
    sum(volume.transactions_count) as transacoes,
    sum(volume.completed_amount) as volume_concluido,
    round(100 * sum(volume.completed_amount)
          / sum(sum(volume.completed_amount)) over (partition by volume.currency), 1)
        as pct_da_moeda
from gold.fct_daily_volume as volume
left join gold.dim_merchant as merchant on merchant.merchant_key = volume.merchant_key
group by divisao_cnae, moeda
order by moeda, volume_concluido desc
