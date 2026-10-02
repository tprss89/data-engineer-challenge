-- Compliance: o que os consumidores viam numa publicação anterior?
--
-- Cada publicação da gold aponta para um snapshot do DuckLake, registrado em
-- ops.publications. Como o DuckLake guarda os snapshots, dá para consultar qualquer tabela
-- exatamente como ela estava publicada (AT (VERSION => ...)), ou num instante qualquer
-- (AT (TIMESTAMP => ...)).
--
-- Aqui: o histórico de publicações e o relatório de saúde dos runs como saiu na primeira.
select run_id, snapshot_id, published_at
from ops.publications
order by published_at desc
limit 5;

set variable first_publication = (select min(snapshot_id) from ops.publications);

select reference_date, health_status, discrepancy_rate, amount_at_risk_brl
from gold_build.agg_run_health at (version => getvariable('first_publication'))
order by reference_date
