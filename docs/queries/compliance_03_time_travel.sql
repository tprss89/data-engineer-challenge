-- Compliance: o que o relatório de saúde dos runs mostrava numa versão anterior?
--
-- O DuckLake guarda um snapshot a cada escrita. Dá para consultar qualquer tabela como ela
-- estava num snapshot (AT (VERSION => ...)) ou num instante (AT (TIMESTAMP => ...)).
-- Aqui: as últimas versões gravadas e o relatório como estava na primeira vez em que foi
-- publicado.
select snapshot_id, snapshot_time, changes
from lake.snapshots()
order by snapshot_id desc
limit 5;

set variable first_version = (
    select min(snapshot_id) from lake.snapshots()
    where list_contains(changes['tables_created'], 'gold.agg_run_health')
);

select reference_date, health_status, discrepancy_rate, amount_at_risk
from gold.agg_run_health at (version => getvariable('first_version'))
order by reference_date
