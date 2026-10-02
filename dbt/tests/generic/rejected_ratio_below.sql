{#
  Barra a publicação quando um lote de ingestão (um arquivo) de uma fonte tem mais do que
  uma fração das linhas na quarentena. Poucas linhas ruins são problema de dado; muitas
  indicam um arquivo quebrado, e aí é melhor não publicar números que parecem certos.

  A proporção é por lote, não sobre o histórico: com 30 dias acumulados, um dia 100%
  quebrado seria só 3% do total e passaria.

  Um lote bloqueado continua bloqueando as execuções seguintes até a fonte ser corrigida.
  Aceitar um arquivo ruim é decisão de alguém, não do pipeline.
#}
{% test rejected_ratio_below(model, source_name, accepted, max_ratio) %}
with rejected as (
    select _batch_id, count(*) as n
    from {{ model }}
    where source = '{{ source_name }}'
    group by _batch_id
),
accepted as (
    select _batch_id, count(*) as n
    from {{ accepted }}
    group by _batch_id
),
per_batch as (
    select
        _batch_id,
        coalesce(rejected.n, 0) as rejected_rows,
        coalesce(accepted.n, 0) as accepted_rows
    from rejected
    full outer join accepted using (_batch_id)
)
select
    per_batch._batch_id,
    log.source_file,
    rejected_rows,
    accepted_rows,
    rejected_rows / (rejected_rows + accepted_rows) as rejected_ratio
from per_batch
left join {{ source('ops', 'ingestion_log') }} as log on log.batch_id = per_batch._batch_id
where rejected_rows > {{ max_ratio }} * (rejected_rows + accepted_rows)
{% endtest %}
