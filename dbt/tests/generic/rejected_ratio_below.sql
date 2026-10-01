{#
  Barra a publicação quando a quarentena de uma fonte passa de uma fração do total.
  Poucas linhas ruins são problema de dado; muitas indicam fonte quebrada, e aí é melhor
  não publicar números que parecem certos mas não são.
#}
{% test rejected_ratio_below(model, source_name, accepted, max_ratio) %}
with rejected as (
    select count(*) as n from {{ model }} where source = '{{ source_name }}'
),
accepted as (
    select count(*) as n from {{ accepted }}
)
select rejected.n as rejected_rows, accepted.n as accepted_rows,
       rejected.n / nullif(rejected.n + accepted.n, 0) as rejected_ratio
from rejected, accepted
where rejected.n > {{ max_ratio }} * (rejected.n + accepted.n)
{% endtest %}
