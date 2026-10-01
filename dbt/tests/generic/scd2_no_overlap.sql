{# Duas versões da mesma chave não podem valer ao mesmo tempo. #}
{% test scd2_no_overlap(model, key, valid_from, valid_to) %}
select a.{{ key }}, a.{{ valid_from }}, b.{{ valid_from }} as overlapping_from
from {{ model }} as a
join {{ model }} as b
  on a.{{ key }} = b.{{ key }}
 and a.{{ valid_from }} < b.{{ valid_from }}
 and b.{{ valid_from }} < a.{{ valid_to }}
{% endtest %}
