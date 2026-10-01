{# Toda linha do modelo precisa satisfazer a expressão. #}
{% test expression_is_true(model, expression) %}
select * from {{ model }} where not ({{ expression }})
{% endtest %}
