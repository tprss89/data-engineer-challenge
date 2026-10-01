{# Falha (ou alerta, conforme a severidade) se o modelo tiver qualquer linha. #}
{% test expect_no_rows(model) %}
select * from {{ model }}
{% endtest %}
