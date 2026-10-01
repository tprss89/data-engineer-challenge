{#
  Valor monetário em texto para decimal(18,2). Aceita os dois formatos do PaySettler:
  "1234.56" e "R$ 1.234,56". Qualquer outra coisa vira null e o registro vai para quarentena.
#}
{% macro parse_amount(column) %}
    case
        when regexp_matches(trim({{ column }}), '^-?\d+(\.\d+)?$')
            then trim({{ column }})::decimal(18, 2)
        when regexp_matches(trim({{ column }}), '^(R\$\s*)?-?\d{1,3}(\.\d{3})*(,\d+)?$')
          or regexp_matches(trim({{ column }}), '^(R\$\s*)?-?\d+(,\d+)?$')
            then replace(replace(regexp_replace(trim({{ column }}), '^R\$\s*', ''), '.', ''), ',', '.')::decimal(18, 2)
    end
{% endmacro %}

{# Timestamp ISO 8601 para timestamptz. Sem fuso, assume UTC (o glossário diz que é UTC). #}
{% macro parse_utc_timestamp(column) %}
    case
        when regexp_matches(trim({{ column }}), '(Z|[+-]\d{2}:?\d{2})$')
            then try_cast(trim({{ column }}) as timestamptz)
        else try_cast(trim({{ column }}) || '+00:00' as timestamptz)
    end
{% endmacro %}

{# Lista de problemas de qualidade, sem os nulls. Vazia = registro aceito. #}
{% macro dq_errors(checks) %}
    list_filter([
        {%- for reason, condition in checks.items() %}
        case when {{ condition }} then '{{ reason }}' end{{ "," if not loop.last }}
        {%- endfor %}
    ], x -> x is not null)
{% endmacro %}
