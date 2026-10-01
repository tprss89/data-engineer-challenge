{# Usa o schema configurado (silver, gold) sem o prefixo padrão do dbt. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
