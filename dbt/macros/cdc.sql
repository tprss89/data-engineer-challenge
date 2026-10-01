{#
  Transforma o log CDC de uma tabela em versões ordenadas por chave.

  Ordem: _timestamp, depois operação (D > U > I quando o timestamp empata) e, por último,
  o hash do evento só para o resultado ser determinístico.

  Colunas adicionadas:
    _changed_at   quando a versão passou a valer (tempo de sistema)
    _valid_to     quando foi substituída (null = ainda vale)
    _version      1, 2, 3... por chave
    _is_current   é a última versão da chave
    _is_deleted   a última operação da chave foi D
#}
{% macro cdc_versions(relation, key) %}
    with events as (
        select
            *,
            _timestamp::timestamptz as _changed_at,
            case Op when 'I' then 1 when 'U' then 2 when 'D' then 3 end as _op_rank
        from {{ relation }}
    )
    select
        * exclude (_op_rank),
        lead(_changed_at) over key_order as _valid_to,
        row_number() over key_order as _version,
        row_number() over key_order_desc = 1 as _is_current,
        first_value(Op) over key_order_desc = 'D' as _is_deleted
    from events
    window
        key_order as (partition by {{ key }} order by _changed_at, _op_rank, _event_hash),
        key_order_desc as (partition by {{ key }} order by _changed_at desc, _op_rank desc, _event_hash desc)
{% endmacro %}
