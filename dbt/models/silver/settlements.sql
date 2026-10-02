-- Liquidações de todos os processadores, num formato só. A gold lê daqui e não conhece
-- nenhum processador pelo nome.
--
-- Para plugar um processador novo: um contrato em contracts/, um modelo
-- <nome>_settlements_checked que devolve as colunas abaixo (mais _dq_errors),
-- um <nome>_settlements com as linhas aceitas, e o nome em vars.settlement_processors.
-- Passo a passo em README.md, seção Extensibilidade.
{% for processor in var('settlement_processors') %}
select
    '{{ processor }}' as processor,
    reference_date,
    transaction_id,
    merchant_id,
    amount,
    amount_raw,
    currency,
    settled_at,
    status,
    processor_reference,
    _batch_id,
    _source_file,
    _source_row
from {{ ref(processor ~ '_settlements') }}
{{ 'union all' if not loop.last }}
{% endfor %}
