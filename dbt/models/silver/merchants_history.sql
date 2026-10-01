-- Histórico do cadastro de merchants (SCD tipo 2). Uma linha por versão, com a janela em que
-- ela valeu. Permite responder qual era a razão social de um merchant numa data passada.
with versions as (
    {{ cdc_versions(source('bronze', 'enterprise_company'), 'id') }}
)

select
    id as source_id,
    upper(trim(merchant_id)) as merchant_id,
    legal_name,
    trade_name,
    document,
    regexp_replace(document, '\D', '', 'g') as document_digits,
    primary_cnae,
    regexp_replace(primary_cnae, '\D', '', 'g') as primary_cnae_digits,
    created_at::timestamptz as created_at,
    _changed_at as valid_from,
    _valid_to as valid_to,
    _version as version,
    _is_current as is_current,
    _is_deleted as is_deleted,
    _batch_id,
    _source_file,
    _source_row
from versions
