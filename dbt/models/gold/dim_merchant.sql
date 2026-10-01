-- Merchant com histórico (SCD tipo 2). Cada versão tem sua chave, então um fato aponta para
-- o cadastro como ele era na data do evento, e não como está hoje.
--
-- A primeira versão vale desde sempre: um fato anterior ao primeiro registro no CDC ainda
-- encontra o merchant. A data real do registro fica em registered_at.
with versions as (
    select
        *,
        min(valid_from) over (partition by merchant_id) as registered_at
    from {{ ref('merchants_history') }}
)

select
    md5(merchant_id || '|' || valid_from::varchar) as merchant_key,
    merchant_id,
    legal_name,
    trade_name,
    document,
    document_digits,
    primary_cnae,
    primary_cnae_digits,
    left(primary_cnae_digits, 2) as cnae_division,
    registered_at,
    case when version = 1 then '-infinity'::timestamptz else valid_from end as valid_from,
    coalesce(valid_to, 'infinity'::timestamptz) as valid_to,
    is_current,
    is_deleted,
    version
from versions

union all

-- Membro "desconhecido": fatos sem merchant_id ou com merchant fora do cadastro.
select
    '-1', null, 'Merchant não identificado', 'Merchant não identificado', null, null, null, null,
    null, null, '-infinity'::timestamptz, 'infinity'::timestamptz, true, false, 1
