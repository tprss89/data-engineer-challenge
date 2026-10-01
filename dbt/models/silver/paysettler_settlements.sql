-- Liquidações do PaySettler aceitas: uma linha por transação por data de referência.
select * exclude (_dq_errors, _copy_number)
from {{ ref('paysettler_settlements_checked') }}
where len(_dq_errors) = 0
