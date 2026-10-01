-- Estado atual das transações internas: uma linha por transaction_id, sem as apagadas.
select * exclude (_dq_errors, is_current, is_deleted, valid_to, cdc_operation)
from {{ ref('transactions_checked') }}
where len(_dq_errors) = 0
