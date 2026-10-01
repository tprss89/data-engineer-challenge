-- Resultados aceitos da reconciliação: uma linha por resultado gravado pelo serviço.
--
-- A mesma transação pode ter mais de um resultado dentro do mesmo run (o serviço regravou).
-- Todos ficam aqui para auditoria; is_effective marca o mais recente, que é o que vale.
select
    * exclude (_dq_errors, run_exists, expected_category),
    row_number() over (
        partition by run_id, transaction_id
        order by created_at desc, result_id desc
    ) = 1 as is_effective
from {{ ref('reconciliation_results_checked') }}
where len(_dq_errors) = 0
