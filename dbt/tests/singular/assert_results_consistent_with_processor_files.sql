{{ config(severity='warn') }}

-- Check cruzado: o lado do processador de cada resultado efetivo bate com o arquivo que o
-- processador mandou para a mesma data? Só vale para datas cujo arquivo foi carregado.
-- Não bloqueia, pelo mesmo motivo do check com o sistema interno.
with loaded_dates as (
    select distinct reference_date from {{ ref('settlements') }}
),

processor as (
    select reference_date, transaction_id, amount
    from {{ ref('settlements') }}
)

select
    results.run_id,
    runs.reference_date,
    results.transaction_id,
    case
        when processor.transaction_id is null then 'transaction_not_in_processor_file'
        else 'processor_amount_differs'
    end as divergence,
    results.processor_amount,
    processor.amount as processor_file_amount
from {{ ref('reconciliation_results') }} as results
join {{ ref('reconciliation_runs') }} as runs on runs.run_id = results.run_id
join loaded_dates on loaded_dates.reference_date = runs.reference_date
left join processor
    on processor.reference_date = runs.reference_date
   and processor.transaction_id = results.transaction_id
where results.is_effective
  and results.processor_amount is not null
  and (processor.transaction_id is null or results.processor_amount <> processor.amount)
