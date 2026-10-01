-- Compliance: quais registros foram barrados, por quê, e de qual arquivo vieram?
--
-- Nada é descartado em silêncio. Cada registro rejeitado fica com o motivo, a linha
-- original e o hash do arquivo de origem, para cobrar correção na fonte.
select
    quarantine.source as fonte,
    quarantine.record_key as registro,
    quarantine.reasons as motivos,
    regexp_extract(quarantine._source_file, '[^/]+$') as arquivo,
    left(log.file_hash, 16) as sha256_arquivo,
    log.finished_at as ingerido_em
from silver.quarantine as quarantine
left join ops.ingestion_log as log on log.batch_id = quarantine._batch_id
order by fonte, registro
