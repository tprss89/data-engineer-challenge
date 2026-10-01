-- Compliance: o que aconteceu com esta transação, e de onde veio cada informação?
--
-- Linha do tempo completa: versões do registro interno, resultados de reconciliação
-- (inclusive regravados e barrados) e liquidações. Cada evento aponta para arquivo, linha
-- e hash SHA-256 do arquivo recebido.
--
-- Exemplo: a transação acc62b77, atualizada e apagada no mesmo instante no CDC.
select
    event_seq as seq,
    event_at as quando,
    event_type as evento,
    details as detalhes,
    regexp_extract(source_file, '[^/]+$') as arquivo,
    source_row as linha,
    left(source_file_sha256, 16) as sha256_arquivo,
    ingested_at as ingerido_em
from gold.audit_transaction_trail
where transaction_id = 'acc62b77-44b8-472a-bfc6-47d62c98145a'
order by event_seq
