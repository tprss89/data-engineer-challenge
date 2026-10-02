# CLAUDE.md

Instruções para o Claude Code neste repositório. Também serve como registro de como usei IA no case.

## O projeto

Case técnico de engenharia de dados: transformar os dados do Settlement Reconciliation Service em produtos de dados para três consumidores (Operações, CFO e Compliance). O enunciado completo está no [README.md](README.md) e as regras de negócio em [docs/domain-glossary.md](docs/domain-glossary.md). Leia os dois antes de mexer em regra de negócio.

Stack obrigatória: Python 3.12+, DuckDB para processamento, Docker Compose para subir o ambiente, armazenamento em filesystem local. Tudo precisa rodar com `docker compose up`.

## Decisões já tomadas

Não reabra essas decisões sem me perguntar. O porquê de cada uma fica em `docs/decisions.md`. As marcadas lá como **provisória** devem ser revistas comigo na fase de testes finais.

- Arquitetura medallion: bronze (cópia fiel da fonte, só append), silver (CDC aplicado, tipado, deduplicado, com quarentena), gold (fatos e dimensões por consumidor).
- Storage em DuckLake numa pasta local, e transformações em dbt-duckdb. Validado no bloco 0 com duckdb 1.5.6, dbt-core 1.12.5 e dbt-duckdb 1.11.0, rodando no container sem rede: build repetido é idempotente, transação que falha no meio faz rollback completo, time travel funciona e o incremental `delete+insert` por `reference_date` substitui só a data reprocessada. A extensão `ducklake` é instalada no build da imagem para não depender de rede em runtime.
- `reconciliation_results` é a fonte de verdade das categorias. O pipeline não refaz a reconciliação. Motivo: o CSV de exemplo não reproduz os resultados (488 de 510 linhas têm valor diferente do interno).
- O CSV do PaySettler entra como dado bruto do processador, usado para checks cruzados e trilha de auditoria.
- Sem Airflow na entrega. A CLI idempotente por `reference_date` é a unidade que um orquestrador chamaria. O orquestrador aparece no desenho de arquitetura (Parte 3).

## Comandos

```bash
make up          # sobe o container
make shell       # shell dentro do container
make test        # roda os testes
make ingest      # carrega o fixture no bronze (o CSV do fixture é de 2025-03-16)
make status      # últimas ingestões, a partir de ops.ingestion_log
make generate    # gera 10k linhas sintéticas em /tmp/generated
```

Dentro do container a CLI é `pipeline`: `ingest` (bronze), `transform` (dbt build; `--reference-date` reprocessa só aquela data, `--full-refresh` reconstrói), `run` (os dois em sequência; se a ingestão bloquear um arquivo, não transforma), `query` (SQL ou arquivo .sql, somente leitura) e `status` (painel: últimas execuções, execuções que não terminaram, frescor por camada e alertas). Saída 0 = ok (pode ter alertas), 1 = dado bloqueado (contrato ou check com severidade error), 2 = erro inesperado.

O dbt roda como subprocesso (`transform.py`), nunca dentro do mesmo processo Python: o dbt-duckdb deixa o catálogo aberto e quebra a conexão seguinte. Artefatos do dbt ficam em `data/dbt_target/`.

Para explorar os dados: `python scripts/explore.py` no host (não no container) abre o lake na DuckDB UI do navegador, em modo leitura. Precisa de `duckdb` 1.5.x instalado no Python do host.

Enquanto alguém (DBeaver, explore.py) estiver com o catálogo aberto, o pipeline não consegue escrever. Feche a conexão antes de rodar.

No Git Bash do Windows, use `MSYS_NO_PATHCONV=1` antes de comandos `docker` que recebem caminhos Linux (`/tmp/...`), senão o shell converte o caminho.

## Regras de engenharia

- Toda execução recebe uma `reference_date` e precisa ser idempotente. Rodar duas vezes a mesma data não pode duplicar nem corromper nada, inclusive se falhar no meio.
- Bronze nunca transforma dado. Só acrescenta metadados de ingestão (`_batch_id`, `_source_file`, `_file_hash`, `_source_row`, `_ingested_at`, mais `_event_hash` no CDC e `_reference_date` no CSV).
- Idempotência do bronze: arquivo com hash já carregado é ignorado; no CDC, cada evento entra uma vez (dedupe por `_event_hash`, porque as extrações são acumuladas). No CSV não há dedupe por linha: duplicata do processador fica no bronze e é tratada no silver.
- Contratos das fontes ficam em `contracts/*.yml`. Coluna obrigatória ausente ou tipo diferente bloqueia o arquivo; coluna nova vira drift (entra na tabela e gera alerta).
- Todo modelo novo nasce com teste. Cada check tem severidade definida: `error` para o pipeline e não publica, `warn` publica e registra o alerta.
- Regra de negócio sempre com referência ao glossário. Tolerância de R$ 0,01 e janela de 7 dias inclusiva vêm de lá.
- Só considere uma mudança pronta depois de rodar os testes no container. "Deveria funcionar" não conta.
- Timestamps são tratados como UTC. Valor sem fuso é assumido UTC e isso está documentado.
- Silver: cada fonte com problema de qualidade tem um modelo `*_checked` (ephemeral) que marca `_dq_errors`; o modelo final pega as linhas sem erro e `silver.quarantine` junta as rejeitadas com motivo e a linha original em JSON. Não descarte registro em silêncio.
- Gold: perguntas por consumidor e o que fica de fora estão em `docs/data-products.md`. Toda pergunta nova vira uma query em `docs/queries/`, que é testada automaticamente (`tests/test_gold.py`).
- `fct_reconciliation_result` é incremental por `reference_date` (var do dbt passada pela CLI). Não use `current_date` em modelo: "hoje" é o run mais recente nos dados.
- Consumidores leem `gold.*`, que são views fixadas num snapshot do DuckLake. O dbt escreve em `gold_build`; `publish.py` só publica se o build inteiro passou. Nunca aponte uma query de consumidor para `gold_build`.
- Toda execução passa por `pipeline.run_pipeline`, que registra `ops.pipeline_runs` e `ops.dq_results`. Não chame dbt ou ingestão por fora dele na CLI.
- Desempenho: meça com `scripts/benchmark.py` (dentro do container) antes de afirmar qualquer coisa sobre escala. Resultados e o plano para 5M/dia estão em `docs/scale.md`. Não mexa no docker-compose.yml com um benchmark rodando: recriar o container apaga o /tmp.
- Quarentena acima de 5% de uma fonte bloqueia o build (`rejected_ratio_below`, severidade error). Qualquer linha em quarentena gera alerta.

## O que já sabemos dos dados de exemplo

Isso é sujeira intencional. Trate, não ignore e não "corrija" silenciosamente.

- Todas as tabelas vêm em formato CDC (`Op` = I/U/D, `_timestamp`). Existe caso de I, D e depois I de novo para a mesma transação, então a ordem por `_timestamp` importa.
- Há empate de `_timestamp` entre U e D da mesma transação (`acc62b77...`, em 2025-03-31T00:00:00). Ordenar só por timestamp deixa o resultado ao acaso. O desempate é determinístico: D > U > I. Sem isso, a transação apagada continua viva (996 em vez de 995 transações ativas).
- `transactions_batch_2` tem a coluna `payment_method`, que não existe no batch 1.
- `enterprise_company` tem 70 linhas para 50 merchants. Os updates mudam a razão social, então a dimensão de merchant guarda histórico (SCD2).
- `reconciliation_runs`: o run 7 está `FAILED`, sem `completed_at` e sem resultados.
- `reconciliation_results`: o `run_id` 99 não existe na tabela de runs, e essas linhas têm `MATCHED` com valores diferentes e `difference = 0`. Também há transação repetida dentro do mesmo run com valores diferentes, e 16 linhas sem `merchant_id`.
- A mesma transação aparece em vários runs (até 4), porque as janelas de 7 dias se sobrepõem.
- CSV: 10 linhas duplicadas exatas, 22 valores no formato `R$ 32.245,91`, `settled_at` com `Z`, `+00:00` ou sem fuso.
- Valores monetários vêm como `double`. Compare sempre em `decimal(18,2)`: em double, 127 linhas `MATCHED` parecem violar a tolerância de R$ 0,01 só por erro de ponto flutuante. Em decimal, sobram 5, todas do `run_id` 99.
- No fixture, `reconciliation_results` não bate com `transactions` (95% divergem em merchant e valor); nos dados do gerador a divergência é zero. O resultado da reconciliação vale por si: não preencher `merchant_id` nulo a partir de `transactions`. A consistência entre fontes é check de alerta, não de bloqueio.

## Estilo da documentação

Escreva em português, primeira pessoa, como um engenheiro explicando uma escolha para um colega. Frases curtas, com números reais dos dados. Mais prosa e menos bullet. Tabela só quando estiver comparando coisas de verdade.

Evite "robusto", "escalável", "é importante destacar", "vale ressaltar", "em suma", emojis e negrito espalhado. Quando algo não for tratado, diga isso direto e diga o que faria em produção.

## O que você não decide sozinho

Proponha e espere minha aprovação antes de:

- mudar a modelagem (grão de tabela, fatos, dimensões);
- mudar um contrato de dados ou a severidade de um check;
- adicionar dependência nova ou serviço no Docker Compose;
- mudar qualquer item da seção "Decisões já tomadas".

Implementação dentro dessas decisões, testes e rascunhos de documentação você pode fazer direto. Eu reviso tudo antes do commit.
