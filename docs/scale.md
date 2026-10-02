# Escala

O case pede para projetar para ~5M transações por mês hoje e se preparar para ~5M por dia em 18 meses (cerca de 1,8 bilhão de linhas por ano em `reconciliation_results`). Medi antes de opinar. Os números abaixo vêm de `scripts/benchmark.py`, que qualquer pessoa pode rodar de novo:

```bash
docker compose exec pipeline python scripts/benchmark.py --rows 5000000 --days 30
```

Máquina: Docker Desktop no Windows, 12 vCPUs e 7,7 GB de RAM para a VM.

## O que medi com 5 milhões de transações

5M de transações viram cerca de 14,5M de linhas somando todas as fontes (eventos CDC de transações, resultados de reconciliação e 45 arquivos diários do PaySettler), ou 1,2 GB de arquivos.

| Etapa | Tempo |
|---|---|
| Ingestão no bronze | 26 s |
| dbt build completo + publicação (67 modelos e testes) | 62 s |
| Segunda execução, sem dados novos | 73 s |
| Reprocessar uma única data | 70 s |

Memória: com o limite do DuckDB em 4 GB, o pico foi de 4,9 GB no dbt e 3,3 GB na ingestão. Com o limite em 2 GB, o build completo também passou, em 107 s, porque o DuckDB despeja em disco em vez de crescer.

Os modelos mais lentos foram `audit_transaction_trail` (17 s, porque é a maior tabela, com uma linha por evento) e os modelos da silver que resolvem CDC (de 4 a 7 s cada).

Conclusão para a premissa atual: **5M transações por mês cabem com folga**. O mês inteiro processa em uns 90 segundos num notebook.

## O que o volume revela

Algumas características da solução só aparecem com volume real. O fixture não chega perto delas.

**Memória.** Por padrão, o DuckDB usa até 80% da RAM da máquina. Sem limite, o build de 5 milhões chega a 6,6 GB de 7,7 GB disponíveis, o que é arriscado numa máquina compartilhada. Por isso o limite é explícito (`DUCKDB_MEMORY_LIMIT`, padrão de 4 GB), com uma pasta para o DuckDB despejar em disco quando passar dele. Com 2 GB, o build ainda termina, em 107 s. O limite e a pasta são definidos na criação do banco, e não por cursor, porque o DuckDB não aceita trocar a pasta temporária depois de usá-la. Isso só se manifesta quando o volume é grande o bastante para usar disco.

**Resolução de CDC calculada uma vez.** Os modelos `*_checked` da silver são tabelas. Como ephemeral, o dbt copiaria o modelo para dentro de cada consumidor, e a resolução de CDC rodaria duas vezes: uma para a tabela final e outra para a quarentena. Com 5 milhões, isso é a diferença entre 62 s e 91 s de build.

**Reprocessar uma data custa quase o mesmo que o build completo.** O fato da gold é incremental e reprocessa uma data em menos de 2 s. Mas a silver é reconstruída inteira a cada execução, e é ela que domina o tempo. Esse é o principal limite do desenho e o assunto da próxima seção.

**Snapshots ocupam espaço.** Depois de alguns builds, o lake ocupa 11,5 GB para 1,2 GB de fontes, porque cada reconstrução guarda a versão anterior das tabelas como snapshot, e é isso que dá o time travel. Expirar os snapshots anteriores à última publicação leva o lake para 4,8 GB em menos de um segundo, e a gold publicada continua respondendo.

## Onde o desenho quebra com 5M por dia

5M por dia é o volume deste benchmark chegando todo dia. Processar o dia novo não é o problema: um dia de 5M processa em cerca de um minuto. O problema é tudo o que hoje é proporcional ao histórico inteiro, e não ao dia. Com um ano de dados, o histórico é 365 vezes este benchmark.

Em ordem de onde quebra primeiro:

**1. A deduplicação de eventos CDC no bronze.** Cada evento novo é comparado com o hash de todos os eventos que já estão no bronze. Hoje isso é barato. Com um ano de histórico, são bilhões de hashes, e a comparação deixa de caber em memória. A correção é usar a posição no log do CDC (o LSN do Postgres ou o offset do Kafka) como chave de idempotência e guardar só a última posição processada por fonte. O hash do conteúdo continuaria só para o CSV, que não tem posição.

**2. A silver reconstruída do zero a cada execução.** É simples e idempotente por construção, e foi a escolha certa para o volume atual. Só que o tempo cresce com o histórico: hoje é um minuto, e com um ano de 5M por dia passaria de horas. A correção é tornar a silver incremental: processar só os eventos que entraram no bronze desde a última execução (marca d'água pelo `_batch_id` do `ops.ingestion_log`) e fazer merge pela chave. As tabelas de histórico (SCD2) também passam a só acrescentar versões.

**3. A trilha de auditoria.** É a maior tabela (uma linha por evento, cerca de 3 vezes o número de transações) e também é reconstruída inteira. Ela é só append por natureza, então vira incremental por data do evento sem dificuldade.

**4. O catálogo num arquivo local.** O catálogo do DuckLake hoje é um arquivo DuckDB: um escritor por vez, e um leitor com conexão aberta (o DBeaver de alguém, por exemplo) já trava a escrita. Isso aconteceu várias vezes durante o desenvolvimento. Em produção, o catálogo vai para Postgres, que o DuckLake suporta, e os dados vão para object storage.

**5. O armazenamento sem retenção.** Sem expirar snapshots, o lake cresce cerca de 7 vezes o volume das fontes. Precisa de uma rotina diária: expirar snapshots mais antigos que a janela de retenção (definida com Compliance), nunca o snapshot publicado, e compactar arquivos pequenos (`ducklake_merge_adjacent_files`).

O que **não** quebra: o DuckDB processando o volume de um dia, que já está medido; o tamanho do arquivo diário do processador; a lógica dos modelos. As regras de negócio não mudam com o volume. Muda só quanto do histórico cada execução relê.

## O plano para os 18 meses

Na ordem em que eu faria, cada passo medido com o gerador antes de ir para produção:

1. **Retenção de snapshots e compactação.** É barato, já está testado neste benchmark e tira o risco mais imediato (disco).
2. **Idempotência por posição do CDC no bronze**, no lugar do hash contra o histórico inteiro.
3. **Silver e trilha de auditoria incrementais**, com marca d'água por lote de ingestão. É a mudança maior, e a que faz o custo de uma execução depender do dia e não do histórico.
4. **Particionar as tabelas grandes por mês** (`fct_reconciliation_result`, `audit_transaction_trail`, os históricos da silver), para consultas e reprocessamentos lerem só as partições que importam.
5. **Catálogo em Postgres e dados em object storage**, que é pré-requisito de produção de qualquer jeito.
6. **Benchmark no CI**, rodando toda semana com volume de produção, para uma regressão de desempenho aparecer antes do usuário.

Com isso, uma máquina de 16 a 32 GB processa um dia de 5M com folga. Eu só sairia de uma máquina (para Spark ou Trino sobre Iceberg) se uma execução diária, já incremental, deixasse de caber na janela noturna, ou se várias equipes precisassem escrever nas mesmas tabelas ao mesmo tempo. Não espero chegar nisso dentro de 18 meses.
