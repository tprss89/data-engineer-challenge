# Produtos de dados

Comecei pelas perguntas de cada consumidor e só depois cheguei às tabelas. Para cada um, descrevo o que o modelo responde, o que deixei de fora de propósito e por quê. Todas as queries citadas estão em [`docs/queries/`](queries/) e rodam contra o fixture:

```bash
docker compose exec pipeline pipeline query docs/queries/ops_01_run_health.sql
```

Um teste automatizado (`tests/test_gold.py`) roda todas elas a cada mudança, então a documentação não envelhece sem ninguém perceber.

## Antes das tabelas: duas coisas que mudam a leitura dos números

**A mesma transação aparece em vários runs.** A reconciliação olha uma janela de 7 dias e, no fixture, roda a cada 3 dias. Então uma transação passa por até 4 runs. Uma discrepância no run de 04/03 pode ter se resolvido no de 07/03. Se Operações somasse as discrepâncias de todos os runs, contaria o mesmo problema várias vezes e incluiria problema que já se resolveu. Por isso existem duas visões: o que cada run disse (`agg_run_health`) e o que está aberto agora (`fct_transaction_reconciliation_status`).

**"Hoje" é a data do run mais recente nos dados, não o relógio.** Idade de pendência e "em aberto" são calculados em relação ao último run. Assim um relatório reproduzido daqui a um mês dá o mesmo número.

## Operações

A dor: saber se a reconciliação está saudável e se a taxa de discrepância está subindo.

| Pergunta | Onde |
|---|---|
| Os runs estão rodando e terminando? Quanto tempo levam? | `ops_01_run_health.sql` |
| A taxa de discrepância está acima do normal? | `ops_01_run_health.sql` (`variacao_pp`, `health_status`) |
| Chegaram todos os resultados que o run declarou? | `ops_01_run_health.sql` (`resultados_faltando`) |
| Houve dia sem run? | `ops_01_run_health.sql` (`dias_desde_run_anterior`) |
| O que está pendente de verdade, e há quanto tempo? | `ops_02_open_backlog.sql` |
| Quais merchants concentram as pendências? | `ops_03_merchants_at_risk.sql` |

`health_status` resume cada run numa palavra, em ordem de gravidade: `FAILED`, `IN_PROGRESS`, `INCOMPLETE` (chegou menos resultado do que o declarado), `DISCREPANCY_SPIKE` e `OK`. No fixture, o run de 22/03 aparece como `FAILED`, com 116 resultados declarados e nenhum recebido. É o tipo de coisa que hoje ninguém vê.

"Taxa subindo" é decidido por um teste estatístico, não por um limiar fixo. A linha de base é a taxa dos até 7 runs anteriores, e o run vira `DISCREPANCY_SPIKE` quando fica mais de 3 desvios padrão acima dela. O desvio depende de quantos resultados o run tem: com 90 resultados, 2 pontos percentuais a mais é ruído; com 5 milhões, meio ponto já é uma mudança real. A coluna `discrepancy_z_score` mostra o valor, e no fixture o maior é 0,46.

**O que não respondo:**

- *Por que* uma transação ficou discrepante. Os dados não trazem causa; isso é investigação caso a caso, e a trilha de auditoria ajuda.
- Alerta em tempo real. O pipeline é diário, como o arquivo do processador.
- SLA de resolução por analista. Não há dado de quem tratou cada pendência.

## CFO

A dor: volume transacionado por dia.

| Pergunta | Onde |
|---|---|
| Quanto foi transacionado por dia, e quanto disso concluiu, ficou pendente ou falhou? | `cfo_01_daily_volume.sql` |
| Quanto o processador liquidou, já descontados os estornos? | `cfo_01_daily_volume.sql` (`liquido`) |
| De quais segmentos vem o volume? | `cfo_02_volume_by_segment.sql` |
| Quanto dinheiro está em discrepância? | `cfo_03_money_at_risk.sql` |

"Volume" tem duas leituras, e as duas estão lado a lado: o transacionado (sistema interno, pela data de criação) e o liquidado (PaySettler, pela data de liquidação). Uma diferença persistente entre as duas é um sinal por si só. No fixture só existe o arquivo do processador de 16/03, por isso o liquidado aparece só nesse dia.

Segmento é a divisão do CNAE (dois primeiros dígitos). Usei a versão do cadastro válida na data da transação.

**O que não respondo:**

- Volume consolidado em reais quando há outras moedas. O gerador produz USD e EUR, e não temos taxa de câmbio. Prefiro mostrar cada moeda separada a somar errado.
- Receita, taxa e margem. Não existe esse dado nas fontes.
- Visão por hora ou em fuso de Brasília. Tudo está em UTC, como o glossário define. Para o CFO, um dia em America/Sao_Paulo seria mais natural; deixo registrado como evolução.

## Compliance

A dor: rastreabilidade e histórico para auditoria.

| Pergunta | Onde |
|---|---|
| O que aconteceu com esta transação, e de onde veio cada informação? | `compliance_01_transaction_trail.sql` |
| Com qual razão social o merchant estava cadastrado na época? | `compliance_02_merchant_as_of.sql` |
| O que um relatório mostrava numa versão anterior? | `compliance_03_time_travel.sql` |
| Quais registros foram barrados, por quê, e de qual arquivo? | `compliance_04_quarantine_evidence.sql` |

`audit_transaction_trail` junta numa linha do tempo cada versão do registro interno, cada resultado de reconciliação (inclusive os regravados e os barrados) e cada liquidação. Cada evento aponta para o arquivo, a linha e o hash SHA-256 do arquivo recebido. O hash é o que permite provar que um dado veio de um arquivo específico e que o arquivo não mudou.

O histórico do merchant é SCD tipo 2: no fixture, 12 merchants passaram de Ltda para S.A. durante o mês, e um relatório de 04/03 mostra o nome de 04/03.

Para "o que o relatório mostrava", não construí nada: o DuckLake guarda um snapshot a cada escrita, e qualquer tabela pode ser consultada como estava numa versão ou num instante.

**O que não respondo:**

- Quem acessou ou consultou os dados. Isso é log de acesso da plataforma, não do pipeline.
- Retenção e expurgo (LGPD). Hoje os snapshots crescem sem limite; em produção precisaria de política de expiração alinhada com o jurídico.
- Assinatura dos arquivos pelo processador. O hash prova que o arquivo não mudou depois de recebido, não quem o enviou.

## As tabelas

| Tabela | Grão | Consumidor |
|---|---|---|
| `gold.fct_reconciliation_result` | um resultado efetivo por run e transação | base para Operações e Compliance |
| `gold.agg_run_health` | um run | Operações |
| `gold.fct_transaction_reconciliation_status` | uma transação | Operações |
| `gold.fct_daily_volume` | dia, merchant e moeda | CFO |
| `gold.audit_transaction_trail` | um evento de uma transação | Compliance |
| `gold.dim_merchant` | uma versão do cadastro de um merchant | todos |

Cada tabela tem contrato: nome e tipo de cada coluna estão fixados em `dbt/models/gold/_gold.yml`, e um modelo que mude o formato faz o build falhar sem publicar.

As tabelas de `gold` são views publicadas pelo pipeline. O dbt constrói e testa tudo em `gold_build`, e só quando o build inteiro passa as views passam a apontar para o snapshot novo. Se um check bloqueante falha, quem consome continua vendo a última versão boa.

`fct_reconciliation_result` é incremental por data de referência: `pipeline run --reference-date 2025-03-16` apaga e regrava só aquela data. As outras são reconstruídas a cada execução, porque são agregações pequenas sobre ela.

`amount_at_risk` é o dinheiro que uma discrepância envolve: a diferença, no `MISMATCHED`, e o valor inteiro do lado que existe, nos `UNRECONCILED`. Os resultados não trazem moeda, então ela vem da transação interna ou, sem ela, do arquivo do processador (`currency_source` diz de onde). `agg_run_health` soma só BRL; outras moedas ficam no fato, por moeda.

Deixei de fora uma dimensão de datas e uma camada semântica. O DuckDB resolve datas com funções nativas, e para o tamanho deste problema as queries de exemplo cumprem o papel da camada semântica. Num ambiente com várias ferramentas de BI consumindo, eu colocaria as métricas num lugar só (dbt semantic layer ou similar) para "taxa de discrepância" ter uma única definição.
