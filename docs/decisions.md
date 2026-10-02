# Decisões

Registro das decisões do projeto e do motivo de cada uma. As marcadas como **provisória** foram tomadas durante a construção e vão ser revistas quando o pipeline inteiro estiver rodando e testado.

## Fundação

**Medallion com DuckLake e dbt-duckdb.** Bronze guarda as fontes como chegaram, silver aplica CDC, tipos e qualidade, gold entrega as tabelas por consumidor. O DuckLake dá transação, snapshot e time travel sobre arquivos Parquet numa pasta local, o que atende ao "filesystem local" do enunciado sem eu ter que implementar atomicidade na mão. Validei antes de começar (bloco 0): build repetido não duplica, falha no meio faz rollback completo e o incremental por data substitui só a data reprocessada. O risco é ser um formato recente; em produção eu avaliaria Iceberg ou DuckLake com catálogo em Postgres.

**O pipeline não refaz a reconciliação.** `reconciliation_results` é a fonte de verdade das categorias. O CSV de exemplo não reproduz os resultados (488 de 510 linhas têm valor diferente do interno), então recalcular daria números que não batem com o que o serviço gravou. O CSV entra como dado bruto do processador, para auditoria e checks cruzados.

**Sem Airflow na entrega.** A CLI recebe uma data de referência e é idempotente, que é o contrato que um orquestrador precisa. Subir Airflow no compose deixaria o ambiente pesado sem mudar nenhum critério de avaliação. O orquestrador aparece no desenho de arquitetura.

**O dbt roda em subprocesso.** Dentro do mesmo processo Python o dbt-duckdb deixava o catálogo aberto e a conexão seguinte falhava. Subprocesso também é como um orquestrador chamaria.

**Write-audit-publish com views fixadas em snapshot.** O dbt grava e testa tudo em `gold_build`. Só se o build inteiro passar, o pipeline aponta as views de `gold` para o snapshot recém-testado, numa transação. Se um check bloqueante falha, as views continuam no snapshot anterior. Não há cópia de dados, e voltar uma publicação é apontar as views para um snapshot anterior. Cheguei nisso porque a primeira versão não protegia de verdade: o `dbt build` substitui cada tabela antes de testá-la e só pula o que depende do teste que falhou, então o fato da gold era atualizado mesmo com a quarentena acima do limite. Hoje há um teste que simula um bug no serviço e verifica que os consumidores continuam vendo a publicação anterior.

**Toda execução é registrada.** `ops.pipeline_runs` ganha uma linha no início (`running`) e é atualizada no fim, inclusive quando a execução quebra. Uma execução que morreu no meio fica como `running`, o que já é um sinal. `ops.dq_results` guarda o resultado de cada modelo e teste por execução, e `ops.publications`, cada snapshot publicado. `pipeline status` junta tudo num painel.

**Limite de memória explícito no DuckDB (padrão de 4 GB).** Sem limite, o DuckDB usa até 80% da RAM; no benchmark de 5M chegou a 6,6 GB. Com o limite, ele despeja em disco. O limite e a pasta temporária são definidos na criação do banco, não por cursor, porque o DuckDB não deixa trocar a pasta temporária depois de usá-la (só apareceu no volume de 5M).

**Modelos `*_checked` materializados como tabela, não ephemeral.** Como ephemeral, a resolução de CDC rodava duas vezes (tabela final e quarentena). Materializar reduziu o build de 5M de 91 s para 62 s.

**Silver reconstruída inteira a cada execução.** É simples e idempotente, e cabe com folga em 5M por mês (62 s). Não escala para 5M por dia, porque o custo cresce com o histórico. O caminho está em `docs/scale.md`.

## CDC

**Desempate de eventos: `_timestamp` primeiro, depois a operação, com D > U > I.** A transação `acc62b77...` tem U e D no mesmo timestamp. Ordenando só pelo timestamp, ela continuava ativa por acaso.

## Qualidade de dados (silver)

**Valores monetários sempre em `decimal(18,2)`.** Em double, 127 resultados `MATCHED` pareciam passar da tolerância de R$ 0,01 por erro de ponto flutuante. Em decimal, sobram 5, e são todos registros inconsistentes de verdade.

**Registro com problema vai para `silver.quarantine`, nunca some.** Cada linha rejeitada fica com o motivo e o registro original em JSON.

**Provisória: quarentena acima de 5% de uma fonte bloqueia a publicação.** Abaixo disso, publica e alerta. A ideia é que poucas linhas ruins são problema de dado, e muitas indicam uma fonte quebrada, quando é melhor não publicar número que parece certo. O valor de 5% foi escolhido sem base histórica; precisa ser calibrado com dados reais.

**Provisória: resultados regravados no mesmo run ficam guardados.** A mesma transação aparece mais de uma vez no mesmo run (44 linhas no fixture). Todas ficam em `silver.reconciliation_results`, e `is_effective` marca a mais recente, que é a que vale nas métricas. Pensei em Compliance: descartar esconderia que o serviço regravou.

**Provisória: `merchant_id` nulo em resultados fica nulo e só gera alerta.** Seriam 16 linhas recuperáveis via `transactions`, mas no fixture resultados e transações divergem em 95% dos merchants. Preencher a partir de outra fonte misturaria dados que não batem.

**Provisória: moeda aceita BRL, USD e EUR, sem conversão.** O README diz que `amount` é em BRL, mas o gerador produz USD e EUR. Mantenho a moeda informada; moeda fora dessas três gera alerta. Somar valores de moedas diferentes fica proibido na gold.

## Gold

**Perguntas antes das tabelas.** O desenho por consumidor, com o que cada tabela responde e o que fica de fora, está em `docs/data-products.md`.

**Fato incremental por data de referência; o resto reconstruído.** `fct_reconciliation_result` é o que cresce com o volume (1,8 bilhão de linhas por ano na projeção), então ele é `delete+insert` por `reference_date`. Na primeira execução, ou sem `--reference-date`, processa tudo. As agregações são pequenas e reconstruídas a cada execução. Testado: reprocessar 16/03 apaga e reinsere só as 93 linhas daquela data (`ducklake_table_changes`).

**Duas visões da discrepância.** O que cada run disse (`agg_run_health`) e o que continua aberto (`fct_transaction_reconciliation_status`). Como a mesma transação passa por até 4 runs, somar discrepâncias de todos os runs contaria o mesmo problema várias vezes.

**"Hoje" é o run mais recente nos dados.** Idade de pendência e "em aberto" não dependem do relógio, então um relatório reproduzido depois dá o mesmo número.

**Merchant na versão da época.** Os fatos apontam para a versão do cadastro válida na data do evento. A primeira versão vale desde sempre (com a data real em `registered_at`), para um fato anterior ao primeiro evento de CDC ainda achar o merchant. Merchant nulo ou fora do cadastro aponta para o membro `-1`.

**Provisória: pico de discrepância = taxa 2 p.p. acima da média dos 3 runs anteriores.** Escolhido olhando a variação do fixture (11% a 15%). Com histórico real, eu calibraria por desvio padrão.

**Provisória: valores de reconciliação tratados como BRL.** Os resultados não trazem moeda. O README diz que os valores internos são em reais.

## Bronze

**Idempotência em dois níveis.** Arquivo com hash já carregado é ignorado. No CDC, cada evento entra uma vez (hash do conteúdo), porque as extrações são acumuladas e uma nova traz eventos antigos junto.

**CSV sem data no nome exige `--reference-date`.** A CLI não tenta adivinhar a data pelo conteúdo.

**Reenvio do arquivo de uma data substitui a versão anterior na silver.** As duas versões continuam no bronze.
