# Decisões

Registro das decisões do projeto e do motivo de cada uma. Algumas foram tomadas como provisórias durante a construção e revistas depois que o pipeline inteiro estava rodando e medido. Essa revisão mudou três delas, e o motivo de cada mudança está registrado junto.

## Fundação

**Medallion com DuckLake e dbt-duckdb.** Bronze guarda as fontes como chegaram, silver aplica CDC, tipos e qualidade, gold entrega as tabelas por consumidor. O DuckLake dá transação, snapshot e time travel sobre arquivos Parquet numa pasta local, o que atende ao "filesystem local" do enunciado sem eu ter que implementar atomicidade na mão. Validei antes de começar (bloco 0): build repetido não duplica, falha no meio faz rollback completo e o incremental por data substitui só a data reprocessada. O risco é ser um formato recente; em produção eu avaliaria Iceberg ou DuckLake com catálogo em Postgres.

**O pipeline não refaz a reconciliação.** `reconciliation_results` é a fonte de verdade das categorias. O CSV de exemplo não reproduz os resultados (488 de 510 linhas têm valor diferente do interno), então recalcular daria números que não batem com o que o serviço gravou. O CSV entra como dado bruto do processador, para auditoria e checks cruzados.

**Sem Airflow na entrega.** A CLI recebe uma data de referência e é idempotente, que é o contrato que um orquestrador precisa. Subir Airflow no compose deixaria o ambiente pesado sem mudar nenhum critério de avaliação. O orquestrador aparece no desenho de arquitetura.

**O dbt roda em subprocesso.** Dentro do mesmo processo Python o dbt-duckdb deixava o catálogo aberto e a conexão seguinte falhava. Subprocesso também é como um orquestrador chamaria.

**Write-audit-publish com views fixadas em snapshot.** O dbt grava e testa tudo em `gold_build`. Só se o build inteiro passar, o pipeline aponta as views de `gold` para o snapshot recém-testado, numa transação. Se um check bloqueante falha, as views continuam no snapshot anterior. Não há cópia de dados, e voltar uma publicação é apontar as views para um snapshot anterior. Cheguei nisso porque a primeira versão não protegia de verdade: o `dbt build` substitui cada tabela antes de testá-la e só pula o que depende do teste que falhou, então o fato da gold era atualizado mesmo com a quarentena acima do limite. Hoje há um teste que simula um bug no serviço e verifica que os consumidores continuam vendo a publicação anterior.

**Toda execução é registrada.** `ops.pipeline_runs` ganha uma linha no início (`running`) e é atualizada no fim, inclusive quando a execução quebra. Uma execução que morreu no meio fica como `running`, o que já é um sinal. `ops.dq_results` guarda o resultado de cada modelo e teste por execução, e `ops.publications`, cada snapshot publicado. `pipeline status` junta tudo num painel.

**Limite de memória explícito no DuckDB (padrão de 4 GB).** Sem limite, o DuckDB usa até 80% da RAM; no benchmark de 5M chegou a 6,6 GB. Com o limite, ele despeja em disco. O limite e a pasta temporária são definidos na criação do banco, não por cursor, porque o DuckDB não deixa trocar a pasta temporária depois de usá-la (só apareceu no volume de 5M).

**Modelos `*_checked` materializados como tabela, não ephemeral.** Como ephemeral, a resolução de CDC rodava duas vezes (tabela final e quarentena). Materializar reduziu o build de 5M de 91 s para 62 s.

**`docker compose up` já roda o pipeline.** O enunciado diz que a solução deve rodar com `docker compose up`. O container sobe, roda `pipeline run` no fixture e continua de pé para os comandos. Como a execução é idempotente, subir de novo não duplica nada. Se o pipeline falhar, o container continua de pé para investigar.

**Contrato também na saída.** Os contratos YAML protegem a entrada. Na saída, as seis tabelas da gold usam o contrato do dbt (`contract: enforced`): nome e tipo de cada coluna ficam fixados em `_gold.yml`, e um modelo que mude o formato faz o build falhar sem publicar. Há um teste que renomeia uma coluna numa cópia do projeto e verifica exatamente isso.

**Versões fixadas.** `duckdb`, `dbt-duckdb` e também o `dbt-core`, que vem como dependência indireta: um `dbt-core` novo poderia mudar o build de quem clona o repositório. Tudo foi validado com duckdb 1.5.6, dbt-core 1.12.5 e dbt-duckdb 1.11.0.

**Silver reconstruída inteira a cada execução.** É simples e idempotente, e cabe com folga em 5M por mês (62 s). Não escala para 5M por dia, porque o custo cresce com o histórico. O caminho está em `docs/scale.md`.

## CDC

**Desempate de eventos: `_timestamp` primeiro, depois a operação, com D > U > I.** A transação `acc62b77...` tem U e D no mesmo timestamp. Ordenando só pelo timestamp, ela continuava ativa por acaso.

**A identidade de um evento vem do contrato, não do arquivo.** O hash que deduplica eventos no bronze usa as colunas declaradas no contrato, na ordem do contrato, com nulo para as ausentes. A primeira versão usava as colunas presentes no arquivo, e a revisão do código achou o problema: uma extração nova com a coluna que surgiu no lote 2 (`payment_method`, vazia) mudava o hash de eventos antigos, e os 742 eventos do lote 1 entravam de novo. É exatamente o schema drift que o gerador simula. Há um teste de regressão com esse caso.

## Qualidade de dados (silver)

**Valores monetários sempre em `decimal(18,2)`.** Em double, 127 resultados `MATCHED` pareciam passar da tolerância de R$ 0,01 por erro de ponto flutuante. Em decimal, sobram 5, e são todos registros inconsistentes de verdade.

**Checks cruzados entre fontes, como alerta.** Dois checks comparam cada resultado efetivo com as outras fontes: o lado interno com `transactions` (a transação existe? valor e merchant batem?) e o lado do processador com o arquivo do mesmo dia. São `warn`, não `error`: o resultado da reconciliação é a fonte de verdade, e uma divergência é sinal para investigar o serviço, não motivo para esconder números. No fixture eles acusam 763 e 91 resultados divergentes, que é a inconsistência que a exploração encontrou; nos dados do gerador, zero.

**Registro com problema vai para `silver.quarantine`, nunca some.** Cada linha rejeitada fica com o motivo e o registro original em JSON.

**Quarentena acima de 5% de um arquivo bloqueia a publicação.** Abaixo disso, publica e alerta. Poucas linhas ruins são problema de dado; muitas indicam um arquivo quebrado, e aí é melhor não publicar número que parece certo. A primeira versão calculava a proporção sobre o histórico inteiro, e a revisão mostrou o defeito: com 30 dias acumulados, um dia 100% quebrado seria só 3% do total e passaria. Hoje a proporção é por lote de ingestão (um arquivo), e há um teste com exatamente esse caso (20 linhas ruins sobre 500 boas, bloqueado). Um lote bloqueado continua bloqueando até a fonte ser corrigida, porque aceitar um arquivo ruim é decisão de alguém. Em produção, eu teria uma lista de lotes aceitos com ressalva. Os 5% foram escolhidos sem histórico e precisam ser calibrados por fonte.

**Resultados regravados no mesmo run ficam guardados, e vale o mais recente.** A mesma transação aparece mais de uma vez no mesmo run (44 linhas no fixture). Todas ficam em `silver.reconciliation_results`, e `is_effective` marca a mais recente, que é a que vale nas métricas. O próprio serviço conta essas linhas: o run 1 declara 119 resultados e tem 119 linhas, de 106 transações distintas. Descartar esconderia algo que o serviço fez. Isso parece contradizer o CSV, onde a mesma transação com conteúdo diferente bloqueia as duas linhas. A diferença é que os resultados têm `created_at`, uma ordem real de escrita, e duas linhas do mesmo CSV não têm nenhum sinal de qual é a correta.

**`merchant_id` nulo em resultados fica nulo e só gera alerta.** Seriam 16 linhas recuperáveis via `transactions`, mas no fixture resultados e transações divergem em 95% dos merchants. Preencher a partir de outra fonte misturaria dados que não batem, e mandar para a quarentena tiraria das métricas resultados válidos. Na gold, essas linhas aparecem no merchant "não identificado". Nos dados do gerador a divergência é zero; em produção, eu preencheria a partir de `transactions` com um check de consistência, se a divergência medida fosse baixa.

**Moeda aceita BRL, USD e EUR, sem conversão.** O README diz que `amount` é em BRL, mas o gerador produz USD e EUR. Mantenho a moeda informada; moeda fora dessas três gera alerta. Sem taxa de câmbio, valores de moedas diferentes nunca são somados.

## Gold

**Perguntas antes das tabelas.** O desenho por consumidor, com o que cada tabela responde e o que fica de fora, está em `docs/data-products.md`.

**Fato incremental por data de referência; o resto reconstruído.** `fct_reconciliation_result` é o que cresce com o volume (1,8 bilhão de linhas por ano na projeção), então ele é `delete+insert` por `reference_date`. Na primeira execução, ou sem `--reference-date`, processa tudo. As agregações são pequenas e reconstruídas a cada execução. Testado: reprocessar 16/03 apaga e reinsere só as 93 linhas daquela data (`ducklake_table_changes`).

**Duas visões da discrepância.** O que cada run disse (`agg_run_health`) e o que continua aberto (`fct_transaction_reconciliation_status`). Como a mesma transação passa por até 4 runs, somar discrepâncias de todos os runs contaria o mesmo problema várias vezes.

**"Hoje" é o run mais recente nos dados.** Idade de pendência e "em aberto" não dependem do relógio, então um relatório reproduzido depois dá o mesmo número.

**Merchant na versão da época.** Os fatos apontam para a versão do cadastro válida na data do evento. A primeira versão vale desde sempre (com a data real em `registered_at`), para um fato anterior ao primeiro evento de CDC ainda achar o merchant. Merchant nulo ou fora do cadastro aponta para o membro `-1`.

**Pico de discrepância por teste estatístico, não por limiar fixo.** A primeira regra era "taxa 2 p.p. acima da média dos 3 runs anteriores". A revisão mostrou que um limiar fixo em p.p. se comporta de forma oposta conforme o tamanho do run. Com 90 resultados (fixture), a variação natural de uma taxa de 13% é de uns 3,5 p.p., então 2 p.p. é ruído. Com 5 milhões, a variação natural é de 0,02 p.p., e uma piora real de 0,5 p.p. passaria batida. Hoje a linha de base é a taxa agregada dos até 7 runs anteriores, e o run é marcado quando fica mais de 3 desvios padrão acima dela (teste z para proporção, que considera o número de resultados). No fixture, o maior z é 0,46; o pico simulado no teste passa com folga de 3. A regra olha só a taxa: um run com poucas discrepâncias de valor muito alto não dispara. Um segundo gatilho por valor fica como evolução.

**Moeda dos resultados de reconciliação recuperada, e não suposta.** Os resultados não trazem moeda, e a primeira versão tratava tudo como BRL. A revisão mediu o efeito nos dados do gerador: cerca de 5% do valor em risco era USD ou EUR somado como real, o que contradizia a regra de nunca somar moedas. Hoje a moeda vem da transação interna e, sem ela (UNRECONCILED_PROCESSOR), do arquivo do processador. Só em último caso assume BRL, e `currency_source` registra de onde veio. No gerador, 100% das moedas vêm de uma das duas fontes. `agg_run_health` soma só BRL (`amount_at_risk_brl`) e conta à parte as discrepâncias em outras moedas; o detalhe por moeda fica no fato. Isso parece contradizer a decisão do merchant nulo, que também seria preencher de outra fonte. A diferença está no risco: moeda errada só aparece se as fontes discordarem, e a coluna de origem deixa isso auditável; merchant errado atribuiria uma discrepância à empresa errada.

**Liquidações num formato comum (`silver.settlements`).** Cada processador tem um modelo que traduz o arquivo dele para o formato comum, e a gold lê só `settlements`, sem conhecer nenhum processador pelo nome. Plugar um processador novo é um contrato, dois modelos pequenos e uma linha em dois arquivos de configuração. `tests/test_extensibility.py` faz isso com um processador fictício de schema diferente e verifica que ele aparece da silver até a gold.

## Bronze

**Idempotência em dois níveis.** Arquivo com hash já carregado é ignorado. No CDC, cada evento entra uma vez (hash do conteúdo), porque as extrações são acumuladas e uma nova traz eventos antigos junto.

**CSV sem data no nome exige `--reference-date`.** A CLI não tenta adivinhar a data pelo conteúdo.

**Reenvio do arquivo de uma data substitui a versão anterior na silver.** As duas versões continuam no bronze.

## Pontos em aberto

Coisas que conheço e deixei como estão, com o motivo.

**`--reference-date` tem dois papéis.** É a data do CSV que não tem data no nome e também a partição que o fato da gold reprocessa. Se uma extração CDC trouxer mudanças em resultados de outra data, elas chegam à silver mas só entram no fato num build sem data (ou com aquela data). O check `assert_gold_is_up_to_date_with_silver` avisa quando isso acontece. Em produção, o orquestrador saberia quais datas cada lote tocou e reprocessaria todas.

**Um arquivo fora do contrato bloqueia as execuções seguintes até alguém agir.** É coerente com não publicar com uma fonte desatualizada, mas não existe um jeito de dizer "aceito este lote com ressalva" ou "ignore este arquivo". Hoje a saída é corrigir a fonte ou tirar o arquivo da pasta. Em produção, eu teria uma tabela de lotes aceitos ou descartados, com quem decidiu e por quê.

**A silver não passa pelo write-audit-publish.** Só a gold é publicada por snapshot. A query de Compliance sobre a quarentena e o painel de status leem a silver ao vivo, então podem mostrar o resultado de uma execução que foi bloqueada. Para o painel isso é desejável (mostra o que barrou); para a query de Compliance, o certo seria publicar a quarentena junto com a gold.

**Sem integração contínua.** Os testes rodam no container com um comando, mas nada os roda a cada push. Um workflow que faça `docker compose build` e `pytest` seria o primeiro passo em produção, junto com o benchmark semanal.

