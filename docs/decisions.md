# Decisões

As decisões da solução e o motivo de cada uma. Em cada item: o problema, o que escolhi, por quê, o que descartei e o que a escolha custa. Os números vêm dos dados de exemplo (o fixture) ou do gerador.

## Arquitetura

**Medallion sobre DuckLake, com transformações em dbt-duckdb.**
Bronze guarda as fontes como chegaram, silver resolve CDC, tipos e qualidade, e gold entrega as tabelas de cada consumidor. Cada camada tem uma responsabilidade só, o que deixa claro onde procurar quando um número está errado. O DuckLake guarda os dados em Parquet numa pasta local e acrescenta transação, snapshot e time travel. Com isso, idempotência, publicação atômica e "o que o relatório mostrava naquele dia" saem do próprio formato, sem código de controle meu. O dbt traz testes com severidade, lineage e documentação, e roda dentro do DuckDB, como o enunciado pede.
Descartei Parquet solto com troca atômica de diretório, porque atomicidade entre várias tabelas e histórico ficariam por minha conta. Descartei também Iceberg, cujo suporte de escrita no DuckDB ainda é limitado.
Custo: o DuckLake é um formato recente, e o catálogo num arquivo DuckDB aceita um escritor por vez. Em produção, o catálogo iria para Postgres, ou eu usaria Iceberg se a empresa já tiver.

**O pipeline não refaz a reconciliação.**
`reconciliation_results` é a fonte de verdade das categorias. O CSV de exemplo não reproduz os resultados: 488 de 510 linhas têm valor diferente do interno. Recalcular daria números que não batem com o que o serviço gravou e que Operações já usa.
O CSV entra como dado do processador: alimenta o volume liquidado do CFO, a trilha de auditoria e os checks cruzados.
Custo: se o serviço de reconciliação errar uma categoria, o pipeline publica o erro. Os checks de coerência entre categoria e valores e os checks cruzados existem para tornar isso visível.

**Sem orquestrador na entrega.**
A CLI recebe uma data de referência, é idempotente e devolve códigos de saída distintos para dado bloqueado (1) e erro técnico (2). É o contrato que um orquestrador precisa. Subir Airflow ou Dagster no compose deixaria o ambiente pesado para quem avalia sem mudar nenhum critério. O orquestrador aparece no desenho de produção, em `architecture.md`.

**O dbt roda em subprocesso.**
Dentro do mesmo processo Python, o dbt-duckdb mantém o catálogo aberto depois de terminar, e a próxima conexão do pipeline falha. Em subprocesso, cada execução começa limpa. O resultado de cada modelo e teste é lido do `run_results.json`. É também como um orquestrador chamaria o dbt.

**Versões fixadas.**
`duckdb`, `dbt-duckdb` e `dbt-core` têm faixa de versão fixada, inclusive o `dbt-core`, que vem como dependência indireta. Uma versão nova poderia mudar o build de quem clona o repositório. A solução foi validada com duckdb 1.5.6, dbt-core 1.12.5 e dbt-duckdb 1.11.0. A extensão `ducklake` é instalada no build da imagem, para o pipeline não depender de rede em runtime.

**Limite de memória explícito no DuckDB.**
Sem limite, o DuckDB usa até 80% da RAM da máquina. No benchmark de 5 milhões de transações, o dbt chegou a 6,6 GB de 7,7 GB disponíveis. O padrão agora é 4 GB (`DUCKDB_MEMORY_LIMIT`). Acima disso, o DuckDB despeja em disco em vez de crescer. Com 2 GB, o build de 5 milhões ainda termina, em 107 s em vez de 62 s. O limite e a pasta temporária são definidos na criação do banco, não por cursor, porque o DuckDB não aceita trocar a pasta temporária depois de usá-la.

**`docker compose up` já roda o pipeline.**
O enunciado pede que a solução rode com `docker compose up`. O container sobe, roda `pipeline run` no fixture e continua de pé para os comandos. Como a execução é idempotente, subir de novo não duplica nada. Se o pipeline falhar, o container continua de pé para investigar.

## Bronze

**Cópia fiel, só append.**
O bronze não transforma nenhum valor: o CSV entra como texto, inclusive `"R$ 32.245,91"`. Cada linha ganha metadados de ingestão: lote, arquivo, hash do arquivo, número da linha e momento da carga. Assim, qualquer número da gold pode ser rastreado até a linha do arquivo de onde veio, e qualquer regra da silver pode ser reprocessada a partir do original.

**Idempotência em dois níveis.**
Um arquivo cujo hash já foi carregado é ignorado. No CDC isso não basta, porque as extrações são acumuladas: uma extração nova traz eventos antigos junto. Por isso cada evento tem um hash e entra uma vez só.
A identidade do evento é calculada sobre as colunas declaradas no contrato, em ordem fixa e com nulo para as ausentes, e não sobre as colunas presentes no arquivo. O motivo é o schema drift: o lote 2 trouxe `payment_method`. Se o hash usasse as colunas do arquivo, uma extração com essa coluna mudaria a identidade dos 742 eventos do lote 1, e eles entrariam de novo. Há um teste exatamente com esse caso.
O CSV não tem dedupe por linha no bronze: a duplicata é do processador e fica guardada como chegou. Quem trata é a silver.

**Contrato de entrada: bloquear ou alertar.**
Cada fonte tem um contrato YAML com colunas, tipos e o que é obrigatório. Coluna obrigatória ausente ou tipo diferente bloqueia o arquivo: carregar mesmo assim produziria número errado mais adiante. Coluna nova (drift) entra na tabela e gera alerta: não quebra nada do que existe e pode ser útil. O arquivo e o seu registro no log de ingestão são gravados na mesma transação, então uma falha no meio não deixa nada pela metade.

**CSV sem data no nome exige `--reference-date`.**
A data de referência sai do nome do arquivo (`settlement_2025-03-16.csv`). O arquivo do fixture não tem data no nome, e a CLI exige a data em vez de deduzir pelo conteúdo. Deduzir pela maior data de liquidação funcionaria no fixture e erraria no primeiro arquivo com uma liquidação tardia.

## Silver

**CDC: ordem por `_timestamp` e, no empate, D > U > I.**
A transação `acc62b77` tem um U e um D com o mesmo `_timestamp`. Ordenando só pelo tempo, o resultado depende do acaso, e a transação apagada pode continuar ativa (996 transações ativas em vez de 995). No empate, a operação mais "final" vence. O hash do evento entra como último critério, só para o resultado ser determinístico.

**Histórico, além do estado atual.**
Transações e merchants têm uma tabela com todas as versões, com a janela de validade de cada uma, além do estado atual. Compliance precisa responder "como estava esta transação no dia X", e o cadastro de merchant muda: 12 merchants passaram de Ltda para S.A. durante o mês.

**Dinheiro sempre em `decimal(18,2)`.**
Os valores chegam como `double`. Em double, 127 resultados `MATCHED` parecem passar da tolerância de R$ 0,01 só por arredondamento (`38272.15 - 38272.14` vira `0.0100000000002`). Em decimal sobram 5, todos de fato inconsistentes.

**Nada é descartado em silêncio.**
Cada fonte passa por um modelo `*_checked` que marca os problemas de cada linha. As linhas sem problema seguem, e as outras vão para `silver.quarantine` com os motivos e a linha original em JSON. O modelo `*_checked` é uma tabela, e não um modelo ephemeral, porque tanto a quarentena quanto o modelo final leem dele. Como ephemeral, a resolução de CDC rodaria duas vezes: no benchmark de 5 milhões, o build passa de 62 s para 91 s.

**Quarentena acima de 5% de um arquivo bloqueia a publicação.**
Poucas linhas ruins são problema de dado e geram alerta. Muitas indicam um arquivo quebrado, e aí é melhor não publicar um número que parece certo. A proporção é calculada por lote de ingestão (um arquivo), e não sobre o histórico. Sobre o histórico, um dia 100% quebrado depois de 30 dias normais seria 3% do total e passaria. Há um teste com esse caso: 20 linhas ruins num arquivo novo, contra 500 boas no histórico, bloqueiam.
Um lote bloqueado continua bloqueando até a fonte ser corrigida, porque aceitar um arquivo ruim é decisão de alguém, não do pipeline.
Custo: o valor de 5% foi escolhido sem histórico. O fixture fica em 1%, e o bug simulado nos testes em 8%. Com dados reais, eu calibraria por fonte.

**Resultado regravado no mesmo run: guarda todos, vale o mais recente.**
A mesma transação aparece mais de uma vez no mesmo run (44 linhas no fixture). Todas ficam na silver, e `is_effective` marca a mais recente, que é a que entra nas métricas. O próprio serviço conta essas linhas: o run 1 declara 119 resultados e tem 119 linhas, de 106 transações distintas. Descartar esconderia de Compliance algo que o serviço fez.
No CSV, a mesma transação com conteúdo diferente vai inteira para a quarentena, sem escolher uma das linhas. A diferença está na ordem: resultados têm `created_at`, uma ordem real de escrita, e duas linhas do mesmo arquivo não têm nenhum sinal de qual é a correta.

**`merchant_id` nulo em resultado continua nulo e gera alerta.**
São 16 linhas. Daria para preenchê-las a partir de `transactions`, mas no fixture resultados e transações divergem em 95% dos merchants, e preencher misturaria fontes que não batem. Mandar para a quarentena tiraria resultados válidos das métricas. Na gold, essas linhas aparecem no merchant "não identificado", visíveis. Nos dados do gerador a divergência é zero; em produção, eu preencheria com um check de consistência, se a divergência medida fosse baixa.

**Moeda sem conversão.**
O README diz que os valores são em BRL, mas o gerador produz USD e EUR. A moeda informada é mantida, e uma moeda fora dessas três gera alerta. Sem taxa de câmbio, valores de moedas diferentes nunca são somados.

**Reenvio do arquivo de uma data substitui a versão anterior.**
Se o processador manda de novo o arquivo de uma data, a silver usa a versão carregada por último. As duas versões continuam no bronze.
Custo: se o processador mandar só as linhas corrigidas, o resto do dia some da silver. Está nos limites do README.

**Checks cruzados entre fontes, como alerta.**
Dois checks comparam cada resultado com as outras fontes. O lado interno é comparado com `transactions`: a transação existe? Valor e merchant batem? O lado do processador é comparado com o arquivo do mesmo dia. São alertas e não bloqueios, porque o resultado da reconciliação é a fonte de verdade, e uma divergência é sinal para investigar o serviço, não motivo para esconder números. No fixture, eles acusam 763 e 91 resultados divergentes; nos dados do gerador, zero.

**Liquidações num formato comum.**
`silver.settlements` junta os processadores num formato só, com uma coluna `processor`. Cada processador tem um modelo que traduz o arquivo dele. A gold lê só `settlements` e não conhece nenhum processador pelo nome. Um processador novo é um contrato, dois modelos pequenos e uma linha em dois arquivos de configuração. `tests/test_extensibility.py` faz isso com um processador fictício de schema diferente.

**Silver reconstruída a cada execução.**
É o jeito mais simples de ser idempotente, e cabe com folga no volume atual: 62 s para 5 milhões de transações.
Custo: o tempo cresce com o histórico, não com o dia. Com 5 milhões por dia, isso deixa de caber em uma execução diária. O caminho incremental está em `scale.md`.

## Gold

**As perguntas vêm antes das tabelas.**
Cada tabela existe para responder perguntas de um consumidor, e `data-products.md` lista quais ela responde e quais deixei de fora de propósito. Cada pergunta tem uma query de exemplo em `docs/queries/`, testada automaticamente.

**Duas visões da discrepância.**
As janelas de 7 dias se sobrepõem, então a mesma transação passa por até 4 runs. Somar discrepâncias de todos os runs contaria o mesmo problema várias vezes e incluiria o que já se resolveu. `agg_run_health` mostra o que cada run disse, e `fct_transaction_reconciliation_status` mostra o que continua aberto e há quantos dias. No fixture, 73 transações estão abertas e 29 tiveram discrepância e se resolveram num run seguinte.

**Pico de discrepância por teste estatístico.**
Um run é marcado quando a taxa fica mais de 3 desvios padrão acima da taxa dos até 7 runs anteriores, com o desvio calculado pelo número de resultados do run (teste z para proporção). Um limiar fixo em pontos percentuais se comporta mal nos dois extremos. Com 90 resultados por run, como no fixture, a variação natural de uma taxa de 13% é de uns 3,5 p.p., então 2 p.p. a mais é ruído. Com 5 milhões, a variação natural é de 0,02 p.p., e uma piora real de 0,5 p.p. já importa. No fixture, o maior z é 0,46.
Custo: a regra olha só a taxa. Um run com poucas discrepâncias de valor muito alto não dispara. Um segundo gatilho por valor fica como evolução.

**Moeda do resultado recuperada, não suposta.**
Os resultados de reconciliação não trazem moeda. A moeda vem da transação interna e, quando ela não existe (`UNRECONCILED_PROCESSOR`), do arquivo do processador. BRL só em último caso, e `currency_source` registra de onde veio. Supor BRL para tudo somaria dólar e euro como real: nos dados do gerador, cerca de 5% do valor em risco está em outra moeda. `agg_run_health` soma só BRL (`amount_at_risk_brl`) e conta à parte as discrepâncias em outras moedas; o detalhe por moeda fica no fato.
Isso preenche um campo a partir de outra fonte, o que não faço com o merchant. A diferença está no risco. Moeda errada só aparece se as fontes discordarem, e a coluna de origem deixa isso auditável. Merchant errado atribuiria uma discrepância à empresa errada.

**Fato incremental por data de referência.**
`fct_reconciliation_result` é a tabela que cresce com o volume (1,8 bilhão de linhas por ano na projeção), então ela é `delete+insert` por `reference_date`. Com `--reference-date`, só aquela data é apagada e regravada; o log de mudanças do DuckLake confirma que reprocessar 16/03 toca só as 93 linhas daquela data. As outras tabelas da gold são agregações pequenas e são reconstruídas a cada execução.

**"Hoje" é a data do run mais recente nos dados.**
Idade de pendência e "em aberto" são calculados em relação ao último run, não ao relógio. Um relatório reproduzido daqui a um mês dá o mesmo número.

**Merchant na versão da época.**
Os fatos apontam para a versão do cadastro válida na data do evento. Um relatório de 04/03 mostra a razão social de 04/03, e não a de hoje. A primeira versão de cada merchant vale desde sempre, com a data real em `registered_at`, para um fato anterior ao primeiro evento do cadastro ainda encontrar o merchant. Merchant nulo ou fora do cadastro aponta para o membro `-1`, "não identificado".

**Contrato na saída.**
As seis tabelas da gold têm nome e tipo de cada coluna fixados em `_gold.yml` (`contract: enforced` do dbt). Um modelo que mude o formato faz o build falhar, e nada é publicado. Mudar o que os consumidores recebem tem que ser uma decisão, não um efeito colateral. Há um teste que renomeia uma coluna e verifica isso.

## Operação

**Publicação em duas etapas (write-audit-publish).**
O dbt grava e testa tudo num schema de build (`gold_build`). Só se o build inteiro passar, o pipeline aponta as views de `gold` para o snapshot que acabou de ser testado, numa única transação. Os consumidores leem só `gold`. Se um check bloqueante falha, as views continuam no snapshot anterior, e os consumidores seguem vendo a última versão boa. Não há cópia de dados, e voltar uma publicação é apontar as views para um snapshot anterior.
Isso é necessário porque o `dbt build` sozinho não protege: ele substitui cada tabela antes de testá-la e só pula os modelos que dependem do teste que falhou. Há um teste que simula um bug no serviço e verifica que os consumidores continuam vendo a publicação anterior.

**Toda execução é registrada.**
`ops.pipeline_runs` ganha uma linha no início (`running`) e é atualizada no fim, inclusive quando a execução quebra. Uma execução que morreu no meio fica como `running`, o que já é uma pista. `ops.dq_results` guarda o resultado de cada modelo e teste por execução, e `ops.publications` guarda cada snapshot publicado e quando. `pipeline status` junta tudo num painel, que é o primeiro comando a rodar quando algo parece errado.

## Pontos em aberto

O que conheço e deixei como está, com o motivo.

**`--reference-date` tem dois papéis.** É a data do CSV que não tem data no nome e também a partição que o fato da gold reprocessa. Se uma extração CDC trouxer mudanças em resultados de outra data, elas chegam à silver, mas só entram no fato num build sem data (ou com aquela data). O check `assert_gold_is_up_to_date_with_silver` avisa quando isso acontece. Em produção, o orquestrador saberia quais datas cada lote tocou e reprocessaria todas.

**Um arquivo fora do contrato bloqueia as execuções seguintes até alguém agir.** É coerente com não publicar com uma fonte desatualizada, mas não existe um jeito de dizer "aceito este lote com ressalva" ou "ignore este arquivo". Hoje a saída é corrigir a fonte ou tirar o arquivo da pasta. Em produção, eu teria uma tabela de lotes aceitos ou descartados, com quem decidiu e por quê.

**A silver não passa pela publicação em duas etapas.** Só a gold é publicada por snapshot. A query de Compliance sobre a quarentena e o painel de status leem a silver ao vivo, então podem mostrar o resultado de uma execução que foi bloqueada. Para o painel isso é desejável, porque mostra o que barrou. Para a query de Compliance, o certo seria publicar a quarentena junto com a gold.

**Sem integração contínua.** Os testes rodam no container com um comando, mas nada os roda a cada push. Um workflow que faça `docker compose build` e `pytest` seria o primeiro passo em produção, junto com o benchmark semanal.
