# Sistema Distribuído de Mensagens - Partes 1, 2, 3 e 4

Projeto da disciplina de Sistemas Distribuídos.

Integrantes:

Gabriel Koiama

João Pedro Lopes

## O que esta entrega implementa

O projeto implementa um sistema distribuído de mensagens com:

- login de usuários (bots)
- listagem de canais
- criação de canais
- publicação de mensagens em canais via Pub/Sub
- inscrição de bots em múltiplos canais usando uma única conexão SUB
- persistência em disco em cada servidor
- troca de mensagens com ZeroMQ
- mensagens serializadas em binário com MessagePack
- execução automatizada com Docker Compose
- uso de 2 linguagens: **Python** e **JavaScript (Node.js)**
- serviço de referência para ranking e heartbeat dos servidores
- relógio lógico nos bots e servidores
- eleição de coordenador entre servidores
- sincronização de relógio físico baseada no algoritmo de Berkeley
- tolerância à queda do coordenador

## Escolhas do projeto

### Serialização

Foi utilizado **MessagePack**, pois:

- é binário
- é simples de usar em Python e Node.js
- atende ao requisito do enunciado

### Comunicação

Foi utilizada a seguinte arquitetura com ZeroMQ:

- **Broker Req/Rep** em Python para o fluxo de requisições e respostas
- **Proxy Pub/Sub** em Python com **XSUB (5557)** e **XPUB (5558)**
- **Reference Service** em Python para rank, heartbeat e lista de servidores ativos
- **Clientes** usam `REQ` para falar com o broker e `SUB` para receber publicações
- **Servidores** usam `DEALER` para atender o broker e `PUB` para publicar no proxy Pub/Sub
- **Servidores** também se comunicam diretamente entre si para sincronização de relógio e eleição de coordenador

### Como o broker trabalha

- `LOGIN` e `CREATE_CHANNEL` são enviados para **todos os servidores**
- `LIST_CHANNELS` é enviado para **um servidor**, em round-robin
- `PUBLISH_MESSAGE` é enviado para **um servidor primário**, que publica no tópico correto
- depois da publicação, o broker envia `SYNC_PUBLICATION` para os demais servidores
- se um servidor não responde, o broker remove esse servidor da lista ativa e continua usando os demais

Essa estratégia faz com que:

- cada servidor mantenha seu próprio arquivo em disco
- todos os servidores tenham os mesmos usuários, canais e publicações
- qualquer cliente possa consultar qualquer servidor ativo
- o sistema continue funcionando mesmo se um servidor cair

### Persistência

Cada servidor salva um arquivo `state.json` no volume próprio.

Cada arquivo contém:

- `users`
- `logins`
- `channels`
- `requests`
- `publications`
- `heartbeats`
- `clock_syncs`
- `elections`

Os volumes **não são compartilhados** entre servidores.

---

# Parte 1 - Login, Listagem e Criação de Canais

A primeira parte implementa a base do sistema.

Ela cobre:

- login de usuários
- listagem de canais
- criação de canais
- persistência em disco
- broker com ZeroMQ
- clientes e servidores em Python e Node.js
- serialização binária com MessagePack
- execução com Docker Compose

As mensagens principais são:

```text
LOGIN
LIST_CHANNELS
CREATE_CHANNEL
```

Todas as mensagens possuem `timestamp`.

---

# Parte 2 - Pub/Sub e Mensagens em Canais

A segunda parte adiciona o envio e recebimento de mensagens via Pub/Sub.

Ela cobre:

- proxy Pub/Sub separado
- publicação de mensagens em canais
- inscrição dos bots em canais
- recebimento assíncrono de mensagens
- persistência das publicações
- sincronização das publicações entre servidores

As mensagens principais adicionadas são:

```text
PUBLISH_MESSAGE
SYNC_PUBLICATION
```

O fluxo principal é:

```text
Cliente -> Broker -> Servidor
Servidor -> PubSub Proxy -> Clientes inscritos
Broker -> Demais servidores para sincronização
```

---

# Parte 3 - Serviço de Referência, Rank, Heartbeat e Relógio Lógico

A terceira parte adiciona o serviço de referência e os relógios lógicos.

Ela cobre:

- criação do `reference_service`
- definição de rank para os servidores
- manutenção de lista de servidores ativos
- envio de heartbeat pelos servidores
- relógio lógico nos bots
- relógio lógico nos servidores
- relógio lógico nas mensagens trocadas

As mensagens principais do serviço de referência são:

```text
GET_RANK
LIST_SERVERS
HEARTBEAT
```

A ordem padrão de rank dos servidores é:

```text
js_server_1 -> rank 1
js_server_2 -> rank 2
py_server_1 -> rank 3
py_server_2 -> rank 4
```

O `HEARTBEAT` é enviado pelos servidores a cada 10 mensagens de clientes processadas.

---

# Parte 4 - Coordenador, Eleição e Berkeley

A quarta parte modifica a sincronização de relógio físico.

Agora, a hora correta não vem mais diretamente do `reference_service`. Um servidor é eleito como coordenador e passa a fornecer a hora para os demais servidores.

Ela cobre:

- variável local em cada servidor com o coordenador atual
- eleição de coordenador
- anúncio do coordenador no tópico `servers`
- sincronização de relógio físico com o coordenador
- algoritmo inspirado em Berkeley
- atualização do relógio a cada 15 mensagens trocadas
- detecção de coordenador indisponível
- eleição de novo coordenador após queda
- broker tolerante à queda de servidor

O coordenador inicial é o servidor vivo com menor rank.

Na execução padrão, o coordenador inicial é:

```text
js_server_1
```

pois ele possui rank 1.

Caso ele caia, o próximo coordenador esperado é:

```text
js_server_2
```

pois ele possui rank 2.

As principais mensagens internas são:

```text
CLOCK_REQUEST
SERVER_INTERNAL_REPLY
ELECTION_REQUEST
COORDINATOR_ANNOUNCEMENT
```

O fluxo de sincronização é:

```text
Servidor -> Coordenador: CLOCK_REQUEST
Coordenador -> Servidor: hora correta
Servidor ajusta seu relógio físico
```

O fluxo de eleição é:

```text
Servidor detecta falha do coordenador
Servidor inicia eleição
Servidores vivos respondem
Menor rank vivo é escolhido
Novo coordenador é publicado no tópico servers
Demais servidores atualizam o coordenador atual
```

---

## Funcionamento dos bots

Ao iniciar, cada bot:

1. efetua login
2. consulta a lista de canais
3. cria um canal próprio caso ainda existam menos de 5 canais
4. se inscreve aleatoriamente em canais disponíveis
5. entra em loop enviando mensagens para canais aleatórios

Cada bot exibe na tela:

- requisições enviadas
- respostas recebidas
- mensagens Pub/Sub recebidas
- timestamp de envio
- timestamp de recebimento
- relógio lógico

---

## Estrutura

```text
Projeto/
├── broker/
│   ├── broker.py
│   ├── Dockerfile
│   └── requirements.txt
├── pubsub_proxy/
│   ├── proxy.py
│   ├── Dockerfile
│   └── requirements.txt
├── reference_service/
│   ├── reference_service.py
│   ├── Dockerfile
│   └── requirements.txt
├── python/
│   ├── client/
│   │   ├── client.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── server/
│       ├── server.py
│       ├── Dockerfile
│       └── requirements.txt
├── node/
│   ├── client/
│   │   ├── client.js
│   │   ├── Dockerfile
│   │   └── package.json
│   └── server/
│       ├── server.js
│       ├── Dockerfile
│       └── package.json
├── data/
├── docker-compose.yml
└── README.md
```

---

## Como executar

Dentro da pasta `Projeto`, execute:

```bash
docker compose up --build
```

Para parar:

```bash
docker compose down
```

---

## Como executar limpando o estado anterior

```bash
docker compose down
sudo rm -rf data/py_server_1 data/py_server_2 data/js_server_1 data/js_server_2
mkdir -p data/py_server_1 data/py_server_2 data/js_server_1 data/js_server_2
docker compose up --build
```

---

## Como validar a queda do coordenador

Para testar a eleição de um novo coordenador, execute o projeto em segundo plano:

```bash
docker compose down
sudo rm -rf data/py_server_1 data/py_server_2 data/js_server_1 data/js_server_2
mkdir -p data/py_server_1 data/py_server_2 data/js_server_1 data/js_server_2
docker compose up --build -d
```

Espere o sistema estabilizar:

```bash
sleep 35
```

Derrube o coordenador inicial:

```bash
docker stop js_server_1
```

Espere a eleição acontecer:

```bash
sleep 75
```

Gere o log:

```bash
docker compose logs --no-color > execucao_queda_parte4.log
```

Para verificar a eleição:

```bash
grep -aE "COORDINATOR_UNAVAILABLE|ELECTION|COORDINATOR_ANNOUNCEMENT|BERKELEY_SYNC" execucao_queda_parte4.log
```

O comportamento esperado é:

- `js_server_1` fica indisponível
- os servidores detectam a queda do coordenador
- uma eleição é iniciada
- `js_server_2` é eleito como novo coordenador
- o novo coordenador é publicado no tópico `servers`
- os demais servidores passam a sincronizar com `js_server_2`

---

## O que sobe no compose

- 1 broker
- 1 proxy Pub/Sub
- 1 serviço de referência
- 2 servidores Python
- 2 servidores Node.js
- 2 clientes Python
- 2 clientes Node.js

---

## Regras adotadas pelo grupo

### Login

- nome deve ter de 3 a 20 caracteres
- caracteres permitidos: letras, números, `_` e `-`
- usuário duplicado gera erro

### Canal

- nome deve ter de 3 a 30 caracteres
- caracteres permitidos: letras, números, `_` e `-`
- canal duplicado gera erro

---

## Observações

- Todas as mensagens possuem `timestamp`
- As mensagens são serializadas em binário com MessagePack
- Os bots executam automaticamente sem interação manual
- Em caso de erro no login, o cliente tenta novamente com um sufixo numérico
- O `reference_service` não retorna mais hora no heartbeat na Parte 4
- A sincronização de relógio físico é feita pelo coordenador eleito
- O broker remove servidores que deixam de responder
- Arquivos de `data/`, logs e `__pycache__` são gerados durante execução e não precisam ser versionados
---

# Parte 5 - Consistência e Replicação

A quinta parte do projeto trata da consistência e da replicação dos dados armazenados nos servidores.

Como o broker faz balanceamento de carga entre os servidores usando round-robin, cada mensagem poderia ser processada inicialmente por um servidor diferente. Se cada servidor armazenasse apenas as mensagens que recebeu diretamente, o histórico ficaria dividido entre os servidores. Nesse caso, se um servidor parasse de funcionar, parte do histórico seria perdida, e uma consulta feita a um servidor específico poderia retornar apenas uma parte das mensagens.

Para resolver esse problema, foi adotada uma estratégia de **replicação primário-cópias com atualização eager**, coordenada pelo broker.

## Método escolhido

O método escolhido foi a replicação em que um servidor atua como primário para uma publicação específica, e os demais servidores recebem cópias dessa publicação logo em seguida.

O fluxo funciona assim:

```text
Cliente -> Broker -> Servidor primário
Servidor primário salva a publicação
Broker -> Demais servidores com SYNC_PUBLICATION
Demais servidores salvam a mesma publicação

Com isso, mesmo que o broker continue usando round-robin para balancear as requisições, todas as mensagens publicadas são copiadas para todos os servidores ativos.

Como foi implementado

Quando um cliente envia uma mensagem do tipo PUBLISH_MESSAGE, o broker escolhe um servidor primário usando round-robin. Esse servidor valida a mensagem, cria uma publicação com um identificador único chamado publication_id e salva essa publicação no seu state.json.

Depois disso, o broker recebe a publicação criada e envia uma mensagem SYNC_PUBLICATION para os demais servidores ativos. Cada servidor que recebe essa mensagem salva a mesma publicação em seu próprio state.json.

Assim, todos os servidores passam a possuir a mesma lista de publicações.

Controle de duplicidade

Para evitar mensagens duplicadas, cada publicação possui um campo:

publication_id

Antes de salvar uma publicação, o servidor verifica se já existe uma publicação com o mesmo publication_id. Se já existir, a publicação é ignorada. Se não existir, ela é salva.

Isso torna a replicação idempotente, ou seja, mesmo que uma mensagem de sincronização seja recebida mais de uma vez, ela não será duplicada no histórico.

Sincronização de estado entre servidores

Além da replicação feita no momento da publicação, os servidores também possuem uma sincronização de estado entre si.

Quando um servidor inicia, ele pode pedir um snapshot do estado de outros servidores usando:

STATE_SNAPSHOT_REQUEST

O servidor que recebe essa requisição responde com usuários, canais, logins e publicações armazenadas. O servidor solicitante mescla esses dados ao seu estado local, sem duplicar publicações.

Essa sincronização reforça a consistência do sistema, principalmente em situações em que um servidor sobe depois dos outros ou precisa recuperar dados que ainda não estavam no seu arquivo local.

Validação da replicação

A validação foi feita comparando os arquivos state.json dos quatro servidores:

data/js_server_1/state.json
data/js_server_2/state.json
data/py_server_1/state.json
data/py_server_2/state.json

O resultado esperado é que todos possuam a mesma quantidade de publicações e os mesmos publication_id.

Em teste realizado, os quatro servidores ficaram com a mesma quantidade de publicações e a comparação retornou:

RESULTADO FINAL: OK - todos os servidores possuem as mesmas publicações.

