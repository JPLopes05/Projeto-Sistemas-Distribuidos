# Sistema Distribuído de Mensagens - Parte 1

Projeto da disciplina de Sistemas Distribuídos.

Integrantes:

Gabriel Koiama

João Pedro Lopes

## O que esta entrega implementa

Esta primeira entrega cobre:
- login de usuários (bots)
- listagem de canais
- criação de canais
- persistência em disco em cada servidor
- troca de mensagens com ZeroMQ
- mensagens serializadas em binário com MessagePack
- execução automatizada com Docker Compose
- uso de 2 linguagens: **Python** e **JavaScript (Node.js)**

## Escolhas do projeto

### Serialização
Foi utilizado **MessagePack**, pois:
- é binário
- é simples de usar em Python e Node.js
- atende ao requisito do enunciado

### Comunicação
Foi utilizado **ZeroMQ** com a seguinte arquitetura:
- **Broker** em Python
- **Clientes** conectam no broker usando `REQ`
- **Servidores** conectam no broker usando `DEALER`
- O broker usa `ROUTER` para falar com clientes e servidores

### Como o broker trabalha
- `LOGIN` e `CREATE_CHANNEL` são enviados para **todos os servidores**
- `LIST_CHANNELS` é enviado para **um servidor**, em round-robin

Essa estratégia faz com que:
- cada servidor mantenha seu próprio arquivo em disco
- todos os servidores tenham os mesmos usuários e canais
- qualquer cliente possa consultar qualquer servidor e obter os mesmos canais

### Persistência
Cada servidor salva um arquivo `state.json` no volume próprio.
Cada arquivo contém:
- `users`
- `logins`
- `channels`

Os volumes **não são compartilhados** entre servidores.

## Estrutura

```text
sd-chat-part1/
├── broker/
│   ├── broker.py
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

## Como executar

```bash
docker compose up --build
```

## O que sobe no compose

- 1 broker
- 2 servidores Python
- 2 servidores Node.js
- 2 clientes Python
- 2 clientes Node.js

## Regras adotadas pelo grupo

### Login
- nome deve ter de 3 a 20 caracteres
- caracteres permitidos: letras, números, `_` e `-`
- usuário duplicado gera erro

### Canal
- nome deve ter de 3 a 30 caracteres
- caracteres permitidos: letras, números, `_` e `-`
- canal duplicado gera erro

## Observações

- Todas as mensagens possuem `timestamp`
- Todas as mensagens exibidas em tela mostram o conteúdo completo decodificado
- Os bots executam automaticamente sem interação manual
- Em caso de erro no login, o cliente tenta novamente com um sufixo numérico
