# RabbitMqStudy

Projeto de estudo focado em mensageria com RabbitMQ, .NET e Docker.

O objetivo deste projeto é colocar em prática conceitos importantes de comunicação assíncrona entre aplicações, como Producer, Consumer, Exchanges, Queues, Routing Keys, ACK/NACK, Retry, Dead Letter Queue, Publisher Confirms, Prefetch e Idempotência.

---

## Tecnologias utilizadas

- .NET 10
- C#
- RabbitMQ
- Docker
- Docker Compose
- Entity Framework Core
- SQLite

---

## Arquitetura

O projeto possui três aplicações principais:

```text
RabbitMqStudy
│
├── RabbitMq.Contracts
│
├── RabbitMq.Producer
│
└── RabbitMq.Consumer
```

O fluxo principal acontece da seguinte forma:

```text
Producer
   ↓
orders.exchange
   ↓
Routing Key: order.created
   ↓
orders.created
   ↓
Consumer
```

O Producer publica um evento de pedido criado na Exchange.

A Exchange utiliza a Routing Key para encaminhar a mensagem para a Queue correta.

O Consumer recebe a mensagem e realiza o processamento.

---

## Estrutura do projeto

```text
RabbitMqStudy
│
├── src
│   │
│   ├── RabbitMq.Contracts
│   │   └── OrderCreatedMessage.cs
│   │
│   ├── RabbitMq.Producer
│   │   ├── Configuration
│   │   │   └── RabbitMqSettings.cs
│   │   │
│   │   ├── Messaging
│   │   │   └── OrderCreatedPublisher.cs
│   │   │
│   │   └── Program.cs
│   │
│   └── RabbitMq.Consumer
│       ├── Configuration
│       │   └── RabbitMqSettings.cs
│       │
│       ├── Data
│       │   ├── ConsumerDbContext.cs
│       │   └── ProcessedMessage.cs
│       │
│       ├── Messaging
│       │   ├── RabbitMqTopology.cs
│       │   └── OrderCreatedConsumer.cs
│       │
│       └── Program.cs
│
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

# RabbitMq.Contracts

O projeto `RabbitMq.Contracts` contém os contratos compartilhados entre Producer e Consumer.

Atualmente existe o contrato:

```text
OrderCreatedMessage
```

Ele representa um evento de criação de pedido.

Exemplo:

```csharp
public record OrderCreatedMessage(
    Guid MessageId,
    Guid Id,
    string Customer,
    decimal Total,
    DateTime CreatedAt
);
```

O `MessageId` identifica a mensagem.

O `Id` identifica o pedido.

Essa separação permite identificar mensagens duplicadas mesmo que estejam relacionadas ao mesmo pedido.

---

# RabbitMq.Producer

O Producer é responsável por criar e publicar mensagens no RabbitMQ.

O fluxo é:

```text
OrderCreatedMessage
      ↓
Serialização JSON
      ↓
Conversão para bytes
      ↓
orders.exchange
      ↓
Routing Key: order.created
```

O Producer utiliza:

- Direct Exchange
- Routing Key
- mensagens persistentes
- Publisher Confirms
- `mandatory: true`

---

## Publisher Confirms

O Producer utiliza Publisher Confirms para saber se o RabbitMQ confirmou a publicação.

Fluxo:

```text
Producer
   ↓
publica mensagem
   ↓
RabbitMQ
   ↓
Publisher Confirm
   ↓
Producer sabe que a publicação foi confirmada
```

Isso aumenta a confiabilidade da publicação.

---

## Mandatory Routing

As mensagens são publicadas utilizando:

```csharp
mandatory: true
```

Isso permite detectar situações onde o RabbitMQ recebe a mensagem, mas não encontra nenhuma Queue compatível com a Routing Key.

Exemplo:

```text
orders.exchange
      ↓
order.created.invalid
      ↓
nenhuma Queue encontrada
      ↓
NO_ROUTE
```

Nesse caso, o Producer consegue tratar o erro.

---

# RabbitMq.Consumer

O Consumer é responsável por receber e processar mensagens da Queue:

```text
orders.created
```

Ele possui:

- ACK manual
- NACK
- Prefetch
- Retry
- TTL
- Dead Letter Queue
- Idempotência persistente

---

# ACK

Quando uma mensagem é processada corretamente, o Consumer envia um ACK.

```text
RabbitMQ
   ↓
Consumer
   ↓
processamento com sucesso
   ↓
ACK
```

O ACK informa ao RabbitMQ:

> A mensagem foi processada corretamente e pode ser removida da Queue.

---

# NACK

Quando uma mensagem não pode ser processada e o limite de retries foi atingido, o Consumer envia um NACK.

```text
Consumer
   ↓
erro
   ↓
NACK
```

Utilizando:

```csharp
requeue: false
```

a mensagem não volta para a Queue principal.

Ela é encaminhada para a Dead Letter Exchange.

---

# Retry

O projeto possui um mecanismo de retry utilizando:

- Retry Exchange
- Retry Queue
- TTL
- Header de contagem de tentativas

O fluxo é:

```text
orders.created
      ↓
Consumer
      ↓
erro
      ↓
orders.retry.exchange
      ↓
order.created.retry
      ↓
orders.created.retry
      ↓
TTL de 5 segundos
      ↓
orders.exchange
      ↓
order.created
      ↓
orders.created
      ↓
Consumer tenta novamente
```

Cada mensagem pode ser reenviada até:

```text
3 retries
```

Além da tentativa original.

Portanto:

```text
Tentativa original
      ↓
Retry 1
      ↓
Retry 2
      ↓
Retry 3
      ↓
DLQ
```

---

# TTL

A Retry Queue utiliza TTL:

```text
5000 ms
```

ou seja:

```text
5 segundos
```

A mensagem fica aguardando na Retry Queue durante esse período.

Depois do TTL, ela é encaminhada novamente para a Exchange principal.

---

# Dead Letter Queue

Caso uma mensagem continue falhando após o limite de retries, ela é enviada para a Dead Letter Queue.

Fluxo:

```text
orders.created
      ↓
Consumer
      ↓
limite de retries atingido
      ↓
NACK
      ↓
orders.dlx
      ↓
order.created.dead
      ↓
orders.created.dlq
```

A Queue:

```text
orders.created.dlq
```

armazena mensagens que não puderam ser processadas.

Isso evita que mensagens problemáticas fiquem retornando infinitamente para a Queue principal.

---

# Prefetch

O Consumer utiliza:

```csharp
prefetchCount: 1
```

Isso significa que o RabbitMQ entrega no máximo uma mensagem ainda não confirmada para o Consumer por vez.

Fluxo:

```text
Mensagem 1
   ↓
Consumer
   ↓
processamento
   ↓
ACK
   ↓
Mensagem 2
```

Isso ajuda a controlar a carga sobre o Consumer.

Também melhora a distribuição das mensagens quando existem vários Consumers.

---

# Idempotência

Sistemas baseados em mensageria precisam considerar que uma mensagem pode ser entregue novamente.

Por exemplo:

```text
Cobrar R$ 200
```

Se a mesma mensagem fosse processada duas vezes:

```text
1ª vez → R$ 200
2ª vez → R$ 200
```

o cliente poderia ser cobrado duas vezes.

Para reduzir esse risco, cada mensagem possui:

```text
MessageId
```

Antes de processar uma mensagem, o Consumer consulta o banco.

Fluxo:

```text
Mensagem chega
      ↓
consulta MessageId
      ↓
já foi processada?
   ├── Sim
   │    ↓
   │  ignora
   │    ↓
   │   ACK
   │
   └── Não
        ↓
      processa
        ↓
salva MessageId
        ↓
       ACK
```

---

# Idempotência persistente

Inicialmente, a idempotência poderia ser implementada usando um:

```csharp
HashSet<Guid>
```

Porém, isso possui uma limitação:

```text
Consumer fecha
      ↓
memória é perdida
      ↓
lista de mensagens processadas desaparece
```

Por isso o projeto utiliza SQLite.

Existe uma tabela:

```text
ProcessedMessages
```

que armazena:

```text
MessageId
ProcessedAt
```

Exemplo:

```text
ProcessedMessages

MessageId                              ProcessedAt
-------------------------------------------------------------
8a73...                                2026-09-22 18:10
991c...                                2026-09-22 18:11
```

Dessa forma, mesmo que o Consumer seja reiniciado, ele continua sabendo quais mensagens já foram processadas.

---

# Topologia RabbitMQ

O projeto possui três Exchanges principais:

```text
orders.exchange
orders.retry.exchange
orders.dlx
```

## Exchange principal

```text
orders.exchange
      ↓
order.created
      ↓
orders.created
```

Responsável pelo fluxo normal de mensagens.

---

## Retry Exchange

```text
orders.retry.exchange
      ↓
order.created.retry
      ↓
orders.created.retry
```

Responsável por encaminhar mensagens para a fila de retry.

Depois do TTL:

```text
orders.created.retry
      ↓
orders.exchange
      ↓
order.created
      ↓
orders.created
```

---

## Dead Letter Exchange

```text
orders.dlx
      ↓
order.created.dead
      ↓
orders.created.dlq
```

Responsável por encaminhar mensagens que não puderam ser processadas.

---

# Fluxo completo

```text
                        ┌──────── SUCESSO ────────┐
                        │                         ↓
Producer            orders.created           Consumer
   ↓                    ↑                         ↓
orders.exchange         │                        ACK
   ↓                    │
order.created ──────────┘

                         ERRO
                          ↓
                orders.retry.exchange
                          ↓
                orders.created.retry
                          ↓
                     espera 5s
                          ↓
                   orders.exchange
                          ↓
                   orders.created
                          ↓
                       Consumer
                          ↓
                    tenta novamente

Depois de 3 retries:

                       Consumer
                          ↓
                         NACK
                          ↓
                      orders.dlx
                          ↓
                  orders.created.dlq
```

---

# Docker

O RabbitMQ é executado através do Docker Compose.

O projeto utiliza a imagem com Management Plugin, permitindo acompanhar:

- Exchanges
- Queues
- Connections
- Channels
- mensagens
- Consumers
- bindings

---

# Como executar

## 1. Clonar o repositório

```bash
git clone https://github.com/devhenriquecastanheira/RabbitMqStudy.git
```

Entre na pasta:

```bash
cd RabbitMqStudy
```

---

## 2. Criar o arquivo `.env`

Existe um arquivo:

```text
.env.example
```

Crie:

```text
.env
```

Exemplo:

```env
RABBITMQ_DEFAULT_USER=rabbituser
RABBITMQ_DEFAULT_PASS=rabbitpass
```

O arquivo `.env` não deve ser enviado para o GitHub.

---

## 3. Subir o RabbitMQ

Execute:

```bash
docker compose up -d
```

Para verificar:

```bash
docker compose ps
```

---

## 4. RabbitMQ Management

O painel estará disponível em:

```text
http://localhost:15672
```

Utilize o usuário e senha configurados no `.env`.

---

## 5. Configurar variáveis de ambiente

Producer e Consumer precisam das variáveis:

```text
RABBITMQ_DEFAULT_USER
RABBITMQ_DEFAULT_PASS
```

Elas podem ser configuradas nas Run Configurations da IDE.

No Rider:

```text
Run
→ Edit Configurations
→ Environment Variables
```

Ou no PowerShell:

```powershell
$env:RABBITMQ_DEFAULT_USER="rabbituser"
$env:RABBITMQ_DEFAULT_PASS="rabbitpass"
```

---

## 6. Executar o Consumer

```bash
dotnet run --project src/RabbitMq.Consumer
```

Resultado esperado:

```text
Consumer iniciado.
Aguardando mensagens...
Pressione ENTER para encerrar.
```

---

## 7. Executar o Producer

Em outro terminal:

```bash
dotnet run --project src/RabbitMq.Producer
```

Exemplo:

```text
Mensagem confirmada pelo RabbitMQ!
{"MessageId":"...","Id":"...","Customer":"Henrique","Total":199.90,"CreatedAt":"..."}
```

---

## 8. Resultado no Consumer

```text
Pedido recebido:

MessageId: ...
Id: ...
Cliente: Henrique
Total: 199.90
Criado em: ...

ACK enviado.
```

---

# Encerrando o ambiente

Para parar o RabbitMQ:

```bash
docker compose down
```

O volume do RabbitMQ continuará salvo.

Para iniciar novamente:

```bash
docker compose up -d
```

Evite utilizar:

```bash
docker compose down -v
```

caso não queira remover os volumes do RabbitMQ.

---

# Conceitos estudados

Durante o desenvolvimento deste projeto foram estudados:

- Message Broker
- Mensageria
- Comunicação assíncrona
- Producer
- Consumer
- Queue
- Exchange
- Direct Exchange
- Routing Key
- Binding
- Durable Queue
- Persistent Messages
- ACK
- NACK
- Manual Acknowledgement
- Retry
- TTL
- Dead Letter Exchange
- Dead Letter Queue
- Prefetch
- Publisher Confirms
- Mandatory Routing
- NO_ROUTE
- Contratos compartilhados
- Serialização JSON
- Idempotência
- Idempotência persistente
- Entity Framework Core
- SQLite
- Separação de responsabilidades

---

# O que aprendi

Este projeto foi desenvolvido com o objetivo de entender RabbitMQ de maneira prática.

Durante o desenvolvimento, pude entender melhor como sistemas podem se comunicar de forma assíncrona e desacoplada.

Também foram estudados problemas que aparecem em sistemas reais, como:

- mensagens duplicadas;
- falhas durante o processamento;
- mensagens que nunca conseguem ser processadas;
- retries infinitos;
- perda de mensagens;
- sobrecarga de Consumers;
- falhas de roteamento;
- reinicialização de aplicações.

A implementação de Retry, DLQ, Publisher Confirms e Idempotência ajudou a entender que utilizar mensageria envolve não apenas enviar mensagens, mas também pensar na confiabilidade de todo o fluxo.

---

# Objetivo do projeto

Este projeto faz parte dos meus estudos de desenvolvimento backend e arquitetura de sistemas distribuídos utilizando .NET.

O foco principal foi aprender os fundamentos do RabbitMQ e implementar na prática mecanismos de confiabilidade utilizados em aplicações reais.
