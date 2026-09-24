# Observa — Estado atual comprovado do OrderFlow

> Inspeção realizada em 22 de setembro de 2026. Este documento separa implementação observada, documentação, resultados de comandos e propostas do briefing. O OrderFlow foi tratado como somente leitura.

## 1. Classificação usada

- **Fato verificado:** comprovado por código, configuração, teste ou comando inspecionado.
- **Informação do briefing:** afirmação de `03-sistema-distribuido-observabilidade.md` ainda não demonstrada no sistema.
- **Hipótese:** suposição que precisa de validação, com impacto e método de validação explícitos.
- **Proposta:** recomendação para o Observa ainda não materializada.
- **Decisão confirmada:** escolha autorizada para o Observa ou vigente e comprovada no OrderFlow.
- **Pendência bloqueante:** ausência que impede avançar com responsabilidade em uma etapa.

## 2. Materiais e verificações executadas

### Fontes inspecionadas

| Fonte | Estado | Uso na análise |
|---|---|---|
| `D:\Work\OrderFlow\README.md` e `CONTEXT.md` | Fato verificado | Jornada de pedido, vocabulário e limites declarados. |
| `D:\Work\OrderFlow\services\*` | Fato verificado | Handlers, consumidores, produtores, logs, métricas e transações. |
| `D:\Work\OrderFlow\packages\py-common\orderflow_messaging` | Fato verificado | Envelope, schemas, RabbitMQ, retry, DLQ e métricas Python. |
| `D:\Work\OrderFlow\schemas\*.json` | Fato verificado | Envelope e payloads versionados. |
| `D:\Work\OrderFlow\infra` e `docker-compose.yml` | Fato verificado | Postgres, RabbitMQ, bancos e execução local. |
| `D:\Work\OrderFlow\docs\adr` | Fato verificado como registro de decisão do OrderFlow | Decisões do projeto-base; não são decisões automáticas do Observa. |
| `D:\Work\OrderFlow\.github\workflows\test.yml` | Fato verificado | Alcance da CI. |
| Testes em `services/*/tests`, `*.spec.ts` e `tests/e2e` | Fato verificado como código de teste | Cobertura pretendida; execução é registrada separadamente. |
| `C:\Users\Esposo\Downloads\03-sistema-distribuido-observabilidade.md` | Informação do briefing | Intenção inicial para Kafka, Kubernetes e observabilidade. |
| `D:\Work\Observa\prompt-po-observa.md` | Decisão confirmada | Limites e entregáveis deste planejamento. |

### Comandos executados

| Comando | Resultado | Classificação |
|---|---|---|
| `git -c safe.directory=D:/Work/OrderFlow -C D:\Work\OrderFlow status --short --branch` | Inicialmente `main...origin/main` sem alterações; durante a sessão surgiram mudanças locais concorrentes descritas abaixo. | Fato verificado |
| `git ... log -1 --oneline` | `03abf27 feat: add DLQ replay, Prometheus metrics, and GitHub Actions CI`. | Fato verificado |
| `npm test -- --runTestsByPath src/payment.spec.ts --forceExit` no Payment | 1 suíte e 2 testes aprovados. | Fato verificado |
| `npx tsc -p tsconfig.json --noEmit` no Payment e Inventory | Ambos aprovados. | Fato verificado |
| `npm run build` no Payment e Inventory | Não verificável: o sandbox somente leitura impediu criar `dist/` (`EPERM`). O typecheck sem emissão foi usado como verificação permitida. | Limitação da inspeção |
| `python -m pytest ...` | Não executado: `pytest` não está instalado no Python disponível. | Limitação da inspeção |
| `docker version` | Cliente localizado, mas daemon/configuração Docker não acessíveis ao usuário do sandbox. | Limitação da inspeção |
| Testes Testcontainers e E2E Compose | Não executados porque dependem do Docker. | Não verificado |

### Estado concorrente do OrderFlow

O repositório estava limpo no snapshot inicial. Depois, sem ação deste trabalho, foram observadas alterações locais em `docker-compose.yml`, `services/order-service/app/main.py` e novos arquivos em `services/order-service/app/static/`. Elas ajustam o healthcheck do Postgres e adicionam uma UI estática; não mudam a mensageria analisada. Foram preservadas e não devem ser atribuídas ao Observa. Qualquer agente posterior deve capturar o estado inicial do OrderFlow e provar que o deixou exatamente igual, em vez de exigir que esteja limpo.

## 3. Arquitetura e jornada atuais

### Serviços e responsabilidades

| Serviço | Runtime | Responsabilidade comprovada | Evidência principal |
|---|---|---|---|
| Order Service | FastAPI/Python | Aceita pedido como `PENDING`, mantém estado e timeline, publica eventos e processa resultados da saga. | `services/order-service/app/service.py`, `routes.py`, `main.py` |
| Payment Service | NestJS/TypeScript | Decide aprovação/rejeição, persiste pagamento e executa compensação de reembolso. | `services/payment-service/src/payment.ts`, `app.module.ts` |
| Inventory Service | NestJS/TypeScript | Reserva estoque ou publica indisponibilidade. | `services/inventory-service/src/inventory.ts`, `app.module.ts` |
| Notification Service | FastAPI/Python | Consome eventos relevantes e registra uma notificação idempotente. | `services/notification-service/app/main.py`, `service.py` |

**Fato verificado:** os serviços de domínio não se chamam por HTTP; o fluxo é uma saga coreografada por eventos RabbitMQ. Um único Postgres hospeda quatro bancos separados, e nenhum serviço lê tabelas de outro serviço.

### Fluxos comprovados

1. **Caminho feliz:** `order.created` → `payment.approved` → `stock.reserved` → `order.completed`; o pedido termina `CONFIRMED`.
2. **Pagamento rejeitado:** `order.created` → `payment.rejected` → `order.failed`; o pedido termina `FAILED`.
3. **Estoque indisponível:** `payment.approved` → `stock.unavailable` → `payment.refund.requested` → `payment.refunded`; o pedido termina `CANCELLED`.
4. **Notification:** recebe cópias de eventos por binding próprio e registra o efeito uma vez. É um ramo da coreografia, não uma etapa síncrona que bloqueia a conclusão do pedido.

Evidências: `README.md`, `services/order-service/app/service.py`, `services/payment-service/src/payment.ts`, `services/inventory-service/src/inventory.ts`, `infra/rabbitmq/definitions.json` e `tests/e2e/test_compose_flow.py`.

### Contratos

**Fato verificado:** o envelope JSON v1 exige `event_id`, `event_type`, `version`, `correlation_id`, `occurred_at` e `payload`. `correlation_id` é sempre o `order_id`. Os payloads têm schemas JSON por tipo de evento e são validados nos dois runtimes.

**Lacuna:** não existe `trace_id`, `traceparent` nem `tracestate` no contrato ou nos metadados persistidos pela outbox. `correlation_id` não deve ser renomeado nem confundido com `trace_id`: o primeiro identifica a entidade de negócio; o segundo identifica uma execução observada.

### Persistência, outbox e idempotência

- **Fato verificado:** Order, Payment e Inventory gravam o efeito de negócio e o evento de saída em `outbox_events` na mesma transação.
- **Fato verificado:** cada consumidor tenta inserir `event_id` em `processed_events` na mesma transação do efeito; duplicata é ignorada e confirmada.
- **Fato verificado:** a publicação é pelo menos uma vez. O relay marca `published_at` somente após publicar.
- **Risco verificado por inspeção:** os relays consultam linhas não publicadas sem claim/lock exclusivo. Duas réplicas do mesmo serviço podem publicar a mesma linha. `processed_events` protege efeitos consumidores, mas não evita publicação, spans ou métricas duplicadas.
- **Consequência para o Observa:** o MVP usará uma réplica por serviço. Escala horizontal e HPA dependem de tornar o relay seguro para concorrência.

### Retry e DLQ

- **Fato verificado:** os consumidores RabbitMQ usam `MAX_RETRIES = 3`, header `x-retry` e DLQs por fila.
- **Fato verificado:** existe API administrativa para inspecionar e republicar mensagens de DLQ.
- **Achado inicial não reproduzido no snapshot final:** uma primeira leitura sugeriu colisão entre atributo e método `_dlq`. A reinspeção atual mostra o atributo `_dlx` e o método `_dlq(...)`, sem a colisão. O replay permanece **não verificado em runtime** porque o teste correspondente depende de pytest/Docker, mas não há defeito estático confirmado a registrar.

### Observabilidade existente

- **Instrumentação — parcial:** endpoints `/metrics` existem nos quatro serviços, com contadores de eventos, erros, pedidos, notificações e replay. Não foram encontrados histogramas de latência, exemplars ou métricas de lag.
- **Logs — parcial:** os quatro serviços produzem JSON e incluem `correlation_id` ao processar eventos.
- **Pipeline — ausente:** não há scrape Prometheus, retenção, OpenTelemetry Collector, backend de traces, Loki, Grafana ou alertas.
- **Experiência de diagnóstico — ausente:** não há navegação comprovada métrica → trace → log, propagação de contexto ou trace distribuída.

## 4. Análise de lacunas para o Observa

| Capacidade | Evidência no OrderFlow | Estado | Mudança necessária no Observa | Risco/dependência | Impacto no MVP |
|---|---|---|---|---|---|
| Jornada de negócio | Handlers, schemas e testes dos três desfechos | Existente | Preservar resultados e compensação | Regressão durante troca de broker | Obrigatório |
| Contrato de evento | Envelope e payloads v1 | Existente | Manter valor JSON; usar headers Kafka para telemetria | Compatibilidade Python/Node | Obrigatório |
| RabbitMQ | Exchange, filas, retry e DLQ | Existente | Substituir por Kafka no Observa | Semânticas não equivalentes | Obrigatório |
| Kafka | Apenas citado em ADR/briefing | Ausente | `order.events.v1`, key `order_id`, groups por serviço | Partições, offsets, retry/DLT | Obrigatório |
| Ordenação | RabbitMQ não demonstra garantia Kafka | Não verificado | Testar ordem dentro da partição, sem prometer ordem global | Retry, DLT e processamento concorrente | Obrigatório |
| Outbox | Implementada nos publishers | Parcial para escala | Persistir carrier OTel; definir claim/lock antes de réplicas | Duplicação por relays concorrentes | Carrier no MVP; escala pós-MVP |
| Idempotência | `processed_events` transacional | Existente | Preservar por `event_id`; commit de offset após commit do banco | Redelivery após crash | Obrigatório |
| DLQ/DLT | DLQ RabbitMQ implementada; replay não executado nesta inspeção | Parcial | Definir estacionamento Kafka e limites do replay | Replay não restaura ordem original | Obrigatório, mínimo |
| Kubernetes | Docker Compose apenas | Ausente | Empacotar em minikube/driver Docker | Capacidade local desconhecida | Obrigatório |
| Self-healing | `restart: on-failure` no Compose | Parcial | Demonstrar recriação de pod por Deployment | Uma réplica não garante disponibilidade contínua | Obrigatório |
| Traces | Nenhuma dependência OTel | Ausente | Produzir/consumir spans e propagar W3C headers | Contexto se perde na outbox | Obrigatório |
| Métricas | Counters no formato Prometheus | Parcial | Coleta, RED/negócio, latência, erro e exemplars | Nomes e cardinalidade | Obrigatório |
| Logs | JSON com `correlation_id` | Parcial | Adicionar `trace_id`/`span_id`, coletar e reter no Loki | Alta cardinalidade como label | Obrigatório |
| Dashboards/correlação | Nenhuma stack | Ausente | Grafana provisionado como código, links entre sinais | Configuração bidirecional | Obrigatório |
| Testes | Unitários/integração e E2E opcional | Parcial | Testes Kafka, propagação, restart e diagnóstico | Ambiente pesado | Obrigatório |
| CI | pytest/jest | Parcial | Manter checks; imagem/scan depois do MVP | Custo e duração | Pós-MVP |
| Segurança | Credenciais locais e token de admin em Compose | Parcial | Secrets locais, nenhum segredo real versionado, acesso só local | Configuração acidentalmente pública | Obrigatório |

## 5. Dependências e limitações

### Pendências bloqueantes

1. **Para iniciar o Spike 0:** Docker daemon precisa estar funcional para o usuário executor e o minikube precisa poder usar o driver Docker.
2. **Para declarar o MVP viável localmente:** medir CPU, memória, disco e tempos de readiness com Kafka + LGTM + probes. A capacidade da máquina não pôde ser consultada.
3. **Para exigir trace completa nos serviços reais:** provar persistência e reidratação do carrier OTel através do atraso da outbox.
4. **Para múltiplas réplicas/HPA:** implementar e verificar exclusão/claim do relay de outbox. Isso está fora do MVP atual.

### Dependências não bloqueantes para o Spike 0

- Versões exatas de minikube, Kafka, Collector e LGTM, que devem ser fixadas pelo agente após verificar compatibilidade.
- Cliente Kafka específico para Python e Node, desde que suporte key, headers, consumer groups, commit manual e configuração de produtor idempotente.
- Contagem final de partições e limites de recursos, que dependem do baseline medido.

## 6. Conclusão do estado atual

O OrderFlow comprova a jornada de pedidos e mecanismos importantes de confiabilidade — saga coreografada, outbox, idempotência e contratos versionados. Ele não comprova operação em Kafka/Kubernetes nem observabilidade ponta a ponta. O Observa deve preservar os resultados do domínio, mas tratar mensageria, propagação assíncrona, coleta de telemetria e experiência de diagnóstico como trabalho novo e verificável.
