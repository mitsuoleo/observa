# PRD — Observa

**Status:** Spike 0 validado; MVP local implementado e verificado conforme o [relatório](mvp-report.md).
**Data:** 22 de setembro de 2026  
**Projeto-base:** OrderFlow, somente leitura  
**Produto:** repositório independente Observa

## 1. Problema e proposta de valor

O OrderFlow demonstra uma jornada assíncrona de pedidos, mas não permite provar como essa jornada é operada e diagnosticada em uma plataforma distribuída. Métricas expostas e logs correlacionados por pedido não são suficientes para responder, de forma reproduzível, qual serviço falhou, onde uma execução ficou lenta ou como o sistema se recuperou.

O Observa será uma peça separada de portfólio que preserva os comportamentos essenciais do OrderFlow e demonstra Kafka, Kubernetes, OpenTelemetry e diagnóstico correlacionado. O valor não está em criar outro domínio de pedidos, mas em apresentar evidência operacional auditável.

## 2. Público e jornada principal

### Público

- Avaliadores técnicos, recrutadores e engenheiros que revisam o portfólio.
- O próprio desenvolvedor, atuando como operador durante uma demonstração ou incidente reproduzível.

### Jornada de diagnóstico

1. Um cenário controlado gera pedidos e uma anomalia observável.
2. O operador identifica erro ou latência no dashboard Grafana.
3. Um exemplar ou link contextual abre uma trace específica no Tempo.
4. A trace mostra spans dos quatro serviços e as operações Kafka relacionadas.
5. A partir do span, o operador abre os logs correspondentes no Loki.
6. `order_id`, `trace_id`, serviço, tópico, partição e offset permitem reconstruir a execução.
7. O runbook descreve os passos e o resultado esperado para outra pessoa reproduzir.

`correlation_id` permanece o `order_id` e identifica a jornada de negócio. `trace_id` identifica uma execução observada; ambos devem aparecer nos logs, sem serem tratados como sinônimos.

## 3. Objetivos e sinais de sucesso

| ID | Objetivo | Sinal de sucesso |
|---|---|---|
| OBJ-001 | Preservar os resultados de domínio e garantias essenciais do OrderFlow. | Os três cenários terminam em `CONFIRMED`, `FAILED` e `CANCELLED`, sem efeitos duplicados em redelivery. |
| OBJ-002 | Demonstrar operação da jornada em Kafka e Kubernetes local. | Os quatro serviços executam no minikube, usam Kafka e sobrevivem à recriação controlada de um pod. |
| OBJ-003 | Tornar uma execução diagnosticável entre métricas, traces e logs. | Um avaliador navega de uma anomalia para a trace e os logs do mesmo caso seguindo um roteiro versionado. |
| OBJ-004 | Produzir evidência reproduzível de portfólio. | Ambiente, cenários, dashboard e runbook são reconstruídos a partir do repositório sem configuração manual oculta. |

Metas numéricas de latência, erro, throughput, recursos e SLO não são aceites do MVP antes de existir baseline. O MVP deve medi-las e registrar o método de medição; metas futuras serão propostas a partir desses dados.

## 4. Escopo

### Incluído no MVP

- Order, Payment, Inventory e Notification usando Kafka no fluxo demonstrável.
- Um tópico de domínio `order.events.v1`, key Kafka igual a `order_id` e consumer group por serviço.
- Apache Kafka KRaft com um broker para o ambiente local.
- Minikube com driver Docker e uma réplica por serviço de domínio.
- Propagação W3C Trace Context em headers Kafka e persistência do carrier como metadado interno da outbox.
- OpenTelemetry Collector, Prometheus, Tempo, Loki e Grafana provisionados como código.
- Métricas de negócio e RED mínimas, dashboard real e correlação métrica → trace → log.
- Logs JSON com `order_id`, `trace_id`, `span_id`, serviço e metadados Kafka pertinentes.
- Fluxos feliz, pagamento rejeitado e estoque indisponível com reembolso.
- Redelivery idempotente, estacionamento mínimo de mensagem inválida e restart de pod.
- Um comando documentado para criar/subir o ambiente e outro para executar a demonstração.
- Runbook e conjunto de evidências reproduzíveis.

### Fora do MVP

- Múltiplas réplicas dos serviços de domínio e HPA.
- Alta disponibilidade de Kafka, Postgres ou componentes LGTM.
- SLOs formais e Alertmanager.
- Chaos testing amplo, circuit breaker e gateway de pagamento instável.
- CI completa de imagens, scan de vulnerabilidade e entrega contínua.
- Service mesh, mTLS e cluster cloud.
- Alteração das APIs públicas ou dos schemas JSON de domínio do OrderFlow.

### Sequência, não escopo final

Uma fatia Order → Payment pode ser entregue antes das demais para reduzir risco. Ela não é denominada MVP. Um estado híbrido RabbitMQ/Kafka não é alvo: exigiria bridge ou dual-publish, ampliaria as semânticas de falha e enfraqueceria a demonstração.

## 5. Requisitos funcionais

| ID | Requisito | Critério de aceite observável |
|---|---|---|
| RF-001 | Transportar a jornada de domínio dos quatro serviços pelo Kafka. | Em execução demonstrável, todos os eventos do pedido trafegam por `order.events.v1`, com key igual ao `order_id`; nenhum broker RabbitMQ participa. |
| RF-002 | Preservar os três resultados de negócio. | Cenários automatizados comprovam `CONFIRMED`, `FAILED` e `CANCELLED`, incluindo `payment.refund.requested` e `payment.refunded` no último caso. |
| RF-003 | Produzir uma trace distribuída completa. | Uma trace iniciada no recebimento do pedido contém spans de Order, Payment, Inventory e Notification, incluindo publish/process Kafka e o evento terminal. Notification pode aparecer como ramo da coreografia. |
| RF-004 | Correlacionar trace e logs nos dois sentidos. | De um span no Tempo abre-se a consulta Loki correspondente; uma linha de log com `trace_id` abre a trace correta. |
| RF-005 | Expor métricas reais e correlacioná-las com traces. | Dashboard mostra ao menos throughput, erros e duração da jornada/serviços com dados gerados pelo cenário; um exemplar ou link equivalente abre uma trace do intervalo. |
| RF-006 | Reproduzir o ambiente local. | Um comando documentado cria/sobe minikube, Kafka, telemetria e aplicações; readiness é verificada automaticamente e falhas terminam com diagnóstico útil. |
| RF-007 | Demonstrar recuperação de processo sem duplicar efeitos. | Durante um cenário controlado, um pod é removido e recriado pelo Deployment; redelivery não duplica pagamento, reserva, notificação ou evento terminal. |
| RF-008 | Fornecer uma jornada de diagnóstico repetível. | Um runbook executável por terceiro parte do dashboard e chega à causa simulada usando trace e logs, registrando evidência. |

## 6. Requisitos não funcionais

| ID | Categoria | Requisito | Critério de aceite |
|---|---|---|---|
| RNF-001 | Confiabilidade | Manter processamento pelo menos uma vez com deduplicação por `event_id` e outbox transacional. | Offset só é confirmado após commit de negócio; crash entre commit e offset causa redelivery neutralizado por `processed_events`. |
| RNF-002 | Ordenação | Qualificar a ordem por pedido sem prometer ordem global. | Teste registra tópico, partição e offset e mostra sequência por `order_id` na mesma partição; documentação exclui retry/DLT, outros tópicos e concorrência interna da garantia. |
| RNF-003 | Reprodutibilidade | Versionar infraestrutura, dashboards, datasources e cenários. | Uma instalação nova não exige configuração manual na UI para cumprir a jornada principal. |
| RNF-004 | Eficiência local | Derivar recursos e tempos de espera de medição real. | Spike registra CPU/memória, readiness e gargalos; valores do MVP citam essa evidência e incluem perfil local enxuto. |
| RNF-005 | Segurança | Não versionar segredos reais nem expor componentes fora do host por padrão. | Verificação do repositório não encontra credenciais reais; segredos locais são injetados e interfaces administrativas ficam limitadas ao ambiente local. |
| RNF-006 | Portabilidade de telemetria | Enviar telemetria por OTLP através do Collector. | Aplicações não dependem diretamente das APIs de Tempo ou Loki; os backends podem ser trocados sem mudar o domínio. |

## 7. Contrato operacional proposto para implementação

### Evento e Kafka

- O valor da mensagem mantém o envelope e payloads JSON v1 do OrderFlow.
- Tópico: `order.events.v1`.
- Key: `order_id`, igual a `correlation_id` no contrato atual.
- Headers mínimos: `traceparent`, `tracestate` quando presente, content type e versão de schema se exigidos pelo adapter.
- Consumer groups: um por responsabilidade de serviço, nunca compartilhado entre domínios que precisam receber o mesmo evento.
- Produtor configurado para idempotência e confirmação forte compatível com o cliente escolhido.
- Offset confirmado somente após transação de negócio bem-sucedida.

### Garantia de ordenação

Para um `order_id`, todos os registros do caminho normal usam a mesma key no mesmo tópico e, portanto, são destinados à mesma partição. Cada consumer group processa essa partição sequencialmente. A garantia não cobre outros tópicos, processamento paralelo introduzido pela aplicação, estacionamento/replay de DLT ou efeitos externos. O sistema permanece pelo menos uma vez e usa `event_id` para neutralizar duplicatas.

### Outbox e tracing

O contexto de criação da mensagem precisa sobreviver ao intervalo entre a transação de domínio e o relay. A linha de outbox deve persistir um carrier interno de telemetria sem alterar o valor JSON público. O relay reidrata o contexto, cria o span de producer e injeta headers; o consumidor extrai headers e cria o span de consumer/process.

### Logs e cardinalidade

Logs de aplicação são JSON em stdout. `trace_id` e `span_id` ficam disponíveis para consulta e ligação, mas não como labels Loki indexados de alta cardinalidade. Labels ficam restritos a dimensões estáveis como serviço, namespace e ambiente.

## 8. Decisões

### Decisões confirmadas

| ID | Decisão | Consequência |
|---|---|---|
| DEC-001 | Observa é projeto independente; OrderFlow é referência somente leitura. | Não copiar ou alterar silenciosamente o projeto #2. |
| DEC-002 | MVP exige os quatro serviços em Kafka. | Migração parcial é incremento, não conclusão. |
| DEC-003 | Usar `order.events.v1` e key `order_id`. | Facilita ordem por pedido dentro da partição e simplifica a garantia demonstrável. |
| DEC-004 | Usar minikube com driver Docker. | O Spike 0 precisa validar daemon e capacidade local. |
| DEC-005 | Usar Prometheus, Tempo, Loki e Grafana. | Profundidade de correlação tem prioridade sobre variedade de backends. |
| DEC-006 | Usar Kafka KRaft single broker local. | Alta disponibilidade de broker fica fora do MVP. |
| DEC-007 | Começar com spike poliglota Python ↔ Node. | Reduz riscos de plataforma e propagação antes de migrar domínio. |
| DEC-008 | Uma réplica por serviço de domínio no MVP. | HPA espera relay de outbox seguro para concorrência. |
| DEC-009 | Coletar logs com OTel Collector e enviar ao endpoint OTLP nativo do Loki. | Promtail não entra no projeto; telemetria permanece vendor-neutral até o Collector. |
| DEC-010 | Persistir o carrier OTel como metadado interno da outbox. | Trace causal atravessa publicação assíncrona sem quebrar schemas públicos. |
| DEC-011 | Confirmar offset somente depois do commit de negócio. | Redelivery é esperado e tratado pela idempotência persistente. |

Os ADRs do briefing não são ADRs aceitos do Observa por origem. As decisões acima foram confirmadas neste planejamento; ADRs formais deverão ser escritos durante a implementação apenas quando o trade-off e suas consequências forem validados pelo spike.

## 9. Hipóteses, riscos e validações

| ID | Tipo | Descrição | Impacto | Validação/mitigação |
|---|---|---|---|---|
| HYP-001 | Hipótese | A máquina suporta minikube, Kafka, LGTM e probes simultaneamente. | Pode exigir perfil mais leve ou mudança de ambiente. | Spike registra recursos e readiness; falha é condição de parada. |
| HYP-002 | Hipótese | Clientes Python e Node escolhidos preservam key, headers e commit manual de forma compatível. | Pode quebrar ordem, tracing ou redelivery. | Probes poliglotas com testes de contrato. |
| RSK-001 | Risco | Contexto causal se perde no relay da outbox. | Trace fragmentada e MVP inválido. | Persistir/reidratar carrier e testar atraso sem contexto ativo. |
| RSK-002 | Risco | Relays concorrentes publicam a mesma linha. | Duplicam mensagens e telemetria. | Uma réplica no MVP; claim/lock antes de HPA. |
| RSK-003 | Risco | Retry ou DLT cria impressão falsa de ordem. | Promessa de produto incorreta. | Aceite explícito por partição e documentação das exclusões. |
| RSK-004 | Risco | LGTM domina os recursos locais. | Ambiente instável ou demonstração impraticável. | Perfil single-node/single-broker e limites derivados do spike. |
| RSK-005 | Risco | Dashboard existe, mas não permite navegação real entre sinais. | Objetivo de diagnóstico não atendido. | Teste de aceitação parte da UI e valida os links nos dois sentidos. |
| RSK-006 | Risco | Mudanças concorrentes no OrderFlow são confundidas com trabalho do Observa. | Perda ou atribuição indevida de código. | Capturar e comparar snapshot do estado antes/depois; nunca escrever no projeto-base. |

## 10. MVP e Definition of Done

O MVP é uma jornada operacional completa, não a instalação isolada de componentes.

Está concluído quando:

1. Os quatro serviços preservam os três desfechos do OrderFlow usando apenas Kafka no caminho demonstrado.
2. `order.events.v1`, key `order_id`, headers W3C e consumer groups estão verificados por testes e evidências de tópico/partição/offset.
3. Redelivery após falha não duplica efeitos de negócio.
4. Uma trace inclui os quatro serviços, as operações Kafka e um evento terminal.
5. Grafana permite métrica → trace → log e log → trace usando dados do cenário.
6. O dashboard contém throughput, erro e duração com dados reais, sem metas arbitrárias.
7. O ambiente minikube sobe com um comando, verifica readiness e pode ser removido de forma documentada.
8. A remoção de um pod demonstra recriação e continuidade eventual da jornada.
9. Infraestrutura, datasources, dashboard, cenários e runbook estão versionados como código.
10. Uma pessoa que não implementou o sistema reproduz o cenário e o diagnóstico apenas com o repositório.

## 11. Rastreabilidade do MVP

As provas de cada RF/RNF, comandos e limites estão na [matriz de evidências do MVP](mvp-report.md#rastreabilidade-dos-requisitos).

| Requisito | Objetivos | Backlog | Evidência de aceite |
|---|---|---|---|
| RF-001 | OBJ-001, OBJ-002 | US-002, US-003, US-004, US-005 | Eventos e ausência de RabbitMQ no fluxo |
| RF-002 | OBJ-001 | US-003, US-004, US-005, US-008 | Estados finais e timeline |
| RF-003 | OBJ-003 | US-001, US-003, US-004, US-005 | Trace dos quatro serviços |
| RF-004 | OBJ-003 | US-001, US-005, US-006 | Links trace ↔ logs |
| RF-005 | OBJ-003, OBJ-004 | US-006, US-008 | Dashboard e exemplar/link |
| RF-006 | OBJ-002, OBJ-004 | US-001, US-007 | Comando único e readiness |
| RF-007 | OBJ-001, OBJ-002 | US-004, US-007 | Restart e ausência de duplicatas |
| RF-008 | OBJ-003, OBJ-004 | US-006, US-008 | Runbook executado por terceiro |
| RNF-001 | OBJ-001 | US-002, US-003, US-004, US-005 | Crash/redelivery idempotente |
| RNF-002 | OBJ-001, OBJ-002 | US-001, US-002, US-004 | Partição/offset e limites documentados |
| RNF-003 | OBJ-004 | US-006, US-007, US-008 | Ambiente reconstruído do repositório |
| RNF-004 | OBJ-002, OBJ-004 | US-001, US-007 | Relatório de recursos e perfil local |
| RNF-005 | OBJ-004 | US-002, US-007 | Scan de segredos e exposição local |
| RNF-006 | OBJ-003 | US-001, US-006 | OTLP através do Collector |

## 12. Referências técnicas externas

Estas fontes sustentam as restrições técnicas, mas não substituem os testes do Observa:

- [Apache Kafka — Design](https://kafka.apache.org/40/design/design/): ordem por partição, consumer groups e semânticas de entrega.
- [Apache Kafka — Producer Configs](https://kafka.apache.org/40/configuration/producer-configs/): idempotência, retries, acknowledgements e risco de reordenação.
- [OpenTelemetry — Semantic conventions for messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/): contexto de criação da mensagem e spans producer/consumer.
- [minikube — Getting started](https://minikube.sigs.k8s.io/docs/start/): requisitos e drivers do Kubernetes local.
- [OpenTelemetry Collector Helm chart](https://opentelemetry.io/docs/platforms/kubernetes/helm/collector/): modos de implantação e coleta de logs de pods.
- [Grafana Loki — OpenTelemetry Collector](https://grafana.com/docs/loki/latest/send-data/otel/otel-collector-getting-started/): ingestão nativa de logs por OTLP.
- [Grafana Tempo](https://grafana.com/docs/tempo/latest/): integração de traces com métricas e logs.
