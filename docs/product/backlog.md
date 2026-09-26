# Backlog de Produto — Observa

O backlog é ordenado por redução de risco e por fatias demonstráveis. Os itens `US-001` a `US-008` compõem o caminho do MVP; os demais são pós-MVP. Não há estimativas de prazo ou atribuição de pessoas.

## Visão de dependências

`US-001 → US-002 → US-003 → US-004 → US-005 → US-006 → US-007 → US-008`

Há paralelismo técnico possível, mas uma história só está pronta para aceite quando suas dependências e evidências anteriores existem. A migração Order → Payment é um incremento intermediário e não recebe o rótulo de MVP.

## MVP

### US-001 — Validar plataforma local e propagação assíncrona poliglota

- **Tipo:** spike executável.
- **Objetivo e valor:** reduzir os riscos de capacidade local, Kafka, minikube, OTLP e propagação Python ↔ Node antes de migrar regras de domínio.
- **Relacionamentos:** OBJ-002, OBJ-003, OBJ-004; RF-003, RF-004, RF-006; RNF-002, RNF-004, RNF-006; DEC-004 a DEC-007, DEC-009 e DEC-010.
- **Escopo:** minikube/driver Docker; Kafka KRaft single broker; OTel Collector; Prometheus, Tempo, Loki e Grafana; probes independentes Python → Kafka → Node → Kafka → Python; carrier W3C; duas réplicas do consumer sintético; captura de partição/offset e recursos.
- **Fora do escopo:** copiar serviços, regras ou schemas do OrderFlow; banco de domínio; outbox real; dashboard final; metas de desempenho.
- **Critérios de aceite:** os critérios integrais estão em `implementation-brief.md`; no mínimo, uma trace cruza os três processos, trace ↔ logs funciona nos dois sentidos, a key mantém um `order_id` na mesma partição e o estado do OrderFlow não muda em relação ao snapshot inicial.
- **Dependências:** Docker daemon acessível; minikube e ferramentas auxiliares instaláveis; portas locais disponíveis.
- **Prontidão:** **Concluída e validada em 24/09/2026**. Evidências, versões e limites no [relatório do Spike 0](spike-0-report.md).
- **Verificações:** testes de contrato dos headers; teste de quebra/reidratação de contexto; duas réplicas no mesmo group; readiness; snapshot de recursos; teardown.
- **Incertezas:** capacidade da máquina; biblioteca Kafka mais adequada em cada runtime; recursos mínimos da stack.

### US-002 — Fixar contrato operacional Kafka e seam de mensageria

- **Tipo:** arquitetura habilitadora.
- **Objetivo e valor:** impedir que detalhes de Kafka/OTel vazem para o domínio e preservar compatibilidade do envelope v1.
- **Relacionamentos:** OBJ-001, OBJ-002; RF-001, RF-003; RNF-001, RNF-002, RNF-005, RNF-006; DEC-003, DEC-010, DEC-011.
- **Escopo:** especificar interface de producer/consumer por runtime; `order.events.v1`; key `order_id`; groups por serviço; headers W3C; commit manual; produtor idempotente; persistência do carrier na outbox; estacionamento mínimo; convenções de spans/logs/métricas.
- **Fora do escopo:** mudar schemas JSON públicos; múltiplos tópicos de retry; exatamente uma vez ponta a ponta; relay multi-réplica.
- **Critérios de aceite:** testes de contrato equivalentes em Python e Node validam o mesmo evento, key e headers; crash após commit e antes de offset resulta em redelivery idempotente; documento de garantia de ordem contém as exclusões de retry/DLT e concorrência.
- **Dependências:** US-001 concluída; capacidades dos clientes Kafka confirmadas.
- **Prontidão:** **Implementada e validada localmente**. Jobs Kafka Python/Node e harness Postgres repetíveis pelo `mvp.ps1 contract`; evidências no [relatório do MVP](mvp-report.md).
- **Verificações:** testes unitários do adapter; teste de integração Kafka; teste de carrier persistido/rehidratado; revisão de cardinalidade e de segredos.
- **Incertezas:** forma interna da coluna/estrutura do carrier e cliente Kafka específico; o agente pode decidir esses detalhes sem mudar o contrato externo.

### US-003 — Entregar a fatia Order → Payment observável

- **Tipo:** incremento vertical.
- **Objetivo e valor:** provar criação de pedido, outbox, publicação Kafka, decisão de pagamento e retorno observável em uma fatia real.
- **Relacionamentos:** OBJ-001, OBJ-002, OBJ-003; RF-001, RF-002, RF-003; RNF-001, RNF-002, RNF-006.
- **Escopo:** comportamento equivalente de criação e aprovação/rejeição; persistências próprias; adapters Kafka Python/Node; spans producer/consumer; logs com `order_id` e `trace_id`; testes de idempotência.
- **Fora do escopo:** Inventory, compensação, Notification, dashboard final e declaração de MVP.
- **Critérios de aceite:** pedido inicia `PENDING`; Payment publica exatamente um efeito lógico por `event_id`; aprovação e rejeição são forçáveis; uma trace liga HTTP → Order/outbox → Kafka → Payment → Kafka; duplicata não cria segundo pagamento.
- **Dependências:** US-002.
- **Prontidão:** **Implementada e validada localmente**. Aprovação e rejeição verificadas pelo `mvp.ps1 demo`.
- **Verificações:** testes unitários de decisão; integração com Postgres/Kafka; contrato de schema; falha entre commit/offset; consulta de trace/log.
- **Incertezas:** organização interna dos módulos e estratégia de fixture, delegadas ao agente.

### US-004 — Integrar Inventory e preservar compensação

- **Tipo:** incremento vertical.
- **Objetivo e valor:** completar reserva/indisponibilidade e provar que ordenação e idempotência sustentam uma saga com compensação.
- **Relacionamentos:** OBJ-001, OBJ-002; RF-001, RF-002, RF-003, RF-007; RNF-001, RNF-002.
- **Escopo:** consumo de `payment.approved`; publicação de `stock.reserved`/`stock.unavailable`; Order publica `payment.refund.requested`; Payment publica `payment.refunded`; Order termina `CONFIRMED` ou `CANCELLED`; spans e logs em cada salto.
- **Fora do escopo:** Notification, HPA, circuit breaker e chaos amplo.
- **Critérios de aceite:** happy path confirma; indisponibilidade reembolsa e cancela; redelivery não reduz estoque duas vezes nem duplica reembolso; relatório mostra mesma partição e offsets crescentes no caminho normal do pedido.
- **Dependências:** US-003.
- **Prontidão:** **Implementada e validada localmente**. Compensação e recriação do pod Inventory verificadas.
- **Verificações:** testes de handlers/transações; integração da compensação; crash/redelivery; ordem observada; restart de Inventory em cenário controlado.
- **Incertezas:** política final de estacionamento de poison pill, desde que respeite os limites documentados da ordem.

### US-005 — Fechar Notification e trace dos quatro serviços

- **Tipo:** incremento vertical.
- **Objetivo e valor:** cumprir a jornada completa do MVP e tornar explícito o ramo de notificação.
- **Relacionamentos:** OBJ-001, OBJ-003; RF-001 a RF-004; RNF-001, RNF-006.
- **Escopo:** Notification com group próprio; seleção dos eventos relevantes; persistência idempotente; spans/logs; trace que contém Order, Payment, Inventory, Notification e evento terminal.
- **Fora do escopo:** notificação externa real, email/SMS e garantia de que Notification bloqueia estado terminal.
- **Critérios de aceite:** Notification recebe os eventos definidos sem competir com outros grupos; duplicatas produzem uma notificação lógica; trace do cenário feliz contém os quatro serviços e deixa claro que Notification é ramo assíncrono; trace ↔ logs funciona para Notification.
- **Dependências:** US-004.
- **Prontidão:** **Implementada e validada localmente**. A trace feliz contém os quatro serviços.
- **Verificações:** teste de group; idempotência; trace tree; correlação por `order_id` e `trace_id`.
- **Incertezas:** quais eventos geram registro de notificação no MVP; deve preservar, no mínimo, a cobertura comprovada do OrderFlow.

### US-006 — Entregar dashboard, exemplars e jornada de diagnóstico

- **Tipo:** incremento vertical de observabilidade.
- **Objetivo e valor:** transformar telemetria coletada em uma experiência de diagnóstico demonstrável, não apenas em componentes instalados.
- **Relacionamentos:** OBJ-003, OBJ-004; RF-004, RF-005, RF-008; RNF-003, RNF-006.
- **Escopo:** métricas de throughput, erro e duração; métricas de negócio mínimas; dashboard provisionado; exemplar ou ligação equivalente métrica → trace; trace → logs e logs → trace; cenário de anomalia controlada.
- **Fora do escopo:** SLO formal, alertas, retenção de produção e metas numéricas sem baseline.
- **Critérios de aceite:** dados reais do cenário aparecem no dashboard; o avaliador parte de uma anomalia, abre a trace correta e chega aos logs do span; datasources e links são provisionados como código; `trace_id` não é label Loki indexado.
- **Dependências:** US-005; dados suficientes para dashboard.
- **Prontidão:** **Implementada e validada localmente**. Navegação exemplar → Tempo → Loki → Tempo exercitada no navegador.
- **Verificações:** teste guiado de UI/API dos backends; validação de queries; cardinalidade; reconstrução em ambiente novo.
- **Incertezas:** thresholds e painéis adicionais, definidos depois do baseline.

### US-007 — Empacotar minikube, self-healing e comando único

- **Tipo:** incremento operacional.
- **Objetivo e valor:** tornar a demonstração reprodutível e provar recuperação de processo.
- **Relacionamentos:** OBJ-002, OBJ-004; RF-006, RF-007; RNF-003 a RNF-005; DEC-004, DEC-006, DEC-008.
- **Escopo:** configuração minikube/driver Docker; manifests ou charts fixados; uma réplica por serviço; probes de liveness/readiness; requests/limits derivados; build/load de imagens; comandos de up/status/down; restart controlado de pod.
- **Fora do escopo:** HPA, HA, cluster cloud e auto-instalação silenciosa de ferramentas do host.
- **Critérios de aceite:** um comando idempotente sobe a stack e espera readiness; falha apresenta componente e orientação; remover um pod causa recriação; pedido eventualmente termina sem efeito duplicado; teardown é documentado e seguro.
- **Dependências:** US-001 e aplicações de US-005; medições de recursos.
- **Prontidão:** **Implementada e validada localmente**. `up` idempotente, `status` completo e `recovery` passaram.
- **Verificações:** ambiente novo; segunda execução do comando; restart; consumo de recursos; verificação de secrets e bindings locais.
- **Incertezas:** valores finais de recursos e timeout, derivados das medições.

### US-008 — Consolidar cenários, evidências e runbook

- **Tipo:** entrega de portfólio e aceite.
- **Objetivo e valor:** permitir que terceiro reproduza a demonstração e avalie o raciocínio operacional.
- **Relacionamentos:** OBJ-001 a OBJ-004; RF-002 a RF-008; RNF-003 a RNF-005.
- **Escopo:** executor dos três fluxos; cenário de anomalia; evidências de dashboard/trace/log; relatório de recursos e latências observadas; runbook; roadmap pós-MVP; instruções de teardown.
- **Fora do escopo:** transformar medições iniciais em SLO aprovado ou alegar capacidade de produção.
- **Critérios de aceite:** pessoa não autora executa instruções sem configuração manual oculta; cada requisito do MVP aponta para evidência; limitações são explícitas; nenhum RF/RNF adiado é apresentado como entregue.
- **Dependências:** US-006 e US-007.
- **Prontidão:** **Implementada e validada localmente**. Revisor não autor repetiu `status` e `demo` seguindo o [runbook](mvp-runbook.md).
- **Verificações:** dry run por terceiro/agente independente; checklist de rastreabilidade; revisão de links e comandos; scan de segredos; comparação do snapshot do OrderFlow.
- **Incertezas:** formato final das capturas/evidências, desde que versionável ou reproduzível.

## Pós-MVP

### US-101 — Tornar o relay de outbox seguro para múltiplas réplicas

- **Tipo:** confiabilidade/escalabilidade.
- **Valor:** habilitar escala horizontal sem publicação concorrente da mesma linha.
- **Escopo:** claim/lock transacional, lease ou relay dedicado; testes de concorrência e recuperação.
- **Dependência:** baseline do MVP.
- **Aceite resumido:** duas réplicas publicam cada linha como um único efeito lógico sob falhas controladas; duplicatas residuais continuam seguras.
- **Estado em 25/09/2026:** implementada e validada localmente com dois claimers PostgreSQL por runtime e carga em duas réplicas; evidências no [relatório pós-MVP](post-mvp-report.md).

### US-102 — HPA por CPU e lag

- **Tipo:** escalabilidade.
- **Valor:** demonstrar ajuste de capacidade baseado em sinal real.
- **Dependências:** US-101 e métricas de lag confiáveis.
- **Aceite resumido:** carga reproduzível provoca escala e retorno, sem quebrar partições, ordem declarada ou idempotência.
- **Estado em 25/09/2026:** validada localmente. KEDA v2.21.0 gera HPA por CPU e lag Kafka para os quatro grupos. Um backlog controlado de Payment acionou escala para duas réplicas por métrica externa; 300 pedidos terminaram, as réplicas retornaram a uma e os efeitos persistidos foram únicos. Evidência no [relatório pós-MVP](post-mvp-report.md).

### US-103 — SLOs e alertas

- **Tipo:** operação.
- **Valor:** transformar baseline em objetivos mensuráveis e alerta acionável.
- **Dependência:** medições do MVP.
- **Aceite resumido:** SLI e orçamento de erro têm justificativa; alerta leva a runbook testado.
- **Estado em 25/09/2026:** baseline e alertas experimentais implementados; regras testadas, sem disparo real nem SLO aprovado. Consulte o [runbook operacional](operations-runbook.md).

### US-104 — Gateway instável, retry e circuit breaker

- **Tipo:** resiliência.
- **Valor:** demonstrar falha de dependência síncrona controlada.
- **Dependência:** fluxo de pagamento estável.
- **Aceite resumido:** falha configurável exibe transições do breaker e retry com backoff sem tempestade ou cobrança duplicada.
- **Estado em 25/09/2026:** gateway sintético validado por demo determinístico e injeção de falha no cluster; pagamentos únicos confirmados. Não há cobrança externa.

### US-105 — Chaos test e recuperação ampliada

- **Tipo:** resiliência.
- **Valor:** verificar broker, consumidores e dependências sob falhas além de restart simples.
- **Dependências:** ambiente estável e observabilidade completa.
- **Aceite resumido:** experimentos têm hipótese, blast radius, condição de parada e evidência de recuperação.
- **Estado em 25/09/2026:** experimentos locais de recriação de Payment e Kafka passaram com evidência de recuperação após a falha; não cobrem disponibilidade contínua durante a queda do broker.

### US-106 — CI de imagens e segurança

- **Tipo:** engenharia de entrega.
- **Valor:** validar testes, builds, manifests e vulnerabilidades automaticamente.
- **Dependência:** estrutura de build estabilizada.
- **Aceite resumido:** CI executa lint/typecheck/testes, build de imagens, validação de manifests e scan sem remover controles para ficar verde.
- **Estado em 25/09/2026:** comando local completo validado por fases e workflow versionado; execução remota da CI pendente até existir remoto Git.

## Critério de prontidão do MVP

O MVP só pode ser declarado pronto quando `US-001` a `US-008` estiverem aceitas e a matriz do `prd.md` apontar para evidência reproduzível. Instalar todos os componentes, migrar apenas dois serviços ou exibir uma trace incompleta não satisfaz a definição.
