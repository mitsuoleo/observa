# Relatório de validação do MVP Observa

**Execução:** 24/09/2026 (BRT), com evidências em 25/09/2026 (UTC). O ambiente é local, single node e single broker. Os dados de teste são sintéticos; `.local/evidence/` e as capturas em `output/playwright/` permanecem fora do Git.

## Resultado por história

| História | Evidência reproduzível |
| --- | --- |
| US-002 | `./scripts/mvp.ps1 contract` executou Jobs Kafka Python e Node e o harness Postgres. O harness força falha após commit e antes do offset, prova redelivery com um efeito por `event_id` e carrier separado da outbox. Logs: `.local/evidence/mvp-20260925T004955418-contract/`. Adapter Python: 16 testes locais, 83,33% de cobertura. |
| US-003 | `./scripts/mvp.ps1 demo` criou pedido `PENDING` e chegou aos ramos aprovado e rejeitado. Payment guarda decisão, marcador de processamento e outbox na mesma transação. |
| US-004 | No mesmo demo, estoque reservado levou a `CONFIRMED`; estoque indisponível publicou `payment.refund.requested` e `payment.refunded` antes de `CANCELLED`. `recovery` recriou Inventory e exigiu uma única reserva e um evento terminal. |
| US-005 | A trace `112c8fe167d4601e94ebcded4166482c` do pedido feliz contém 7 spans Order, 5 Payment, 5 Inventory e 4 Notification. Notification tem group e persistência idempotente próprios. |
| US-006 | Dashboard `observa-mvp` provisionado exibe eventos, erros, duração, efeitos de negócio e exemplars. No navegador, o clique em um exemplar abriu a trace dos quatro serviços, **Related logs** trouxe 10 linhas do Loki, e **View trace** retornou ao Tempo. |
| US-007 | `up` foi executado mais de uma vez; `status` verificou aplicações, Postgres, Kafka, Collector e backends. `recovery` substituiu o pod Inventory e confirmou o pedido sem reserva duplicada. |
| US-008 | `demo` gerou os três estados finais. Um revisor que não implementou o fluxo executou `status` e `demo` somente pelo runbook, sem passos manuais adicionais, e confirmou os três estados e o OrderFlow inalterado. |

## Evidência do último ciclo

- Três cenários: `.local/evidence/mvp-20260925T005908148-demo/scenarios.json` — `CONFIRMED`, `FAILED` e `CANCELLED`.
- Recuperação: `.local/evidence/mvp-20260925T010520110-recovery/recovery.json` — novo pod, uma reserva, um `stock.reserved` e um `order.completed`.
- Reprodução independente: `.local/evidence/mvp-20260925T010627611-status/` e `.local/evidence/mvp-20260925T010638436-demo/`; ambos registram `orderflow-comparison.json` com `unchanged: true`.
- Navegação visual: `output/playwright/mvp-exemplars-final.png` e `output/playwright/mvp-metric-trace-log-final.png`. Essas capturas são locais e podem ser refeitas com o [runbook](mvp-runbook.md).
- No pedido feliz `6864c8d0-8ace-4dc2-a8bf-01e079c66c97`, os logs Loki dos quatro serviços registraram a partição 1. O caminho normal percorreu os offsets 46 (`order.created`), 47 (`payment.approved`), 48 (`stock.reserved`) e 49 (`order.completed`). Notification leu o mesmo tópico em seu group como ramo assíncrono.

## Rastreabilidade dos requisitos

| Requisito | Prova principal |
| --- | --- |
| RF-001 | `scenarios.json`, tópico `order.events.v1` e logs de partição/offset nos quatro serviços. |
| RF-002 | `scenarios.json` com os três estados e `payment.refund.requested`/`payment.refunded` no ramo de estoque indisponível. |
| RF-003 | Trace final `112c8fe167d4601e94ebcded4166482c` com quatro `service.name`. |
| RF-004 | Navegação Tempo → Related logs → View trace validada no navegador. |
| RF-005 | Dashboard `observa-mvp` com cinco painéis e clique real no exemplar. |
| RF-006 | `mvp.ps1 up` idempotente e `status` independente. |
| RF-007 | `recovery.json`, uma reserva e um evento terminal após recriação do pod. |
| RF-008 | Dry run independente pelo runbook com `status` e `demo`. |
| RNF-001 | Harness Postgres do `contract` e marcadores `processed_events` dos handlers. |
| RNF-002 | Partição 1, offsets 46–49 no caminho normal; limites no [contrato Kafka](../architecture/kafka-contract.md). |
| RNF-003 | Manifests, imagens e dashboard versionados; `up` repetido e execução independente. |
| RNF-004 | Amostra de recursos acima e baseline do [Spike 0](spike-0-report.md). |
| RNF-005 | `.gitignore`, Secrets locais, scan de segredos e auditorias de dependência. |
| RNF-006 | Serviços exportam OTLP para Collector; trace consultada no Tempo e logs no Loki. |

As durações entre o primeiro e o último registro da timeline de cada pedido foram **0,97 s** (feliz), **0,73 s** (rejeição) e **1,81 s** (estoque indisponível). São três observações, não percentis nem SLO. Uma amostra de `kubectl top pods --containers` após o rollout somou **534 millicores e 1.282 MiB** nos workloads listados; foi salva em `.local/evidence/mvp-20260925T003833316-up/resources-final.txt`. O Grafana reiniciou por OOM durante a inspeção da UI com limite de 512 MiB; o limite foi elevado para 768 MiB e o percurso visual completo passou na repetição. Essa amostra não representa pico contínuo.

## Checks de entrega

- Builds, tipos e lint dos pacotes Node, Payment e Inventory; 7, 10 e 6 testes unitários, respectivamente. Order e Notification: lint e 7 e 3 testes em imagens Python finais. Adapter Python: 16 testes e cobertura de 83,33%. Jobs Kafka Python/Node e harness Postgres passaram no cluster.
- `npm audit --omit=dev --audit-level=high` encontrou zero vulnerabilidades nos três pacotes Node. `pip-audit` nas imagens Order e Notification finais não encontrou vulnerabilidades conhecidas; o pacote local `observa-messaging` não está no PyPI e foi avaliado por testes e revisão de código.
- Segredos Postgres e Grafana são gerados em `.local/` e injetados por Secret Kubernetes. `.local/` e `output/playwright/` são ignorados pelo Git. O OrderFlow permaneceu somente leitura em todas as ações verificadas.

## Limites

O relay de outbox assume uma réplica por serviço; múltiplas réplicas, HPA, HA, replay automático do tópico estacionado e SLOs são pós-MVP. A promessa de ordem cobre o tópico e a partição no caminho normal, com redelivery idempotente; estacionamento, replay e concorrência não a preservam. A recuperação de pod prova recriação e ausência de efeito duplicado nesse cenário, enquanto a falha precisa entre commit Postgres e offset é coberta pelo harness. O comando `down` destrói apenas o cluster e os dados sintéticos locais e não foi executado no ciclo final.
