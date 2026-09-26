# Operação experimental do Observa

## Baseline da jornada

Com o cluster saudável, execute `pwsh ./scripts/operations-baseline.ps1 -Rounds 10`. O comando cria dez pedidos sequenciais por cenário (`happy`, `payment_rejected`, `stock_unavailable`) e grava `samples.json` e `summary.json` em `.local/evidence/operations-*`. Cada tentativa, inclusive timeout, estado inesperado e erro de consulta, permanece no denominador. O script não usa cobranças nem notificações externas. Para recalcular sem gerar pedidos: `pwsh ./scripts/operations-baseline.ps1 -AnalyzeOnly .local/evidence/operations-<id>/samples.json`.

O **SLI de conclusão** é a fração de pedidos que alcançam o estado terminal esperado com exatamente um evento causal na timeline antes do timeout configurado (30 s no ciclo abaixo; 120 s por padrão). O **SLI de duração** mede, apenas entre pedidos concluídos, a diferença entre `occurred_at` persistido de `order.created` e o evento causal que muda o estado: `stock.reserved`, `payment.rejected` ou `payment.refunded`. A métrica não inclui o tempo de entrada HTTP anterior à gravação, nem a notificação assíncrona. Percentis usam nearest rank por cenário; com dez amostras, p95 é o máximo observado.

Em 25/09/2026 UTC, `-Rounds 10 -TimeoutSeconds 30` resultou em **30/30 conclusões corretas**. Para feliz, rejeição e estoque indisponível, respectivamente, p50 foi **0,824 s / 0,579 s / 1,466 s**, e p95 foi **1,293 s / 0,952 s / 1,829 s**. A evidência bruta está em `.local/evidence/operations-20260925T171510612/`; contém IDs de pedidos sintéticos e permanece fora do Git. O máximo de 1,829 s oferece apenas uma referência local e sequencial.

**Hipótese de objetivo, ainda não SLO aprovado:** pelo menos 95% dos pedidos sintéticos devem terminar corretamente em até 5 s, em uma janela de 300 pedidos distribuídos entre os três cenários. Os 5 s são cerca de 2,7 vezes o maior tempo visto; o limite de 95% deixa um orçamento experimental de 15 falhas por 300 tentativas. A execução de 30 pedidos consumiu zero desse orçamento, mas é pequena e não cobre concorrência, cargas externas, dias distintos ou falhas de infraestrutura. Repetir a amostragem em pelo menos três dias e com carga concorrente antes de confirmar ou revisar o objetivo. Não extrapolar para produção.

## Alertas provisionados

O Prometheus avalia as regras em `infra/kubernetes/config/operations-rules.yaml`. `ObservaDomainTargetDown` dispara após 2 min com um alvo de serviço `up == 0`. `ObservaProcessingErrors` dispara após 1 min se um serviço incrementar `observa_errors_total` na janela de 5 min. Ambos são avisos experimentais, sem notificação externa: consultar **Prometheus → Alerts** ou `/api/v1/alerts` via port-forward. Eles detectam sinais de infraestrutura/processamento, não medem diretamente o SLI de conclusão. A condição de 5 s acima é avaliada pelo `summary.json`, pois ainda não há série Prometheus da jornada completa. Não tratar o silêncio dos alertas como prova de SLO.

Inicie o acesso local ao Prometheus com:

```powershell
kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 port-forward service/prometheus 13090:9090 --address 127.0.0.1
```

Verifique a carga das regras em `http://127.0.0.1:13090/api/v1/rules` e os alertas em `http://127.0.0.1:13090/api/v1/alerts`. Um teste de sintaxe com `promtool check rules` não comprova entrega de aviso, pois Alertmanager não está instalado.

### Domain target down

1. Consulte `up{job="domain"}` e `/api/v1/targets` para identificar o alvo indisponível. Confirme o pod e os endpoints: `kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 get pods,endpoints`.
2. Veja `describe pod <nome>` e `logs deployment/<serviço> --tail=100`; procure falha de prontidão, reinício, OOM, indisponibilidade de Kafka ou Postgres. Compare com `./scripts/mvp.ps1 status`.
3. Corrija a causa e confirme `up{job="domain"} == 1` em todos os quatro serviços durante pelo menos duas avaliações. Execute uma rodada do baseline e confirme os três estados. Se houver pedido pendente, use seu `order_id` para consultar a timeline e logs antes de qualquer replay.

### Processing errors

1. Identifique o serviço em `sum by (service) (increase(observa_errors_total{job="domain"}[5m]))`. Consulte `kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 logs deployment/<serviço> --since=10m` e procure `order_id`, `trace_id`, `partition` e `offset`.
2. No Grafana `http://127.0.0.1:13000/d/observa-mvp`, abra a trace pelo exemplar e use **Related logs**; o [runbook do MVP](mvp-runbook.md) detalha os port-forwards e filtros. Verifique a timeline do pedido e os efeitos persistidos antes de decidir por retentativa.
3. Erros transitórios mantêm o offset sem confirmação. Registros inválidos vão para `order.events.v1.parked`; não há replay automático. Confira a recuperação com novo baseline, `increase(observa_errors_total[5m])` estabilizado e efeitos únicos no banco. Não faça replay manual sem classificar a causa e checar idempotência.

## Prova operacional

Em 25/09/2026, `promtool test rules tests/operations/operations-rules.test.yaml` passou dentro do pod Prometheus: ambos os avisos estavam ausentes em 2 min e dispararam em 3 min com labels e anotações esperados. `promtool check config /etc/prometheus/prometheus.yml` também confirmou o arquivo provisionado e suas duas regras no cluster. Para ensaio real do alvo indisponível, registrar estado inicial, reduzir **apenas uma** réplica de serviço a zero por mais de 2 min, confirmar `ObservaDomainTargetDown` em `/api/v1/alerts`, restaurar a réplica e seguir o procedimento acima. Execute esse ensaio somente quando não houver outra carga em andamento; os scripts de carga e chaos têm janelas próprias. Para `ObservaProcessingErrors`, prefira teste sintético da regra: injetar falha de negócio não deve ser usado como substituto de erro de processamento. Nenhum disparo real é alegado aqui até a evidência ser capturada.
