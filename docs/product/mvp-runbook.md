# Runbook do MVP Observa

## Preparação e subida

Em PowerShell 7, com Docker Desktop em Linux containers e os pré-requisitos do [Spike 0](spike-0-report.md), execute na raiz do Observa:

```powershell
./scripts/spike.ps1 preflight
./scripts/mvp.ps1 up
./scripts/mvp.ps1 status
./scripts/mvp.ps1 contract
```

`up` mantém o perfil minikube `observa-spike0` com 4 CPUs e 8 GiB, cria os tópicos, gera a credencial Postgres em `.local/`, constrói e carrega quatro imagens e espera Postgres e as quatro aplicações. A segunda execução deve concluir sem recriar dados. O OrderFlow é somente leitura: cada ação grava sua comparação em `.local/evidence/mvp-*/orderflow-comparison.json`. Não copie credenciais nem evidências locais para o Git.

`contract` constrói e carrega as imagens de teste Python, Node e do harness Postgres, recria os Jobs e salva os logs em `.local/evidence/mvp-*-contract`. Execute depois de `up` quando quiser repetir a prova com broker e banco reais. Os eventos Kafka do contrato usam pedidos inexistentes e não criam pagamentos ou notificações; eles ainda aparecem no tráfego e nas métricas de consumo do tópico compartilhado.

## Três fluxos e recuperação

```powershell
./scripts/mvp.ps1 demo
./scripts/mvp.ps1 recovery
```

`demo` envia pedidos com um produto sintético e espera os estados `CONFIRMED`, `FAILED` e `CANCELLED`. Cada execução grava IDs, estados e timelines em `.local/evidence/mvp-*-demo/scenarios.json`. `recovery` envia um pedido, remove o pod Inventory, espera outro pod ficar pronto e exige `CONFIRMED`, um `stock.reserved`, um `order.completed` e uma reserva no Postgres. A prova e os nomes dos pods ficam em `.local/evidence/mvp-*-recovery/recovery.json`. O momento da exclusão não garante que a mensagem já estivesse em processamento; a verificação de crash entre commit de banco e offset está nos testes da US-002.

O cenário feliz produz `order.created → payment.approved → stock.reserved → order.completed`; a notificação é um ramo assíncrono. A rejeição produz `payment.rejected → order.failed`. Estoque indisponível produz `stock.unavailable → payment.refund.requested → payment.refunded` e o pedido fica `CANCELLED`. Payment, Inventory e Notification persistem seus próprios dados no Postgres; não há cobrança nem envio externo.

## Diagnóstico por métrica, trace e log

Inicie um port-forward local enquanto consulta o Grafana:

```powershell
kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 port-forward service/grafana 13000:3000 --address 127.0.0.1
```

Abra `http://127.0.0.1:13000/d/observa-mvp`. Usuário e senha estão em `.local/grafana-secret.json` (`stringData`); o arquivo é ignorado pelo Git. O dashboard provisionado **Observa | Jornada do pedido** mostra eventos, erros, duração e efeitos de negócio. Gere um fluxo com `demo`, selecione os últimos 15 minutos e abra um exemplar do painel de duração do Order para ir à trace no Tempo. Na trace, **Related logs** abre os logs Loki; o campo **View trace** do log retorna ao Tempo. Para a anomalia controlada, escolha o pedido `payment_rejected` em `scenarios.json` e filtre os logs pelo `order_id` ou `trace_id` do evento `order_created` no pod Order.

Consultas de apoio, com port-forwards próprios para Prometheus (`13090:9090`), Tempo (`13200:3200`) e Loki (`13100:3100`):

- Prometheus: `up{job="domain"}` deve ter quatro séries com valor 1. `observa_processing_duration_seconds_bucket{service="order"}` tem exemplars com `trace_id`; a API `/api/v1/query_exemplars` confirma a associação.
- Tempo: `/api/traces/<trace_id>` deve listar `order-service`, `payment-service`, `inventory-service` e `notification-service` no cenário feliz.
- Loki: `{k8s_namespace_name="observa-spike0"} | json | trace_id="<trace_id>"` retorna logs dos quatro serviços. `trace_id` e `order_id` são campos consultáveis, não labels indexados.
- Kafka: logs JSON de consumidores incluem `partition` e `offset`. Para um `order_id` novo no caminho normal, a partição deve ser a mesma e os offsets devem crescer. Os producers Python e Node usam `murmur2_random`.

## Falhas e limites

Se `up` parar, rode `status` e consulte `kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 get pods`. Veja `describe pod` e `logs deployment/<serviço>` para a causa. Os serviços anunciam prontidão por HTTP; Kafka, Postgres e serviços de domínio têm uma réplica no MVP. Erros transitórios no processamento mantêm o offset sem confirmação; registros irrecuperavelmente inválidos são publicados em `order.events.v1.parked` antes de confirmar a origem. O contrato e as exclusões de ordem estão em [kafka-contract.md](../architecture/kafka-contract.md).

Este ambiente demonstra cenários locais, não HA, HPA, SLO, entrega exatamente uma vez ponta a ponta ou retry automático do tópico estacionado. O relay de outbox ainda é seguro apenas com uma réplica por serviço. O dashboard apresenta amostras de um ambiente de demonstração; não há meta de latência aprovada.

## Encerramento

```powershell
./scripts/mvp.ps1 down
```

O comando remove o perfil local do minikube. Ele destrói os volumes e dados sintéticos do cluster, mas preserva `.local/evidence` e as credenciais locais; remova esses arquivos manualmente apenas se não precisar mais deles.
