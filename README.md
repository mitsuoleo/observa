# Observa

Laboratório local para demonstrar **como diagnosticar e recuperar uma jornada de pedidos distribuída**. Order (Python), Payment (Node.js), Inventory (Node.js) e Notification (Python) trocam eventos via Kafka, persistem efeitos no Postgres e enviam métricas, traces e logs para uma stack observável no Kubernetes local. Pedidos, pagamentos e estoque são sintéticos; não há cobrança nem notificação externa. O [OrderFlow](docs/product/current-state.md) serviu como referência funcional somente leitura.

## O que o avaliador consegue verificar

| Cenário | Estado final | Evidência de domínio |
|---|---|---|
| Pagamento aprovado e estoque reservado | `CONFIRMED` | `payment.approved`, `stock.reserved`, `order.completed` |
| Pagamento recusado | `FAILED` | `payment.rejected`, `order.failed` |
| Estoque indisponível após aprovação | `CANCELLED` | Compensação `payment.refund.requested` → `payment.refunded` |

O comando `demo` executa os três casos. No [dashboard Grafana](http://127.0.0.1:13000/d/observa-mvp), uma amostra de duração leva à trace no Tempo; **Related logs** abre os registros no Loki, e **View trace** retorna à mesma execução. O roteiro exato está no [runbook do MVP](docs/product/mvp-runbook.md#diagnóstico-por-métrica-trace-e-log). A [síntese de evidências atual](docs/portfolio/evidence-2026-09-26.md) e os [relatórios de validação](docs/product/post-mvp-report.md) distinguem resultados observados dos limites dos ensaios.

## Reproduzir a demonstração

Use Windows, PowerShell 7, Git, Docker Desktop com Linux containers e `kubectl`. Reserve **4 CPUs, 8 GiB de memória e 20 GiB livres** para o Docker. O ambiente usa o perfil minikube e namespace dedicados `observa-spike0`; nenhuma cloud é necessária. Em um checkout novo:

```powershell
git clone https://github.com/mitsuoleo/observa.git
cd observa
./scripts/install-tools.ps1
./scripts/mvp.ps1 up
./scripts/mvp.ps1 status
./scripts/mvp.ps1 contract
./scripts/mvp.ps1 demo
./scripts/mvp.ps1 recovery
```

Para abrir o Grafana, deixe este comando ativo em outro terminal e acesse o dashboard indicado acima:

```powershell
kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 port-forward service/grafana 13000:3000 --address 127.0.0.1
```

As credenciais locais geradas estão em `.local/grafana-secret.json`. IDs de pedidos e evidências brutas ficam em `.local/evidence/`; ambos os diretórios são ignorados pelo Git. Encerre o port-forward com Ctrl+C. Quando terminar, `./scripts/mvp.ps1 down` remove **somente** o perfil dedicado e seus dados sintéticos. Não execute comandos que alteram o cluster em paralelo.

## Qualidade e ensaios opcionais

`./scripts/check.ps1` executa build, lint, tipos, testes Node/Python, testes dos relays com PostgreSQL descartável, build das imagens, auditorias de dependências, busca de segredos no histórico e na árvore atual, validação dos manifests e regras Prometheus. Ele requer Docker, Python, `kubectl`, Git e Gitleaks; a [CI](.github/workflows/check.yml) chama o mesmo script sem cluster. `./scripts/check.ps1 -Only manifests` repete uma fase isolada; as opções estão no [runbook pós-MVP](docs/product/post-mvp-runbook.md).

Após `up`, execute o baseline separadamente. Para o alerta, instale primeiro o perfil optativo de escala KEDA; a verificação `relay-db` comprova a concorrência dos relays exigida por `scale-apply`:

```powershell
./scripts/operations-baseline.ps1 -Rounds 10
./scripts/check.ps1 -Only relay-db
./scripts/scale-apply.ps1 -RelayConcurrencyVerified
./scripts/alert-verify.ps1 -Target payment
```

O baseline mede conclusão e duração causal dos três cenários. O ensaio de alerta pausa Payment temporariamente, observa o aviso no Prometheus e restaura o serviço. Aguarde cada comando terminar antes de iniciar o próximo. Os detalhes, limites e outros ensaios de escala e falha estão no [runbook operacional](docs/product/operations-runbook.md) e no [runbook pós-MVP](docs/product/post-mvp-runbook.md).

## Limites da demonstração

- O cluster tem um nó e um broker Kafka; a recriação de pods comprova recuperação eventual, não alta disponibilidade.
- O processamento é pelo menos uma vez, com outbox e deduplicação por `event_id`. A ordem observada vale por tópico e partição no fluxo normal; replay e estacionamento não preservam posição original.
- A hipótese de concluir 95% dos pedidos sintéticos em até 5 s **não é um SLO aprovado**. Os alertas são experimentais e não enviam notificações externas.
- O sucesso dos comandos de API não substitui a inspeção visual do percurso Grafana. Consulte os [relatórios](docs/product/post-mvp-report.md) para saber quais passos foram efetivamente verificados.

O [relatório do Spike 0](docs/product/spike-0-report.md) registra a validação histórica da propagação Python → Kafka → Node → Kafka → Python. O [PRD](docs/product/prd.md), o [backlog](docs/product/backlog.md) e o [contrato Kafka](docs/architecture/kafka-contract.md) documentam escopo e decisões.

Commits usam `<tipo>: <descrição em português>`, com os tipos `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf` e `ci`.
