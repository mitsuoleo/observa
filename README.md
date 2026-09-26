# Observa

Laboratório local de uma jornada de pedidos orientada a eventos. Order (Python), Payment (Node), Inventory (Node) e Notification (Python) usam Kafka e Postgres; métricas, traces e logs são explorados no Grafana. Os pagamentos, estoques e clientes são sintéticos. O OrderFlow é uma referência de contrato somente leitura.

## O que demonstrar

| Cenário | Entrada | Resultado esperado |
|---|---|---|
| Pedido concluído | Pagamento aprovado, estoque reservado | `CONFIRMED`, com `payment.approved`, `stock.reserved` e `order.completed` |
| Pagamento recusado | Pagamento rejeitado | `FAILED`, com `payment.rejected` e `order.failed` |
| Estoque indisponível | Pagamento aprovado, estoque indisponível | `CANCELLED`, com compensação `payment.refunded` |

`demo` executa os três cenários e grava resultados em `.local/evidence/`. `recovery` recria um pod Inventory durante outro pedido e verifica o estado final, a timeline e uma única reserva persistida. A [evidência resumida](docs/portfolio/evidence-2026-09-25.md) contém apenas estados e contagens, sem IDs ou credenciais.

## Reproduzir o MVP

Use Windows com PowerShell 7, Git, Docker Desktop em Linux containers e `kubectl`. Disponibilize ao Docker **4 CPUs, 8 GiB de memória e 20 GiB livres** para imagens e evidências. O instalador usa ferramentas locais em `.tools/`; nenhuma cloud é necessária. Os comandos de `up` e `down` manipulam somente o perfil minikube `observa-spike0`; `down` apaga seus dados sintéticos. Não execute ações do cluster em paralelo.

```powershell
./scripts/install-tools.ps1
./scripts/mvp.ps1 up
./scripts/mvp.ps1 status
./scripts/mvp.ps1 contract
./scripts/mvp.ps1 demo
./scripts/mvp.ps1 recovery
./scripts/mvp.ps1 down
```

Para checks sem cluster, execute `./scripts/check.ps1`. Ele instala dependências Node via lockfiles, executa build, lint, tipos e testes, roda os testes Python em imagens Docker, verifica os relays em PostgreSQL descartável, constrói as imagens de runtime, audita dependências de produção, procura segredos no histórico Git e na árvore atual e renderiza os manifests offline. Use, por exemplo, `./scripts/check.ps1 -Only manifests` para repetir uma etapa; as opções são `node`, `python`, `relay-db`, `images`, `audit`, `secrets`, `manifests` e `rules`. Exige Docker, Python, `kubectl`, Git e Gitleaks (no `PATH` ou em `.tools/gitleaks/`). Se o host não tiver Node 22, a fase Node usa as imagens de teste fixadas pelo projeto. O [workflow de CI](.github/workflows/check.yml) executa o mesmo comando sem cluster. Os contratos Kafka e o harness Postgres permanecem em `contract`.

O [runbook pós-MVP](docs/product/post-mvp-runbook.md) descreve escala optativa com KEDA por CPU e lag Kafka, baseline, gateway sintético e falhas controladas. O [relatório desta revisão](docs/product/post-mvp-report.md) separa resultados validados dos limites.

O [runbook do MVP](docs/product/mvp-runbook.md) detalha comandos, cenários e limites. O [relatório de validação](docs/product/mvp-report.md) registra as medições históricas. Os arquivos brutos e credenciais ficam em `.local/` e não devem ser versionados.

## Spike 0

Os probes Python → Kafka → Node → Kafka → Python validaram Kubernetes local, OpenTelemetry e navegação Grafana trace ↔ logs antes dos serviços de domínio.

## Spike 0 e pré-requisitos históricos

Windows, PowerShell 7, Git, Docker Desktop em modo Linux e kubectl. O Docker precisa disponibilizar 4 CPUs e 8 GiB; reserve 20 GiB livres para imagens/evidências. Downloads usam registros públicos, sem cloud obrigatória. Nenhum comando altera o PATH global ou o kubeconfig pessoal.

```powershell
./scripts/install-tools.ps1
./scripts/spike.ps1 preflight
./scripts/spike.ps1 up
./scripts/spike.ps1 test -UnitOnly
./scripts/spike.ps1 test
./scripts/spike.ps1 status
./scripts/spike.ps1 down
```

`test` executa unitários, integração por Kafka/Tempo/Loki e falha antes do publish/commit. `demo` repete apenas o cenário de propagação/ordenação. `up` constrói/carrega imagens locais e usa exclusivamente o perfil/namespace `observa-spike0`. `down` apaga esse perfil, inclusive dados de demonstração. Evidências em `.local/evidence/` são preservadas. Não execute `up`, `test`, `demo` ou `down` simultaneamente.

## Diagnóstico: métrica → trace → log

Após `up`, abra um terminal PowerShell na raiz:

```powershell
$env:KUBECONFIG = Join-Path (Get-Location) '.local/kubeconfig'
kubectl --context observa-spike0 -n observa-spike0 port-forward service/grafana 13000:3000 --address 127.0.0.1
```

Abra [o dashboard do MVP](http://127.0.0.1:13000/d/observa-mvp). As credenciais locais geradas estão em `.local/grafana-secret.json` (não versionado). Após `demo`, escolha os últimos 15 minutos e observe eventos, erros, duração e efeitos de negócio. Abra um exemplar de duração do Order para seguir à trace no Tempo; de lá, abra **Related logs** no Loki e use **View trace** em um log para retornar. Para investigar a recusa de pagamento, filtre os logs pelo pedido ou trace registrado em `.local/evidence/mvp-*-demo/scenarios.json`. Encerre o port-forward com Ctrl+C antes de `down`.

O sucesso da API não certifica os cliques de UI. O verificador também não afirma aprovação completa sem teste de recuperação, reconstrução e preservação do OrderFlow.

## Contratos e limites

- Tópicos `observa.probe.started.v1` e `observa.probe.completed.v1`: duas partições, replicação 1; key = `order_id` UTF-8.
- Payload: `probe_id`, `order_id`, `step`, `occurred_at`. W3C Trace Context fica nos headers; fixtures em `tests/fixtures`.
- Init container cria/persiste contexto e termina; outro container lê o carrier e publica. O volume temporário não é uma outbox transacional.
- Duas réplicas Node dividem partições. O commit ocorre após confirmação do publish; entrega pelo menos uma vez permite duplicatas em falhas posteriores ao publish.
- Ordenação é comprovada separadamente por tópico/partição, sem promessa global ou exatamente uma vez.
- O cluster single-node/single-broker não oferece alta disponibilidade. As medições de demonstração não definem um SLO. Escala horizontal, HPA e falhas controladas têm critérios próprios no backlog; consulte o runbook para o estado validado da execução atual.

## Estrutura

- `probes/`: aplicações, contratos, Dockerfiles e testes por runtime.
- `infra/`: manifests, configurações e digests externos fixados.
- `scripts/`: automação PowerShell e teste controlado de recuperação.
- `tests/evidence/`: auditoria offline de respostas Tempo/Loki.
- `docs/product/`: escopo, handoff e relatório do spike.

Consulte `docs/product/spike-0-report.md` para a evidência histórica do Spike 0 e o runbook do MVP para a jornada atual.

## Convenção de commits

Use `<tipo>: <descrição em português>`, com os tipos convencionais `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `perf` e `ci`. Exemplo: `feat: adicionar confirmação de pedidos`.
