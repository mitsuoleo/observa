# Observa — Spike 0

Probes Python → Kafka → Node → Kafka → Python em Kubernetes local, com OpenTelemetry e navegação Grafana trace ↔ logs. Este incremento não migra os serviços do OrderFlow.

## Pré-requisitos

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

## Navegação no Grafana

Após `up`, abra um terminal PowerShell na raiz:

```powershell
$env:KUBECONFIG = Join-Path (Get-Location) '.local/kubeconfig'
kubectl --context observa-spike0 -n observa-spike0 port-forward service/grafana 13000:3000 --address 127.0.0.1
```

Abra http://127.0.0.1:13000. As credenciais locais geradas estão em `.local/grafana-secret.json` (não versionado). Em Explore, selecione Tempo e informe um `trace_id` do `manifest.json` mais recente. Abra um span, siga o link de logs e use **View trace** em uma linha de log para retornar. Verifique a identidade do trace nos dois sentidos. Encerre o port-forward com Ctrl+C antes do teardown.

O sucesso da API não certifica os cliques de UI. O verificador também não afirma aprovação completa sem teste de recuperação, reconstrução e preservação do OrderFlow.

## Contratos e limites

- Tópicos `observa.probe.started.v1` e `observa.probe.completed.v1`: duas partições, replicação 1; key = `order_id` UTF-8.
- Payload: `probe_id`, `order_id`, `step`, `occurred_at`. W3C Trace Context fica nos headers; fixtures em `tests/fixtures`.
- Init container cria/persiste contexto e termina; outro container lê o carrier e publica. O volume temporário não é uma outbox transacional.
- Duas réplicas Node dividem partições. O commit ocorre após confirmação do publish; entrega pelo menos uma vez permite duplicatas em falhas posteriores ao publish.
- Ordenação é comprovada separadamente por tópico/partição, sem promessa global ou exatamente uma vez.
- O cluster single-node/single-broker não oferece alta disponibilidade. Não há exposição pública, serviços de domínio, DLT, SLO ou HPA.

## Estrutura

- `probes/`: aplicações, contratos, Dockerfiles e testes por runtime.
- `infra/`: manifests, configurações e digests externos fixados.
- `scripts/`: automação PowerShell e teste controlado de recuperação.
- `tests/evidence/`: auditoria offline de respostas Tempo/Loki.
- `docs/product/`: escopo, handoff e relatório do spike.

Consulte `docs/product/spike-0-report.md` para distinguir o que foi implementado do que foi efetivamente verificado.
