# Incrementos pós-MVP locais

Execute os comandos em PowerShell 7 na raiz, com o perfil `observa-spike0` pronto. Eles usam apenas pedidos sintéticos, o namespace dedicado e evidências ignoradas pelo Git em `.local/evidence/`. Não execute ações do cluster em paralelo. `./scripts/mvp.ps1 down` remove o perfil e seus dados sintéticos ao terminar.

## Qualidade e relays

`./scripts/check.ps1` executa build, lint, tipos, testes, auditorias, scan de segredos, renderização de manifests e regras Prometheus. A fase `relay-db` sobe um PostgreSQL descartável e executa os testes de concorrência e redelivery dos três relays; também pode ser repetida isoladamente com `-Only relay-db`. Se o host não tiver Node 22, os testes Node usam as imagens fixadas pelo repositório. `./scripts/mvp.ps1 contract` exercita contratos Kafka e o harness Postgres.

Os relays Order, Payment e Inventory reivindicam uma linha pendente por vez com `FOR UPDATE SKIP LOCKED`, mantêm a transação até o ACK do Kafka e só então confirmam `published_at`. Se o ACK ocorrer e o commit falhar, a linha volta a ser elegível e a mensagem pode ser publicada de novo. Os consumidores continuam deduplicando por `event_id`; não há promessa de exatamente uma vez ponta a ponta.

## Escala optativa

Após validar os três relays com PostgreSQL real, execute:

```powershell
./scripts/mvp.ps1 up
./scripts/scale-apply.ps1 -RelayConcurrencyVerified
./scripts/scale-verify.ps1 -Count 300 -RatePerSecond 40 -TimeoutSeconds 600 -CreateLagBacklog
```

`scale-apply` exige uma réplica pronta por serviço e instala [KEDA v2.21.0](https://keda.sh/docs/2.21/deploy/) somente no perfil minikube dedicado. O manifesto oficial é baixado para `.local/keda/` e conferido por SHA-256 antes de `kubectl apply --server-side`. Quatro [ScaledObjects Kafka](https://keda.sh/docs/2.21/scalers/apache-kafka/) geram HPAs com CPU (70%) e lag por consumer group (alvo de cinco mensagens). O exportador de lag permanece como medição independente. Consulte `kubectl --kubeconfig .local/kubeconfig --context observa-spike0 -n observa-spike0 get scaledobjects,hpa` para inspecionar os triggers.

`scale-verify -CreateLagBacklog` pausa temporariamente o Payment pelo próprio KEDA em zero réplicas, envia pelo menos 250 pedidos sintéticos com estoque indisponível e espera o lag passar de 200. Para isolar a causa da escala, remove temporariamente o gatilho de CPU do Payment e limita esse consumidor a 50m; ao final restaura CPU (70%), limite (300m) e pausa, inclusive se houver erro. A verificação exige um evento `SuccessfulRescale` atribuído à métrica externa Kafka, retorno a uma réplica, lag zero nos quatro grupos e efeitos persistidos únicos. Para repetir apenas a análise de uma execução, use `-CreateLagBacklog -EvidenceDirectory .local/evidence/scale-<id>`. Não execute outra carga ou experimento durante esta janela. O tópico tem duas partições e o overlay limita cada serviço a duas réplicas. A ordem continua qualificada por partição no caminho normal; a pausa é apenas um experimento local, não uma configuração permanente.

## Baseline, gateway e falhas controladas

O [runbook operacional](operations-runbook.md) define os SLIs, a hipótese experimental de 5 s e os dois alertas Prometheus. Meça novamente com `./scripts/operations-baseline.ps1 -Rounds 10`; os thresholds não são SLO de produção.

`npm --prefix services/payment run gateway:demo` mostra retry com backoff de 50/100 ms, o bloqueio com breaker `open` e a recuperação para `closed` após o cooldown em um relógio determinístico. `./scripts/gateway-verify.ps1` injeta as duas primeiras falhas sintéticas no Payment local, executa os três cenários, verifica métricas e uma linha de pagamento por pedido, e restaura as variáveis do Deployment. O gateway não envia cobranças externas. A configuração por variáveis de ambiente fica restrita ao experimento; `PAYMENT_GATEWAY_FAIL_FIRST=0` é o padrão.

Com o cluster saudável e sem outra carga, execute separadamente `./scripts/chaos.ps1 -Target payment` e `./scripts/chaos.ps1 -Target kafka`. Cada experimento registra hipótese, raio de impacto, condição de parada, UID antigo/novo, resultado dos três cenários e contagem dos pagamentos. A prova cobre recuperação **após** a recriação do pod. Ela não afirma continuidade sem interrupção durante a indisponibilidade do broker nem redelivery de uma mensagem já em processamento.

## Diagnóstico e encerramento

Siga o [percurso Grafana](mvp-runbook.md#diagnóstico-por-métrica-trace-e-log) após `demo`: exemplar de duração → trace com quatro serviços → **Related logs** no Loki → **View trace**. Os IDs concretos e as evidências brutas ficam em `.local/`, fora do Git. Registre qualquer falha antes de repetir um experimento; não classifique um cenário como aprovado apenas pelo comando de subida.
