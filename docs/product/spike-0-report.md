# Spike 0 — Plataforma local e propagação poliglota

**Data:** 24/09/2026. **Estado:** Spike 0 aprovado pelos critérios técnicos do plano. **Recomendação: prosseguir para US-002.** A migração do domínio não foi iniciada.

## Entrega

Fluxo sintético Python → Kafka → duas réplicas Node → Kafka → Python, com instrumentação OpenTelemetry manual, contexto W3C persistido/reidratado, commit manual e produtores idempotentes. O processo de entrada termina antes do relay; o span producer é criado pelo relay após extrair o carrier. A simulação usa arquivo em volume temporário, sem banco ou outbox transacional.

A automação está em `scripts/spike.ps1`: `preflight`, `up`, `status`, `test`, `demo`, `down`. Todas as operações Kubernetes usam kubeconfig local e contexto explícito. Há limites de espera, exclusão mútua das ações, diagnóstico em falhas e verificação do OrderFlow por execução. Evidências e credenciais ficam em `.local/`, fora do Git e do cluster. O uso está em `README.md`.

## Versões fixadas

| Componente | Versão testada |
|---|---|
| Docker Desktop / servidor Docker | servidor 29.7.2, Linux containers |
| minikube / Kubernetes | 1.39.0 / 1.35.0 |
| Python / confluent-kafka | 3.12.12 / 2.12.0 |
| Node / cliente Kafka / TypeScript | 22.22.0 / 1.10.1 / 5.8.3 |
| Kafka KRaft | 4.2.1 |
| Collector Contrib | 0.153.0 |
| Tempo / Loki | 2.10.7 / 3.7.8 |
| Grafana / Prometheus | 13.2.2 / 3.14.0 |

Digests externos em `infra/images.lock.json`; bases dos probes nos Dockerfiles; dependências Python em requirements/constraints e Node em package-lock. O instalador valida o SHA-256 do minikube e não altera PATH global.

## Verificação funcional

- Contratos compartilhados: payload, key UTF-8, W3C, headers inválidos/duplicados/ausentes, descarte de tracestate inválido e preservação do válido. Testes de subprocessos comprovam reidratação sem contexto ativo nos dois runtimes.
- Tempo: mesma trace e relações parent/child entre criação, relay producer, processamento Node, producer Node e sink Python. Logs carregam trace/span/order/instância e `tracestate`.
- Kafka: cinco probes distintos para o mesmo pedido, sequência esperada e offsets crescentes verificados separadamente nos dois tópicos. Keys adicionais comprovam ambas as partições e as duas instâncias Node, sem sobreposição no cenário observado.
- Loki: API consultada com `X-Loki-Response-Encoding-Flags: categorize-labels`, distinguindo labels indexados de structured metadata. Apenas serviço, namespace e ambiente são labels estáveis; IDs não são indexados.
- Grafana: cliques reais Tempo → Related logs → expansão do log → View trace, retornando ao trace `7167cf871372d8c7b3546e99d3579b14`. Registro em `.local/evidence/ui-verification.json` e quatro capturas em `output/playwright/`.
- Falha controlada antes do publish/commit: no primeiro ensaio, mensagem na partição 1, offset 25; commit permaneceu 25 durante a falha e avançou a 26 somente após redelivery e conclusão no sink. Evidência em `.local/evidence/recovery-20260924/recovery-verification.json`.
- Operação: dois `down` consecutivos concluídos; reconstrução integral concluída em `.local/evidence/20260924T214400692-up`. Evidências anteriores sobreviveram ao teardown.

## Testes e segurança

Python: 22 testes, cobertura 93,59%, Ruff aprovado. Node: 30 testes, cobertura de linhas 91,73%, statements 90,47%, branches 89,74%, functions 86,36%; build, typecheck e lint aprovados. Verificador offline: 14 testes. Automação: stdout, exit code, stderr, timeout, porta ocupada e kubeconfig explícito.

Kustomize e configurações nativas do Collector, Loki, Tempo e Prometheus foram validados. Auditorias de dependências: npm sem vulnerabilidades; pip-audit sem vulnerabilidades conhecidas após atualização de pytest para 9.0.3, corrigindo PYSEC-2026-1845. Gitleaks não encontrou segredos nos arquivos entregáveis. Esses scans não constituem prova de segurança geral nem scan completo das camadas de todas as imagens.

Revisão independente concluída. Foram corrigidos uso de kubeconfig ambiente, verificação de ownership/readiness do port-forward, diagnóstico de todos os workloads e sobrescrita do snapshot inicial. A confirmação de recuperação filtra probe, pedido, tópico, partição e offset.

## Capacidade e inicialização

Host disponibilizou 12 CPUs e aproximadamente 15,18 GiB à VM Docker. O perfil exclusivo usa limites Docker de 4 CPUs e 8 GiB; a automação verifica esses limites e memória disponível, sem aumentar orçamento. Limites agregados dos workloads: 3,8 CPU / 5 GiB, incluindo bootstrap; PVCs: 5,25 GiB.

Na reconstrução fria, foram registradas 83 amostras entre 21:44:01 e 21:51:53 UTC; 51 já continham métricas do nó. Picos amostrados: **1.879 millicores de CPU e 2.145 MiB de memória**. Resumo em `.local/evidence/20260924T214400692-up/capacity-summary.json`.

Durante a reconstrução fria, houve espera por imagens, falhas transitórias de readiness e reinícios dos probes antes da disponibilidade do broker/tópicos. Depois do bootstrap e rollout todos os componentes ficaram prontos. As amostras de recursos são observações periódicas, não máximos contínuos ou SLO; percentuais de `kubectl top node` podem usar a capacidade da VM Docker, portanto devem ser interpretados junto aos limites do container minikube.

## Preservação e limites

OrderFlow permaneceu no HEAD `03abf2775cab1a8cd5bb355f656cf0457db5cbd7`, com status, diff e hashes dos cinco arquivos alterados/não rastreados iguais ao snapshot inicial. Comparação em `.local/evidence/orderflow-comparison.json`. Nenhuma restauração foi usada. Git local inicializado no Observa, sem remoto, stage, commit ou push.

A prova vale para o cenário controlado por tópico/partição. Não demonstra exactly-once, idempotência de negócio, alta disponibilidade ou segurança sob qualquer rebalanceamento. O armazenamento dos backends é local e descartado no teardown. Não foram implementados domínio, Postgres, outbox real, DLT, HPA, cloud, dashboard definitivo ou métricas → traces.

O campo `spike_approved: false` do verificador offline é intencional: ele só julga evidência estrutural e não substitui os gates de UI, recuperação, reconstrução, capacidade e preservação documentados aqui.

## Encerramento e evidência final

A rodada completa sobre o cluster reconstruído terminou com sucesso em `.local/evidence/20260924T215341041-test`: testes unitários, verificador, isolamento por containers distintos com término da entrada anterior ao relay, integração Tempo/Loki, scrapes do Collector e três probes, ordenação e recuperação. O segundo `up` terminou em `.local/evidence/20260924T215233768-up`; o diagnóstico negativo/restauração em `.local/evidence/diagnostic-test`; o status final saudável em `.local/evidence/20260924T215545159-status`.

Na repetição da falha, partição 1, offset 5: commit antes 5, após recuperação 6. No teste completo, 23 amostras registraram picos de 1103 millicores e 2466 MiB. Não foram encontrados OOM nos estados/eventos capturados. Todos os containers persistentes do status final estavam prontos e com zero reinícios após os rollouts; isso não apaga os reinícios transitórios observados na inicialização fria.

Índice do aceite: `.local/evidence/acceptance.json`. O cluster foi deixado pronto, dentro do orçamento, sem port-forwards da automação ativos. Para liberar recursos, execute `./scripts/spike.ps1 down`; os arquivos de evidência permanecem disponíveis.
