# Implementation Brief — US-001: Spike 0 de plataforma e propagação poliglota

**Estado:** Spike 0 implementado e validado em 24/09/2026; ver [relatório de aceite](spike-0-report.md).  
**Natureza:** spike com código descartável ou evolutivo apenas dentro do Observa  
**Projeto-base:** `D:\Work\OrderFlow`, referência somente leitura

## 1. Objetivo e evidência esperada

Validar, antes da migração de domínio, que o ambiente local consegue executar a plataforma escolhida e que contexto OpenTelemetry atravessa Kafka entre Python e Node mesmo quando a publicação ocorre depois que o contexto ativo original deixou de existir.

O spike é aprovado quando produz:

1. uma trace Python → Kafka → Node → Kafka → Python no Tempo;
2. navegação trace → logs e logs → trace no Grafana;
3. registros de `order_id`, tópico, partição e offset que demonstrem a ordem observada;
4. evidência de duas réplicas no mesmo consumer group sem processamento simultâneo da mesma partição;
5. snapshot de CPU, memória e tempos de readiness dos componentes;
6. comandos reproduzíveis de subida, verificação, demonstração e teardown;
7. prova de que o estado inicial do OrderFlow permaneceu inalterado.

## 2. Referências obrigatórias

- **Objetivos:** OBJ-002, OBJ-003 e OBJ-004.
- **Requisitos:** RF-003, RF-004 e RF-006; RNF-002, RNF-004 e RNF-006.
- **Decisões:** DEC-001, DEC-003 a DEC-007, DEC-009 e DEC-010.
- **Riscos:** HYP-001, HYP-002, RSK-001, RSK-003, RSK-004 e RSK-006.
- **Contexto factual:** `current-state.md`.

## 3. Escopo

### Incluído

- Minikube single-node com driver Docker.
- Apache Kafka em modo KRaft com um broker.
- OpenTelemetry Collector.
- Prometheus, Tempo, Loki e Grafana em configuração local enxuta.
- Probes independentes:
  - entrada/producer em Python;
  - consumer/producer em Node;
  - consumer final em Python.
- Tópico de probe separado do futuro tópico de domínio, com ao menos duas etapas de mensagem.
- Key sintética chamada `order_id` para exercitar o mesmo particionamento pretendido.
- Headers W3C `traceparent` e `tracestate` quando presente.
- Duas réplicas do consumer Node no mesmo consumer group.
- Logs JSON e dados de partição/offset.
- Automação local de up, status, demo e down.
- Testes escritos antes da implementação para contratos críticos.

### Excluído

- Copiar ou modificar qualquer arquivo do OrderFlow.
- Migrar Order, Payment, Inventory ou Notification.
- Reutilizar schemas ou regras de negócio do OrderFlow nos probes.
- Implementar banco/outbox de produção; o spike simula apenas a persistência e reidratação do carrier.
- Dashboard definitivo, SLO, alertas, HPA, circuit breaker, chaos amplo ou CI de imagens.
- Alta disponibilidade de Kafka/LGTM.
- Exposição pública ou dependência cloud/paga.

## 4. Contrato mínimo do spike

### Mensagem

O probe usa um payload próprio e pequeno, sem se apresentar como contrato de domínio:

```json
{
  "probe_id": "uuid",
  "order_id": "uuid",
  "step": "started|completed",
  "occurred_at": "RFC3339"
}
```

- A key Kafka é o `order_id` serializado como string.
- `traceparent` deve existir nos dois publishes; `tracestate` é preservado quando recebido.
- Campos de tracing não são adicionados ao payload.
- O agente pode escolher nomes dos tópicos de probe, desde que sejam versionados, distintos de `order.events.v1` e documentados.
- Producer e consumer precisam expor tópico, partição, offset, key e consumer group como atributos de span/log quando semanticamente aplicável.

### Topologia de trace

1. Python recebe/dispara a operação e cria o span raiz.
2. Python serializa o carrier do span raiz em arquivo interno, exporta a telemetria e encerra o processo de entrada.
3. Um processo relay separado inicia sem contexto ativo, reidrata o carrier, cria o span producer e injeta o contexto desse span na primeira mensagem; isso simula o atraso da outbox.
4. Node extrai o carrier e cria span de consumer/process.
5. Node produz a segunda mensagem com contexto propagado.
6. Python extrai e cria o span final de consumer/process.

Uma única trace, com o mesmo `trace_id`, deve conter todos esses spans. Como o spike processa mensagens individualmente, o contexto de criação da mensagem deve ser usado como parent do span de processamento. Span links podem ser registrados adicionalmente, mas não substituem a trace única exigida pelo produto.

### Logs

Cada processo escreve JSON em stdout com, no mínimo:

- `timestamp`, `level`, `message`;
- `service_name` e `service_instance_id`;
- `order_id`, `trace_id` e `span_id`;
- `messaging_system`, tópico, partição, offset e consumer group quando disponíveis.

`trace_id`, `span_id`, `order_id`, partição e offset não devem virar labels Loki de alta cardinalidade. Eles ficam no corpo/metadado estruturado. Labels estáveis podem incluir `service_name`, namespace e ambiente.

### Telemetria

- Aplicações enviam OTLP ao Collector, não diretamente a Tempo/Loki.
- Logs de stdout são coletados pelo OTel Collector e enviados ao endpoint OTLP nativo do Loki.
- O Collector exporta traces para Tempo.
- Prometheus coleta saúde e métricas mínimas da plataforma/probes suficientes para confirmar o pipeline; a ligação métrica → trace completa será fechada em US-006.
- Grafana recebe datasources provisionados como código, incluindo links trace → logs e logs → trace.

## 5. Referências do OrderFlow, sem autorização de alteração

O agente pode consultar:

- `schemas/envelope.v1.json` para entender a separação entre payload e metadados;
- `packages/py-common/orderflow_messaging/rabbit.py` para compreender o atraso do relay atual, não para copiar o adapter;
- `services/payment-service/src/rabbit.ts` e `services/inventory-service/src/rabbit.ts` para comparar responsabilidades de mensageria;
- `packages/py-common/orderflow_messaging/logging.py` e `services/*/src/logger.ts` para observar o formato de logs atual;
- `CONTEXT.md` para preservar que `correlation_id` representa `order_id`.

Esses arquivos são evidência e referência. O spike deve ser implementado do zero no Observa e não pode escrever no OrderFlow.

## 6. Ordem de trabalho

1. **Preflight:** capturar versões e disponibilidade de Docker, minikube, kubectl e ferramenta de automação; capturar `git status --porcelain` e hashes dos arquivos modificados/não rastreados do OrderFlow.
2. **Testes RED:** criar testes de serialização da key, injeção/extração W3C e reidratação do carrier em Python e Node; validar que inicialmente falham pelo comportamento ausente.
3. **Contrato dos probes:** implementar o mínimo para deixar os testes unitários verdes, sem cliente Kafka real.
4. **Kafka isolado:** subir minikube e Kafka KRaft; validar health, tópico, key, groups, partição e commit.
5. **Fluxo poliglota:** conectar os três processos e verificar duas mensagens com o mesmo `order_id`.
6. **Tracing:** adicionar spans producer/consumer e o intervalo sem contexto ativo; validar a trace no backend.
7. **Logs:** coletar stdout pelo Collector, exportar ao Loki e configurar links bidirecionais no Grafana.
8. **Concorrência controlada:** executar duas réplicas Node no mesmo group, produzir sequência conhecida e registrar owner/partição/offset.
9. **Automação e recursos:** consolidar comandos idempotentes, readiness, timeout, diagnóstico de falha, consumo e teardown.
10. **Revisão:** executar todos os testes, comparar o snapshot final do OrderFlow com o inicial e produzir relatório curto do spike.

O agente deve manter cada slice verificável e não avançar para migração de domínio dentro deste incremento.

## 7. Critérios de aceite

### Plataforma

- Um comando inicia ou cria a plataforma e termina somente quando os componentes necessários estão prontos.
- Segunda execução é idempotente ou falha com orientação explícita e sem corromper o ambiente.
- Kafka reporta modo KRaft e um broker; nenhuma dependência ZooKeeper é criada.
- Grafana, Prometheus, Tempo e Loki respondem aos health checks definidos.
- O comando de teardown remove apenas recursos do Observa/minikube escolhido e é documentado.

### Mensageria e ordem observada

- Para uma sequência de pelo menos cinco mensagens do mesmo `order_id`, os registros mostram a mesma partição e offsets monotonicamente crescentes em cada tópico de probe.
- Duas réplicas Node no mesmo group não processam simultaneamente a mesma partição.
- Uma segunda key pode ser distribuída independentemente; o teste não afirma ordem entre keys, tópicos ou partições.
- Falha de processamento não é silenciosamente confirmada.

### Propagação e correlação

- `traceparent` é injetado, persistido como carrier, reidratado e extraído nos dois runtimes.
- A trace no Tempo contém os spans dos três processos e os publishes/processamentos Kafka.
- Todos os logs do caso contêm `order_id` e, quando existe span ativo, `trace_id`/`span_id` corretos.
- A partir do span abre-se a consulta Loki filtrada; a partir do log abre-se a trace correta.
- Um cenário sem `traceparent` inicia uma nova trace e registra o fato sem quebrar o consumo.
- Um carrier inválido é tratado de forma definida e observável, sem confiar em entrada externa inválida.

### Capacidade e preservação

- O relatório registra CPU/memória observadas, tempo de subida/readiness, versões e falhas encontradas, sem convertê-los em SLO.
- O relatório recomenda continuar, ajustar o perfil ou parar, com evidência.
- O snapshot final do OrderFlow é idêntico ao inicial, inclusive preservando alterações locais que já existiam.

## 8. Verificações obrigatórias

- Testes unitários Python e Node dos carriers e contratos.
- Teste de integração com Kafka real para key, headers, groups e offsets.
- Teste de propagação após encerramento do contexto ativo.
- Consulta automática/API que confirme a trace no Tempo e os logs no Loki, além da inspeção visual.
- Validação da configuração do Collector e datasources Grafana.
- Scan de segredos e revisão do diff do Observa.
- Comparação de snapshot do OrderFlow antes/depois.

Nenhuma verificação pode ser declarada aprovada sem ter sido executada. Falhas ambientais devem ser distinguidas de falhas de implementação.

## 9. Riscos e condições de parada

Pare o incremento e devolva evidências ao PO/usuário se ocorrer qualquer condição abaixo:

1. Docker daemon não está acessível ao usuário executor ou minikube não inicia com o driver Docker.
2. A stack não estabiliza mesmo após um perfil mínimo documentado, ou pressiona a máquina a ponto de invalidar a demonstração.
3. Os clientes Kafka escolhidos não oferecem key, headers, consumer groups e controle de commit necessários.
4. O contexto W3C diverge entre Python e Node ou não sobrevive à reidratação.
5. Cumprir a jornada exige cloud, serviço pago, mudança do objetivo central ou alteração do OrderFlow.
6. Qualquer ferramenta exige versionar credencial real.

Uma condição de parada gera relatório de hipótese, tentativa, evidência e alternativas. Ela não autoriza migrar serviços nem trocar silenciosamente uma decisão confirmada.

## 10. Fronteiras de decisão

O agente de desenvolvimento pode decidir:

- organização interna, nomes locais e linguagem de automação;
- versões compatíveis e fixadas das ferramentas;
- clientes Kafka/OTel de Python e Node;
- nomes dos tópicos de probe e fixtures;
- estratégia de testes e formato do relatório;
- manifests ou charts, desde que a configuração resultante permaneça versionada e reproduzível.

O agente deve retornar ao usuário antes de:

- mudar minikube, Kafka KRaft single broker ou a stack LGTM;
- reduzir a jornada poliglota ou remover a quebra/reidratação do contexto;
- introduzir serviço cloud/pago ou exposição pública;
- mudar autenticação/autorização;
- alterar API pública ou contrato de evento do OrderFlow;
- modificar/copiar o OrderFlow;
- ampliar o spike para migração dos serviços de domínio;
- contrariar qualquer decisão confirmada do PRD.

## 11. Definition of Done do incremento

- Todos os critérios de aceite aplicáveis estão aprovados e ligados a evidências.
- Testes unitários e de integração do spike passam.
- Up/status/demo/down são reproduzíveis e documentados.
- Trace e links bidirecionais com logs foram verificados por API e visualmente.
- Ordem observada, ownership da partição e offsets foram registrados sem exagerar a garantia.
- Recursos e readiness foram medidos.
- Não há segredos reais no diff.
- OrderFlow está idêntico ao snapshot inicial.
- Relatório recomenda explicitamente prosseguir para US-002, ajustar o perfil ou interromper, com justificativa.
