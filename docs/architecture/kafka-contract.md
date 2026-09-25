# Contrato operacional Kafka v1

## Evento e roteamento

`order.events.v1` carrega o envelope e os payloads JSON v1 de `contracts/schemas/`, copiados do OrderFlow como referência versionada. O valor não recebe campos de transporte ou telemetria. A key é o UUID `correlation_id`, que na jornada do pedido é o `order_id`, codificado em UTF-8. Cada serviço que precisa observar um evento usa um consumer group próprio; réplicas do mesmo serviço compartilham seu group.

Os producers Python e Node configuram `murmur2_random` explicitamente. O default de librdkafka usa outro hash; só a key igual não garante mesma partição entre runtimes.

Os headers `traceparent` e, quando presente, `tracestate` carregam W3C Trace Context. A outbox persiste o carrier em coluna JSONB separada do envelope. O relay inicia sem contexto ativo, extrai esse carrier, cria o span producer, injeta novos headers e espera o ack do Kafka antes de marcar a linha publicada. IDs de negócio ou de trace não são labels Loki.

## Entrega e commit

Os produtores usam idempotência e confirmação forte. Cada consumidor processa uma partição sequencialmente, valida envelope e key e passa um evento imutável ao handler. O handler grava efeito de negócio e `processed_events(event_id)` na mesma transação. Se o `event_id` já existe, não repete o efeito. O adapter confirma o próximo offset somente após o commit do handler, inclusive no redelivery deduplicado.

Um crash entre o commit do banco e o commit do offset gera redelivery esperado; não implica um segundo efeito lógico. A outbox e o commit Kafka não formam uma transação distribuída, portanto não há promessa de exactly-once ponta a ponta.

Registros permanentemente inválidos são publicados em `order.events.v1.parked`, com bytes originais, tópico, partição, offset e motivo. O offset de origem só é confirmado após ack do estacionamento. Uma falha transitória não confirma o offset: o consumidor interrompe ou pausa a partição para recuperação operacional. Replay do tópico estacionado é operação explícita; não restaura a posição original do evento.

## Limite da ordenação

No caminho normal, todos os eventos de um pedido usam a mesma key no mesmo tópico, logo vão à mesma partição. Um group que processa essa partição sequencialmente observa offsets crescentes. Essa afirmação não se estende a outros tópicos, ações externas, estacionamento e replay, nem a processamento paralelo introduzido por um handler. Redelivery pode repetir um offset; idempotência por `event_id` evita repetir o efeito de negócio.

## Evidência necessária

- Testes de contrato nos dois runtimes comprovam envelope v1, key UTF-8 e headers W3C com a fixture compartilhada.
- Testes de integração com Kafka real comprovam producer ack, grupos distintos, partição/offset e commit manual.
- Teste Postgres força crash após o commit transacional e antes do offset; o redelivery mantém um único efeito.
- Testes de erro comprovam ack antes do commit para estacionamento e ausência de commit em falha transitória.
