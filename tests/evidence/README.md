# Captured evidence audit

Run `python tests/evidence/verify.py --directory .local/evidence/RUN`.
Run tests: `python -m unittest discover -s tests/evidence -v`.
Only Python standard library is required. Exit 0 means structural checks
passed, never approval of Spike 0 or completion of Grafana UI checks.
Missing or malformed evidence returns exit 1 and JSON errors on stdout.

The directory contains three UTF-8 JSON files (BOM accepted):

- `manifest.json`: `{ "probe_ids": ["p1","p2","p3","p4","p5"], "order_id": "order-1", "topics": ["observa.probe.started.v1", "observa.probe.completed.v1"], "expected_tracestate": "vendor=value" }`.
  IDs are in expected publication order for the same order. Additional probes
  with other orders may demonstrate both Node instances.
- `traces.json`: array of raw Tempo responses containing `resourceSpans` or
  `batches`, resource attribute `service.name`, and `scopeSpans` or
  `instrumentationLibrarySpans`. Spans carry attribute `probe_id` or `probe.id`,
  `traceId`, `spanId`, `parentSpanId`. Hex/protobuf base64 IDs are supported.
  Services: `probe-entry`, `probe-relay`, `probe-node`, `probe-python-sink`.
  The parent chain must follow this order, allowing adjacent spans of the same
  service (Node processing and producer). Each probe has one root and sink;
  every selected span must belong to that chain.
- `loki.json`: raw successful query_range response, `data.result[].stream`
  and `values: [["nanoseconds", "JSON log"]]`. Capture the complete run without
  truncation. Logs contain `event` (`process_start`/`process_end`), `probe_id`,
  `order_id`, `topic`, integer `partition` and nonnegative `offset` (integer or canonical decimal string), `service_name`,
  `service_instance_id`, hex `trace_id` and `span_id`, and `tracestate`.

Node completion consumes started; sink completion consumes completed. Both
must preserve tracestate. Node intervals pair by topic/partition/offset/instance.
Every interval finishes, and intervals in one partition cannot overlap. At
least two instances complete work. Same-order offsets increase independently
per topic (gaps allowed), all on one partition in each topic. IDs cannot be
Loki stream labels.

Audit a successful controlled run. Keep intentional failure/redelivery evidence
separate: incomplete intervals and duplicated completions deliberately fail
this audit. The tool does not certify fault recovery, process isolation,
reconstruction, resource budget, OrderFlow preservation or real Grafana clicks.

Kafka offsets use arbitrary-precision Python integers for comparisons; digit strings
never pass through floating point. Leading zeros, signs and whitespace are rejected.
Optional Loki `values` third-element structured metadata is accepted and is not
interpreted as indexed stream labels.

Collect Loki query_range with `X-Loki-Response-Encoding-Flags: categorize-labels`; otherwise its default response flattens structured metadata into stream fields and cannot be used to prove indexed-label cardinality.
