import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import Counter

from .contract import sanitize_headers, validate_payload
from .telemetry import log

PROPAGATOR = TraceContextTextMapPropagator()
PROCESSED = Counter("observa_probe_processed_total", "Messages successfully processed")
STARTED = "observa.probe.started.v1"
COMPLETED = "observa.probe.completed.v1"


def parent_context(headers):
    carrier, warning = sanitize_headers(headers)
    if warning:
        log(warning, level="WARNING")
    return PROPAGATOR.extract(carrier, context=Context())


def entry(directory, count, order_id, tracer, probe_id=None):
    if probe_id is not None and count != 1:
        raise ValueError('explicit probe_id requires count=1')
    if not 1 <= count <= 1000:
        raise ValueError("count must be between 1 and 1000")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    probes = []
    for index in range(count):
        payload = validate_payload({"probe_id": probe_id or str(uuid4()), "order_id": order_id,
                                    "step": "started", "occurred_at": datetime.now(timezone.utc).isoformat()})
        with tracer.start_as_current_span("probe.create", context=Context(), attributes=payload) as span:
            carrier = {}
            PROPAGATOR.inject(carrier)
            carrier['tracestate'] = 'observa=spike0'
            record = {"payload": payload, "carrier": carrier, "creator_pid": os.getpid()}
            path = directory / f"{index:04d}-{payload['probe_id']}.json"
            path.write_text(json.dumps(record), encoding="utf-8")
            probes.append({**payload, "trace_id": f"{span.get_span_context().trace_id:032x}",
                           "span_id": f"{span.get_span_context().span_id:016x}", "file": path.name})
            log("persisted", **payload)
    manifest = {"creator_pid": os.getpid(), "probes": probes}
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    log('entry_manifest', **manifest)
    return manifest


def publish(producer, payload, tracer, headers):
    with tracer.start_as_current_span("probe.publish", context=parent_context(headers),
                                     kind=SpanKind.PRODUCER, attributes={**payload, "messaging.destination.name": STARTED}):
        carrier = {}
        PROPAGATOR.inject(carrier)
        result = []
        def delivered(error, message):
            result.append((error, message))
        producer.produce(topic=STARTED, key=payload["order_id"].encode(),
                         value=json.dumps(payload).encode(), headers=list(carrier.items()), on_delivery=delivered)
        remaining = producer.flush(30)
        if remaining or not result or result[0][0] is not None:
            raise RuntimeError("Kafka publish not acknowledged")
        message = result[0][1]
        log("published", **payload, topic=STARTED, partition=message.partition(), offset=message.offset())


def relay(directory, producer, tracer):
    if trace.get_current_span().get_span_context().is_valid:
        raise RuntimeError("relay requires no active span")
    log("relay_started", active_context=False)
    manifest = json.loads((Path(directory) / "manifest.json").read_text(encoding="utf-8"))
    for item in manifest["probes"]:
        filename = item["file"]
        if Path(filename).name != filename:
            raise ValueError("invalid record filename")
        record = json.loads((Path(directory) / filename).read_text(encoding="utf-8"))
        publish(producer, validate_payload(record["payload"]), tracer, list(record["carrier"].items()))


def consume_one(consumer, message, tracer):
    payload = validate_payload(json.loads(message.value()))
    if payload["step"] != "completed" or message.key() != payload["order_id"].encode():
        raise ValueError("unexpected step or Kafka key")
    details = {**payload, "topic": message.topic(), "partition": message.partition(), "offset": message.offset()}
    with tracer.start_as_current_span("probe.consume", context=parent_context(message.headers()),
                                     kind=SpanKind.CONSUMER, attributes=details):
        log("process_start", **details)
        log("process_end", **details)
        consumer.commit(message=message, asynchronous=False)
        PROCESSED.inc()


