import json
import os
from datetime import datetime, timezone

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON


def log(event, level="INFO", **fields):
    context = trace.get_current_span().get_span_context()
    print(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "level": level,
                      "event": event, "message": event, "service_name": os.getenv("SERVICE_NAME", "probe-python"),
                      "service_instance_id": os.getenv("HOSTNAME", str(os.getpid())),
                      "tracestate": context.trace_state.to_header(), "process_id": os.getpid(), "trace_id": f"{context.trace_id:032x}",
                      "span_id": f"{context.span_id:016x}", **fields}), flush=True)


def configure(service):
    provider = TracerProvider(sampler=ALWAYS_ON, resource=Resource.create({
        "service.name": service, "service.instance.id": os.getenv("HOSTNAME", str(os.getpid()))}))
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318").rstrip("/")
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=10)))
    return provider

