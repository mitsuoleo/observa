"""Kafka consumer, health endpoint and telemetry for Notification."""

import json
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, Producer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram, generate_latest

from observa_messaging import KafkaSubscriber, consumer_config, producer_config

from .service import handle_notification


PROCESSED = Counter("observa_events_processed_total", "Processed domain events", ["service", "outcome"])
DURATION = Histogram("observa_processing_duration_seconds", "Processing duration", ["service"])
ERRORS = Counter("observa_errors_total", "Runtime errors", ["service", "operation"])
state = {"ready": False, "stop": False}


def log(event: str, **fields) -> None:
    span = trace.get_current_span().get_span_context()
    print(json.dumps({"event": event, "service_name": "notification-service",
                      "service_instance_id": os.getenv("HOSTNAME", "local"),
                      "trace_id": f"{span.trace_id:032x}" if span.is_valid else None,
                      "span_id": f"{span.span_id:016x}" if span.is_valid else None, **fields}, default=str), flush=True)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/ready"):
            status = 200 if state["ready"] else 503
            body = json.dumps({"ready": state["ready"]}).encode()
            content_type = "application/json"
        elif self.path == "/metrics":
            status, body, content_type = 200, generate_latest(), "text/plain; version=0.0.4"
        else:
            status, body, content_type = 404, b"", "text/plain"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    provider = TracerProvider(resource=Resource.create({"service.name": "notification-service",
                                                         "service.instance.id": os.getenv("HOSTNAME", "local")}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("observa.notification")
    connection = psycopg.connect(dsn, autocommit=True)
    connection.execute(Path(__file__).resolve().parents[1].joinpath("schema.sql").read_text(encoding="utf-8"))
    producer = Producer(producer_config(broker))
    consumer = Consumer(consumer_config(broker, "observa.notification"))
    consumer.list_topics(timeout=15)
    adapter = KafkaSubscriber(consumer, producer, tracer)
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8000"))), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    def stop(*_args):
        state["stop"] = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe(["order.events.v1"])
    state["ready"] = True
    try:
        while not state["stop"]:
            message = consumer.poll(0.5)
            if message is None:
                continue

            def handler(event, _context, metadata):
                with DURATION.labels("notification").time():
                    created = handle_notification(connection, event)
                if created:
                    PROCESSED.labels("notification", "created").inc()
                    log("notification_recorded", order_id=event["correlation_id"], event_id=event["event_id"],
                        topic=metadata.topic, partition=metadata.partition, offset=metadata.offset)

            adapter.process_message(message, handler)
    except Exception as exc:
        ERRORS.labels("notification", "consume").inc()
        log("notification_fatal", level="ERROR", error=str(exc))
        raise
    finally:
        state["ready"] = False
        consumer.close()
        connection.close()
        server.shutdown()
        server.server_close()
        provider.force_flush()
        provider.shutdown()


if __name__ == "__main__":
    main()
