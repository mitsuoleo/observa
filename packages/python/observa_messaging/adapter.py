"""Acknowledged Kafka publish and manually committed consume seams."""

import base64
import json
from dataclasses import dataclass

from opentelemetry.context import Context
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .contract import InvalidRecord, validate_event

TOPIC = "order.events.v1"
PARKED_TOPIC = "order.events.v1.parked"
PROPAGATOR = TraceContextTextMapPropagator()


@dataclass(frozen=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int


def producer_config(bootstrap_servers):
    return {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all",
            "partitioner": "murmur2_random"}


def consumer_config(bootstrap_servers, group_id):
    if not group_id or not isinstance(group_id, str):
        raise ValueError("group_id is required")
    return {
        "bootstrap.servers": bootstrap_servers, "group.id": group_id,
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    }


def capture_carrier():
    """Capture the active W3C context for persistence beside an outbox event."""
    carrier = {}
    PROPAGATOR.inject(carrier)
    return carrier


def _headers_carrier(headers):
    carrier = {}
    for name, value in headers or []:
        if name not in ("traceparent", "tracestate"):
            continue
        if name in carrier:
            return {}
        try:
            carrier[name] = value.decode("ascii") if isinstance(value, bytes) else value
        except UnicodeError:
            return {}
    return carrier


def _extract(carrier):
    clean = {key: value for key, value in (carrier or {}).items()
             if key in ("traceparent", "tracestate") and isinstance(value, str)}
    return PROPAGATOR.extract(clean, context=Context())


def _acknowledged_publish(producer, *, topic, key, value, headers):
    acknowledgements = []

    def delivered(error, message):
        acknowledgements.append((error, message))

    producer.produce(topic=topic, key=key, value=value, headers=headers, on_delivery=delivered)
    outstanding = producer.flush(30)
    if outstanding or len(acknowledgements) != 1 or acknowledgements[0][0] is not None:
        raise RuntimeError("Kafka publish not acknowledged")
    message = acknowledgements[0][1]
    return RecordMetadata(message.topic(), message.partition(), message.offset())


class KafkaPublisher:
    def __init__(self, producer, tracer):
        self.producer = producer
        self.tracer = tracer

    def publish(self, event, carrier):
        event = validate_event(event)
        parent = _extract(carrier)
        with self.tracer.start_as_current_span(
            "order.events.publish", context=parent, kind=SpanKind.PRODUCER,
            attributes={"messaging.destination.name": TOPIC, "messaging.operation.name": "publish"},
        ):
            headers = capture_carrier()
            if "traceparent" not in headers and carrier and "traceparent" in carrier:
                headers = {key: value for key, value in carrier.items() if key in ("traceparent", "tracestate")}
            headers = {**headers, "content-type": "application/json", "schema-version": "1"}
            return _acknowledged_publish(
                self.producer, topic=TOPIC, key=event["correlation_id"].encode("utf-8"),
                value=json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                headers=[(name, value.encode("ascii")) for name, value in headers.items()],
            )


class KafkaSubscriber:
    def __init__(self, consumer, parking_producer, tracer):
        self.consumer = consumer
        self.parking_producer = parking_producer
        self.tracer = tracer

    def process_message(self, message, handler):
        if message.error():
            raise RuntimeError(f"Kafka consume failed: {message.error()}")
        metadata = RecordMetadata(message.topic(), message.partition(), message.offset())
        try:
            if message.topic() != TOPIC:
                raise InvalidRecord("unexpected topic")
            raw = message.value()
            if not isinstance(raw, bytes):
                raise InvalidRecord("value must be bytes")
            event = validate_event(json.loads(raw.decode("utf-8")))
            if message.key() != event["correlation_id"].encode("utf-8"):
                raise InvalidRecord("Kafka key does not match correlation_id")
        except (InvalidRecord, UnicodeError, json.JSONDecodeError) as exc:
            self._park(message, metadata, str(exc))
            self.consumer.commit(message=message, asynchronous=False)
            return
        context = _extract(_headers_carrier(message.headers()))
        with self.tracer.start_as_current_span(
            "order.events.process", context=context, kind=SpanKind.CONSUMER,
            attributes={"messaging.destination.name": TOPIC, "messaging.operation.name": "process",
                        "messaging.kafka.partition": metadata.partition, "messaging.kafka.offset": metadata.offset},
        ):
            try:
                handler(event, context, metadata)
            except InvalidRecord as exc:
                self._park(message, metadata, str(exc))
                self.consumer.commit(message=message, asynchronous=False)
                return
        self.consumer.commit(message=message, asynchronous=False)

    def _park(self, message, metadata, reason):
        raw = message.value()
        parked = {
            "raw_value_base64": base64.b64encode(raw or b"").decode("ascii"),
            "raw_key_base64": base64.b64encode(message.key() or b"").decode("ascii"),
            "origin": {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset},
            "reason": reason[:512],
        }
        _acknowledged_publish(
            self.parking_producer, topic=PARKED_TOPIC, key=message.key(),
            value=json.dumps(parked, separators=(",", ":")).encode("utf-8"), headers=[],
        )

    def consume(self, handler, *, max_messages=None, poll_timeout=1.0):
        self.consumer.subscribe([TOPIC])
        count = 0
        while max_messages is None or count < max_messages:
            message = self.consumer.poll(poll_timeout)
            if message is None:
                continue
            self.process_message(message, handler)
            count += 1
