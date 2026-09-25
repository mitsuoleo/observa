"""Real broker contract test; enabled only in the isolated Kubernetes job."""

import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest

from observa_messaging import KafkaPublisher, KafkaSubscriber, consumer_config, producer_config


BROKER = os.getenv("OBSERVA_TEST_KAFKA")
ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.skipif(not BROKER, reason="set OBSERVA_TEST_KAFKA for real broker contract")
def test_real_broker_key_headers_group_and_commit():
    from confluent_kafka import Consumer, Producer, TopicPartition
    from opentelemetry import trace

    event = json.loads((ROOT / "tests/contracts/order-created.json").read_text(encoding="utf-8"))
    event["event_id"] = str(uuid4())
    event["correlation_id"] = str(uuid4())
    # A valid event for an unknown order exercises the live topic without
    # creating a synthetic payment or notification in the MVP services.
    event["event_type"] = "payment.refunded"
    event["payload"] = {"order_id": event["correlation_id"], "payment_id": str(uuid4()), "amount": 0}
    parent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
    group = f"observa-us002-{uuid4()}"
    config = {**consumer_config(BROKER, group), "auto.offset.reset": "latest"}
    consumer = Consumer(config)
    producer = Producer(producer_config(BROKER))
    parking = Producer(producer_config(BROKER))
    try:
        consumer.subscribe(["order.events.v1"])
        deadline = time.monotonic() + 30
        while not consumer.assignment() and time.monotonic() < deadline:
            consumer.poll(0.5)
        assert consumer.assignment(), "consumer group assignment timed out"
        KafkaPublisher(producer, trace.get_tracer("observa-us002-test")).publish(event, {"traceparent": parent})
        received = None
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            candidate = consumer.poll(0.5)
            if candidate is None:
                continue
            if candidate.error():
                raise RuntimeError(str(candidate.error()))
            if candidate.key() == event["correlation_id"].encode("utf-8"):
                received = candidate
                break
        assert received is not None, "published event was not delivered"
        assert json.loads(received.value()) == event
        assert dict(received.headers())["traceparent"].decode("ascii") == parent
        subscriber = KafkaSubscriber(consumer, parking, trace.get_tracer("observa-us002-test"))
        handled = []
        subscriber.process_message(received, lambda value, _context, meta: handled.append((value, meta)))
        assert handled[0][0] == event
        assert handled[0][1].partition == received.partition()
        committed = consumer.committed([TopicPartition(received.topic(), received.partition())])
        assert committed[0].offset == received.offset() + 1
    finally:
        consumer.close()
