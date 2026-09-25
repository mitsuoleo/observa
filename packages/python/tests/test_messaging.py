import base64
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from observa_messaging import (
    KafkaPublisher,
    KafkaSubscriber,
    InvalidRecord,
    validate_event,
    producer_config,
)


def test_producer_uses_same_key_partitioner_as_node():
    assert producer_config("kafka:9092")["partitioner"] == "murmur2_random"


ROOT = Path(__file__).resolve().parents[3]
EVENT = json.loads((ROOT / "tests/contracts/order-created.json").read_text(encoding="utf-8"))
PARENT = "00-11111111111111111111111111111111-2222222222222222-01"


class Span:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class Tracer:
    def start_as_current_span(self, *args, **kwargs):
        return Span()


class Message:
    def __init__(self, value=None, key=None, headers=None):
        self._value = value if value is not None else json.dumps(EVENT).encode()
        self._key = key if key is not None else EVENT["correlation_id"].encode()
        self._headers = headers if headers is not None else [("traceparent", PARENT.encode())]

    def value(self):
        return self._value

    def key(self):
        return self._key

    def headers(self):
        return self._headers

    def topic(self):
        return "order.events.v1"

    def partition(self):
        return 1

    def offset(self):
        return 9

    def error(self):
        return None


def producer(ack_error=None):
    mock = Mock()

    def produce(**kwargs):
        kwargs["on_delivery"](ack_error, Message())

    mock.produce.side_effect = produce
    mock.flush.return_value = 0
    return mock


def test_contract_matches_orderflow_v1_without_requiring_payload_order_id():
    result = validate_event(EVENT)
    assert result == EVENT
    assert result is not EVENT
    assert result["payload"] is not EVENT["payload"]
    payment = {**EVENT, "event_type": "payment.approved", "payload": {}}
    assert validate_event(payment) == payment


@pytest.mark.parametrize("change", [
    {"event_id": "bad"}, {"correlation_id": "bad"}, {"version": True}, {"version": 2},
    {"event_type": "unknown"}, {"occurred_at": "2026-09-24T12:00:00"},
    {"payload": []},
])
def test_invalid_envelope(change):
    with pytest.raises(InvalidRecord):
        validate_event({**EVENT, **change})


def test_publisher_uses_key_envelope_and_ack():
    kafka = producer()
    result = KafkaPublisher(kafka, Tracer()).publish(EVENT, {"traceparent": PARENT})
    kwargs = kafka.produce.call_args.kwargs
    assert kwargs["topic"] == "order.events.v1"
    assert kwargs["key"] == EVENT["correlation_id"].encode()
    assert json.loads(kwargs["value"]) == EVENT
    assert result.partition == 1 and result.offset == 9
    assert kafka.flush.called


def test_publisher_does_not_claim_success_without_broker_ack():
    with pytest.raises(RuntimeError):
        KafkaPublisher(producer(RuntimeError("down")), Tracer()).publish(EVENT, {})
    kafka = producer()
    kafka.flush.return_value = 1
    with pytest.raises(RuntimeError):
        KafkaPublisher(kafka, Tracer()).publish(EVENT, {})


def test_consumer_commits_only_after_handler_and_passes_metadata():
    consumer = Mock()
    adapter = KafkaSubscriber(consumer, producer(), Tracer())
    calls = []

    def handler(event, context, metadata):
        assert event == EVENT
        assert metadata.topic == "order.events.v1"
        assert metadata.partition == 1 and metadata.offset == 9
        assert context is not None
        calls.append("handler")
        assert not consumer.commit.called

    adapter.process_message(Message(), handler)
    assert calls == ["handler"]
    consumer.commit.assert_called_once()


def test_transient_handler_failure_does_not_commit_or_park():
    consumer, parking = Mock(), producer()
    adapter = KafkaSubscriber(consumer, parking, Tracer())
    with pytest.raises(RuntimeError):
        adapter.process_message(Message(), lambda *_: (_ for _ in ()).throw(RuntimeError("db down")))
    consumer.commit.assert_not_called()
    parking.produce.assert_not_called()


def test_invalid_domain_payload_parks_before_commit():
    consumer, parking = Mock(), producer()
    adapter = KafkaSubscriber(consumer, parking, Tracer())
    adapter.process_message(Message(), lambda *_: (_ for _ in ()).throw(InvalidRecord("missing order_id")))
    assert parking.produce.call_args.kwargs["topic"] == "order.events.v1.parked"
    consumer.commit.assert_called_once()


def test_invalid_record_parks_raw_bytes_then_commits():
    consumer, parking = Mock(), producer()
    raw = b"not-json\xff"
    adapter = KafkaSubscriber(consumer, parking, Tracer())
    adapter.process_message(Message(value=raw), lambda *_: pytest.fail("invalid record delivered"))
    kwargs = parking.produce.call_args.kwargs
    assert kwargs["topic"] == "order.events.v1.parked"
    body = json.loads(kwargs["value"])
    assert base64.b64decode(body["raw_value_base64"]) == raw
    assert body["origin"] == {"topic": "order.events.v1", "partition": 1, "offset": 9}
    assert body["reason"]
    consumer.commit.assert_called_once()


def test_failed_parking_ack_keeps_original_offset():
    consumer = Mock()
    adapter = KafkaSubscriber(consumer, producer(RuntimeError("down")), Tracer())
    with pytest.raises(RuntimeError):
        adapter.process_message(Message(value=b"not-json"), lambda *_: None)
    consumer.commit.assert_not_called()
