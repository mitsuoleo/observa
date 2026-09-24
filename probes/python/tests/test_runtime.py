import json
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from observa_probe.runtime import entry, relay, consume_one
from test_contract import PAYLOAD, PARENT


@pytest.fixture
def telemetry():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def test_separate_entry_relay_context(tmp_path, telemetry):
    tracer, exporter = telemetry
    manifest = entry(tmp_path, 5, PAYLOAD["order_id"], tracer)
    producer = MagicMock()
    producer.flush.return_value = 0
    def produce(**kwargs):
        message = MagicMock()
        message.partition.return_value = 0
        message.offset.return_value = 10
        kwargs["on_delivery"](None, message)
    producer.produce.side_effect = produce
    relay(tmp_path, producer, tracer)
    assert len(manifest["probes"]) == 5
    assert producer.produce.call_count == 5
    roots = exporter.get_finished_spans()[:5]
    children = exporter.get_finished_spans()[5:]
    assert [x.context.trace_id for x in roots] == [x.context.trace_id for x in children]
    assert [x.context.span_id for x in roots] == [x.parent.span_id for x in children]


def test_publish_failure(tmp_path, telemetry):
    tracer, _ = telemetry
    entry(tmp_path, 1, PAYLOAD["order_id"], tracer)
    producer = MagicMock()
    producer.flush.return_value = 1
    with pytest.raises(RuntimeError):
        relay(tmp_path, producer, tracer)


def message(payload=None):
    msg = MagicMock()
    msg.value.return_value = json.dumps(payload or {**PAYLOAD, "step": "completed"}).encode()
    msg.key.return_value = PAYLOAD["order_id"].encode()
    msg.headers.return_value = [("traceparent", PARENT)]
    msg.topic.return_value = "observa.probe.completed.v1"
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    return msg


def test_commit_only_after_processing(telemetry):
    tracer, exporter = telemetry
    consumer = MagicMock()
    consume_one(consumer, message(), tracer)
    assert consumer.commit.call_args.kwargs["asynchronous"] is False
    assert exporter.get_finished_spans()[0].context.trace_id == int("1" * 32, 16)


def test_invalid_message_not_committed(telemetry):
    tracer, _ = telemetry
    consumer = MagicMock()
    with pytest.raises(ValueError):
        consume_one(consumer, message({**PAYLOAD, "step": "bad"}), tracer)
    consumer.commit.assert_not_called()


def test_wrong_key_not_committed(telemetry):
    tracer, _ = telemetry
    consumer = MagicMock()
    msg = message()
    msg.key.return_value = b"wrong"
    with pytest.raises(ValueError):
        consume_one(consumer, msg, tracer)
    consumer.commit.assert_not_called()

def test_entry_process_exits_before_relay_process(tmp_path):
    import subprocess
    import sys
    script = '''
import json, sys, os
from unittest.mock import MagicMock
from opentelemetry.sdk.trace import TracerProvider
from observa_probe.runtime import entry, relay
tracer=TracerProvider().get_tracer('isolation')
if sys.argv[1]=='entry':
    entry(sys.argv[2], 1, '3e7723b4-cdb5-4ee0-aea5-c6fd4ca63bc0', tracer)
else:
    producer=MagicMock()
    producer.flush.return_value=0
    def produce(**kw):
        msg=MagicMock()
        msg.partition.return_value=0
        msg.offset.return_value=1
        kw['on_delivery'](None,msg)
    producer.produce.side_effect=produce
    relay(sys.argv[2],producer,tracer)
'''
    created = subprocess.run([sys.executable, '-c', script, 'entry', str(tmp_path)], capture_output=True, text=True, check=True)
    published = subprocess.run([sys.executable, '-c', script, 'relay', str(tmp_path)], capture_output=True, text=True, check=True)
    first = [json.loads(line) for line in created.stdout.splitlines()]
    second = [json.loads(line) for line in published.stdout.splitlines()]
    assert first[0]['process_id'] != second[0]['process_id']
    assert second[0]['active_context'] is False
    assert first[0]['trace_id'] == second[-1]['trace_id']
    assert second[-1]['tracestate'] == 'observa=spike0'

def test_explicit_probe_id_only_single(tmp_path, telemetry):
    tracer, _ = telemetry
    with pytest.raises(ValueError):
        entry(tmp_path, 5, PAYLOAD['order_id'], tracer, PAYLOAD['probe_id'])
    manifest = entry(tmp_path, 1, PAYLOAD['order_id'], tracer, PAYLOAD['probe_id'])
    assert manifest['probes'][0]['probe_id'] == PAYLOAD['probe_id']
