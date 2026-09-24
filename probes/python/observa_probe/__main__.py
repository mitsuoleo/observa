import argparse
import os
import signal

from confluent_kafka import Consumer, Producer
from .health import start_server

from .runtime import COMPLETED, consume_one, entry, relay
from .telemetry import configure, log


def sink(tracer, consumer=None, stop=None):
    stopping = stop or {"value": False}
    if stop is None:
        signal.signal(signal.SIGTERM, lambda *_: stopping.update(value=True))
        signal.signal(signal.SIGINT, lambda *_: stopping.update(value=True))
    client = consumer or Consumer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                                  "group.id": "observa-spike0-sink", "enable.auto.commit": False,
                                  "enable.auto.offset.store": False, "auto.offset.reset": "earliest"})
    state = {'ready': False}
    server = start_server(state)
    def assigned(client, partitions):
        state['ready'] = True
        log('assigned', partitions=str(partitions))
    def revoked(client, partitions):
        state['ready'] = False
        log('revoked', partitions=str(partitions))
    client.subscribe([COMPLETED], on_assign=assigned,
                     on_revoke=revoked)
    try:
        while not stopping["value"]:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            consume_one(client, message, tracer)
    finally:
        state['ready'] = False
        client.close()
        server.shutdown()
        server.server_close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["entry", "relay", "sink"])
    parser.add_argument("--directory", default="/work")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--order-id")
    parser.add_argument("--probe-id")
    args = parser.parse_args(argv)
    service = os.environ.setdefault("SERVICE_NAME", {"entry": "probe-entry", "relay": "probe-relay", "sink": "probe-python-sink"}[args.mode])
    provider = configure(service)
    tracer = provider.get_tracer("observa.spike0")
    try:
        if args.mode == "entry":
            entry(args.directory, args.count, args.order_id, tracer, args.probe_id)
        elif args.mode == "relay":
            producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                                 "partitioner": "murmur2_random", "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 25000})
            relay(args.directory, producer, tracer)
        else:
            sink(tracer)
    except Exception as error:
        log("fatal", level="ERROR", error=str(error))
        raise
    finally:
        provider.force_flush(timeout_millis=15000)
        provider.shutdown()


if __name__ == "__main__":
    main()



