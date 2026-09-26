"""Expose committed Kafka consumer lag for the four domain consumer groups."""

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer, TopicPartition


TOPIC = "order.events.v1"
GROUPS = ("observa.order", "observa.payment", "observa.inventory", "observa.notification")
BROKER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def collect() -> bytes:
    rows = [
        "# HELP observa_consumer_group_lag Unconsumed records at the committed offset.",
        "# TYPE observa_consumer_group_lag gauge",
        "# HELP observa_consumer_group_offset_known Whether the group has committed an offset.",
        "# TYPE observa_consumer_group_offset_known gauge",
        "# HELP observa_lag_exporter_scrape_success Whether all Kafka lag queries succeeded.",
        "# TYPE observa_lag_exporter_scrape_success gauge",
    ]
    clients = []
    try:
        for group in GROUPS:
            client = Consumer({"bootstrap.servers": BROKER, "group.id": group,
                               "enable.auto.commit": False})
            clients.append(client)
            metadata = client.list_topics(TOPIC, timeout=5)
            topic = metadata.topics.get(TOPIC)
            if topic is None or topic.error:
                raise RuntimeError(f"Topic {TOPIC} unavailable: {topic.error if topic else 'absent'}")
            partitions = [TopicPartition(TOPIC, index) for index in sorted(topic.partitions)]
            committed = client.committed(partitions, timeout=5)
            for partition in committed:
                labels = f'group="{group}",topic="{TOPIC}",partition="{partition.partition}"'
                known = partition.offset >= 0
                rows.append(f"observa_consumer_group_offset_known{{{labels}}} {int(known)}")
                if known:
                    _low, high = client.get_watermark_offsets(partition, timeout=5, cached=False)
                    rows.append(f"observa_consumer_group_lag{{{labels}}} {max(0, high - partition.offset)}")
        rows.append("observa_lag_exporter_scrape_success 1")
    except Exception as exc:
        # A partial scrape must not be interpreted as a complete lag sample.
        print(f"lag query failed: {exc}", flush=True)
        rows = [line for line in rows if not line.startswith("observa_consumer_group_")]
        rows.append("observa_lag_exporter_scrape_success 0")
    finally:
        for client in clients:
            client.close()
    return ("\n".join(rows) + "\n").encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body, status = b"ok\n", 200
        elif self.path == "/metrics":
            body, status = collect(), 200
        else:
            body, status = b"not found\n", 404
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9108), Handler).serve_forever()
