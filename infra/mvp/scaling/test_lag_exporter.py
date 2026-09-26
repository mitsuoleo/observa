"""Offline contract checks for the Kafka lag metrics exporter."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class Partition:
    def __init__(self, topic, partition, offset=0):
        self.topic = topic
        self.partition = partition
        self.offset = offset


class FakeConsumer:
    fail = False
    closed = 0

    def __init__(self, config):
        self.group = config["group.id"]

    def list_topics(self, _topic, timeout):
        if self.fail:
            raise RuntimeError("broker unavailable")
        return types.SimpleNamespace(topics={"order.events.v1": types.SimpleNamespace(
            error=None, partitions={0: object(), 1: object()})})

    def committed(self, partitions, timeout):
        return [Partition(part.topic, part.partition, 7 if part.partition == 0 else -1)
                for part in partitions]

    def get_watermark_offsets(self, partition, timeout, cached):
        return 0, 10

    def close(self):
        type(self).closed += 1


class LagExporterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fake = types.ModuleType("confluent_kafka")
        fake.Consumer = FakeConsumer
        fake.TopicPartition = Partition
        path = Path(__file__).with_name("lag_exporter.py")
        spec = importlib.util.spec_from_file_location("lag_exporter", path)
        cls.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"confluent_kafka": fake}):
            spec.loader.exec_module(cls.module)

    def setUp(self):
        FakeConsumer.fail = False
        FakeConsumer.closed = 0

    def test_known_and_unknown_offsets(self):
        body = self.module.collect().decode()
        self.assertIn('observa_consumer_group_lag{group="observa.order",topic="order.events.v1",partition="0"} 3', body)
        self.assertIn('observa_consumer_group_offset_known{group="observa.order",topic="order.events.v1",partition="1"} 0', body)
        self.assertNotIn('observa_consumer_group_lag{group="observa.order",topic="order.events.v1",partition="1"}', body)
        self.assertIn("observa_lag_exporter_scrape_success 1", body)
        self.assertEqual(FakeConsumer.closed, 4)

    def test_broker_failure_has_no_partial_lag_samples(self):
        FakeConsumer.fail = True
        body = self.module.collect().decode()
        self.assertIn("observa_lag_exporter_scrape_success 0", body)
        self.assertNotIn('observa_consumer_group_lag{', body)
        self.assertEqual(FakeConsumer.closed, 1)


if __name__ == "__main__":
    unittest.main()
