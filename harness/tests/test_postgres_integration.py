"""Run with OBSERVA_HARNESS_DSN set to a disposable Postgres database."""

from __future__ import annotations

import os
import unittest
from uuid import uuid4

from observa_harness.postgres import PostgresStore
from observa_harness.workflow import consume_with_manual_commit, relay_pending


class Publisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    def publish(self, event, carrier):
        self.calls.append((event, carrier))
        if self.fail:
            raise RuntimeError("Kafka acknowledgement failed")
        return {"partition": 1, "offset": 42}


@unittest.skipUnless(os.getenv("OBSERVA_HARNESS_DSN"), "set OBSERVA_HARNESS_DSN for Postgres integration")
class PostgresIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import psycopg
        except ImportError as error:
            raise unittest.SkipTest("psycopg 3 is required") from error
        cls.connection = psycopg.connect(os.environ["OBSERVA_HARNESS_DSN"], autocommit=True)
        cls.store = PostgresStore(cls.connection)
        cls.store.init_schema()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def setUp(self):
        self.event = {
            "event_id": str(uuid4()),
            "event_type": "order.created",
            "version": 1,
            "correlation_id": str(uuid4()),
            "occurred_at": "2026-09-24T12:00:00Z",
            "payload": {"synthetic": True},
        }
        self.carrier = {"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}

    def tearDown(self):
        event_id = self.event["event_id"]
        with self.connection.transaction():
            self.connection.execute("DELETE FROM synthetic_effects WHERE event_id = %s", (event_id,))
            self.connection.execute("DELETE FROM processed_events WHERE event_id = %s", (event_id,))
            self.connection.execute("DELETE FROM synthetic_outbox WHERE event_id = %s", (event_id,))

    def test_ack_precedes_outbox_publish_marker_and_carrier_stays_outside_envelope(self):
        self.store.enqueue(self.event, self.carrier)
        with self.assertRaisesRegex(RuntimeError, "acknowledgement"):
            relay_pending(self.store, Publisher(fail=True))
        row = self.connection.execute(
            "SELECT envelope, carrier, published_at FROM synthetic_outbox WHERE event_id = %s",
            (self.event["event_id"],),
        ).fetchone()
        self.assertEqual(row[0], self.event)
        self.assertEqual(row[1], self.carrier)
        self.assertNotIn("traceparent", row[0])
        self.assertIsNone(row[2])
        receipt = relay_pending(self.store, Publisher())
        self.assertEqual(receipt, {"partition": 1, "offset": 42})
        self.assertIsNotNone(
            self.connection.execute(
                "SELECT published_at FROM synthetic_outbox WHERE event_id = %s", (self.event["event_id"],)
            ).fetchone()[0]
        )

    def test_db_commit_then_crash_then_redelivery_has_one_effect(self):
        commits = []

        def crash():
            raise RuntimeError("crash after DB commit")

        with self.assertRaisesRegex(RuntimeError, "crash after DB commit"):
            consume_with_manual_commit(
                self.store, self.event, lambda: commits.append(1), after_db_commit=crash
            )
        self.assertEqual(commits, [])
        consume_with_manual_commit(self.store, self.event, lambda: commits.append(1))
        self.assertEqual(commits, [1])
        count = self.connection.execute(
            "SELECT count(*) FROM synthetic_effects WHERE event_id = %s", (self.event["event_id"],)
        ).fetchone()[0]
        self.assertEqual(count, 1)
