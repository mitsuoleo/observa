from __future__ import annotations

import unittest

from observa_harness.workflow import consume_with_manual_commit, relay_pending


EVENT = {
    "event_id": "11111111-1111-4111-8111-111111111111",
    "correlation_id": "22222222-2222-4222-8222-222222222222",
    "event_type": "order.created",
    "version": 1,
    "occurred_at": "2026-09-24T12:00:00Z",
    "payload": {"synthetic": True},
}
CARRIER = {"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}


class FakeStore:
    def __init__(self):
        self.row = (1, EVENT, CARRIER)
        self.published = False
        self.effects = set()

    def claim_and_publish(self, publisher):
        if self.published:
            return False
        result = publisher.publish(self.row[1], self.row[2])
        self.published = True
        return result

    def apply_effect(self, event):
        self.effects.add(event["event_id"])
        return len(self.effects) == 1


class Publisher:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def publish(self, event, carrier):
        self.calls.append((event, carrier))
        if self.fail:
            raise RuntimeError("broker did not acknowledge")
        return {"partition": 0, "offset": 7}


class WorkflowTest(unittest.TestCase):
    def test_relay_passes_stored_carrier_after_original_context_is_gone(self):
        store, publisher = FakeStore(), Publisher()
        self.assertEqual(relay_pending(store, publisher), {"partition": 0, "offset": 7})
        self.assertEqual(publisher.calls, [(EVENT, CARRIER)])
        self.assertTrue(store.published)

    def test_relay_does_not_mark_published_without_broker_ack(self):
        store = FakeStore()
        with self.assertRaisesRegex(RuntimeError, "acknowledge"):
            relay_pending(store, Publisher(fail=True))
        self.assertFalse(store.published)

    def test_crash_after_effect_commit_before_offset_redelivers_without_second_effect(self):
        store = FakeStore()
        commits = []

        def crash():
            raise RuntimeError("injected crash")

        with self.assertRaisesRegex(RuntimeError, "injected crash"):
            consume_with_manual_commit(store, EVENT, lambda: commits.append("offset"), after_db_commit=crash)
        self.assertEqual(store.effects, {EVENT["event_id"]})
        self.assertEqual(commits, [])

        consume_with_manual_commit(store, EVENT, lambda: commits.append("offset"))
        self.assertEqual(store.effects, {EVENT["event_id"]})
        self.assertEqual(commits, ["offset"])
