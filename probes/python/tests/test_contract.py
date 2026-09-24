import unittest

from observa_probe.contract import validate_payload, sanitize_headers

PAYLOAD = {"probe_id": "a7eec836-2b44-4b32-ae22-2d3a146fd58d", "order_id": "3e7723b4-cdb5-4ee0-aea5-c6fd4ca63bc0", "step": "started", "occurred_at": "2026-09-24T10:00:00Z"}
PARENT = "00-11111111111111111111111111111111-2222222222222222-01"


class ContractTests(unittest.TestCase):
    def test_valid_payload_is_copy(self):
        self.assertEqual(validate_payload(PAYLOAD), PAYLOAD)
        self.assertIsNot(validate_payload(PAYLOAD), PAYLOAD)

    def test_invalid_payload(self):
        for field, value in [("probe_id", "bad"), ("step", "other"), ("occurred_at", "2026-09-24"), ("order_id", None)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_payload({**PAYLOAD, field: value})

    def test_headers_roundtrip(self):
        carrier, warning = sanitize_headers([("traceparent", PARENT.encode()), ("tracestate", b"vendor=value")])
        self.assertEqual(carrier, {"traceparent": PARENT, "tracestate": "vendor=value"})
        self.assertIsNone(warning)

    def test_missing_invalid_duplicate_parent(self):
        for headers in [[], [("traceparent", b"bad")], [("traceparent", PARENT), ("traceparent", PARENT)], [("traceparent", PARENT.replace("11111111111111111111111111111111", "0" * 32))]]:
            carrier, warning = sanitize_headers(headers)
            self.assertEqual(carrier, {})
            self.assertIsNotNone(warning)

    def test_invalid_state_discarded(self):
        carrier, warning = sanitize_headers([("traceparent", PARENT), ("tracestate", "bad")])
        self.assertEqual(carrier, {"traceparent": PARENT})
        self.assertEqual(warning, "invalid_tracestate")

def test_invalid_state_bytes_keeps_valid_parent():
    carrier, warning = sanitize_headers([('traceparent', PARENT), ('tracestate', b'\xff')])
    assert carrier == {'traceparent': PARENT}
    assert warning == 'invalid_tracestate'
