"""Transport-level validation of the unchanged OrderFlow v1 envelope."""

import copy
from datetime import datetime
from uuid import UUID

EVENT_TYPES = frozenset({
    "order.created", "order.completed", "order.failed", "payment.approved",
    "payment.rejected", "payment.refund.requested", "payment.refunded",
    "stock.reserved", "stock.unavailable",
})
REQUIRED = frozenset({"event_id", "event_type", "version", "correlation_id", "occurred_at", "payload"})


class InvalidRecord(ValueError):
    """A deterministic wire or contract error; the original Kafka record may be parked."""


def _uuid(value, field):
    if not isinstance(value, str):
        raise InvalidRecord(f"{field} must be a UUID string")
    try:
        UUID(value)
    except ValueError as exc:
        raise InvalidRecord(f"{field} must be a UUID string") from exc


def validate_event(event):
    """Return a copy of a valid v1 envelope without changing its wire shape."""
    if not isinstance(event, dict) or not REQUIRED.issubset(event):
        raise InvalidRecord("missing envelope fields")
    _uuid(event["event_id"], "event_id")
    _uuid(event["correlation_id"], "correlation_id")
    if not isinstance(event["event_type"], str) or event["event_type"] not in EVENT_TYPES:
        raise InvalidRecord("unsupported event_type")
    if type(event["version"]) is not int or event["version"] != 1:
        raise InvalidRecord("version must be 1")
    value = event["occurred_at"]
    if not isinstance(value, str) or "T" not in value:
        raise InvalidRecord("occurred_at must be RFC3339")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidRecord("occurred_at must be RFC3339") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidRecord("occurred_at needs timezone")
    if not isinstance(event["payload"], dict):
        raise InvalidRecord("payload must be an object")
    return copy.deepcopy(event)
