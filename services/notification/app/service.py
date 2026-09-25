"""Idempotent notification persistence without an external delivery side effect."""

from uuid import UUID

from observa_messaging import InvalidRecord


RELEVANT_EVENTS = frozenset({
    "payment.approved", "payment.rejected", "stock.reserved",
    "stock.unavailable", "order.completed", "order.failed",
})


def notification_fields(event: dict) -> tuple[str, str, str] | None:
    kind = event.get("event_type")
    if kind not in RELEVANT_EVENTS:
        return None
    event_id = str(UUID(event["event_id"]))
    order_id = str(UUID(event["correlation_id"]))
    payload = event.get("payload")
    if not isinstance(payload, dict) or payload.get("order_id") != order_id:
        raise InvalidRecord("notification order_id must equal correlation_id")
    return event_id, order_id, kind


def handle_notification(connection, event: dict) -> bool:
    fields = notification_fields(event)
    if fields is None:
        return False
    event_id, order_id, kind = fields
    with connection.transaction():
        inserted = connection.execute(
            "INSERT INTO processed_events (event_id) VALUES (%s) "
            "ON CONFLICT DO NOTHING RETURNING event_id", (event_id,)
        ).fetchone()
        if inserted is None:
            return False
        connection.execute(
            "INSERT INTO notifications (event_id, order_id, event_type) VALUES (%s, %s, %s)",
            (event_id, order_id, kind),
        )
    return True
