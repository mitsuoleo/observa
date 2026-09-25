from uuid import uuid4

import pytest

from app.service import RELEVANT_EVENTS, notification_fields


def test_reference_event_selection():
    assert RELEVANT_EVENTS == {
        "payment.approved", "payment.rejected", "stock.reserved",
        "stock.unavailable", "order.completed", "order.failed",
    }


def test_notification_fields_keep_business_correlation():
    order_id = str(uuid4())
    event_id = str(uuid4())
    event = {"event_id": event_id, "event_type": "order.completed", "correlation_id": order_id,
             "payload": {"order_id": order_id}}
    assert notification_fields(event) == (event_id, order_id, "order.completed")


def test_notification_rejects_mismatched_order_id():
    event = {"event_id": str(uuid4()), "event_type": "order.completed",
             "correlation_id": str(uuid4()), "payload": {"order_id": str(uuid4())}}
    with pytest.raises(ValueError):
        notification_fields(event)
