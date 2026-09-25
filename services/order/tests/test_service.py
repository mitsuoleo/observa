from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.api_schemas import CreateOrderRequest
from app.models import Order, OutboxEvent, ProcessedEvent
from app.service import create_order, handle_event


def request(*, payment="approve", stock="reserve"):
    return CreateOrderRequest.model_validate({
        "customer_id": str(uuid4()),
        "items": [{"product_id": str(uuid4()), "quantity": 2, "unit_price": "12.50"}],
        "simulate": {"payment": payment, "stock": stock},
    })


def event(kind, order_id, **payload):
    return {
        "event_id": str(uuid4()), "event_type": kind, "version": 1,
        "correlation_id": str(order_id), "occurred_at": "2026-09-24T12:00:00Z",
        "payload": {"order_id": str(order_id), **payload},
    }


@pytest.mark.asyncio
async def test_creation_is_atomic_and_preserves_contract(session_factory):
    carrier = {"traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    async with session_factory() as session:
        order = await create_order(session, request(), carrier)
        rows = (await session.execute(select(OutboxEvent))).scalars().all()
        assert order.status == "PENDING"
        assert order.total_amount == Decimal("25.00")
        assert len(rows) == 1
        assert rows[0].payload["event_type"] == "order.created"
        assert rows[0].payload["correlation_id"] == str(order.id)
        assert rows[0].payload["payload"]["total_amount"] == 25.0
        assert rows[0].carrier == carrier
        assert "traceparent" not in rows[0].payload


@pytest.mark.asyncio
@pytest.mark.parametrize("incoming,details,status,outgoing", [
    ("payment.rejected", {"payment_id": "11111111-1111-1111-1111-111111111111", "amount": 25.0, "reason": "declined"}, "FAILED", "order.failed"),
    ("stock.reserved", {"items": [{"product_id": "11111111-1111-1111-1111-111111111111", "quantity": 2}]}, "CONFIRMED", "order.completed"),
    ("stock.unavailable", {"reason": "missing"}, "PENDING", "payment.refund.requested"),
])
async def test_transitions_are_idempotent(session_factory, incoming, details, status, outgoing):
    async with session_factory() as session:
        order = await create_order(session, request(), {})
        order_id = order.id
    incoming_event = event(incoming, order_id, **details)
    carrier = {"traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    async with session_factory() as session:
        await handle_event(session, incoming_event, carrier)
        await handle_event(session, incoming_event, carrier)
    async with session_factory() as session:
        order = await session.get(Order, order_id)
        assert order.status == status
        rows = (await session.execute(select(OutboxEvent))).scalars().all()
        assert [row.event_type for row in rows] == (["order.created", outgoing] if outgoing else ["order.created"])
        assert all(row.carrier == carrier for row in rows[1:])
        count = (await session.execute(select(func.count()).select_from(ProcessedEvent))).scalar_one()
        assert count == 1


@pytest.mark.asyncio
async def test_unknown_order_is_ignored_without_marker(session_factory):
    missing = uuid4()
    incoming = event("stock.unavailable", missing, reason="missing")
    async with session_factory() as session:
        await handle_event(session, incoming, {})
        count = (await session.execute(select(func.count()).select_from(ProcessedEvent))).scalar_one()
        assert count == 0


@pytest.mark.asyncio
async def test_refund_requires_prior_stock_unavailable(session_factory):
    async with session_factory() as session:
        order = await create_order(session, request(), {})
        order_id = order.id
    refund = event("payment.refunded", order_id, payment_id=str(uuid4()), amount=25.0)
    async with session_factory() as session:
        await handle_event(session, refund, {})
    async with session_factory() as session:
        order = await session.get(Order, order_id)
        assert order.status == "PENDING"


@pytest.mark.asyncio
async def test_stock_failure_then_refund_cancels_once(session_factory):
    async with session_factory() as session:
        order = await create_order(session, request(stock="unavailable"), {})
        order_id = order.id
    unavailable = event("stock.unavailable", order_id, reason="missing")
    refund = event("payment.refunded", order_id, payment_id=str(uuid4()), amount=25.0)
    async with session_factory() as session:
        await handle_event(session, unavailable, {})
        await handle_event(session, refund, {})
        await handle_event(session, refund, {})
    async with session_factory() as session:
        order = await session.get(Order, order_id)
        assert order.status == "CANCELLED"
        rows = (await session.execute(select(OutboxEvent))).scalars().all()
        assert [row.event_type for row in rows] == ["order.created", "payment.refund.requested"]
