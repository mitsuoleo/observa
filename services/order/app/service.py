from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from observa_messaging import InvalidRecord, validate_event

from .api_schemas import CreateOrderRequest
from .models import Order, OrderEventTimeline, OrderItem, OutboxEvent, ProcessedEvent


def build_event(event_type: str, order_id: UUID, payload: dict) -> dict:
    return validate_event({
        "event_id": str(uuid4()),
        "event_type": event_type,
        "version": 1,
        "correlation_id": str(order_id),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    })


def add_event(session: AsyncSession, order_id: UUID, event: dict, carrier: dict) -> None:
    session.add(OrderEventTimeline(order_id=order_id, event_type=event["event_type"], payload=event))
    session.add(OutboxEvent(
        event_id=UUID(event["event_id"]), event_type=event["event_type"],
        payload=event, carrier=dict(carrier),
    ))


async def create_order(session: AsyncSession, body: CreateOrderRequest, carrier: dict) -> Order:
    order_id = uuid4()
    total = sum((item.unit_price * item.quantity for item in body.items), Decimal("0"))
    order = Order(
        id=order_id, customer_id=body.customer_id, status="PENDING", total_amount=total,
        items=[OrderItem(product_id=item.product_id, quantity=item.quantity, unit_price=item.unit_price)
               for item in body.items],
    )
    payload = {
        "order_id": str(order_id), "customer_id": str(body.customer_id),
        "items": [{"product_id": str(item.product_id), "quantity": item.quantity,
                   "unit_price": float(item.unit_price)} for item in body.items],
        "total_amount": float(total),
        "simulate": {"payment": body.simulate.payment.value, "stock": body.simulate.stock.value},
    }
    async with session.begin():
        session.add(order)
        add_event(session, order_id, build_event("order.created", order_id, payload), carrier)
    await session.refresh(order)
    return order


async def get_order(session: AsyncSession, order_id: UUID) -> Order | None:
    return await session.get(Order, order_id)


async def handle_event(session: AsyncSession, incoming: dict, carrier: dict) -> None:
    incoming = validate_event(incoming)
    event_type = incoming["event_type"]
    if event_type not in {"payment.rejected", "stock.reserved", "stock.unavailable", "payment.refunded"}:
        return
    payload = incoming["payload"]
    try:
        order_id = UUID(payload["order_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidRecord("payload order_id must be a UUID") from exc
    if str(order_id) != incoming["correlation_id"]:
        raise InvalidRecord("event order_id does not match correlation_id")
    event_id = UUID(incoming["event_id"])
    async with session.begin():
        # Lock the order before the marker: distinct events for one order serialize.
        order = (await session.execute(select(Order).where(Order.id == order_id).with_for_update())).scalar_one_or_none()
        if order is None:
            return
        if await session.get(ProcessedEvent, event_id) is not None:
            return
        session.add(ProcessedEvent(event_id=event_id))
        session.add(OrderEventTimeline(order_id=order_id, event_type=event_type, payload=incoming))
        outgoing = None
        if event_type == "payment.rejected" and order.status == "PENDING" and not order.awaiting_refund:
            order.status = "FAILED"
            outgoing = build_event("order.failed", order_id, {
                "order_id": str(order_id), "status": "FAILED", "reason": payload.get("reason", "payment_rejected"),
            })
        elif event_type == "stock.reserved" and order.status == "PENDING" and not order.awaiting_refund:
            order.status = "CONFIRMED"
            outgoing = build_event("order.completed", order_id, {"order_id": str(order_id), "status": "CONFIRMED"})
        elif event_type == "stock.unavailable" and order.status == "PENDING" and not order.awaiting_refund:
            order.awaiting_refund = True
            outgoing = build_event("payment.refund.requested", order_id, {
                "order_id": str(order_id), "reason": payload.get("reason", "stock_unavailable"),
            })
        elif event_type == "payment.refunded" and order.status == "PENDING" and order.awaiting_refund:
            order.status = "CANCELLED"
            order.awaiting_refund = False
        if outgoing is not None:
            add_event(session, order_id, outgoing, carrier)
