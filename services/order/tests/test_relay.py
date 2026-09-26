"""Outbox relay transaction and acknowledgement behavior."""

import asyncio
import os
import threading
from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.main import relay_pending
from app.models import OutboxEvent


def outbox_event():
    event_id = uuid4()
    return OutboxEvent(
        event_id=event_id,
        event_type="order.created",
        payload={"event_id": str(event_id), "correlation_id": str(uuid4())},
        carrier={"traceparent": "stored-context"},
    )


class Publisher:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def publish(self, payload, carrier):
        self.calls.append((payload, carrier))
        if self.fail:
            raise RuntimeError("Kafka acknowledgement failed")


@pytest.mark.asyncio
async def test_relay_marks_published_only_after_ack(session_factory):
    row = outbox_event()
    async with session_factory() as session:
        session.add(row)
        await session.commit()

    publisher = Publisher(fail=True)
    with pytest.raises(RuntimeError, match="acknowledgement"):
        await relay_pending(session_factory, publisher)

    async with session_factory() as session:
        assert (await session.get(OutboxEvent, row.id)).published_at is None

    assert await relay_pending(session_factory, publisher := Publisher()) is True
    assert publisher.calls == [(row.payload, row.carrier)]
    async with session_factory() as session:
        assert (await session.get(OutboxEvent, row.id)).published_at is not None
    assert await relay_pending(session_factory, publisher) is False
    assert len(publisher.calls) == 1


@pytest.mark.asyncio
async def test_ack_then_db_commit_failure_leaves_event_for_redelivery(session_factory):
    row = outbox_event()
    async with session_factory() as session:
        session.add(row)
        await session.commit()

    def fail_commit(_session):
        raise RuntimeError("database commit failed")

    publisher = Publisher()
    event.listen(AsyncSession.sync_session_class, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="database commit failed"):
            await relay_pending(session_factory, publisher)
    finally:
        event.remove(AsyncSession.sync_session_class, "before_commit", fail_commit)

    async with session_factory() as session:
        assert (await session.get(OutboxEvent, row.id)).published_at is None
    assert len(publisher.calls) == 1
    assert await relay_pending(session_factory, publisher) is True
    assert len(publisher.calls) == 2


@pytest.mark.asyncio
async def test_claim_query_uses_skip_locked(session_factory):
    statements = []
    engine = session_factory.kw["bind"]

    def capture(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        await relay_pending(session_factory, Publisher())
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)

    # SQLite drops FOR UPDATE; compile the actual query for PostgreSQL separately.
    from app.main import pending_outbox_query
    from sqlalchemy.dialects import postgresql

    sql = str(pending_outbox_query().compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert any("outbox_events" in statement for statement in statements)


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("OBSERVA_ORDER_TEST_DATABASE_URL"),
                    reason="set OBSERVA_ORDER_TEST_DATABASE_URL to a disposable PostgreSQL database")
async def test_two_replicas_claim_different_events_on_postgres():
    from app.models import Base

    engine = create_async_engine(os.environ["OBSERVA_ORDER_TEST_DATABASE_URL"])
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first, second = outbox_event(), outbox_event()
    entered, release = threading.Event(), threading.Event()

    class BlockingPublisher(Publisher):
        def publish(self, payload, carrier):
            entered.set()
            if not release.wait(10):
                raise TimeoutError("first publish was not released")
            return super().publish(payload, carrier)

    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as session:
            session.add_all([first, second])
            await session.commit()

        first_publisher, second_publisher = BlockingPublisher(), Publisher()
        first_task = asyncio.create_task(relay_pending(factory, first_publisher))
        assert await asyncio.to_thread(entered.wait, 10)
        assert await asyncio.wait_for(relay_pending(factory, second_publisher), 10) is True
        release.set()
        assert await asyncio.wait_for(first_task, 10) is True
        assert first_publisher.calls[0][0]["event_id"] != second_publisher.calls[0][0]["event_id"]
        assert {first_publisher.calls[0][0]["event_id"], second_publisher.calls[0][0]["event_id"]} == {
            str(first.event_id), str(second.event_id)
        }
        async with factory() as session:
            assert (await session.get(OutboxEvent, first.id)).published_at is not None
            assert (await session.get(OutboxEvent, second.id)).published_at is not None
    finally:
        release.set()
        async with factory() as session:
            for row in (first, second):
                if row.id is not None:
                    await session.delete(row)
            await session.commit()
        await engine.dispose()
