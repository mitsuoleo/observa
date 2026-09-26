"""HTTP and background Kafka runtime for the Order service."""

import asyncio
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, HTTPException, Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import REGISTRY, Counter, Histogram
from prometheus_client.openmetrics.exposition import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import select

from observa_messaging import KafkaPublisher, KafkaSubscriber, capture_carrier, consumer_config, producer_config

from .api_schemas import CreateOrderRequest, OrderOut, TimelineEntry
from .db import SessionLocal, engine
from .models import Base, OrderEventTimeline, OutboxEvent
from .service import create_order, get_order, handle_event
from .settings import settings


PROCESSED = Counter("observa_events_processed_total", "Processed domain events", ["service", "outcome"])
DURATION = Histogram("observa_processing_duration_seconds", "Domain event processing duration", ["service"])
ERRORS = Counter("observa_errors_total", "Domain runtime errors", ["service", "operation"])
state = {"ready": False, "fatal": None}
stop_event = threading.Event()
provider = TracerProvider(resource=Resource.create({"service.name": "order-service", "service.instance.id": os.getenv("HOSTNAME", "local")}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{settings.otel_exporter_otlp_endpoint}/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("observa.order")


def log(event: str, **fields) -> None:
    span = trace.get_current_span().get_span_context()
    record = {"event": event, "service_name": "order-service", "service_instance_id": os.getenv("HOSTNAME", "local"),
              "trace_id": f"{span.trace_id:032x}" if span.is_valid else None,
              "span_id": f"{span.span_id:016x}" if span.is_valid else None, **fields}
    print(json.dumps(record, default=str), flush=True)


def pending_outbox_query():
    """Claim one unpublished event without waiting for another relay's claim."""
    return (select(OutboxEvent).where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id).limit(1).with_for_update(skip_locked=True))


async def relay_pending(session_factory, publisher: KafkaPublisher) -> bool:
    """Hold the claim until Kafka acknowledges and the publication marker commits."""
    async with session_factory() as session:
        async with session.begin():
            row = (await session.execute(pending_outbox_query())).scalar_one_or_none()
            if row is None:
                return False
            await asyncio.to_thread(publisher.publish, row.payload, row.carrier)
            row.published_at = datetime.now(timezone.utc)
            order_id = row.payload["correlation_id"]
            event_id = row.event_id
    log("order_event_published", order_id=order_id, event_id=str(event_id))
    return True


async def relay_loop(publisher: KafkaPublisher) -> None:
    while not stop_event.is_set():
        try:
            await relay_pending(SessionLocal, publisher)
        except Exception as exc:
            ERRORS.labels("order", "relay").inc()
            log("order_relay_error", level="ERROR", error=str(exc))
        await asyncio.sleep(settings.outbox_poll_interval)


async def apply_incoming(event: dict, carrier: dict) -> None:
    started = time.perf_counter()
    async with SessionLocal() as session:
        try:
            await handle_event(session, event, carrier)
        finally:
            traceparent = carrier.get("traceparent", "")
            exemplar = {"trace_id": traceparent[3:35]} if len(traceparent) >= 55 else None
            DURATION.labels("order").observe(time.perf_counter() - started, exemplar=exemplar)
    PROCESSED.labels("order", "success").inc()


def consume_loop(consumer: Consumer, producer: Producer, loop: asyncio.AbstractEventLoop) -> None:
    adapter = KafkaSubscriber(consumer, producer, tracer)
    try:
        consumer.subscribe(["order.events.v1"])
        while not stop_event.is_set():
            message = consumer.poll(0.5)
            if message is None:
                continue

            def handler(event, _context, metadata):
                carrier = capture_carrier()
                future = asyncio.run_coroutine_threadsafe(apply_incoming(event, carrier), loop)
                future.result(timeout=60)
                log("order_event_processed", order_id=event["correlation_id"], event_id=event["event_id"],
                    topic=metadata.topic, partition=metadata.partition, offset=metadata.offset)

            adapter.process_message(message, handler)
    except Exception as exc:
        state["ready"] = False
        state["fatal"] = str(exc)
        ERRORS.labels("order", "consume").inc()
        log("order_consumer_fatal", level="ERROR", error=str(exc))
    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    producer = Producer(producer_config(settings.kafka_bootstrap_servers))
    await asyncio.to_thread(producer.list_topics, timeout=15)
    consumer = Consumer(consumer_config(settings.kafka_bootstrap_servers, "observa.order"))
    stop_event.clear()
    state["fatal"] = None
    worker = threading.Thread(target=consume_loop, args=(consumer, producer, asyncio.get_running_loop()), daemon=True)
    worker.start()
    relay_task = asyncio.create_task(relay_loop(KafkaPublisher(producer, tracer)))
    state["ready"] = True
    try:
        yield
    finally:
        state["ready"] = False
        stop_event.set()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        await asyncio.to_thread(worker.join, 5)
        provider.force_flush()
        provider.shutdown()
        await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
@app.get("/ready")
async def health():
    if not state["ready"]:
        return Response(content=json.dumps({"ready": False}), status_code=503, media_type="application/json")
    return {"ready": True}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.post("/orders", status_code=202, response_model=OrderOut)
async def post_order(body: CreateOrderRequest):
    with tracer.start_as_current_span("order.create"):
        async with SessionLocal() as session:
            order = await create_order(session, body, capture_carrier())
            log("order_created", order_id=str(order.id))
            return OrderOut.model_validate(order)


@app.get("/orders/{order_id}", response_model=OrderOut)
async def read_order(order_id: UUID):
    async with SessionLocal() as session:
        order = await get_order(session, order_id)
        if order is None:
            raise HTTPException(404, "order not found")
        return OrderOut.model_validate(order)


@app.get("/orders/{order_id}/timeline", response_model=list[TimelineEntry])
async def read_timeline(order_id: UUID):
    async with SessionLocal() as session:
        if await get_order(session, order_id) is None:
            raise HTTPException(404, "order not found")
        rows = (await session.execute(select(OrderEventTimeline).where(OrderEventTimeline.order_id == order_id)
                                      .order_by(OrderEventTimeline.occurred_at, OrderEventTimeline.id))).scalars()
        return [TimelineEntry.model_validate(row) for row in rows]
