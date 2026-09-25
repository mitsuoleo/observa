import { createRequire } from "node:module";
import { context, trace, SpanKind, SpanStatusCode, type Context } from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";

export const TOPIC = "order.events.v1";
export const PARKED_TOPIC = "order.events.v1.parked";
export class InvalidRecord extends Error {}
const EVENTS = new Set([
  "order.created", "order.completed", "order.failed", "payment.approved",
  "payment.rejected", "payment.refund.requested", "payment.refunded",
  "stock.reserved", "stock.unavailable",
]);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const DATE_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;
const propagator = new W3CTraceContextPropagator();
const getter = { get: (carrier: Carrier, key: string) => carrier[key], keys: (carrier: Carrier) => Object.keys(carrier) };
const setter = { set: (carrier: Record<string, string>, key: string, value: string) => { carrier[key] = value; } };

export function carrierFromContext(ctx: Context = context.active()): Carrier {
  const carrier: Record<string, string> = {};
  propagator.inject(ctx, carrier, setter);
  return Object.freeze(carrier);
}

export type DomainEvent = Readonly<{
  event_id: string;
  event_type: string;
  version: number;
  correlation_id: string;
  occurred_at: string;
  payload: Record<string, unknown>;
  [extension: string]: unknown;
}>;
export type Carrier = Readonly<Record<string, string>>;
export type Headers = Record<string, string | Buffer | Array<string | Buffer> | undefined>;
export type KafkaRecord = {
  topic: string;
  partition: number;
  message: { value: Buffer | null; key: Buffer | null; offset: string; headers?: Headers };
};
export type Metadata = Readonly<{
  topic: string;
  partition: number;
  offset: string;
  groupId: string;
  key: string;
}>;
export type Handler = (event: DomainEvent, parentContext: Context, metadata: Metadata) => Promise<void>;
export type Producer = {
  send(record: { topic: string; messages: Array<{ key: Buffer | null; value: Buffer; headers?: Record<string, Buffer> }> }): Promise<unknown>;
};
export type Consumer = {
  subscribe(options: { topics: string[] }): Promise<unknown>;
  run(options: { partitionsConsumedConcurrently: number; eachMessage: (record: KafkaRecord) => Promise<void> }): Promise<unknown>;
  commitOffsets(offsets: Array<{ topic: string; partition: number; offset: string }>): Promise<unknown>;
};

export function validateEnvelope(input: unknown): DomainEvent {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new Error("envelope must be an object");
  const value = input as Record<string, unknown>;
  if (typeof value.event_id !== "string" || !UUID.test(value.event_id)) throw new Error("invalid event_id");
  if (typeof value.event_type !== "string" || !EVENTS.has(value.event_type)) throw new Error("invalid event_type");
  if (value.version !== 1) throw new Error("invalid version");
  if (typeof value.correlation_id !== "string" || !UUID.test(value.correlation_id)) throw new Error("invalid correlation_id");
  if (typeof value.occurred_at !== "string" || !DATE_TIME.test(value.occurred_at) || !Number.isFinite(Date.parse(value.occurred_at))) throw new Error("invalid occurred_at");
  if (!value.payload || typeof value.payload !== "object" || Array.isArray(value.payload)) throw new Error("invalid payload");
  return Object.freeze({ ...value }) as DomainEvent;
}

function cleanCarrier(headers: Headers = {}): Carrier {
  const carrier: Record<string, string> = {};
  for (const name of ["traceparent", "tracestate"] as const) {
    const raw = headers[name];
    if (raw !== undefined && !Array.isArray(raw)) carrier[name] = raw.toString();
  }
  return carrier;
}

function nextOffset(offset: string): string {
  if (!/^\d+$/.test(offset)) throw new Error("invalid Kafka offset");
  return (BigInt(offset) + 1n).toString();
}

export function createMessaging(deps: { producer: Producer; consumer: Consumer; groupId: string }) {
  if (!deps.groupId.trim()) throw new Error("groupId is required");
  async function publish(eventInput: unknown, carrier: Carrier = {}): Promise<void> {
    const tracer = trace.getTracer("observa-messaging-node");
    const event = validateEnvelope(eventInput);
    const parent = propagator.extract(context.active(), carrier, getter);
    await tracer.startActiveSpan("kafka.publish", { kind: SpanKind.PRODUCER,
      attributes: { "messaging.system": "kafka", "messaging.destination.name": TOPIC, "messaging.kafka.message.key": event.correlation_id } }, parent,
    async (span) => {
      try {
        const headers: Record<string, string> = {};
        propagator.inject(context.active(), headers, setter);
        // A service without a registered tracer provider still preserves a valid
        // outbox carrier. With a provider, the active producer span takes precedence.
        if (!headers.traceparent) propagator.inject(parent, headers, setter);
        await deps.producer.send({ topic: TOPIC, messages: [{
          key: Buffer.from(event.correlation_id, "utf8"),
          value: Buffer.from(JSON.stringify(event), "utf8"),
          headers: Object.fromEntries(Object.entries(headers).map(([name, value]) => [name, Buffer.from(value, "utf8")])),
        }] });
      } catch (error) {
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally { span.end(); }
    });
  }
  async function park(input: KafkaRecord, reason: string): Promise<void> {
    const { topic, partition, message } = input;
    const value = Buffer.from(JSON.stringify({
      raw_base64: message.value?.toString("base64") ?? null,
      origin: { topic, partition, offset: message.offset, group_id: deps.groupId },
      reason,
    }), "utf8");
    await deps.producer.send({ topic: PARKED_TOPIC, messages: [{ key: message.key, value }] });
  }
  async function process(input: KafkaRecord, handler: Handler): Promise<void> {
    const tracer = trace.getTracer("observa-messaging-node");
    const { topic, partition, message } = input;
    let event: DomainEvent;
    let offset: string;
    try {
      offset = nextOffset(message.offset);
      if (topic !== TOPIC) throw new Error("invalid topic");
      event = validateEnvelope(JSON.parse(message.value?.toString("utf8") ?? "null"));
      if (!message.key?.equals(Buffer.from(event.correlation_id, "utf8"))) throw new Error("Kafka key must equal correlation_id");
    } catch (error) {
      // A malformed offset cannot be committed and needs operator intervention.
      if (!/^\d+$/.test(message.offset)) throw error;
      await park(input, error instanceof Error ? error.message : String(error));
      await deps.consumer.commitOffsets([{ topic, partition, offset: nextOffset(message.offset) }]);
      return;
    }
    const parent = propagator.extract(context.active(), cleanCarrier(message.headers), getter);
    const metadata: Metadata = Object.freeze({ topic, partition, offset: message.offset,
      groupId: deps.groupId, key: event.correlation_id });
    await tracer.startActiveSpan("kafka.consume", { kind: SpanKind.CONSUMER,
      attributes: { "messaging.system": "kafka", "messaging.destination.name": topic,
        "messaging.kafka.partition": partition, "messaging.kafka.offset": message.offset,
        "messaging.consumer.group.name": deps.groupId } }, parent,
    async (span) => {
      try {
        const handlerContext = trace.getSpanContext(context.active()) ? context.active() : parent;
        await handler(event, handlerContext, metadata);
        await deps.consumer.commitOffsets([{ topic, partition, offset }]);
      } catch (error) {
        span.setStatus({ code: SpanStatusCode.ERROR });
        if (error instanceof InvalidRecord) {
          await park(input, error.message);
          await deps.consumer.commitOffsets([{ topic, partition, offset }]);
          return;
        }
        throw error;
      } finally { span.end(); }
    });
  }
  async function consume(handler: Handler): Promise<void> {
    await deps.consumer.subscribe({ topics: [TOPIC] });
    await deps.consumer.run({ partitionsConsumedConcurrently: 1, eachMessage: (record) => process(record, handler) });
  }
  return { publish, consume, process };
}

export function createKafkaMessaging(bootstrapServers: string, groupId: string) {
  const require = createRequire(import.meta.url);
  const kafkaPackage = require("@confluentinc/kafka-javascript") as typeof import("@confluentinc/kafka-javascript");
  const { Kafka } = kafkaPackage.KafkaJS;
  const kafka = new Kafka({ "bootstrap.servers": bootstrapServers });
  const producer = kafka.producer({ "enable.idempotence": true, acks: -1, partitioner: "murmur2_random" });
  const consumer = kafka.consumer({ "group.id": groupId, "enable.auto.commit": false,
    "auto.offset.reset": "earliest", "partition.assignment.strategy": "range" });
  return { ...createMessaging({ producer, consumer, groupId }), producer, consumer };
}
