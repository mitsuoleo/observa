import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { createMessaging, validateEnvelope, InvalidRecord, TOPIC, PARKED_TOPIC } from "../src/index.js";

const event = {
  event_id: "22222222-2222-4222-8222-222222222222",
  event_type: "order.created",
  version: 1,
  correlation_id: "11111111-1111-4111-8111-111111111111",
  occurred_at: "2026-09-24T12:00:00Z",
  payload: { customer_id: "customer-1" },
};
const parent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
const record = (value: Buffer | null = Buffer.from(JSON.stringify(event)), key: Buffer | null = Buffer.from(event.correlation_id)) => ({
  topic: TOPIC, partition: 2,
  message: { value, key, offset: "3", headers: { traceparent: Buffer.from(parent), tracestate: Buffer.from("observa=test") } },
});
const mock = () => {
  const producer = { send: vi.fn(async () => [{ partition: 2, baseOffset: "4" }]) };
  const consumer = { subscribe: vi.fn(async () => {}), run: vi.fn(async (_: unknown) => {}), commitOffsets: vi.fn(async () => {}) };
  return { producer, consumer };
};

describe("Node messaging contract", () => {
  it("accepts the shared OrderFlow v1 fixture", () => {
    const shared = JSON.parse(readFileSync(new URL("../../../tests/contracts/order-created.json", import.meta.url), "utf8"));
    expect(validateEnvelope(shared)).toEqual(shared);
  });
  it("validates the six required v1 fields while preserving extension fields", () => {
    expect(validateEnvelope({ ...event, extension: true })).toEqual({ ...event, extension: true });
    expect(() => validateEnvelope({ ...event, correlation_id: "bad" })).toThrow();
    expect(() => validateEnvelope({ ...event, event_type: "unknown" })).toThrow();
    expect(() => validateEnvelope({ ...event, version: 2 })).toThrow();
  });
  it("publishes with UTF-8 correlation key and rehydrated trace without changing JSON", async () => {
    const { producer, consumer } = mock();
    const messaging = createMessaging({ producer, consumer, groupId: "payment" });
    await messaging.publish(event, { traceparent: parent, tracestate: "observa=test" });
    const sent = producer.send.mock.calls[0][0] as any;
    expect(sent.topic).toBe(TOPIC);
    expect(sent.messages[0].key).toEqual(Buffer.from(event.correlation_id));
    expect(JSON.parse(sent.messages[0].value.toString())).toEqual(event);
    expect(sent.messages[0].headers.traceparent.toString()).toMatch(/^00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-/);
    expect(sent.messages[0].headers.tracestate.toString()).toBe("observa=test");
  });
  it("delivers validated event/context/metadata and commits next offset after handler", async () => {
    const { producer, consumer } = mock();
    const messaging = createMessaging({ producer, consumer, groupId: "payment" });
    const handler = vi.fn(async () => { expect(consumer.commitOffsets).not.toHaveBeenCalled(); });
    await messaging.consume(handler);
    expect(consumer.subscribe).toHaveBeenCalledWith({ topics: [TOPIC] });
    const options = consumer.run.mock.calls[0][0] as any;
    await options.eachMessage(record());
    expect(handler).toHaveBeenCalledWith(event, expect.anything(), expect.objectContaining({ groupId: "payment", partition: 2, offset: "3" }));
    expect(consumer.commitOffsets).toHaveBeenCalledWith([{ topic: TOPIC, partition: 2, offset: "4" }]);
  });
  it("parks invalid records with raw bytes and commits only after parking ack", async () => {
    const { producer, consumer } = mock();
    const messaging = createMessaging({ producer, consumer, groupId: "payment" });
    await messaging.consume(vi.fn());
    const options = consumer.run.mock.calls[0][0] as any;
    await options.eachMessage(record(Buffer.from("{")));
    expect(producer.send.mock.calls[0][0]).toMatchObject({ topic: PARKED_TOPIC });
    const parked = JSON.parse((producer.send.mock.calls[0][0] as any).messages[0].value.toString());
    expect(parked).toMatchObject({ raw_base64: Buffer.from("{").toString("base64"), origin: { topic: TOPIC, partition: 2, offset: "3", group_id: "payment" } });
    expect(parked.reason).toBeTruthy();
    expect(consumer.commitOffsets).toHaveBeenCalledTimes(1);
  });
  it("does not commit after handler or parking failure", async () => {
    const { producer, consumer } = mock();
    const messaging = createMessaging({ producer, consumer, groupId: "payment" });
    await messaging.consume(async () => { throw new Error("database down"); });
    const options = consumer.run.mock.calls[0][0] as any;
    await expect(options.eachMessage(record())).rejects.toThrow("database down");
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
    producer.send.mockRejectedValueOnce(new Error("broker down"));
    await expect(options.eachMessage(record(Buffer.from("{")))).rejects.toThrow("broker down");
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
  });
  it("parks a permanently invalid domain payload before committing its offset", async () => {
    const { producer, consumer } = mock();
    const messaging = createMessaging({ producer, consumer, groupId: "inventory" });
    await messaging.process(record(), async () => { throw new InvalidRecord("missing items"); });
    expect(producer.send.mock.calls[0][0]).toMatchObject({ topic: PARKED_TOPIC });
    expect(consumer.commitOffsets).toHaveBeenCalledWith([{ topic: TOPIC, partition: 2, offset: "4" }]);
  });
});
