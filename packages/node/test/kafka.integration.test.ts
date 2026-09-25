import { readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { it, expect } from "vitest";
import { trace } from "@opentelemetry/api";
import { createKafkaMessaging } from "../src/index.js";

const broker = process.env.OBSERVA_TEST_KAFKA;

it.skipIf(!broker)("publishes and consumes the shared v1 event with a real Kafka group", async () => {
  const fixture = JSON.parse(readFileSync(new URL("../../../tests/contracts/order-created.json", import.meta.url), "utf8"));
  const orderId = randomUUID();
  const event = { ...fixture, event_id: randomUUID(), event_type: "payment.refunded", correlation_id: orderId,
    payload: { order_id: orderId, payment_id: randomUUID(), amount: 0 } };
  const group = `observa-us002-node-${randomUUID()}`;
  const messaging = createKafkaMessaging(broker!, group);
  let completed!: () => void;
  const seen = new Promise<void>(resolve => { completed = resolve; });
  let consumed: { topic: string; partition: number; offset: string } | undefined;
  await messaging.producer.connect();
  await messaging.consumer.connect();
  try {
    await messaging.consume(async (value, parentContext, metadata) => {
      if (value.event_id !== event.event_id) return;
      expect(value).toEqual(event);
      expect(metadata.key).toBe(orderId);
      expect(metadata.groupId).toBe(group);
      expect(trace.getSpanContext(parentContext)?.traceId).toBe("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      consumed = metadata;
      completed();
    });
    await messaging.publish(event, { traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01" });
    await Promise.race([seen, new Promise((_, reject) => setTimeout(() => reject(new Error("Kafka delivery timeout")), 30000))]);
    expect(consumed).toBeDefined();
    const deadline = Date.now() + 10000;
    let offset = "-1";
    while (Date.now() < deadline) {
      const positions = await messaging.consumer.committed([{ topic: consumed!.topic, partition: consumed!.partition }]);
      offset = String(positions[0]?.offset);
      if (BigInt(offset) >= BigInt(consumed!.offset) + 1n) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    expect(BigInt(offset)).toBeGreaterThanOrEqual(BigInt(consumed!.offset) + 1n);
  } finally {
    await messaging.consumer.disconnect();
    await messaging.producer.disconnect();
  }
}, 45000);
