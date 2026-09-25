import { describe, expect, it, vi } from "vitest";
import { InvalidRecord } from "@observa/messaging-node";
import { decidePayment, buildEvent, handlePayment, type Db, type QueryResult } from "../src/payment.js";

const orderId = "11111111-1111-4111-8111-111111111111";
const input = (type = "order.created", simulate = "approve") => ({
  event_id: "22222222-2222-4222-8222-222222222222",
  event_type: type,
  version: 1,
  correlation_id: orderId,
  occurred_at: "2026-09-24T00:00:00Z",
  payload: type === "order.created" ? {
    order_id: orderId, customer_id: "33333333-3333-4333-8333-333333333333", total_amount: 12,
    items: [{ product_id: "44444444-4444-4444-8444-444444444444", quantity: 1, unit_price: 12 }],
    simulate: { payment: simulate, stock: "unavailable" },
  } : { order_id: orderId, reason: "stock_unavailable" },
});
function db(status?: string, duplicate = false) {
  const calls: Array<{ sql: string; params?: unknown[] }> = [];
  let payment = status ? { id: "55555555-5555-4555-8555-555555555555", amount: "12.00", status } : undefined;
  const query = vi.fn(async (sql: string, params?: unknown[]): Promise<QueryResult> => {
    calls.push({ sql, params });
    if (sql.includes("INSERT INTO processed_events")) return { rowCount: duplicate ? 0 : 1, rows: duplicate ? [] : [{ event_id: input().event_id }] };
    if (sql.includes("SELECT id, amount, status FROM payments")) return { rowCount: payment ? 1 : 0, rows: payment ? [payment] : [] };
    if (sql.includes("INSERT INTO payments")) payment = { id: params?.[0] as string, amount: String(params?.[3]), status: params?.[2] as string };
    if (sql.includes("UPDATE payments SET status")) payment = { ...payment!, status: "REFUNDED" };
    return { rowCount: 1, rows: [] };
  });
  return { pool: { connect: async () => ({ query, release: vi.fn() }) } as Db, calls, query };
}

describe("payment domain transaction", () => {
  it("honors forced decisions and bounded random rate", () => {
    expect(decidePayment("approve", 0, () => 1)).toBe("APPROVED");
    expect(decidePayment("reject", 1, () => 0)).toBe("REJECTED");
    expect(decidePayment("random", 0.8, () => 0.9)).toBe("REJECTED");
  });
  it("writes approval, processed marker and outbox with separate carrier before commit", async () => {
    const { pool, calls } = db();
    await handlePayment(pool, input(), { traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01" });
    expect(calls.map(x => x.sql.trim())).toEqual(expect.arrayContaining(["BEGIN", "COMMIT"]));
    const insert = calls.find(x => x.sql.includes("INSERT INTO outbox_events"))!;
    expect(JSON.parse(insert.params![2] as string)).toMatchObject({ event_type: "payment.approved", payload: { order_id: orderId, simulate: { stock: "unavailable" } } });
    expect(JSON.parse(insert.params![2] as string)).not.toHaveProperty("traceparent");
    expect(JSON.parse(insert.params![3] as string)).toHaveProperty("traceparent");
    expect(calls.findIndex(x => x.sql.includes("INSERT INTO outbox_events"))).toBeLessThan(calls.findIndex(x => x.sql === "COMMIT"));
  });
  it("rejects with v1 payload reason", async () => {
    const { pool, calls } = db();
    await handlePayment(pool, input("order.created", "reject"), {});
    const out = JSON.parse(calls.find(x => x.sql.includes("INSERT INTO outbox_events"))!.params![2] as string);
    expect(out).toMatchObject({ event_type: "payment.rejected", payload: { reason: "payment_declined" } });
  });
  it("parks an invalid payment mode without persisting an effect", async () => {
    const { pool, calls } = db();
    await expect(handlePayment(pool, input("order.created", "unknown"), {})).rejects.toBeInstanceOf(InvalidRecord);
    expect(calls.some(x => x.sql.includes("INSERT INTO payments") || x.sql.includes("INSERT INTO outbox_events"))).toBe(false);
    expect(calls.at(-1)?.sql).toBe("ROLLBACK");
  });
  it("refunds only approved payments and emits once", async () => {
    const { pool, calls } = db("APPROVED");
    await handlePayment(pool, input("payment.refund.requested"), {});
    expect(calls.some(x => x.sql.includes("UPDATE payments SET status"))).toBe(true);
    expect(JSON.parse(calls.find(x => x.sql.includes("INSERT INTO outbox_events"))!.params![2] as string).event_type).toBe("payment.refunded");
  });
  it("does not produce a second effect for duplicate event", async () => {
    const { pool, calls } = db(undefined, true);
    await handlePayment(pool, input(), {});
    expect(calls.some(x => x.sql.includes("INSERT INTO payments") || x.sql.includes("INSERT INTO outbox_events"))).toBe(false);
  });
  it("rolls back on a database error so Kafka offset can remain uncommitted", async () => {
    const { pool, query, calls } = db();
    query.mockImplementationOnce(async () => { throw new Error("database down"); });
    await expect(handlePayment(pool, input(), {})).rejects.toThrow("database down");
    expect(calls.at(-1)?.sql).toBe("ROLLBACK");
  });
  it("does not refund a missing payment", async () => {
    const { pool, calls } = db();
    await expect(handlePayment(pool, input("payment.refund.requested"), {})).rejects.toBeInstanceOf(InvalidRecord);
    expect(calls.at(-1)?.sql).toBe("ROLLBACK");
  });
  it("does not refund a rejected payment", async () => {
    const { pool, calls } = db("REJECTED");
    await expect(handlePayment(pool, input("payment.refund.requested"), {})).rejects.toBeInstanceOf(InvalidRecord);
    expect(calls.at(-1)?.sql).toBe("ROLLBACK");
  });
  it("builds an unmodified six-field v1 envelope", () => {
    expect(buildEvent("payment.rejected", orderId, { order_id: orderId })).toMatchObject({ event_type: "payment.rejected", version: 1, correlation_id: orderId });
  });
});
