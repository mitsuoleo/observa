import { describe, expect, it, vi } from "vitest";
import type { Db, QueryResult } from "../src/payment.js";
import { relayPaymentOutbox } from "../src/relay.js";

const event = {
  event_id: "22222222-2222-4222-8222-222222222222",
  event_type: "payment.approved",
  version: 1,
  correlation_id: "11111111-1111-4111-8111-111111111111",
  occurred_at: "2026-09-24T00:00:00Z",
  payload: { order_id: "11111111-1111-4111-8111-111111111111" },
};

function fakeDb() {
  const sql: string[] = [];
  let pending = true;
  const release = vi.fn();
  const query = vi.fn(async (statement: string): Promise<QueryResult> => {
    sql.push(statement);
    if (statement.startsWith("SELECT")) return pending
      ? { rowCount: 1, rows: [{ event_id: event.event_id, payload: event, trace_carrier: { traceparent: "parent" } }] }
      : { rowCount: 0, rows: [] };
    if (statement.startsWith("UPDATE")) pending = false;
    return { rowCount: 1, rows: [] };
  });
  const pool = { connect: async () => ({ query, release }) } as Db;
  return { pool, sql, release, get pending() { return pending; } };
}

describe("payment outbox relay", () => {
  it("holds the row claim through ACK and marker commit", async () => {
    const db = fakeDb();
    const publish = vi.fn(async () => { expect(db.sql.at(-1)).toContain("SKIP LOCKED"); });
    expect(await relayPaymentOutbox(db.pool, publish)).toBe(1);
    expect(publish).toHaveBeenCalledWith(event, { traceparent: "parent" });
    expect(db.sql.slice(0, 4).map(statement => statement.split(" ")[0])).toEqual(["BEGIN", "SELECT", "UPDATE", "COMMIT"]);
    expect(db.pending).toBe(false);
    expect(db.release).toHaveBeenCalledTimes(2);
  });

  it("rolls back without a marker when Kafka does not acknowledge", async () => {
    const db = fakeDb();
    await expect(relayPaymentOutbox(db.pool, async () => { throw new Error("no ACK"); })).rejects.toThrow("no ACK");
    expect(db.sql.at(-1)).toBe("ROLLBACK");
    expect(db.sql.some(statement => statement.startsWith("UPDATE"))).toBe(false);
    expect(db.pending).toBe(true);
    expect(db.release).toHaveBeenCalledTimes(1);
  });
});
