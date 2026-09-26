import { describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { relayOutbox } from "../src/service.js";

const event = {
  event_id: "22222222-2222-4222-8222-222222222222",
  event_type: "stock.reserved",
  version: 1,
  correlation_id: "11111111-1111-4111-8111-111111111111",
  occurred_at: "2026-09-24T00:00:00Z",
  payload: { order_id: "11111111-1111-4111-8111-111111111111" },
};

function fakePool(options: { failCommitOnce?: boolean } = {}) {
  let locked = false;
  let published = false;
  let failCommit = options.failCommitOnce ?? false;
  const calls: string[] = [];
  const pool = {
    async connect() {
      let selected = false;
      let marked = false;
      return {
        async query(sql: string, params?: unknown[]) {
          calls.push(sql);
          if (sql === "BEGIN") return { rows: [], rowCount: null };
          if (sql.includes("FOR UPDATE SKIP LOCKED")) {
            if (locked || published) return { rows: [], rowCount: 0 };
            locked = true;
            selected = true;
            return { rows: [{ event_id: event.event_id, payload: event, carrier: {} }], rowCount: 1 };
          }
          if (sql.startsWith("UPDATE outbox_events")) {
            expect(selected).toBe(true);
            expect(params).toEqual([event.event_id]);
            marked = true;
            return { rows: [], rowCount: 1 };
          }
          if (sql === "COMMIT") {
            if (marked && failCommit) {
              failCommit = false;
              throw new Error("commit failed");
            }
            if (marked) published = true;
            locked = false;
            return { rows: [], rowCount: null };
          }
          if (sql === "ROLLBACK") {
            locked = false;
            return { rows: [], rowCount: null };
          }
          throw new Error(`unexpected query: ${sql}`);
        },
        release() {},
      };
    },
  };
  return { pool: pool as unknown as Pool, calls, isPublished: () => published };
}

describe("inventory outbox relay", () => {
  it("holds the row claim until ACK and excludes a concurrent relay", async () => {
    const db = fakePool();
    let releaseAck!: () => void;
    const ack = new Promise<void>(resolve => { releaseAck = resolve; });
    let publishing!: () => void;
    const enteredPublish = new Promise<void>(resolve => { publishing = resolve; });
    const first = relayOutbox(db.pool, async () => { publishing(); await ack; });
    await enteredPublish;
    expect(db.isPublished()).toBe(false);
    expect(await relayOutbox(db.pool, async () => { throw new Error("duplicate publish"); })).toBe(0);
    releaseAck();
    expect(await first).toBe(1);
    expect(db.isPublished()).toBe(true);
    expect(db.calls.findIndex(sql => sql.startsWith("UPDATE outbox_events"))).toBeLessThan(db.calls.lastIndexOf("COMMIT"));
  });

  it("redelivers after an ACK followed by failed database commit", async () => {
    const db = fakePool({ failCommitOnce: true });
    const sent: string[] = [];
    const publish = async () => { sent.push(event.event_id); };
    await expect(relayOutbox(db.pool, publish)).rejects.toThrow("commit failed");
    expect(db.isPublished()).toBe(false);
    expect(await relayOutbox(db.pool, publish)).toBe(1);
    expect(sent).toEqual([event.event_id, event.event_id]);
    expect(db.calls).toContain("ROLLBACK");
  });

  it("leaves a row pending when Kafka rejects publication", async () => {
    const db = fakePool();
    await expect(relayOutbox(db.pool, async () => { throw new Error("no ACK"); })).rejects.toThrow("no ACK");
    expect(db.isPublished()).toBe(false);
    expect(db.calls.some(sql => sql.startsWith("UPDATE outbox_events"))).toBe(false);
    expect(db.calls).toContain("ROLLBACK");
  });
});
