import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { expect, it } from "vitest";
import type { Carrier, DomainEvent } from "@observa/messaging-node";
import type { Db } from "../src/payment.js";
import { relayPaymentOutbox } from "../src/relay.js";

const databaseUrl = process.env.OBSERVA_PAYMENT_TEST_DATABASE_URL;

async function withDisposableSchema(run: (pool: pg.Pool) => Promise<void>): Promise<void> {
  if (!databaseUrl) throw new Error("OBSERVA_PAYMENT_TEST_DATABASE_URL is required");
  const schema = `observa_payment_test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  let pool: pg.Pool | undefined;
  try {
    await admin.query(`CREATE SCHEMA "${schema}"`);
    pool = new pg.Pool({ connectionString: databaseUrl, options: `-c search_path=${schema}`, max: 4 });
    await pool.query(await readFile(new URL("../schema.sql", import.meta.url), "utf8"));
    await run(pool);
  } finally {
    if (pool) await pool.end();
    await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await admin.end();
  }
}

async function enqueue(pool: pg.Pool): Promise<string> {
  const eventId = randomUUID();
  const orderId = randomUUID();
  const event: DomainEvent = {
    event_id: eventId, event_type: "payment.approved", version: 1,
    correlation_id: orderId, occurred_at: new Date().toISOString(),
    payload: { order_id: orderId },
  };
  await pool.query(
    "INSERT INTO outbox_events (event_id, event_type, payload, trace_carrier) VALUES ($1, $2, $3::jsonb, $4::jsonb)",
    [eventId, event.event_type, JSON.stringify(event), JSON.stringify({ traceparent: "stored-context" })],
  );
  return eventId;
}

it.skipIf(!databaseUrl)("two PostgreSQL relay instances claim different pending events", async () => {
  await withDisposableSchema(async pool => {
    const expected = new Set([await enqueue(pool), await enqueue(pool)]);
    let entered!: () => void;
    const firstEntered = new Promise<void>(resolve => { entered = resolve; });
    let release!: () => void;
    const ack = new Promise<void>(resolve => { release = resolve; });
    const sent: string[] = [];
    const first = relayPaymentOutbox(pool, async (event: DomainEvent) => {
      sent.push(event.event_id);
      entered();
      await ack;
    }, 1);
    try {
      await firstEntered;
      expect(await relayPaymentOutbox(pool, async (event: DomainEvent) => {
        sent.push(event.event_id);
      }, 1)).toBe(1);
    } finally {
      release();
      expect(await first).toBe(1);
    }
    expect(new Set(sent)).toEqual(expected);
    const rows = await pool.query("SELECT event_id FROM outbox_events WHERE published_at IS NOT NULL");
    expect(new Set(rows.rows.map(row => row.event_id))).toEqual(expected);
  });
}, 20_000);

it.skipIf(!databaseUrl)("ACK before failed commit leaves the PostgreSQL row for redelivery", async () => {
  await withDisposableSchema(async pool => {
    const eventId = await enqueue(pool);
    const sent: string[] = [];
    const publish = async (event: DomainEvent, _carrier: Carrier) => { sent.push(event.event_id); };
    const commitFaultDb: Db = {
      async connect() {
        const client = await pool.connect();
        let marked = false;
        return {
          async query(sql, params) {
            if (sql.startsWith("UPDATE outbox_events")) marked = true;
            if (sql === "COMMIT" && marked) throw new Error("simulated commit failure after ACK");
            return client.query(sql, params);
          },
          release() { client.release(); },
        };
      },
    };

    await expect(relayPaymentOutbox(commitFaultDb, publish, 1)).rejects.toThrow("simulated commit failure after ACK");
    const pending = await pool.query("SELECT published_at FROM outbox_events WHERE event_id = $1", [eventId]);
    expect(pending.rows[0].published_at).toBeNull();
    expect(await relayPaymentOutbox(pool, publish, 1)).toBe(1);
    expect(sent).toEqual([eventId, eventId]);
    const complete = await pool.query("SELECT published_at FROM outbox_events WHERE event_id = $1", [eventId]);
    expect(complete.rows[0].published_at).not.toBeNull();
  });
}, 20_000);
