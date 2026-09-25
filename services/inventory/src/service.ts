import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { carrierFromContext, InvalidRecord, type DomainEvent, type Carrier } from "@observa/messaging-node";
import type { Context } from "@opentelemetry/api";
import { decideReservation, type Item, type Mode } from "./decision.js";

export function paymentPayload(event: DomainEvent) {
  if (event.event_type !== "payment.approved") return null;
  const data = event.payload;
  const items = data.items;
  if (data.order_id !== event.correlation_id || !Array.isArray(items) || items.length === 0) throw new InvalidRecord("invalid payment.approved payload");
  for (const item of items) {
    if (!item || typeof item !== "object" || typeof item.product_id !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(item.product_id) ||
        !Number.isInteger(item.quantity) || item.quantity <= 0) {
      throw new InvalidRecord("invalid inventory item");
    }
  }
  const simulation = data.simulate as { stock?: string } | undefined;
  const mode = simulation?.stock ?? "catalog";
  if (!["catalog", "reserve", "unavailable"].includes(mode)) throw new InvalidRecord("invalid stock mode");
  return { orderId: event.correlation_id, items: items as Item[], mode: mode as Mode };
}

export async function handlePaymentApproved(pool: Pool, event: DomainEvent, parentContext: Context): Promise<boolean> {
  const payload = paymentPayload(event);
  if (!payload) return false;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const marker = await client.query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING event_id", [event.event_id]);
    if (marker.rowCount === 0) { await client.query("COMMIT"); return false; }
    const ids = [...new Set(payload.items.map(item => item.product_id))].sort();
    const rows = await client.query("SELECT id, quantity FROM products WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE", [ids]);
    const available = new Map<string, number>(rows.rows.map(row => [row.id, Number(row.quantity)]));
    const decision = decideReservation(payload.mode, payload.items, available);
    if (decision.reserved) {
      for (const item of decision.payload.items) {
        const result = await client.query("UPDATE products SET quantity = quantity - $1 WHERE id = $2 AND quantity >= $1 RETURNING id", [item.quantity, item.product_id]);
        if (result.rowCount !== 1) throw new Error("stock changed during reservation");
        await client.query("INSERT INTO reservations (id, order_id, product_id, quantity) VALUES ($1,$2,$3,$4)",
          [randomUUID(), payload.orderId, item.product_id, item.quantity]);
      }
    }
    const outgoing: DomainEvent = {
      event_id: randomUUID(), event_type: decision.reserved ? "stock.reserved" : "stock.unavailable",
      version: 1, correlation_id: payload.orderId, occurred_at: new Date().toISOString(),
      payload: { order_id: payload.orderId, ...decision.payload },
    };
    const carrier: Carrier = carrierFromContext(parentContext);
    await client.query("INSERT INTO outbox_events (event_id,event_type,payload,carrier) VALUES ($1,$2,$3::jsonb,$4::jsonb)",
      [outgoing.event_id, outgoing.event_type, JSON.stringify(outgoing), JSON.stringify(carrier)]);
    await client.query("COMMIT");
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally { client.release(); }
}

export async function relayOutbox(pool: Pool, publish: (event: DomainEvent, carrier: Carrier) => Promise<void>): Promise<number> {
  const rows = await pool.query("SELECT event_id,payload,carrier FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 50");
  for (const row of rows.rows) {
    await publish(row.payload as DomainEvent, row.carrier as Carrier);
    await pool.query("UPDATE outbox_events SET published_at=now() WHERE event_id=$1 AND published_at IS NULL", [row.event_id]);
  }
  return rows.rowCount ?? 0;
}
