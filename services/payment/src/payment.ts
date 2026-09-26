import { randomUUID } from "node:crypto";
import { InvalidRecord, type DomainEvent, type Carrier } from "@observa/messaging-node";

export type QueryResult = { rowCount: number | null; rows: Record<string, unknown>[] };
export type DbClient = { query(sql: string, params?: unknown[]): Promise<QueryResult>; release(): void };
export type Db = { connect(): Promise<DbClient> };
type Payment = { id: string; amount: string | number; status: string };
type Authorization = "APPROVED" | "REJECTED";
export type AuthorizePayment = (orderId: string, desired: Authorization) => Promise<Authorization>;

export function decidePayment(
  simulate: unknown,
  approveRate = 0.8,
  random: () => number = Math.random,
): "APPROVED" | "REJECTED" {
  if (simulate === "approve") return "APPROVED";
  if (simulate === "reject") return "REJECTED";
  return random() < approveRate ? "APPROVED" : "REJECTED";
}

export function buildEvent(eventType: string, orderId: string, payload: Record<string, unknown>): DomainEvent {
  return {
    event_id: randomUUID(), event_type: eventType, version: 1,
    correlation_id: orderId, occurred_at: new Date().toISOString(), payload,
  };
}

function orderIdOf(event: DomainEvent): string {
  const orderId = event.payload.order_id;
  if (typeof orderId !== "string" || orderId !== event.correlation_id) {
    throw new InvalidRecord("payload order_id must equal correlation_id");
  }
  return orderId;
}

async function enqueue(client: DbClient, outgoing: DomainEvent, carrier: Carrier): Promise<void> {
  await client.query(
    `INSERT INTO outbox_events (event_id, event_type, payload, trace_carrier)
     VALUES ($1, $2, $3::jsonb, $4::jsonb) ON CONFLICT (event_id) DO NOTHING`,
    [outgoing.event_id, outgoing.event_type, JSON.stringify(outgoing), JSON.stringify(carrier)],
  );
}

async function onCreated(client: DbClient, event: DomainEvent, carrier: Carrier, approveRate: number, authorize: AuthorizePayment): Promise<void> {
  const orderId = orderIdOf(event);
  const payload = event.payload;
  if (!Array.isArray(payload.items) || payload.items.length === 0 ||
      typeof payload.total_amount !== "number" || !Number.isFinite(payload.total_amount) || payload.total_amount < 0) {
    throw new InvalidRecord("invalid order.created payload");
  }
  const existing = await client.query("SELECT id, amount, status FROM payments WHERE order_id = $1 FOR UPDATE", [orderId]);
  if (existing.rowCount) return;
  const simulate = payload.simulate && typeof payload.simulate === "object"
    ? payload.simulate as Record<string, unknown> : {};
  if (simulate.payment !== undefined && !["approve", "reject", "random"].includes(simulate.payment as string)) {
    throw new InvalidRecord("invalid payment mode");
  }
  if (simulate.stock !== undefined && !["catalog", "reserve", "unavailable"].includes(simulate.stock as string)) {
    throw new InvalidRecord("invalid stock mode");
  }
  const status = await authorize(orderId, decidePayment(simulate.payment, approveRate));
  const paymentId = randomUUID();
  await client.query(
    `INSERT INTO payments (id, order_id, status, amount) VALUES ($1, $2, $3, $4)`,
    [paymentId, orderId, status, payload.total_amount],
  );
  const outgoing = status === "APPROVED"
    ? buildEvent("payment.approved", orderId, {
      order_id: orderId, payment_id: paymentId, amount: payload.total_amount,
      items: payload.items, simulate: { stock: simulate.stock ?? "catalog" },
    })
    : buildEvent("payment.rejected", orderId, {
      order_id: orderId, payment_id: paymentId, amount: payload.total_amount, reason: "payment_declined",
    });
  await enqueue(client, outgoing, carrier);
}

async function onRefund(client: DbClient, event: DomainEvent, carrier: Carrier): Promise<void> {
  const orderId = orderIdOf(event);
  const result = await client.query("SELECT id, amount, status FROM payments WHERE order_id = $1 FOR UPDATE", [orderId]);
  if (!result.rowCount) throw new InvalidRecord("payment not found for refund");
  const payment = result.rows[0] as Payment;
  if (payment.status === "REFUNDED") return;
  if (payment.status !== "APPROVED") throw new InvalidRecord("only approved payments can be refunded");
  await client.query("UPDATE payments SET status = 'REFUNDED', updated_at = now() WHERE order_id = $1", [orderId]);
  await enqueue(client, buildEvent("payment.refunded", orderId, {
    order_id: orderId, payment_id: payment.id, amount: Number(payment.amount),
  }), carrier);
}

export async function handlePayment(
  pool: Db, event: DomainEvent, carrier: Carrier,
  approveRate = 0.8,
  authorize: AuthorizePayment = async (_orderId, desired) => desired,
): Promise<void> {
  if (event.event_type !== "order.created" && event.event_type !== "payment.refund.requested") return;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const processed = await client.query(
      `INSERT INTO processed_events (event_id) VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING RETURNING event_id`, [event.event_id],
    );
    if (processed.rowCount) {
      if (event.event_type === "order.created") await onCreated(client, event, carrier, approveRate, authorize);
      else await onRefund(client, event, carrier);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
