import type { Carrier, DomainEvent } from "@observa/messaging-node";
import type { Db } from "./payment.js";

type Pending = { event_id: string; payload: DomainEvent; trace_carrier: Carrier };

/** Keep the claim until the broker ACK and publish marker are committed. */
export async function relayPaymentOutbox(
  pool: Db,
  publish: (event: DomainEvent, carrier: Carrier) => Promise<void>,
  limit = 50,
): Promise<number> {
  let published = 0;
  for (let index = 0; index < limit; index++) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        "SELECT event_id, payload, trace_carrier FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED",
      );
      if (!result.rowCount) {
        await client.query("COMMIT");
        break;
      }
      const row = result.rows[0] as Pending;
      await publish(row.payload, row.trace_carrier);
      await client.query("UPDATE outbox_events SET published_at = now() WHERE event_id = $1", [row.event_id]);
      await client.query("COMMIT");
      published++;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
  return published;
}
