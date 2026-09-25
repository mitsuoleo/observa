import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import pg from "pg";
import { context } from "@opentelemetry/api";
import { carrierFromContext, createKafkaMessaging, type DomainEvent, type Carrier } from "@observa/messaging-node";
import { handlePayment } from "./payment.js";
import { log, startTelemetry } from "./telemetry.js";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL is required");
const pool = new pg.Pool({ connectionString: databaseUrl });
const messaging = createKafkaMessaging(process.env.KAFKA_BOOTSTRAP_SERVERS ?? "kafka:9092", "observa.payment");
const provider = startTelemetry();
const approveRate = Number(process.env.PAYMENT_APPROVE_RATE ?? "0.8");
if (!Number.isFinite(approveRate) || approveRate < 0 || approveRate > 1) throw new Error("PAYMENT_APPROVE_RATE must be between 0 and 1");
let ready = false;
let stopping = false;
let consumed = 0;
let published = 0;
let errors = 0;
let durationSeconds = 0;
let relaying = false;

const server = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(ready ? 200 : 503, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ready }));
  } else if (req.url === "/metrics") {
    res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4" });
    res.end([
      "# TYPE payment_consumed_total counter", `payment_consumed_total ${consumed}`,
      "# TYPE payment_published_total counter", `payment_published_total ${published}`,
      "# TYPE payment_errors_total counter", `payment_errors_total ${errors}`,
      "# TYPE payment_processing_duration_seconds_sum counter", `payment_processing_duration_seconds_sum ${durationSeconds}`,
    ].join("\n") + "\n");
  } else { res.writeHead(404); res.end(); }
}).listen(Number(process.env.PORT ?? "8000"), "0.0.0.0");

async function relay(): Promise<void> {
  if (relaying) return;
  relaying = true;
  try {
    const result = await pool.query<{ event_id: string; payload: DomainEvent; trace_carrier: Carrier }>(
      "SELECT event_id, payload, trace_carrier FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 50",
    );
    for (const row of result.rows) {
      await messaging.publish(row.payload, row.trace_carrier);
      await pool.query("UPDATE outbox_events SET published_at = now() WHERE event_id = $1 AND published_at IS NULL", [row.event_id]);
      published++;
      log("payment_event_published", { order_id: row.payload.correlation_id, event_id: row.event_id, event_type: row.payload.event_type });
    }
  } finally { relaying = false; }
}

async function stop(code: number): Promise<void> {
  if (stopping) return;
  stopping = true; ready = false;
  const timeout = setTimeout(() => process.exit(code || 1), 10000); timeout.unref();
  try {
    await messaging.consumer.disconnect();
    await messaging.producer.disconnect();
    await pool.end();
    await provider.shutdown();
    server.close();
  } catch (error) { log("payment_shutdown_error", { level: "ERROR", error: String(error) }); code = 1; }
  process.exit(code);
}
process.once("SIGTERM", () => void stop(0));
process.once("SIGINT", () => void stop(0));

try {
  await pool.query(await readFile(new URL("../schema.sql", import.meta.url), "utf8"));
  await messaging.producer.connect();
  await messaging.consumer.connect();
  await messaging.consume(async (event, parent, metadata) => {
    if (event.event_type !== "order.created" && event.event_type !== "payment.refund.requested") return;
    const start = performance.now();
    try {
      await context.with(parent, () => handlePayment(pool, event, carrierFromContext(parent), approveRate));
      consumed++;
      durationSeconds += (performance.now() - start) / 1000;
      log("payment_event_processed", { order_id: event.correlation_id, event_id: event.event_id, event_type: event.event_type, partition: metadata.partition, offset: metadata.offset });
    } catch (error) {
      errors++;
      log("payment_process_error", { level: "ERROR", order_id: event.correlation_id, event_id: event.event_id, error: String(error) });
      throw error;
    }
  });
  ready = true;
  log("payment_ready");
  setInterval(() => void relay().catch(error => {
    errors++; log("payment_relay_error", { level: "ERROR", error: String(error) });
  }), 400);
} catch (error) {
  log("payment_start_error", { level: "ERROR", error: String(error) });
  await stop(1);
}
