import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import pg from "pg";
import { createKafkaMessaging } from "@observa/messaging-node";
import { handlePaymentApproved, relayOutbox } from "./service.js";
import { log, startTelemetry } from "./telemetry.js";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL is required");
const pool = new pg.Pool({ connectionString: databaseUrl });
const messaging = createKafkaMessaging(process.env.KAFKA_BOOTSTRAP_SERVERS ?? "kafka:9092", "observa.inventory");
const provider = startTelemetry();
let ready = false;
let consumed = 0;
let published = 0;
let errors = 0;
let durationSeconds = 0;
let relaying = false;
let timer: NodeJS.Timeout | undefined;

const server = createServer((req, res) => {
  if (req.url === "/health" || req.url === "/ready") {
    res.writeHead(ready ? 200 : 503, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ready }));
  } else if (req.url === "/metrics") {
    res.writeHead(200, { "Content-Type": "text/plain; version=0.0.4" });
    res.end([
      "# TYPE inventory_consumed_total counter", `inventory_consumed_total ${consumed}`,
      "# TYPE inventory_published_total counter", `inventory_published_total ${published}`,
      "# TYPE inventory_errors_total counter", `inventory_errors_total ${errors}`,
      "# TYPE inventory_processing_duration_seconds_sum counter", `inventory_processing_duration_seconds_sum ${durationSeconds}`,
    ].join("\n") + "\n");
  } else { res.writeHead(404); res.end(); }
}).listen(Number(process.env.PORT ?? "8000"), "0.0.0.0");

async function stop(code: number): Promise<void> {
  ready = false;
  if (timer) clearInterval(timer);
  const timeout = setTimeout(() => process.exit(code || 1), 10000); timeout.unref();
  try {
    await messaging.consumer.disconnect();
    await messaging.producer.disconnect();
    await pool.end();
    await provider.shutdown();
    server.close();
  } catch (error) { log("inventory_shutdown_error", { level: "ERROR", error: String(error) }); code = 1; }
  process.exit(code);
}

process.once("SIGTERM", () => void stop(0));
process.once("SIGINT", () => void stop(0));

try {
  await pool.query(await readFile(new URL("../schema.sql", import.meta.url), "utf8"));
  await messaging.producer.connect();
  await messaging.consumer.connect();
  await messaging.consume(async (event, parent, metadata) => {
    if (event.event_type !== "payment.approved") return;
    const start = performance.now();
    try {
      await handlePaymentApproved(pool, event, parent);
      consumed++;
      durationSeconds += (performance.now() - start) / 1000;
      log("inventory_event_processed", { order_id: event.correlation_id, event_id: event.event_id,
        partition: metadata.partition, offset: metadata.offset });
    } catch (error) {
      errors++;
      log("inventory_process_error", { level: "ERROR", order_id: event.correlation_id,
        event_id: event.event_id, error: String(error) });
      throw error;
    }
  });
  ready = true;
  timer = setInterval(() => {
    if (relaying) return;
    relaying = true;
    void relayOutbox(pool, messaging.publish).then(count => {
      published += count;
    }).catch(error => {
      errors++; log("inventory_relay_error", { level: "ERROR", error: String(error) });
    }).finally(() => { relaying = false; });
  }, 400);
  log("inventory_ready");
} catch (error) {
  log("inventory_start_error", { level: "ERROR", error: String(error) });
  await stop(1);
}
