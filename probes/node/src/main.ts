import kafkaPackage from "@confluentinc/kafka-javascript";
import { createServer } from "node:http";
import { processRecord, STARTED } from "./processor.js";
import { telemetry, log } from "./telemetry.js";
const { Kafka, ErrorCodes } = kafkaPackage.KafkaJS;
const provider = telemetry(process.env.HOSTNAME ?? "local");
const kafka = new Kafka({
  "bootstrap.servers": process.env.KAFKA_BOOTSTRAP_SERVERS ?? "kafka:9092",
  "socket.timeout.ms": 10000,
});
const producer = kafka.producer({
  "enable.idempotence": true,
  partitioner: "murmur2_random",
  acks: -1,
  "message.timeout.ms": 30000,
});
const consumer = kafka.consumer({
  "group.id": "observa-spike0-transform",
  "enable.auto.commit": false,
  "auto.offset.reset": "earliest",
  "partition.assignment.strategy": "range",
  rebalance_cb: (error: { code: number }, assignment: unknown) => {
    log(
      error.code === ErrorCodes.ERR__ASSIGN_PARTITIONS
        ? "assignment"
        : error.code === ErrorCodes.ERR__REVOKE_PARTITIONS
          ? "revocation"
          : "rebalance_error",
      { assignment, error_code: error.code },
    );
  },
});
let ready = false;
let processed = 0;
let failures = 0;
const server = createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(ready ? 200 : 503);
    res.end(JSON.stringify({ ready }));
    return;
  }
  if (req.url === "/metrics") {
    res.setHeader("Content-Type", "text/plain; version=0.0.4");
    res.end(
      "# TYPE probe_processed_total counter\nprobe_processed_total " +
        processed +
        "\n# TYPE probe_failures_total counter\nprobe_failures_total " +
        failures +
        "\n",
    );
    return;
  }
  res.writeHead(404);
  res.end();
}).listen(8000, "0.0.0.0");
async function stop(code: number) {
  ready = false;
  const timer = setTimeout(() => process.exit(code || 1), 10000);
  timer.unref();
  try {
    await consumer.disconnect();
    await producer.disconnect();
    await provider.shutdown();
    server.close();
  } catch (error) {
    log("shutdown_error", { level: "ERROR", error: String(error) });
    code = 1;
  }
  process.exit(code);
}
process.once("SIGTERM", () => void stop(0));
process.once("SIGINT", () => void stop(0));
try {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topics: [STARTED] });
  await consumer.run({
    partitionsConsumedConcurrently: 1,
    eachMessage: async (input) => {
      try {
        await processRecord(input, {
          send: (record) => producer.send(record),
          commit: (offset) => consumer.commitOffsets([offset]),
          log,
          failProbeId: process.env.FAIL_BEFORE_PUBLISH_PROBE_ID,
        });
        processed++;
      } catch (error) {
        failures++;
        log("fatal_processing_error", { level: "ERROR", error: String(error) });
        await provider.forceFlush();
        process.exit(17);
      }
    },
  });
  ready = true;
  log("ready");
} catch (error) {
  log("startup_error", { level: "ERROR", error: String(error) });
  await stop(1);
}
