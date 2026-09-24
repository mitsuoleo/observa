import { it, expect, vi } from "vitest";
const mocks = vi.hoisted(() => ({
  connect: vi.fn(async () => {}),
  disconnect: vi.fn(async () => {}),
  send: vi.fn(),
  commitOffsets: vi.fn(),
  subscribe: vi.fn(async () => {}),
  run: vi.fn(async () => {}),
  consumer: vi.fn(),
  producer: vi.fn(),
  handler: null as unknown,
  log: vi.fn(),
  shutdown: vi.fn(async () => {}),
  forceFlush: vi.fn(async () => {}),
}));
vi.mock("@confluentinc/kafka-javascript", () => ({
  default: {
    KafkaJS: {
      Kafka: class {
        producer(config: unknown) {
          mocks.producer(config);
          return mocks;
        }
        consumer(config: unknown) {
          mocks.consumer(config);
          return mocks;
        }
      },
      ErrorCodes: { ERR__ASSIGN_PARTITIONS: 1, ERR__REVOKE_PARTITIONS: 2 },
    },
  },
}));
vi.mock("node:http", () => ({
  createServer: (handler: unknown) => {
    mocks.handler = handler;
    return { listen: () => ({ close: vi.fn() }) };
  },
}));
vi.mock("../src/telemetry.js", () => ({
  telemetry: () => mocks,
  log: mocks.log,
}));
vi.mock("../src/processor.js", () => ({
  STARTED: "observa.probe.started.v1",
  processRecord: vi.fn(async () => {}),
}));
it("wires broker-confirmed commit, rebalance logging and health/metrics", async () => {
  const listeners = vi.spyOn(process, "once").mockReturnValue(process);
  await import("../src/main.js");
  const settings = mocks.consumer.mock.calls[0][0];
  expect(settings["enable.auto.commit"]).toBe(false);
  settings.rebalance_cb({ code: 1 }, []);
  settings.rebalance_cb({ code: 2 }, []);
  settings.rebalance_cb({ code: 3 }, []);
  const handler = mocks.handler as Function;
  const response = { writeHead: vi.fn(), setHeader: vi.fn(), end: vi.fn() };
  for (const url of ["/health", "/metrics", "/missing"])
    handler({ url }, response);
  expect(response.writeHead).toHaveBeenCalledWith(200);
  expect(response.writeHead).toHaveBeenCalledWith(404);
  const callback = mocks.run.mock.calls[0][0].eachMessage;
  await callback({});
  const { processRecord } = await import("../src/processor.js");
  const dependencies = vi.mocked(processRecord).mock.calls[0][1];
  await dependencies.send({ topic: "topic", messages: [] });
  await dependencies.commit({ topic: "topic", partition: 0, offset: "1" });
  expect(mocks.commitOffsets).toHaveBeenCalledWith([
    { topic: "topic", partition: 0, offset: "1" },
  ]);
  const exit = vi
    .spyOn(process, "exit")
    .mockImplementation((() => undefined) as never);
  const sigterm = listeners.mock.calls.find(
    (call) => call[0] === "SIGTERM",
  )?.[1];
  sigterm?.();
  await vi.waitFor(() => expect(exit).toHaveBeenCalledWith(0));
  exit.mockRestore();
  listeners.mockRestore();
});
