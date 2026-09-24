import { it, expect, vi } from "vitest";
import { context, trace } from "@opentelemetry/api";
import { telemetry, log } from "../src/telemetry.js";
it("creates provider and emits correlated structured logs", async () => {
  const writer = vi.spyOn(console, "log").mockImplementation(() => {});
  const provider = telemetry("test-instance");
  log("plain");
  const span = trace.getTracer("test").startSpan("test");
  context.with(trace.setSpan(context.active(), span), () =>
    log("correlated", { probe_id: "example" }),
  );
  expect(JSON.parse(writer.mock.calls[0][0]).event).toBe("plain");
  expect(JSON.parse(writer.mock.calls[1][0]).trace_id).toBe(
    span.spanContext().traceId,
  );
  writer.mockRestore();
  await provider.shutdown();
});
