import { it, expect } from "vitest";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
const child = (code: string) => {
  const result = spawnSync(
    process.execPath,
    ["--input-type=module", "-e", code],
    { encoding: "utf8", timeout: 10000 },
  );
  expect(result.status, result.stderr).toBe(0);
  return JSON.parse(result.stdout);
};
it("rehydrates serialized carrier in a fresh process without active span", () => {
  const created = child(
    "import {NodeTracerProvider} from '@opentelemetry/sdk-trace-node'; import {trace,propagation,context} from '@opentelemetry/api'; new NodeTracerProvider().register(); trace.getTracer('test').startActiveSpan('root',span=>{const carrier={}; propagation.inject(context.active(),carrier);console.log(JSON.stringify({carrier,pid:process.pid}));span.end();});",
  );
  const restored = child(
    "import {NodeTracerProvider} from '@opentelemetry/sdk-trace-node'; import {trace,propagation,context,ROOT_CONTEXT} from '@opentelemetry/api'; new NodeTracerProvider().register(); const absent=trace.getSpan(context.active())===undefined; const carrier=" +
      JSON.stringify(created.carrier) +
      "; const span=trace.getTracer('test').startSpan('relay',{},propagation.extract(ROOT_CONTEXT,carrier)); console.log(JSON.stringify({pid:process.pid,absent,trace:span.spanContext().traceId}));span.end();",
  );
  expect(restored.pid).not.toBe(created.pid);
  expect(restored.absent).toBe(true);
  expect(restored.trace).toBe(created.carrier.traceparent.split("-")[1]);
});
it("reads the shared Python and Node fixtures", async () => {
  const { validatePayload, cleanCarrier } = await import("../src/contracts.js");
  const file =
    process.env.CONTRACT_FIXTURES ??
    new URL("../../../tests/fixtures/contracts.json", import.meta.url);
  const fixture = JSON.parse(readFileSync(file, "utf8"));
  expect(
    validatePayload(
      fixture.valid_payload,
      Buffer.from(fixture.valid_payload.order_id),
    ),
  ).toEqual(fixture.valid_payload);
  expect(
    cleanCarrier({
      traceparent: fixture.traceparent,
      tracestate: fixture.tracestate,
    }).carrier.tracestate,
  ).toBe(fixture.tracestate);
});
