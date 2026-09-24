import { describe, it, expect } from "vitest";
import { validatePayload, cleanCarrier } from "../src/contracts.js";
const payload = {
  probe_id: "11111111-1111-4111-8111-111111111111",
  order_id: "22222222-2222-4222-8222-222222222222",
  step: "started",
  occurred_at: "2026-09-24T00:00:00Z",
};
const tp = "00-11111111111111111111111111111111-2222222222222222-01";
describe("wire contracts", () => {
  it("accepts payload with matching UTF8 key", () =>
    expect(validatePayload(payload, Buffer.from(payload.order_id))).toEqual(
      payload,
    ));
  it.each([
    null,
    {},
    { ...payload, step: "bad" },
    { ...payload, probe_id: "bad" },
    { ...payload, occurred_at: "yesterday" },
    { ...payload, extra: true },
  ])("rejects malformed payload %j", (p) =>
    expect(() => validatePayload(p, Buffer.from(payload.order_id))).toThrow(),
  );
  it("rejects incorrect key", () =>
    expect(() => validatePayload(payload, Buffer.from("bad"))).toThrow());
  it("preserves valid trace context", () =>
    expect(
      cleanCarrier({ traceparent: Buffer.from(tp), tracestate: "vendor=value" })
        .carrier,
    ).toEqual({ traceparent: tp, tracestate: "vendor=value" }));
  it.each([
    undefined,
    {},
    { traceparent: "bad" },
    { traceparent: [tp, tp] },
    { traceparent: "00-00000000000000000000000000000000-2222222222222222-01" },
  ])("starts new trace for invalid context %j", (h) => {
    const r = cleanCarrier(h);
    expect(r.carrier).toEqual({});
    expect(r.warning).toBeTruthy();
  });
  it.each(["bad", "vendor=", "vendor=one,vendor=two", "Vendor=value"])(
    "discards invalid tracestate %s",
    (ts) =>
      expect(cleanCarrier({ traceparent: tp, tracestate: ts }).carrier).toEqual(
        { traceparent: tp },
      ),
  );
});
it.each([
  "2026-02-30T00:00:00Z",
  "2026-09-24T24:00:00Z",
  "2026-13-01T00:00:00Z",
])("rejects impossible calendar time %s", (occurred_at) =>
  expect(() =>
    validatePayload({ ...payload, occurred_at }, Buffer.from(payload.order_id)),
  ).toThrow(),
);
