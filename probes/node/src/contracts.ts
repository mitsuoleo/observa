export type Payload = Readonly<{
  probe_id: string;
  order_id: string;
  step: "started" | "completed";
  occurred_at: string;
}>;
export type Headers = Record<
  string,
  string | Buffer | Array<string | Buffer> | undefined
>;
const uuid =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export function validatePayload(input: unknown, key: Buffer | null): Payload {
  if (!input || typeof input !== "object")
    throw new Error("payload must be object");
  const p = input as Record<string, unknown>;
  if (
    Object.keys(p).sort().join(",") !== "occurred_at,order_id,probe_id,step" ||
    typeof p.probe_id !== "string" ||
    !uuid.test(p.probe_id) ||
    typeof p.order_id !== "string" ||
    !uuid.test(p.order_id) ||
    !["started", "completed"].includes(String(p.step)) ||
    typeof p.occurred_at !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/.test(
      p.occurred_at,
    ) ||
    !Number.isFinite(Date.parse(p.occurred_at)) ||
    !validCalendar(p.occurred_at)
  )
    throw new Error("invalid probe payload");
  if (!key?.equals(Buffer.from(p.order_id, "utf8")))
    throw new Error("Kafka key must equal order_id");
  return Object.freeze({ ...p }) as Payload;
}
function validCalendar(value: string): boolean {
  const year = Number(value.slice(0, 4)),
    month = Number(value.slice(5, 7)),
    day = Number(value.slice(8, 10));
  const days = new Date(Date.UTC(year, month, 0)).getUTCDate();
  return (
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= days &&
    Number(value.slice(11, 13)) < 24 &&
    Number(value.slice(14, 16)) < 60 &&
    Number(value.slice(17, 19)) < 60
  );
}
function validState(value: string): boolean {
  const members = value.split(",").map((v) => v.trim());
  const keys = members.map((v) => v.split("=")[0]);
  return (
    value.length <= 512 &&
    members.length <= 32 &&
    new Set(keys).size === keys.length &&
    members.every(
      (v) =>
        /^(?:[a-z][a-z0-9_*\-/]{0,255}|[a-z0-9][a-z0-9_*\-/]{0,240}@[a-z][a-z0-9_*\-/]{0,13})=[\x20-\x2b\x2d-\x3c\x3e-\x7e]{1,256}$/.test(
          v,
        ) && !v.endsWith(" "),
    )
  );
}
export function cleanCarrier(headers: Headers = {}): {
  carrier: Record<string, string>;
  warning?: string;
} {
  const parent = headers.traceparent;
  if (parent === undefined || Array.isArray(parent))
    return { carrier: {}, warning: "missing_or_duplicate_traceparent" };
  const tp = parent.toString();
  const match = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/.exec(tp);
  if (!match || /^0+$/.test(match[1]) || /^0+$/.test(match[2]))
    return { carrier: {}, warning: "invalid_traceparent" };
  const state = headers.tracestate;
  if (state !== undefined) {
    const text = Array.isArray(state)
      ? state.map((s) => s.toString()).join(",")
      : state.toString();
    if (!validState(text))
      return { carrier: { traceparent: tp }, warning: "invalid_tracestate" };
    return { carrier: { traceparent: tp, tracestate: text } };
  }
  return { carrier: { traceparent: tp } };
}
