import {
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  propagation,
  trace,
  context,
} from "@opentelemetry/api";
import { cleanCarrier, validatePayload, type Headers } from "./contracts.js";
export const STARTED = "observa.probe.started.v1";
export const COMPLETED = "observa.probe.completed.v1";
export type RecordInput = {
  topic: string;
  partition: number;
  message: {
    value: Buffer | null;
    key: Buffer | null;
    offset: string;
    headers?: Headers;
  };
};
export type Dependencies = {
  send: (record: {
    topic: string;
    messages: Array<{
      key: string;
      value: string;
      headers: Record<string, string>;
    }>;
  }) => Promise<Array<{ partition: number; baseOffset?: string }>>;
  commit: (offset: {
    topic: string;
    partition: number;
    offset: string;
  }) => Promise<void>;
  log: (event: string, fields: Record<string, unknown>) => void;
  failProbeId?: string;
};
export async function processRecord(
  input: RecordInput,
  deps: Dependencies,
): Promise<void> {
  const { topic, partition, message } = input;
  const payload = validatePayload(
    JSON.parse(message.value?.toString("utf8") ?? "null"),
    message.key,
  );
  if (topic !== STARTED || payload.step !== "started")
    throw new Error("expected started probe");
  const { carrier, warning } = cleanCarrier(message.headers);
  const fields = {
    ...payload,
    topic,
    partition,
    offset: message.offset,
    tracestate: carrier.tracestate,
  };
  if (warning)
    deps.log("context_warning", { ...fields, level: "WARN", reason: warning });
  const parent = propagation.extract(ROOT_CONTEXT, carrier);
  const tracer = trace.getTracer("observa-spike0-transform");
  await tracer.startActiveSpan(
    "probe.process",
    {
      kind: SpanKind.CONSUMER,
      attributes: { probe_id: payload.probe_id, order_id: payload.order_id },
    },
    parent,
    async (span) => {
      try {
        deps.log("process_start", fields);
        if (payload.probe_id === deps.failProbeId)
          throw new Error("fault_before_publish");
        await tracer.startActiveSpan(
          "probe.publish",
          {
            kind: SpanKind.PRODUCER,
            attributes: {
              probe_id: payload.probe_id,
              order_id: payload.order_id,
            },
          },
          async (producerSpan) => {
            try {
              const headers: Record<string, string> = {};
              propagation.inject(context.active(), headers);
              const reports = await deps.send({
                topic: COMPLETED,
                messages: [
                  {
                    key: payload.order_id,
                    value: JSON.stringify({
                      ...payload,
                      step: "completed",
                      occurred_at: new Date().toISOString(),
                    }),
                    headers,
                  },
                ],
              });
              for (const report of reports)
                deps.log("published", {
                  ...fields,
                  topic: COMPLETED,
                  partition: report.partition,
                  offset: report.baseOffset,
                });
            } catch (error) {
              producerSpan.setStatus({ code: SpanStatusCode.ERROR });
              throw error;
            } finally {
              producerSpan.end();
            }
          },
        );
        await deps.commit({
          topic,
          partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        });
        deps.log("process_end", fields);
      } catch (error) {
        span.setStatus({ code: SpanStatusCode.ERROR });
        deps.log("process_error", {
          ...fields,
          level: "ERROR",
          error: String(error),
        });
        throw error;
      } finally {
        span.end();
      }
    },
  );
}
