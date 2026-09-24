import { context, trace } from "@opentelemetry/api";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  NodeTracerProvider,
  BatchSpanProcessor,
  AlwaysOnSampler,
} from "@opentelemetry/sdk-trace-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
export function telemetry(instance: string) {
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      "service.name": "probe-node",
      "service.instance.id": instance,
    }),
    sampler: new AlwaysOnSampler(),
    spanProcessors: [
      new BatchSpanProcessor(
        new OTLPTraceExporter({
          url: `${process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? "http://collector:4318"}/v1/traces`,
        }),
      ),
    ],
  });
  provider.register();
  return provider;
}
export function log(event: string, fields: Record<string, unknown> = {}) {
  const span = trace.getSpan(context.active())?.spanContext();
  console.log(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level: "INFO",
      message: event,
      event,
      service_name: "probe-node",
      service_instance_id: process.env.HOSTNAME ?? "local",
      trace_id: span?.traceId,
      span_id: span?.spanId,
      messaging_system: "kafka",
      ...fields,
    }),
  );
}
