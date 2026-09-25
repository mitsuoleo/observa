import { context, trace } from "@opentelemetry/api";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { NodeTracerProvider, BatchSpanProcessor, AlwaysOnSampler } from "@opentelemetry/sdk-trace-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

const instance = process.env.HOSTNAME ?? "local";

export function startTelemetry() {
  const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? "http://collector:4318";
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ "service.name": "inventory-service", "service.instance.id": instance }),
    sampler: new AlwaysOnSampler(),
    spanProcessors: [new BatchSpanProcessor(new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }))],
  });
  provider.register();
  return provider;
}

export function log(event: string, fields: Record<string, unknown> = {}): void {
  const span = trace.getSpan(context.active())?.spanContext();
  console.log(JSON.stringify({ timestamp: new Date().toISOString(), level: "INFO",
    service_name: "inventory-service", service_instance_id: instance, event,
    trace_id: span?.traceId, span_id: span?.spanId, ...fields }));
}
