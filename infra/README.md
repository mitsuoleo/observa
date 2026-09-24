# Spike 0 infrastructure

Apply through the repository PowerShell entry point, with explicit context `observa-spike0`. Kustomize itself can be rendered offline with `kubectl kustomize infra/kubernetes`.

All upstream images are patch-pinned and registry-index-digest-pinned in `images.lock.json`; local probe images are built by the entry point. Runtime validation evidence is recorded in `../docs/product/spike-0-report.md`; the lock identifies the tested releases. Grafana requires an externally generated `grafana-admin` Secret (`admin-user`, `admin-password`). No host or public ports are declared.

The single-node workload limits total 3.8 CPU and 5 GiB including the transient topic-creation Job. Requests are lower to leave room for Kubernetes. The demo replaces, rather than overlaps, the completed topic Job's CPU use. PVC capacity totals 5.25 GiB. This is a starting allocation, not a measured capacity claim.

The Collector runs as root solely to read container log files on the minikube node. Its root filesystem is read-only, all capabilities are dropped, and service-account token mounting is disabled. Log reads are restricted to `probe-python*` and `probe-node` container files in the dedicated namespace. File offsets persist in a dedicated node directory until the profile is deleted. It retains the JSON log body and attaches stable resource labels; Loki indexes only service name, namespace and environment. Trace, span, order and instance IDs stay in body/structured metadata.

Grafana datasource UIDs are `tempo`, `loki`, and `prometheus`. Trace-to-log lookup uses `{k8s_namespace_name="observa-spike0"}` followed by JSON parsing and trace ID filtering. The reverse link extracts `trace_id` from JSON. Provisioning escapes Grafana interpolation with `$$`. Links still require a real UI click to meet acceptance.

Kafka uses static single-member KRaft quorum, replication factor one and a persisted data volume. Its headless service publishes the member before readiness to avoid controller discovery deadlock. Topic creation waits for the broker with a bounded loop; Job activeDeadlineSeconds provides the hard cap. Teardown of the minikube profile intentionally discards all backend storage; keep acceptance evidence outside it.

Verified release sources:
- https://kafka.apache.org/community/downloads/
- https://github.com/open-telemetry/opentelemetry-collector/releases
- https://github.com/grafana/tempo/blob/main/CHANGELOG.md
- https://github.com/grafana/loki/releases/tag/v3.7.8
- https://github.com/grafana/grafana/releases/tag/v13.2.2
- https://github.com/prometheus/prometheus/releases/tag/v3.14.0

Configuration references:
- https://grafana.com/docs/loki/latest/send-data/otel/
- https://grafana.com/docs/grafana-cloud/observe-and-act/connect-externally-hosted/data-sources/tempo/configure-tempo-data-source/configure-trace-to-logs/
