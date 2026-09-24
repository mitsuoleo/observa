"""Offline behavioral tests; no backend or third-party packages required."""
import unittest

from verify import verify


def fixture():
    ids = [f"probe-{n}" for n in range(5)]
    manifest = {"probe_ids": ids, "order_id": "order-1", "topics": ["started", "completed"],
                "node_instances": ["node-a", "node-b"], "expected_tracestate": "vendor=value"}
    resources, logs = [], []
    services = ["probe-entry", "probe-relay", "probe-node", "probe-python-sink"]
    for n, probe in enumerate(ids):
        trace_id = f"{n + 1:032x}"
        for i, service in enumerate(services):
            span_id = f"{n * 10 + i + 1:016x}"
            span = {"traceId": trace_id, "spanId": span_id,
                    "parentSpanId": "" if i == 0 else f"{n * 10 + i:016x}",
                    "attributes": [{"key": "probe_id", "value": {"stringValue": probe}}]}
            resources.append({"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
                              "scopeSpans": [{"spans": [span]}]})
        for topic, service, index in [("started", "probe-node", 3), ("completed", "probe-python-sink", 4)]:
            record = {"event": "process_end", "probe_id": probe, "order_id": "order-1",
                      "topic": topic, "partition": 0, "offset": n * 2,
                      "service_name": service, "service_instance_id": "node-a" if n < 3 else "node-b",
                      "trace_id": trace_id, "span_id": f"{n * 10 + index:016x}", "tracestate": "vendor=value"}
            if service == "probe-node":
                logs.append([str(n * 100 + 1), {**record, "event": "process_start"}])
            logs.append([str(n * 100 + 2), record])
    import json
    return manifest, [{"resourceSpans": resources}], {"status": "success", "data": {"result": [
        {"stream": {"service_name": "probes"}, "values": [[stamp, json.dumps(log)] for stamp, log in logs]}]}}


class EvidenceTests(unittest.TestCase):
    def test_complete_structural_evidence_does_not_approve_ui(self):
        result = verify(*fixture())
        self.assertTrue(result["structural_pass"])
        self.assertFalse(result["spike_approved"])

    def test_missing_traces_fail_closed(self):
        manifest, _, logs = fixture()
        self.assertFalse(verify(manifest, [], logs)["structural_pass"])

    def test_broken_parent_chain_fails(self):
        manifest, traces, logs = fixture()
        traces[0]["resourceSpans"][2]["scopeSpans"][0]["spans"][0]["parentSpanId"] = "f" * 16
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_indexed_identifier_fails(self):
        manifest, traces, logs = fixture()
        logs["data"]["result"][0]["stream"]["trace_id"] = "bad"
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_missing_start_fails(self):
        manifest, traces, logs = fixture()
        logs["data"]["result"][0]["values"].pop(0)
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_fewer_than_five_ids_fails(self):
        manifest, traces, logs = fixture()
        manifest["probe_ids"] = manifest["probe_ids"][:4]
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_malformed_evidence_fails(self):
        self.assertFalse(verify({}, {}, {})["structural_pass"])

    def test_large_string_offsets_preserve_precision(self):
        import json
        manifest, traces, logs = fixture()
        values = logs["data"]["result"][0]["values"]
        for value in values:
            record = json.loads(value[1])
            record["offset"] = str(9007199254740993 + record["offset"])
            value[1] = json.dumps(record)
            value.append({"trace_id": record["trace_id"]})
        self.assertTrue(verify(manifest, traces, logs)["structural_pass"])

    def test_noncanonical_string_offsets_fail(self):
        import json
        manifest, traces, logs = fixture()
        values = logs["data"]["result"][0]["values"]
        record = json.loads(values[1][1])
        record["offset"] = "01"
        values[1][1] = json.dumps(record)
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_reordered_offsets_fail(self):
        import json
        manifest, traces, logs = fixture()
        values = logs["data"]["result"][0]["values"]
        record = json.loads(values[4][1])
        record["offset"] = 0
        values[4][1] = json.dumps(record)
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_overlap_across_instances_fails(self):
        manifest, traces, logs = fixture()
        # Extend the first interval beyond the fourth processing start.
        logs["data"]["result"][0]["values"][1][0] = "350"
        result = verify(manifest, traces, logs)
        self.assertTrue(any("Overlapping" in error for error in result["errors"]))

    def test_missing_tracestate_fails(self):
        import json
        manifest, traces, logs = fixture()
        values = logs["data"]["result"][0]["values"]
        record = json.loads(values[2][1])
        del record["tracestate"]
        values[2][1] = json.dumps(record)
        self.assertFalse(verify(manifest, traces, logs)["structural_pass"])

    def test_node_producer_span_is_supported(self):
        import copy
        manifest, traces, logs = fixture()
        resources = traces[0]["resourceSpans"]
        for n in range(5):
            node = copy.deepcopy(resources[n * 4 + 2])
            span = node["scopeSpans"][0]["spans"][0]
            original = span["spanId"]
            span["spanId"] = f"{100 + n:016x}"
            span["parentSpanId"] = original
            resources[n * 4 + 3]["scopeSpans"][0]["spans"][0]["parentSpanId"] = span["spanId"]
            resources.append(node)
        self.assertTrue(verify(manifest, traces, logs)["structural_pass"])

    def test_tempo_batches_supported(self):
        manifest, traces, logs = fixture()
        traces = [{"batches": traces[0]["resourceSpans"]}]
        self.assertTrue(verify(manifest, traces, logs)["structural_pass"])


if __name__ == "__main__":
    unittest.main()

