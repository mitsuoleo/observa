"""Fail-closed structural audit of captured Tempo/Loki evidence (stdlib only)."""
import argparse
import base64
import json
import re
from pathlib import Path

SERVICES = ("probe-entry", "probe-relay", "probe-node", "probe-python-sink")
FORBIDDEN_LABELS = {"trace_id", "span_id", "order_id", "service_instance_id",
                    "traceId", "spanId", "service.instance.id"}


def attributes(items):
    return {item["key"]: next(iter(item["value"].values())) for item in items}


def identifier(value, length):
    if re.fullmatch(r"[0-9a-fA-F]{%d}" % length, value or ""):
        return value.lower()
    try:
        decoded = base64.b64decode(value, validate=True).hex()
    except (ValueError, TypeError):
        return value
    return decoded if len(decoded) == length else value


def spans_from(responses):
    spans = []
    for response in responses:
        for resource in response.get("resourceSpans", response.get("batches", [])):
            service = attributes(resource.get("resource", {}).get("attributes", [])).get("service.name")
            for scope in resource.get("scopeSpans", resource.get("instrumentationLibrarySpans", [])):
                for span in scope.get("spans", []):
                    attrs = attributes(span.get("attributes", []))
                    spans.append({"service": service, "probe": attrs.get("probe_id", attrs.get("probe.id")),
                                  "trace": identifier(span.get("traceId", ""), 32),
                                  "id": identifier(span.get("spanId", ""), 16),
                                  "parent": identifier(span.get("parentSpanId", ""), 16)})
    return spans


def logs_from(response, errors):
    if response.get("status") != "success":
        errors.append("Loki response must have status=success")
    logs = []
    for stream in response.get("data", {}).get("result", []):
        if FORBIDDEN_LABELS.intersection(stream.get("stream", {})):
            errors.append("High-cardinality identifier indexed as Loki label")
        for value in stream.get("values", []):
            record = json.loads(value[1])
            offset = record.get("offset")
            if isinstance(offset, str) and re.fullmatch(r"0|[1-9][0-9]*", offset):
                record = {**record, "offset": int(offset)}
            logs.append({**record, "_time": int(value[0])})
    return logs


def check_trace(probe, spans, logs, errors):
    selected = [s for s in spans if s["probe"] == probe]
    traces = {s["trace"] for s in selected}
    if len(traces) != 1 or not all(re.fullmatch("[0-9a-f]{32}", t) and int(t, 16) for t in traces):
        errors.append(f"{probe}: exactly one valid trace required")
        return
    by_id = {s["id"]: s for s in selected}
    if len(by_id) != len(selected) or any(
            not re.fullmatch("[0-9a-f]{16}", s["id"]) or int(s["id"], 16) == 0 for s in selected):
        errors.append(f"{probe}: invalid/duplicate span identifiers")
        return
    roots = [s for s in selected if s["parent"] in ("", "0" * 16)]
    sinks = [s for s in selected if s["service"] == SERVICES[-1]]
    if len(roots) != 1 or roots[0]["service"] != SERVICES[0] or len(sinks) != 1:
        errors.append(f"{probe}: one entry root and one sink required")
        return
    chain, seen, current = [], set(), sinks[0]
    while current:
        if current["id"] in seen:
            errors.append(f"{probe}: cycle in parent chain")
            return
        seen.add(current["id"])
        chain.append(current["service"])
        if current["parent"] in ("", "0" * 16):
            break
        current = by_id.get(current["parent"])
        if current is None:
            errors.append(f"{probe}: missing parent span")
            return
    collapsed = []
    for service in reversed(chain):
        if not collapsed or collapsed[-1] != service:
            collapsed.append(service)
    if tuple(collapsed) != SERVICES or len(seen) != len(selected):
        errors.append(f"{probe}: broken cross-service parent chain or disconnected span")
    for log in logs:
        if log.get("probe_id") == probe and log.get("event") in ("process_start", "process_end"):
            candidates = [s for s in selected if s["service"] == log.get("service_name")]
            if not any(s["trace"] == log.get("trace_id") and s["id"] == log.get("span_id") for s in candidates):
                errors.append(f"{probe}: log does not correlate to its service span")


def check_order(manifest, logs, errors):
    for topic in manifest["topics"]:
        records = [r for r in logs if r.get("topic") == topic and r.get("event") == "process_end"
                   and r.get("probe_id") in manifest["probe_ids"]]
        records.sort(key=lambda r: r["_time"])
        if [r.get("probe_id") for r in records] != manifest["probe_ids"]:
            errors.append(f"{topic}: missing, duplicated or reordered fixture completions")
        if any(r.get("order_id") != manifest["order_id"] for r in records):
            errors.append(f"{topic}: order_id changed")
        if any(type(r.get("partition")) is not int or r["partition"] < 0 for r in records):
            errors.append(f"{topic}: partitions must be nonnegative integers")
        if len({r.get("partition") for r in records}) != 1:
            errors.append(f"{topic}: same order must remain in one partition")
        offsets = [r.get("offset") for r in records]
        if any(not isinstance(o, int) or isinstance(o, bool) or o < 0 for o in offsets):
            errors.append(f"{topic}: offsets must be nonnegative integers")
        elif any(a >= b for a, b in zip(offsets, offsets[1:])):
            errors.append(f"{topic}: offsets must increase (gaps permitted)")


def check_processing(logs, errors):
    active, intervals, instances = {}, [], set()
    for log in sorted(logs, key=lambda r: r["_time"]):
        if log.get("service_name") != "probe-node" or log.get("event") not in ("process_start", "process_end"):
            continue
        key = (log["topic"], log["partition"], log["offset"], log["service_instance_id"])
        if log["event"] == "process_start":
            if key in active:
                errors.append("Duplicate Node processing start")
            active[key] = log["_time"]
        elif key not in active:
            errors.append("Node processing end without start")
        else:
            intervals.append((key, active.pop(key), log["_time"]))
            instances.add(key[3])
    if active:
        errors.append("Unfinished Node processing interval")
    if len(instances - {None, ""}) < 2:
        errors.append("Two distinct Node instances must complete work")
    for index, (key, start, end) in enumerate(intervals):
        if end <= start:
            errors.append("Node processing interval must have positive duration")
        for other, other_start, other_end in intervals[index + 1:]:
            if key[:2] == other[:2] and max(start, other_start) < min(end, other_end):
                errors.append("Overlapping Node processing in one topic/partition")


def verify(manifest, traces, loki):
    errors = []
    try:
        ids = manifest["probe_ids"]
        if len(ids) < 5 or len(ids) != len(set(ids)) or not all(isinstance(p, str) and p for p in ids):
            errors.append("At least five distinct probe_ids required")
        if len(manifest["topics"]) != 2 or len(set(manifest["topics"])) != 2:
            errors.append("Two distinct topics required")
        if not manifest["order_id"] or not manifest["expected_tracestate"]:
            errors.append("Nonempty order_id and expected_tracestate required")
        spans = spans_from(traces)
        logs = logs_from(loki, errors)
        for probe in ids:
            check_trace(probe, spans, logs, errors)
        check_order(manifest, logs, errors)
        check_processing(logs, errors)
        for probe in ids:
            for service in ("probe-node", "probe-python-sink"):
                matches = [r for r in logs if r.get("probe_id") == probe and r.get("service_name") == service
                           and r.get("event") == "process_end"]
                if not matches or any(r.get("tracestate") != manifest["expected_tracestate"] for r in matches):
                    errors.append(f"{probe}: tracestate not preserved at {service}")
    except (KeyError, TypeError, ValueError, AttributeError, IndexError) as exc:
        errors.append(f"Malformed or missing evidence: {exc}")
    return {"structural_pass": not errors, "spike_approved": False,
            "ui_verification": "not assessed; real Grafana clicks required",
            "limitations": "Does not verify fault recovery, reconstruction, capacity or OrderFlow preservation",
            "errors": errors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    try:
        inputs = [json.loads((args.directory / name).read_text(encoding="utf-8-sig"))
                  for name in ("manifest.json", "traces.json", "loki.json")]
        report = verify(*inputs)
    except (OSError, ValueError) as exc:
        report = {"structural_pass": False, "spike_approved": False, "errors": [str(exc)]}
    print(json.dumps(report, indent=2))
    return 0 if report["structural_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
