"""Wire validation shared by all Python entry points."""
import re
from datetime import datetime
from uuid import UUID

PARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
STATE_KEY = re.compile(r"(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})$")


def validate_payload(payload):
    if not isinstance(payload, dict) or set(payload) != {"probe_id", "order_id", "step", "occurred_at"}:
        raise ValueError("payload must contain exactly the four contract fields")
    for field in ("probe_id", "order_id"):
        if not isinstance(payload[field], str):
            raise ValueError(f"{field} must be a UUID string")
        UUID(payload[field])
    if payload["step"] not in ("started", "completed"):
        raise ValueError("invalid step")
    value = payload["occurred_at"]
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("occurred_at must be RFC3339")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("occurred_at requires timezone")
    return dict(payload)


def valid_state(value):
    if len(value) > 512:
        return False
    members = value.split(",")
    keys = set()
    if len(members) > 32:
        return False
    for member in members:
        key, sep, val = member.strip().partition("=")
        if not sep or not STATE_KEY.fullmatch(key) or key in keys or not val or len(val) > 256:
            return False
        if val.endswith(" ") or any(ord(c) < 32 or ord(c) > 126 or c == "=" for c in val):
            return False
        keys.add(key)
    return True


def sanitize_headers(headers):
    values = {}
    duplicates = set()
    try:
        for key, value in headers or []:
            if key in ("traceparent", "tracestate"):
                if key in values:
                    duplicates.add(key)
                try:
                    values[key] = value.decode('ascii') if isinstance(value, bytes) else value
                except UnicodeError:
                    values[key] = ""
    except (UnicodeError, AttributeError):
        return {}, "invalid_traceparent"
    if 'traceparent' in duplicates:
        return {}, 'duplicate_traceparent'
    parent = values.get("traceparent", "")
    match = PARENT.fullmatch(parent) if isinstance(parent, str) else None
    if not match or int(match[1], 16) == 0 or int(match[2], 16) == 0:
        return {}, "missing_or_invalid_traceparent"
    state = values.get("tracestate")
    if "tracestate" in duplicates or (state is not None and (not isinstance(state, str) or not valid_state(state))):
        return {"traceparent": parent}, "invalid_tracestate"
    return values, None



