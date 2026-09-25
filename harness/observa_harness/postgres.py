"""Minimal Postgres repository for the synthetic US-002 proof."""

from __future__ import annotations

from collections.abc import Mapping
from importlib.resources import files
from typing import Any
from uuid import UUID


class PostgresStore:
    """Requires a psycopg 3 connection opened with autocommit=True.

    The relay is deliberately single replica for the MVP. Row locking still
    prevents concurrent claims in a test, but no leader/lease is provided.
    """

    def __init__(self, connection):
        if not connection.autocommit:
            raise ValueError("PostgresStore requires autocommit=True")
        self.connection = connection

    def init_schema(self) -> None:
        schema = files("observa_harness").joinpath("schema.sql").read_text(encoding="utf-8")
        with self.connection.transaction():
            self.connection.execute(schema)

    def enqueue(self, event: Mapping[str, Any], carrier: Mapping[str, str]) -> None:
        """Persist wire envelope and internal trace carrier as separate JSONB values."""
        from psycopg.types.json import Jsonb

        if not isinstance(event, Mapping) or not isinstance(carrier, Mapping):
            raise ValueError("event and carrier must be objects")
        event_id = UUID(str(event["event_id"]))
        if "traceparent" not in carrier:
            raise ValueError("carrier requires traceparent")
        with self.connection.transaction():
            self.connection.execute(
                "INSERT INTO synthetic_outbox (event_id, envelope, carrier) VALUES (%s, %s, %s)",
                (event_id, Jsonb(dict(event)), Jsonb(dict(carrier))),
            )

    def claim_and_publish(self, publisher) -> Any | None:
        """Hold the row lock until acknowledged publish and DB update commit."""
        with self.connection.transaction():
            row = self.connection.execute(
                "SELECT id, envelope, carrier FROM synthetic_outbox "
                "WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
            ).fetchone()
            if row is None:
                return None
            row_id, event, carrier = row
            receipt = publisher.publish(event, carrier)
            partition, offset = _receipt_location(receipt)
            self.connection.execute(
                "UPDATE synthetic_outbox SET published_at = now(), "
                "published_partition = %s, published_offset = %s WHERE id = %s",
                (partition, offset, row_id),
            )
            return receipt

    def apply_effect(self, event: Mapping[str, Any]) -> bool:
        """Commit dedup marker and synthetic effect in one DB transaction."""
        event_id = UUID(str(event["event_id"]))
        correlation_id = UUID(str(event["correlation_id"]))
        with self.connection.transaction():
            inserted = self.connection.execute(
                "INSERT INTO processed_events (event_id) VALUES (%s) "
                "ON CONFLICT DO NOTHING RETURNING event_id",
                (event_id,),
            ).fetchone()
            if inserted is None:
                return False
            self.connection.execute(
                "INSERT INTO synthetic_effects (event_id, correlation_id) VALUES (%s, %s)",
                (event_id, correlation_id),
            )
        return True


def _receipt_location(receipt: Any) -> tuple[int, int]:
    if isinstance(receipt, Mapping):
        return int(receipt["partition"]), int(receipt["offset"])
    for accessor in ("partition", "offset"):
        if not hasattr(receipt, accessor):
            raise TypeError("publisher receipt needs partition and offset")
    partition = receipt.partition() if callable(receipt.partition) else receipt.partition
    offset = receipt.offset() if callable(receipt.offset) else receipt.offset
    return int(partition), int(offset)
