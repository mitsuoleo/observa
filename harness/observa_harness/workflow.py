"""Broker and offset ordering around the synthetic persistence boundary."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol


class Publisher(Protocol):
    def publish(self, event: Mapping[str, Any], carrier: Mapping[str, str]) -> Any:
        """Return only after broker delivery acknowledgement, or raise."""


class Store(Protocol):
    def claim_and_publish(self, publisher: Publisher) -> Any | None: ...

    def apply_effect(self, event: Mapping[str, Any]) -> bool: ...


def relay_pending(store: Store, publisher: Publisher) -> Any | None:
    """Publish one unpublished row using its persisted carrier."""
    return store.claim_and_publish(publisher)


def consume_with_manual_commit(
    store: Store,
    event: Mapping[str, Any],
    commit_offset: Callable[[], None],
    *,
    after_db_commit: Callable[[], None] | None = None,
) -> bool:
    """Apply one logical effect, then confirm its Kafka offset.

    The optional hook injects a crash between the two confirmations.
    A redelivery repeats the database check and remains safe.
    """
    inserted = store.apply_effect(event)
    if after_db_commit is not None:
        after_db_commit()
    commit_offset()
    return inserted
