"""Kafka transport boundary for OrderFlow v1 domain events."""

from .adapter import KafkaPublisher, KafkaSubscriber, RecordMetadata, capture_carrier, producer_config, consumer_config
from .contract import InvalidRecord, validate_event

__all__ = [
    "KafkaPublisher", "KafkaSubscriber", "RecordMetadata", "InvalidRecord",
    "capture_carrier", "producer_config", "consumer_config", "validate_event",
]
