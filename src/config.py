"""Environment-based settings shared by all applications."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _read_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _read_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    orders_topic: str = os.getenv("ORDERS_TOPIC", "orders")
    retry_topic: str = os.getenv("RETRY_TOPIC", "orders.retry")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "orders.dlq")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "order-processor-v1")
    dlq_group: str = os.getenv("DLQ_GROUP", "order-dlq-monitor-v1")
    max_retries: int = _read_int("MAX_RETRIES", 3)
    retry_backoff_seconds: float = _read_float("RETRY_BACKOFF_SECONDS", 1.0)
    kafka_wait_seconds: int = _read_int("KAFKA_WAIT_SECONDS", 60)
    temporary_failures_before_success: int = _read_int(
        "TEMPORARY_FAILURES_BEFORE_SUCCESS", 2
    )

    @property
    def all_topics(self) -> tuple[str, str, str]:
        return self.orders_topic, self.retry_topic, self.dlq_topic
