"""Small Kafka helpers shared by producer and consumers."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


LOGGER = logging.getLogger(__name__)


def wait_for_kafka(bootstrap_servers: str, timeout_seconds: int) -> AdminClient:
    """Wait until broker metadata is available or raise a clear error."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=3)
            return admin
        except KafkaException as exc:
            last_error = exc
            LOGGER.info("Waiting for Kafka at %s...", bootstrap_servers)
            time.sleep(2)
    raise RuntimeError(
        f"Kafka did not become ready at {bootstrap_servers} within "
        f"{timeout_seconds} seconds"
    ) from last_error


def ensure_topics(
    bootstrap_servers: str, topics: Iterable[str], timeout_seconds: int = 60
) -> None:
    """Create assignment topics idempotently."""
    admin = wait_for_kafka(bootstrap_servers, timeout_seconds)
    metadata = admin.list_topics(timeout=5)
    missing = [name for name in topics if name not in metadata.topics]
    if not missing:
        return

    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in missing]
    )
    for topic, future in futures.items():
        try:
            future.result(timeout=15)
            LOGGER.info("Created Kafka topic %s", topic)
        except KafkaException as exc:
            # Another application may have created it after the metadata check.
            refreshed = admin.list_topics(timeout=5)
            if topic not in refreshed.topics:
                raise RuntimeError(f"Could not create topic {topic}") from exc


def reliable_producer(bootstrap_servers: str) -> Producer:
    """Create an idempotent producer suitable for retry/DLQ forwarding."""
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-assignment",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def produce_sync(
    producer: Producer,
    *,
    topic: str,
    key: str,
    value: bytes,
    headers: list[tuple[str, bytes]],
    timeout_seconds: float = 10,
) -> None:
    """Publish one record and confirm delivery before returning."""
    delivery_errors: list[Exception] = []

    def delivered(error: Exception | None, _message: object) -> None:
        if error is not None:
            delivery_errors.append(error)

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=value,
        headers=headers,
        on_delivery=delivered,
    )
    remaining = producer.flush(timeout_seconds)
    if remaining:
        raise TimeoutError(f"Timed out with {remaining} Kafka message(s) undelivered")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")


def header_dict(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    """Decode Kafka headers to a simple string dictionary."""
    result: dict[str, str] = {}
    for name, value in headers or []:
        result[name] = value.decode("utf-8", errors="replace") if value else ""
    return result


def encoded_headers(values: dict[str, object]) -> list[tuple[str, bytes]]:
    """Encode string-like header values for confluent-kafka."""
    return [(name, str(value).encode("utf-8")) for name, value in values.items()]
