"""Consume Avro orders, aggregate prices, retry, and route failures to a DLQ."""

from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError

from .avro_codec import deserialize_order
from .config import Settings
from .kafka_helpers import (
    encoded_headers,
    ensure_topics,
    header_dict,
    produce_sync,
    reliable_producer,
)
from .processing import (
    PermanentProcessingError,
    RunningAverages,
    TemporaryProcessingError,
    simulate_processing_failure,
    validate_order,
)


LOGGER = logging.getLogger(__name__)
RUNNING = True


def _stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


def _source_headers(message: object, current: dict[str, str]) -> dict[str, object]:
    values: dict[str, object] = dict(current)
    values.setdefault("original-topic", message.topic())
    values.setdefault("original-partition", message.partition())
    values.setdefault("original-offset", message.offset())
    values.setdefault("first-failure-at", datetime.now(UTC).isoformat())
    values["content-type"] = "avro/binary"
    return values


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    settings = Settings()
    ensure_topics(
        settings.bootstrap_servers, settings.all_topics, settings.kafka_wait_seconds
    )
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "client.id": "order-consumer",
        }
    )
    producer = reliable_producer(settings.bootstrap_servers)
    averages = RunningAverages()
    consumer.subscribe([settings.orders_topic, settings.retry_topic])
    LOGGER.info(
        "Listening to %s and %s (max retries=%d)",
        settings.orders_topic,
        settings.retry_topic,
        settings.max_retries,
    )

    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Kafka consume error: %s", message.error())
                continue

            headers = header_dict(message.headers())
            retry_attempt = int(headers.get("retry-attempt", "0") or 0)

            try:
                order = deserialize_order(message.value())
                validate_order(order)
                simulate_processing_failure(
                    headers.get("failure-mode"),
                    retry_attempt,
                    settings.temporary_failures_before_success,
                )
                result = averages.add(order)
                if result.duplicate:
                    LOGGER.warning("DUPLICATE_SKIPPED order=%s", result.order_id)
                else:
                    LOGGER.info(
                        "AGGREGATED order=%s price=%.2f | global count=%d "
                        "average=%.2f | %s count=%d average=%.2f",
                        result.order_id,
                        result.price,
                        result.global_count,
                        result.global_average,
                        result.product,
                        result.product_count,
                        result.product_average,
                    )
                consumer.commit(message=message, asynchronous=False)

            except TemporaryProcessingError as exc:
                next_attempt = retry_attempt + 1
                outgoing = _source_headers(message, headers)
                outgoing["retry-attempt"] = next_attempt
                outgoing["last-error"] = str(exc)

                if next_attempt <= settings.max_retries:
                    delay = settings.retry_backoff_seconds * (2 ** (next_attempt - 1))
                    LOGGER.warning(
                        "RETRY order=%s attempt=%d/%d delay=%.1fs error=%s",
                        headers.get("order-id", message.key().decode("utf-8")),
                        next_attempt,
                        settings.max_retries,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                    produce_sync(
                        producer,
                        topic=settings.retry_topic,
                        key=message.key().decode("utf-8"),
                        value=message.value(),
                        headers=encoded_headers(outgoing),
                    )
                else:
                    outgoing["dlq-reason"] = "retry-exhausted"
                    LOGGER.error(
                        "DLQ retry exhausted order=%s attempts=%d error=%s",
                        message.key().decode("utf-8"),
                        retry_attempt,
                        exc,
                    )
                    produce_sync(
                        producer,
                        topic=settings.dlq_topic,
                        key=message.key().decode("utf-8"),
                        value=message.value(),
                        headers=encoded_headers(outgoing),
                    )
                consumer.commit(message=message, asynchronous=False)

            except (PermanentProcessingError, ValueError, TypeError) as exc:
                outgoing = _source_headers(message, headers)
                outgoing["retry-attempt"] = retry_attempt
                outgoing["last-error"] = str(exc)
                outgoing["dlq-reason"] = "permanent-failure"
                LOGGER.error(
                    "DLQ permanent failure order=%s error=%s",
                    message.key().decode("utf-8", errors="replace"),
                    exc,
                )
                produce_sync(
                    producer,
                    topic=settings.dlq_topic,
                    key=message.key().decode("utf-8", errors="replace"),
                    value=message.value(),
                    headers=encoded_headers(outgoing),
                )
                consumer.commit(message=message, asynchronous=False)
    finally:
        producer.flush(10)
        consumer.close()
        LOGGER.info("Consumer stopped cleanly")


if __name__ == "__main__":
    main()
