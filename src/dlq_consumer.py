"""Display failed Avro orders and their DLQ metadata."""

from __future__ import annotations

import logging
import signal

from confluent_kafka import Consumer, KafkaError

from .avro_codec import deserialize_order
from .config import Settings
from .kafka_helpers import ensure_topics, header_dict


LOGGER = logging.getLogger(__name__)
RUNNING = True


def _stop(_signum: int, _frame: object) -> None:
    global RUNNING
    RUNNING = False


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
            "group.id": settings.dlq_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "client.id": "order-dlq-monitor",
        }
    )
    consumer.subscribe([settings.dlq_topic])
    LOGGER.info("DLQ monitor listening to %s", settings.dlq_topic)
    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Kafka consume error: %s", message.error())
                continue
            order = deserialize_order(message.value())
            headers = header_dict(message.headers())
            LOGGER.error(
                "DLQ_MESSAGE order=%s product=%s price=%.2f reason=%s "
                "attempts=%s error=%s source=%s[%s]@%s",
                order["orderId"],
                order["product"],
                order["price"],
                headers.get("dlq-reason"),
                headers.get("retry-attempt"),
                headers.get("last-error"),
                headers.get("original-topic"),
                headers.get("original-partition"),
                headers.get("original-offset"),
            )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
