"""Produce random orders or a deterministic live-demo data set."""

from __future__ import annotations

import argparse
import logging
import random
import time
import uuid
from typing import Any

from .avro_codec import serialize_order
from .config import Settings
from .kafka_helpers import encoded_headers, ensure_topics, produce_sync, reliable_producer


LOGGER = logging.getLogger(__name__)


DEMO_ORDERS: list[tuple[dict[str, Any], str | None]] = [
    ({"orderId": "1001", "product": "Item1", "price": 10.0}, None),
    ({"orderId": "1002", "product": "Item2", "price": 20.0}, None),
    ({"orderId": "1003", "product": "Item1", "price": 30.0}, None),
    ({"orderId": "1004", "product": "Item2", "price": 40.0}, None),
    ({"orderId": "1005", "product": "Item3", "price": 50.0}, "temporary"),
    ({"orderId": "1006", "product": "Item4", "price": 60.0}, "permanent"),
]


def random_orders(count: int) -> list[tuple[dict[str, Any], str | None]]:
    products = ["Item1", "Item2", "Item3", "Item4"]
    return [
        (
            {
                "orderId": uuid.uuid4().hex[:12],
                "product": random.choice(products),
                "price": round(random.uniform(5, 250), 2),
            },
            None,
        )
        for _ in range(count)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--demo",
        action="store_true",
        help="send six deterministic messages including retry and DLQ cases",
    )
    group.add_argument(
        "--count", type=int, default=10, help="number of random orders to send"
    )
    parser.add_argument("--interval", type=float, default=0.5)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    args = parse_args()
    settings = Settings()
    ensure_topics(
        settings.bootstrap_servers, settings.all_topics, settings.kafka_wait_seconds
    )
    producer = reliable_producer(settings.bootstrap_servers)
    orders = DEMO_ORDERS if args.demo else random_orders(args.count)

    for order, failure_mode in orders:
        header_values: dict[str, object] = {
            "content-type": "avro/binary",
            "retry-attempt": 0,
        }
        if failure_mode:
            header_values["failure-mode"] = failure_mode
        produce_sync(
            producer,
            topic=settings.orders_topic,
            key=order["orderId"],
            value=serialize_order(order),
            headers=encoded_headers(header_values),
        )
        LOGGER.info(
            "PRODUCED order=%s product=%s price=%.2f failure_mode=%s",
            order["orderId"],
            order["product"],
            order["price"],
            failure_mode or "none",
        )
        time.sleep(max(0, args.interval))

    LOGGER.info("Producer finished after %d order(s)", len(orders))


if __name__ == "__main__":
    main()
