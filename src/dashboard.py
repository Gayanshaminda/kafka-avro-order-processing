"""Local web dashboard for the Kafka order-processing demonstration."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .avro_codec import deserialize_order, serialize_order
from .config import Settings
from .kafka_helpers import (
    encoded_headers,
    ensure_topics,
    header_dict,
    produce_sync,
    reliable_producer,
)
from .producer import DEMO_ORDERS
from .processing import RunningAverages


LOGGER = logging.getLogger(__name__)
UI_DIR = Path(__file__).resolve().parents[1] / "ui"


class DashboardStore:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.connected = False
        self.sequence = 0
        self.reset()

    def reset(self) -> None:
        with self.lock:
            self.started_at = datetime.now(UTC)
            self.orders_received = 0
            self.processed = 0
            self.retry_events = 0
            self.dlq_count = 0
            self.averages = RunningAverages()
            self.events: deque[dict[str, Any]] = deque(maxlen=60)
            self.dlq_orders: deque[dict[str, Any]] = deque(maxlen=20)
            self._event("system", "Dashboard ready", "Waiting for Kafka activity")

    def _event(
        self,
        kind: str,
        title: str,
        detail: str,
        order: dict[str, Any] | None = None,
    ) -> None:
        self.sequence += 1
        self.events.appendleft(
            {
                "id": self.sequence,
                "kind": kind,
                "title": title,
                "detail": detail,
                "orderId": order.get("orderId") if order else None,
                "time": datetime.now().astimezone().strftime("%H:%M:%S"),
            }
        )

    def set_connected(self, connected: bool) -> None:
        with self.lock:
            if connected != self.connected:
                self.connected = connected
                self._event(
                    "system",
                    "Kafka connected" if connected else "Kafka disconnected",
                    "Three topics are ready" if connected else "Trying to reconnect",
                )

    def consume(
        self, topic: str, order: dict[str, Any], headers: dict[str, str]
    ) -> None:
        with self.lock:
            if topic == Settings().orders_topic:
                self.orders_received += 1
                mode = headers.get("failure-mode")
                if mode == "temporary":
                    self._event(
                        "retry",
                        "Temporary failure detected",
                        f"{order['product']} will enter the retry path",
                        order,
                    )
                elif mode == "permanent":
                    self._event(
                        "warning",
                        "Permanent failure detected",
                        f"{order['product']} will be routed to the DLQ",
                        order,
                    )
                else:
                    self._aggregate(order, "Processed successfully")

            elif topic == Settings().retry_topic:
                self.retry_events += 1
                attempt = int(headers.get("retry-attempt", "0") or 0)
                threshold = Settings().temporary_failures_before_success
                if headers.get("failure-mode") == "temporary" and attempt >= threshold:
                    self._aggregate(order, f"Retry {attempt} succeeded")
                else:
                    self._event(
                        "retry",
                        f"Retry attempt {attempt}",
                        headers.get("last-error", "Temporary processing failure"),
                        order,
                    )

            elif topic == Settings().dlq_topic:
                self.dlq_count += 1
                entry = {
                    "orderId": order["orderId"],
                    "product": order["product"],
                    "price": float(order["price"]),
                    "reason": headers.get("dlq-reason", "permanent-failure"),
                    "error": headers.get("last-error", "Processing failed"),
                    "attempts": int(headers.get("retry-attempt", "0") or 0),
                    "time": datetime.now().astimezone().strftime("%H:%M:%S"),
                }
                self.dlq_orders.appendleft(entry)
                self._event(
                    "dlq",
                    "Moved to Dead Letter Queue",
                    entry["error"],
                    order,
                )

    def _aggregate(self, order: dict[str, Any], detail: str) -> None:
        result = self.averages.add(order)
        if result.duplicate:
            return
        self.processed += 1
        self._event("success", "Order aggregated", detail, order)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            products = [
                {
                    "name": name,
                    "count": totals.count,
                    "average": round(totals.average, 2),
                    "total": round(totals.total, 2),
                }
                for name, totals in sorted(self.averages.product_totals.items())
            ]
            return {
                "connected": self.connected,
                "ordersReceived": self.orders_received,
                "processed": self.processed,
                "retryEvents": self.retry_events,
                "dlqCount": self.dlq_count,
                "globalAverage": round(self.averages.global_totals.average, 2),
                "globalTotal": round(self.averages.global_totals.total, 2),
                "products": products,
                "events": list(self.events),
                "dlqOrders": list(self.dlq_orders),
                "uptimeSeconds": int((datetime.now(UTC) - self.started_at).total_seconds()),
            }


STORE = DashboardStore()
STOP_EVENT = threading.Event()
DEMO_LOCK = threading.Lock()


def consume_for_dashboard() -> None:
    settings = Settings()
    try:
        ensure_topics(
            settings.bootstrap_servers, settings.all_topics, settings.kafka_wait_seconds
        )
        consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": "order-dashboard-v1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "client.id": "order-dashboard",
            }
        )
        consumer.subscribe(list(settings.all_topics))
        STORE.set_connected(True)
        while not STOP_EVENT.is_set():
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOGGER.error("Dashboard Kafka error: %s", message.error())
                continue
            try:
                STORE.consume(
                    message.topic(),
                    deserialize_order(message.value()),
                    header_dict(message.headers()),
                )
            except (ValueError, TypeError, EOFError) as exc:
                LOGGER.warning("Dashboard skipped unreadable record: %s", exc)
        consumer.close()
    except Exception:
        STORE.set_connected(False)
        LOGGER.exception("Dashboard consumer stopped unexpectedly")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    STOP_EVENT.clear()
    thread = threading.Thread(target=consume_for_dashboard, daemon=True)
    thread.start()
    yield
    STOP_EVENT.set()
    thread.join(timeout=5)


app = FastAPI(title="Kafka Order Flow", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=UI_DIR / "static"), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(UI_DIR / "index.html")


@app.get("/api/state")
def get_state() -> dict[str, Any]:
    return STORE.snapshot()


@app.post("/api/reset")
def reset_state() -> dict[str, str]:
    STORE.reset()
    return {"status": "reset"}


@app.post("/api/demo")
def run_demo() -> dict[str, Any]:
    if not DEMO_LOCK.acquire(blocking=False):
        return {"status": "already-running"}
    try:
        settings = Settings()
        producer = reliable_producer(settings.bootstrap_servers)
        batch = uuid.uuid4().hex[:6].upper()
        sent: list[str] = []
        for template, failure_mode in DEMO_ORDERS:
            order = dict(template)
            order["orderId"] = f"{template['orderId']}-{batch}"
            headers: dict[str, object] = {
                "content-type": "avro/binary",
                "retry-attempt": 0,
                "demo-batch": batch,
            }
            if failure_mode:
                headers["failure-mode"] = failure_mode
            produce_sync(
                producer,
                topic=settings.orders_topic,
                key=order["orderId"],
                value=serialize_order(order),
                headers=encoded_headers(headers),
            )
            sent.append(order["orderId"])
            time.sleep(0.45)
        return {"status": "completed", "batch": batch, "orders": sent}
    finally:
        DEMO_LOCK.release()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
