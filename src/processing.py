"""Business rules and running-average aggregation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


class TemporaryProcessingError(RuntimeError):
    """A retryable processing failure."""


class PermanentProcessingError(RuntimeError):
    """A non-retryable processing failure."""


def validate_order(order: dict[str, Any]) -> None:
    """Raise a permanent error when required order data is invalid."""
    order_id = order.get("orderId")
    product = order.get("product")
    price = order.get("price")

    if not isinstance(order_id, str) or not order_id.strip():
        raise PermanentProcessingError("orderId must be a non-empty string")
    if not isinstance(product, str) or not product.strip():
        raise PermanentProcessingError("product must be a non-empty string")
    if not isinstance(price, (int, float)) or not math.isfinite(float(price)):
        raise PermanentProcessingError("price must be a finite number")
    if float(price) <= 0:
        raise PermanentProcessingError("price must be greater than zero")


def simulate_processing_failure(
    failure_mode: str | None,
    retry_attempt: int,
    temporary_failures_before_success: int,
) -> None:
    """Provide deterministic failure cases for the live demonstration."""
    if failure_mode == "permanent":
        raise PermanentProcessingError("simulated permanent payment failure")
    if (
        failure_mode == "temporary"
        and retry_attempt < temporary_failures_before_success
    ):
        raise TemporaryProcessingError("simulated temporary service outage")


@dataclass(frozen=True)
class AggregationResult:
    order_id: str
    product: str
    price: float
    global_count: int
    global_average: float
    product_count: int
    product_average: float
    duplicate: bool = False


@dataclass
class _Totals:
    count: int = 0
    total: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


@dataclass
class RunningAverages:
    """Track overall and per-product averages for the current process."""

    global_totals: _Totals = field(default_factory=_Totals)
    product_totals: dict[str, _Totals] = field(default_factory=dict)
    seen_order_ids: set[str] = field(default_factory=set)

    def add(self, order: dict[str, Any]) -> AggregationResult:
        order_id = str(order["orderId"])
        product = str(order["product"])
        price = float(order["price"])
        duplicate = order_id in self.seen_order_ids

        product_total = self.product_totals.setdefault(product, _Totals())
        if not duplicate:
            self.seen_order_ids.add(order_id)
            self.global_totals.add(price)
            product_total.add(price)

        return AggregationResult(
            order_id=order_id,
            product=product,
            price=price,
            global_count=self.global_totals.count,
            global_average=self.global_totals.average,
            product_count=product_total.count,
            product_average=product_total.average,
            duplicate=duplicate,
        )
