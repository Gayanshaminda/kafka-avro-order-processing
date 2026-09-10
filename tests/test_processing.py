import pytest

from src.processing import (
    PermanentProcessingError,
    RunningAverages,
    TemporaryProcessingError,
    simulate_processing_failure,
    validate_order,
)


def test_running_average_overall_and_per_product() -> None:
    averages = RunningAverages()

    first = averages.add({"orderId": "1", "product": "A", "price": 10.0})
    second = averages.add({"orderId": "2", "product": "A", "price": 30.0})
    third = averages.add({"orderId": "3", "product": "B", "price": 50.0})

    assert first.global_average == 10.0
    assert second.global_average == 20.0
    assert second.product_average == 20.0
    assert third.global_average == 30.0
    assert third.product_average == 50.0


def test_duplicate_order_is_not_counted_twice() -> None:
    averages = RunningAverages()
    order = {"orderId": "1", "product": "A", "price": 10.0}

    averages.add(order)
    duplicate = averages.add(order)

    assert duplicate.duplicate is True
    assert duplicate.global_count == 1
    assert duplicate.global_average == 10.0


@pytest.mark.parametrize(
    "order",
    [
        {"orderId": "", "product": "A", "price": 10.0},
        {"orderId": "1", "product": "", "price": 10.0},
        {"orderId": "1", "product": "A", "price": 0.0},
    ],
)
def test_invalid_order_is_permanent_failure(order: dict[str, object]) -> None:
    with pytest.raises(PermanentProcessingError):
        validate_order(order)


def test_temporary_failure_eventually_succeeds() -> None:
    with pytest.raises(TemporaryProcessingError):
        simulate_processing_failure("temporary", 0, 2)
    with pytest.raises(TemporaryProcessingError):
        simulate_processing_failure("temporary", 1, 2)

    simulate_processing_failure("temporary", 2, 2)


def test_permanent_failure_never_retries() -> None:
    with pytest.raises(PermanentProcessingError):
        simulate_processing_failure("permanent", 0, 2)
