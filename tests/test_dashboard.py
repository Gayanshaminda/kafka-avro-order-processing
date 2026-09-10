from src.config import Settings
from src.dashboard import DashboardStore, UI_DIR


def test_dashboard_tracks_success_retry_and_dlq() -> None:
    store = DashboardStore()
    settings = Settings()

    store.consume(
        settings.orders_topic,
        {"orderId": "1", "product": "Item1", "price": 10.0},
        {},
    )
    store.consume(
        settings.retry_topic,
        {"orderId": "2", "product": "Item2", "price": 20.0},
        {"failure-mode": "temporary", "retry-attempt": "1"},
    )
    store.consume(
        settings.retry_topic,
        {"orderId": "2", "product": "Item2", "price": 20.0},
        {"failure-mode": "temporary", "retry-attempt": "2"},
    )
    store.consume(
        settings.dlq_topic,
        {"orderId": "3", "product": "Item3", "price": 60.0},
        {"dlq-reason": "permanent-failure", "retry-attempt": "0"},
    )

    state = store.snapshot()
    assert state["ordersReceived"] == 1
    assert state["processed"] == 2
    assert state["retryEvents"] == 2
    assert state["dlqCount"] == 1
    assert state["globalAverage"] == 15.0
    assert state["dlqOrders"][0]["orderId"] == "3"


def test_dashboard_static_assets_exist() -> None:
    assert (UI_DIR / "index.html").is_file()
    assert (UI_DIR / "static" / "styles.css").is_file()
    assert (UI_DIR / "static" / "app.js").is_file()
