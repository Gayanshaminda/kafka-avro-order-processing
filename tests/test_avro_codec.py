from src.avro_codec import deserialize_order, serialize_order


def test_order_avro_round_trip() -> None:
    order = {"orderId": "1001", "product": "Item1", "price": 19.5}
    payload = serialize_order(order)

    assert isinstance(payload, bytes)
    assert not payload.startswith(b"{")
    assert deserialize_order(payload) == order
