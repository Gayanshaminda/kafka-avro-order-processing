"""Binary Avro serialization for the Order schema."""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from fastavro import parse_schema, schemaless_reader, schemaless_writer


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "order.avsc"
ORDER_SCHEMA = parse_schema(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def serialize_order(order: dict[str, Any]) -> bytes:
    """Serialize one order as Avro binary using order.avsc."""
    buffer = io.BytesIO()
    schemaless_writer(buffer, ORDER_SCHEMA, order)
    return buffer.getvalue()


def deserialize_order(payload: bytes) -> dict[str, Any]:
    """Deserialize one Avro order and reject trailing bytes."""
    buffer = io.BytesIO(payload)
    order = schemaless_reader(buffer, ORDER_SCHEMA)
    if buffer.read(1):
        raise ValueError("Unexpected trailing bytes after Avro order")
    return order
