from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import ulid

ORDER_EVENT_TYPES = {
    "order.created",
    "order.confirmed",
    "order.sold_out",
    "order.failed",
}
ORDER_STATUSES = {"pending", "confirmed", "sold_out", "failed"}
FLASHDROP_SKUS = {
    "DROP-SNEAKER-RED",
    "DROP-HOODIE-BLACK",
    "DROP-CAP-LIME",
}


class InvalidOrderEvent(ValueError):
    """A Kafka record cannot be normalized into an order event envelope."""


@dataclass(frozen=True, slots=True)
class IncomingOrderEvent:
    event_id: str
    event_type: str
    occurred_at: str
    trace_id: str
    order_id: str
    aggregate_version: int
    sku: str
    quantity: int
    currency: str
    total_cents: int
    status: str
    reason: str | None = None
    run_id: str | None = None


def decode_order_event(raw: bytes) -> IncomingOrderEvent:
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidOrderEvent("record must contain UTF-8 JSON") from error
    if not isinstance(document, dict):
        raise InvalidOrderEvent("record JSON must be an object")
    data = document.get("data")
    if not isinstance(data, dict):
        raise InvalidOrderEvent("record must contain data object")

    event_id = _required_string(document, "eventId")
    trace_id = _required_string(document, "traceId")
    order_id = _required_string(document, "orderId")
    for name, value in (
        ("eventId", event_id),
        ("traceId", trace_id),
        ("orderId", order_id),
    ):
        try:
            ulid.from_str(value)
        except ValueError as error:
            raise InvalidOrderEvent(f"{name} must be a ULID") from error

    event_type = _required_string(document, "eventType")
    if event_type not in ORDER_EVENT_TYPES:
        raise InvalidOrderEvent("unsupported eventType")
    status = _required_string(data, "status")
    if status not in ORDER_STATUSES:
        raise InvalidOrderEvent("unsupported order status")
    sku = _required_string(data, "sku")
    if sku not in FLASHDROP_SKUS:
        raise InvalidOrderEvent("unsupported sku")

    aggregate_version = document.get("aggregateVersion")
    quantity = data.get("quantity")
    total_cents = data.get("totalCents")
    if not isinstance(aggregate_version, int) or aggregate_version <= 0:
        raise InvalidOrderEvent("aggregateVersion must be a positive integer")
    if not isinstance(quantity, int) or not 1 <= quantity <= 5:
        raise InvalidOrderEvent("quantity must be between 1 and 5")
    if not isinstance(total_cents, int) or total_cents < 0:
        raise InvalidOrderEvent("totalCents must be a non-negative integer")

    reason = data.get("reason")
    run_id = document.get("runId")
    return IncomingOrderEvent(
        event_id=event_id,
        event_type=event_type,
        occurred_at=_required_string(document, "occurredAt"),
        trace_id=trace_id,
        order_id=order_id,
        aggregate_version=aggregate_version,
        sku=sku,
        quantity=quantity,
        currency=_required_string(data, "currency"),
        total_cents=total_cents,
        status=status,
        reason=reason if isinstance(reason, str) and reason else None,
        run_id=run_id if isinstance(run_id, str) and run_id else None,
    )


def _required_string(document: dict[str, Any], field_name: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise InvalidOrderEvent(f"{field_name} must be a non-empty string")
    return value.strip()


@dataclass(slots=True)
class AnalyticsStats:
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _in_flight: int = field(default=0, init=False)
    _total: int = field(default=0, init=False)
    _applied: int = field(default=0, init=False)
    _ignored: int = field(default=0, init=False)
    _invalid: int = field(default=0, init=False)
    _last_order_id: str | None = field(default=None, init=False)
    _last_event_at: str | None = field(default=None, init=False)

    async def begin(self) -> None:
        async with self._lock:
            self._in_flight += 1

    async def finish(
        self,
        event: IncomingOrderEvent | None,
        *,
        applied: bool = False,
        invalid: bool = False,
    ) -> None:
        async with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._total += 1
            if invalid:
                self._invalid += 1
            elif applied:
                self._applied += 1
            else:
                self._ignored += 1
            if event is not None:
                self._last_order_id = event.order_id
                self._last_event_at = event.occurred_at

    async def abort(self) -> None:
        async with self._lock:
            self._in_flight = max(0, self._in_flight - 1)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "inFlight": self._in_flight,
                "total": self._total,
                "applied": self._applied,
                "ignored": self._ignored,
                "invalid": self._invalid,
                "lastOrderId": self._last_order_id,
                "lastEventAt": self._last_event_at,
            }
