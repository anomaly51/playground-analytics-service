from __future__ import annotations

import json

import pytest

from lab_analytics.domain import (
    AnalyticsStats,
    InvalidOrderEvent,
    decode_order_event,
)


def order_event(**overrides: object) -> bytes:
    document: dict[str, object] = {
        "schemaVersion": 1,
        "eventId": "01ARZ3NDEKTSV4RRFFQ69G5FAA",
        "eventType": "order.created",
        "occurredAt": "2026-08-30T10:00:00.000Z",
        "traceId": "01ARZ3NDEKTSV4RRFFQ69G5FAB",
        "orderId": "01ARZ3NDEKTSV4RRFFQ69G5FAC",
        "runId": "load-1",
        "aggregateVersion": 1,
        "data": {
            "sku": "DROP-CAP-LIME",
            "quantity": 2,
            "currency": "USD",
            "totalCents": 9_800,
            "status": "pending",
        },
    }
    document.update(overrides)
    return json.dumps(document).encode()


def test_decodes_flashdrop_order_envelope() -> None:
    event = decode_order_event(order_event())
    assert event.order_id == "01ARZ3NDEKTSV4RRFFQ69G5FAC"
    assert event.aggregate_version == 1
    assert event.sku == "DROP-CAP-LIME"
    assert event.run_id == "load-1"


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"eventId": "bad"}, "eventId must be a ULID"),
        ({"eventType": "message.created"}, "unsupported eventType"),
        ({"aggregateVersion": 0}, "positive integer"),
        ({"data": []}, "data object"),
    ],
)
def test_rejects_invalid_order_records(
    override: dict[str, object], reason: str
) -> None:
    with pytest.raises(InvalidOrderEvent, match=reason):
        decode_order_event(order_event(**override))


@pytest.mark.asyncio
async def test_runtime_stats_track_real_inflight_and_outcomes() -> None:
    stats = AnalyticsStats()
    event = decode_order_event(order_event())
    await stats.begin()
    assert (await stats.snapshot())["inFlight"] == 1
    await stats.finish(event, applied=True)
    snapshot = await stats.snapshot()
    assert snapshot["inFlight"] == 0
    assert snapshot["total"] == 1
    assert snapshot["applied"] == 1
    assert snapshot["ignored"] == 0
    assert snapshot["lastOrderId"] == event.order_id
