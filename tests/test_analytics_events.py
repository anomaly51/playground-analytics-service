from __future__ import annotations

import re

from lab_analytics.events import trace_event

ULID_PATTERN = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


def test_trace_event_generates_ulid_and_shared_envelope() -> None:
    event = trace_event(
        trace_id="01K3TQ1YW4H3G2VJ8ZA0Z5X8RN",
        source="analytics",
        target="gateway",
        transport="kafka",
        stage="analytics.aggregate",
        status="succeeded",
        summary="updated",
        order_id="01ARZ3NDEKTSV4RRFFQ69G5FAS",
        run_id="load-1",
    )

    assert ULID_PATTERN.fullmatch(event["id"])
    assert event["traceId"] == "01K3TQ1YW4H3G2VJ8ZA0Z5X8RN"
    assert event["transport"] == "kafka"
    assert event["status"] == "succeeded"
    assert event["orderId"] == "01ARZ3NDEKTSV4RRFFQ69G5FAS"
    assert event["runId"] == "load-1"
