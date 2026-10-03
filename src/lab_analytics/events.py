from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Literal

import ulid

Transport = Literal[
    "http",
    "kafka",
    "rabbitmq",
    "redis",
    "postgresql",
    "mysql",
    "airflow",
    "sse",
]
Status = Literal["started", "succeeded", "failed", "retrying"]


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def trace_event(
    *,
    trace_id: str,
    source: str,
    transport: Transport,
    stage: str,
    status: Status,
    summary: str,
    target: str | None = None,
    payload: dict[str, Any] | None = None,
    order_id: str | None = None,
    run_id: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "id": str(ulid.new()),
        "traceId": trace_id,
        "timestamp": timestamp or utc_now(),
        "source": source,
        "transport": transport,
        "stage": stage,
        "status": status,
        "summary": summary,
    }
    if target is not None:
        event["target"] = target
    if order_id is not None:
        event["orderId"] = order_id
    if run_id is not None:
        event["runId"] = run_id
    enriched_payload = dict(payload or {})
    if order_id is not None:
        enriched_payload.setdefault("orderId", order_id)
    if run_id is not None:
        enriched_payload.setdefault("runId", run_id)
    if enriched_payload:
        event["payload"] = enriched_payload
    return event


def encode_event(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
