from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from typing import Any

import pytest

from lab_analytics import kafka as kafka_module
from lab_analytics.config import Settings
from lab_analytics.domain import AnalyticsStats, IncomingOrderEvent
from lab_analytics.kafka import KafkaRuntime
from lab_analytics.storage import ProjectionWrite


class FakeProducer:
    def __init__(self, outcomes: list[Any], client: Any | None = None) -> None:
        self.outcomes = deque(outcomes)
        self.client = client or AlwaysHealthyClient()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def send_and_wait(self, *_: Any, **__: Any) -> None:
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome

    async def stop(self) -> None:
        self.stopped = True


class AlwaysHealthyClient:
    async def fetch_all_metadata(self) -> object:
        return object()


class RecoveringProbeClient:
    def __init__(self) -> None:
        self.calls = 0
        self.failure_observed = asyncio.Event()
        self.recovery_gate = asyncio.Event()
        self.recovery_observed = asyncio.Event()

    async def fetch_all_metadata(self) -> object:
        self.calls += 1
        if self.calls == 1:
            self.failure_observed.set()
            raise RuntimeError("metadata unavailable")
        await self.recovery_gate.wait()
        self.recovery_observed.set()
        return object()


class FakeConsumer:
    def __init__(self, start_gate: asyncio.Event | None = None) -> None:
        self.start_gate = start_gate
        self.records: asyncio.Queue[Any] = asyncio.Queue()
        self.start_called_event = asyncio.Event()
        self.started_event = asyncio.Event()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.start_called_event.set()
        if self.start_gate is not None:
            await self.start_gate.wait()
        self.started = True
        self.started_event.set()

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self) -> FakeConsumer:
        return self

    async def __anext__(self) -> Any:
        record = await self.records.get()
        if isinstance(record, BaseException):
            raise record
        return record


class NoopStorage:
    async def upsert_order(self, event: IncomingOrderEvent) -> ProjectionWrite:
        return ProjectionWrite(applied=True, aggregate_version=event.aggregate_version)

    async def persist_traces(self, *_: Any, **__: Any) -> None:
        return None

    async def count_projections(self) -> int:
        return 0


def settings(probe_interval_seconds: float = 60.0) -> Settings:
    return Settings(
        http_host="127.0.0.1",
        http_port=8002,
        kafka_enabled=True,
        kafka_bootstrap_servers="kafka:29092",
        kafka_input_topic="flashdrop.orders.v1",
        kafka_analytics_topic="flashdrop.analytics.v1",
        kafka_trace_topic="flashdrop.traces.v1",
        kafka_group_id="test-analytics",
        kafka_client_id="test-analytics",
        kafka_publish_timeout_seconds=1.0,
        kafka_probe_interval_seconds=probe_interval_seconds,
        kafka_probe_timeout_seconds=0.1,
        processing_retry_max_seconds=1.0,
    )


@pytest.mark.asyncio
async def test_readiness_tracks_producer_consumer_and_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = FakeProducer([RuntimeError("publish failed"), None])
    recovery_gate = asyncio.Event()
    first_consumer = FakeConsumer()
    recovered_consumer = FakeConsumer(start_gate=recovery_gate)
    consumers = iter((first_consumer, recovered_consumer))
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", lambda **_: producer)
    monkeypatch.setattr(
        kafka_module, "AIOKafkaConsumer", lambda *_, **__: next(consumers)
    )
    runtime = KafkaRuntime(settings(), AnalyticsStats(), NoopStorage())
    event = {
        "id": "01ARZ3NDEKTSV4RRFFQ69G5FAA",
        "traceId": "01ARZ3NDEKTSV4RRFFQ69G5FAB",
    }

    assert runtime.ready is False
    await runtime.start()
    assert runtime.ready is True

    with pytest.raises(RuntimeError, match="publish failed"):
        await runtime._publish("flashdrop.traces.v1", event, event["traceId"])
    assert runtime.ready is False

    await runtime._publish("flashdrop.traces.v1", event, event["traceId"])
    assert runtime.ready is True

    first_consumer.records.put_nowait(RuntimeError("consumer failed"))
    await asyncio.wait_for(recovered_consumer.start_called_event.wait(), 1.0)
    assert first_consumer.stopped is True
    assert runtime.ready is False

    recovery_gate.set()
    await asyncio.wait_for(recovered_consumer.started_event.wait(), 1.0)
    assert runtime.ready is True

    consumer_task = runtime._consumer_task
    assert consumer_task is not None
    consumer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer_task
    await asyncio.sleep(0)
    assert runtime.ready is False

    await runtime.stop()
    assert producer.stopped is True
    assert recovered_consumer.stopped is True
    assert runtime.ready is False


@pytest.mark.asyncio
async def test_periodic_probe_detects_outage_and_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecoveringProbeClient()
    producer = FakeProducer([], client=client)
    consumer = FakeConsumer()
    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", lambda **_: producer)
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", lambda *_, **__: consumer)
    runtime = KafkaRuntime(
        settings(probe_interval_seconds=0.01), AnalyticsStats(), NoopStorage()
    )

    await runtime.start()
    probe_task = runtime._probe_task
    assert probe_task is not None
    assert runtime.ready is True

    await asyncio.wait_for(client.failure_observed.wait(), 1.0)
    assert runtime.ready is False

    client.recovery_gate.set()
    await asyncio.wait_for(client.recovery_observed.wait(), 1.0)
    assert runtime.ready is True

    await runtime.stop()
    assert probe_task.done()
    assert producer.stopped is True
    assert consumer.stopped is True
    assert runtime.ready is False
