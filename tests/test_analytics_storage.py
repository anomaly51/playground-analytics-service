from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from lab_analytics.config import Settings
from lab_analytics.domain import AnalyticsStats, IncomingOrderEvent, decode_order_event
from lab_analytics.kafka import KafkaRuntime
from lab_analytics.storage import (
    INSERT_TRACE_SQL,
    POSTGRES_ACCESS_SQL,
    SCHEMA_SQL,
    UPSERT_ORDER_SQL,
    PostgresStore,
    ProjectionWrite,
)


class FakePool:
    def __init__(self) -> None:
        self.projections: dict[str, dict[str, Any]] = {}
        self.traces: dict[str, dict[str, Any]] = {}
        self.executed: list[str] = []
        self.failure: Exception | None = None
        self.closed = False
        self.writable = True

    async def create(self, *_: Any, **__: Any) -> FakePool:
        return self

    def check_failure(self) -> None:
        if self.failure is not None:
            raise self.failure

    async def execute(self, sql: str) -> None:
        self.check_failure()
        self.executed.append(sql)

    async def fetchval(self, sql: str, *args: Any) -> int | bool | None:
        self.check_failure()
        if sql == POSTGRES_ACCESS_SQL:
            return self.writable
        if sql.startswith("SELECT count(*)"):
            return len(self.projections)
        assert sql == UPSERT_ORDER_SQL
        order_id, version, payload = args
        current = self.projections.get(order_id)
        if current is not None and current["aggregateVersion"] >= version:
            return None
        document = json.loads(payload)
        if current is not None:
            document["createdAt"] = current["createdAt"]
        self.projections[order_id] = document
        return int(version)

    async def executemany(self, sql: str, rows: Any) -> None:
        self.check_failure()
        assert sql == INSERT_TRACE_SQL
        for event_id, payload in rows:
            self.traces.setdefault(event_id, json.loads(payload))

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.closed = True


class RetryStorage:
    def __init__(self) -> None:
        self.calls = 0
        self.retry_started = asyncio.Event()
        self.allow_success = asyncio.Event()
        self.succeeded = asyncio.Event()
        self.persisted: list[list[dict[str, Any]]] = []

    async def upsert_order(self, event: IncomingOrderEvent) -> ProjectionWrite:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("PostgreSQL unavailable")
        self.retry_started.set()
        await self.allow_success.wait()
        self.succeeded.set()
        return ProjectionWrite(applied=True, aggregate_version=event.aggregate_version)

    async def persist_traces(self, events: list[dict[str, Any]]) -> None:
        self.persisted.append(events)

    async def count_projections(self) -> int:
        return 0


class RecordingProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, *, value: bytes, key: bytes) -> None:
        self.events.append((topic, value, key))


class RecordingConsumer:
    def __init__(self) -> None:
        self.commits: list[dict[Any, int]] = []

    async def commit(self, offsets: dict[Any, int]) -> None:
        self.commits.append(offsets)


def settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "http_host": "127.0.0.1",
        "http_port": 8002,
        "kafka_enabled": True,
        "kafka_bootstrap_servers": "kafka:29092",
        "kafka_input_topic": "flashdrop.orders.v1",
        "kafka_analytics_topic": "flashdrop.analytics.v1",
        "kafka_trace_topic": "flashdrop.traces.v1",
        "kafka_group_id": "test-analytics",
        "kafka_client_id": "test-analytics",
        "kafka_publish_timeout_seconds": 1.0,
        "kafka_probe_interval_seconds": 60.0,
        "kafka_probe_timeout_seconds": 0.1,
        "processing_retry_max_seconds": 0.001,
        "postgres_url": "postgresql://airflow:airflow@postgres:5432/airflow",
        "postgres_operation_timeout_seconds": 0.1,
        "postgres_probe_interval_seconds": 60.0,
        "postgres_probe_timeout_seconds": 0.1,
    }
    values.update(overrides)
    return Settings(**values)


def incoming_order() -> IncomingOrderEvent:
    return decode_order_event(
        json.dumps(
            {
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
                    "totalCents": 9800,
                    "status": "pending",
                },
            }
        ).encode()
    )


@pytest.mark.asyncio
async def test_store_initializes_schema_and_guards_newer_versions() -> None:
    from dataclasses import replace

    pool = FakePool()
    store = PostgresStore(settings(), pool_factory=pool.create)
    event = incoming_order()
    await store.start()
    try:
        assert pool.executed == [SCHEMA_SQL]
        assert store.ready is True
        first = await store.upsert_order(event)
        original_created_at = pool.projections[event.order_id]["createdAt"]
        duplicate = await store.upsert_order(event)
        newer = await store.upsert_order(
            replace(event, aggregate_version=3, status="completed")
        )
        stale = await store.upsert_order(replace(event, aggregate_version=2))
        assert [first.applied, duplicate.applied, newer.applied, stale.applied] == [
            True,
            False,
            True,
            False,
        ]
        assert pool.projections[event.order_id]["aggregateVersion"] == 3
        assert pool.projections[event.order_id]["status"] == "completed"
        assert pool.projections[event.order_id]["createdAt"] == original_created_at
        assert await store.count_projections() == 1
    finally:
        await store.stop()
    assert pool.closed is True
    assert store.ready is False


@pytest.mark.asyncio
async def test_duplicate_trace_ids_preserve_original_document() -> None:
    pool = FakePool()
    store = PostgresStore(settings(), pool_factory=pool.create)
    event = {"id": "trace-event-1", "traceId": "trace-1", "summary": "original"}
    await store.start()
    try:
        await store.persist_traces([event, event])
        await store.persist_traces([{**event, "summary": "replayed"}])
        assert len(pool.traces) == 1
        assert pool.traces[event["id"]]["summary"] == "original"
        assert pool.traces[event["id"]]["storedAt"]
    finally:
        await store.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upsert", "traces", "count", "probe"])
async def test_database_failures_drop_readiness_and_probe_recovers(
    operation: str,
) -> None:
    pool = FakePool()
    store = PostgresStore(settings(), pool_factory=pool.create)
    await store.start()
    try:
        pool.failure = RuntimeError("database unavailable")
        if operation == "probe":
            await store._probe_connectivity()
        else:
            with pytest.raises(RuntimeError, match="database unavailable"):
                if operation == "upsert":
                    await store.upsert_order(incoming_order())
                elif operation == "traces":
                    await store.persist_traces([{"id": "trace-event-1"}])
                else:
                    await store.count_projections()
        assert store.ready is False
        pool.failure = None
        await store._probe_connectivity()
        assert store.ready is True
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_failed_schema_initialization_closes_pool_and_is_not_ready() -> None:
    pool = FakePool()
    pool.failure = RuntimeError("schema unavailable")
    store = PostgresStore(settings(), pool_factory=pool.create)
    with pytest.raises(RuntimeError, match="schema unavailable"):
        await store.start()
    assert store.ready is False
    assert pool.closed is True


@pytest.mark.asyncio
async def test_postgres_failure_retries_before_committing_kafka_offset() -> None:
    storage = RetryStorage()
    runtime = KafkaRuntime(settings(), AnalyticsStats(), storage)
    producer = RecordingProducer()
    consumer = RecordingConsumer()
    runtime._producer = producer  # type: ignore[assignment]
    runtime._consumer = consumer  # type: ignore[assignment]
    record = SimpleNamespace(
        topic="flashdrop.orders.v1",
        partition=0,
        offset=41,
        value=json.dumps(
            {
                "schemaVersion": 1,
                "eventId": "01ARZ3NDEKTSV4RRFFQ69G5FAA",
                "eventType": "order.created",
                "occurredAt": "2026-08-30T10:00:00.000Z",
                "traceId": "01ARZ3NDEKTSV4RRFFQ69G5FAB",
                "orderId": "01ARZ3NDEKTSV4RRFFQ69G5FAC",
                "aggregateVersion": 1,
                "data": {
                    "sku": "DROP-CAP-LIME",
                    "quantity": 2,
                    "currency": "USD",
                    "totalCents": 9800,
                    "status": "pending",
                },
            }
        ).encode(),
    )

    processing = asyncio.create_task(runtime._process_with_retry(record))
    await asyncio.wait_for(storage.retry_started.wait(), 1.0)
    assert consumer.commits == []

    storage.allow_success.set()
    await asyncio.wait_for(processing, 1.0)

    assert storage.calls == 2
    assert storage.succeeded.is_set()
    assert len(consumer.commits) == 1
    assert next(iter(consumer.commits[0].values())) == 42
    published_topics = [topic for topic, _, _ in producer.events]
    assert published_topics.count("flashdrop.analytics.v1") == 1
    assert published_topics.count("flashdrop.traces.v1") >= 4


@pytest.mark.asyncio
async def test_trace_storage_failure_prevents_offset_commit() -> None:
    class TraceRetryStorage(RetryStorage):
        async def upsert_order(self, event: IncomingOrderEvent) -> ProjectionWrite:
            return ProjectionWrite(
                applied=True, aggregate_version=event.aggregate_version
            )

        async def persist_traces(self, events: list[dict[str, Any]]) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("trace storage unavailable")
            self.retry_started.set()
            await self.allow_success.wait()
            self.persisted.append(events)

    storage = TraceRetryStorage()
    runtime = KafkaRuntime(settings(), AnalyticsStats(), storage)
    producer = RecordingProducer()
    consumer = RecordingConsumer()
    runtime._producer = producer  # type: ignore[assignment]
    runtime._consumer = consumer  # type: ignore[assignment]
    event = incoming_order()
    record = SimpleNamespace(
        topic="flashdrop.orders.v1",
        partition=0,
        offset=8,
        value=json.dumps(
            {
                "schemaVersion": 1,
                "eventId": event.event_id,
                "eventType": event.event_type,
                "occurredAt": event.occurred_at,
                "traceId": event.trace_id,
                "orderId": event.order_id,
                "aggregateVersion": event.aggregate_version,
                "data": {
                    "sku": event.sku,
                    "quantity": event.quantity,
                    "currency": event.currency,
                    "totalCents": event.total_cents,
                    "status": event.status,
                },
            }
        ).encode(),
    )
    processing = asyncio.create_task(runtime._process_with_retry(record))
    await asyncio.wait_for(storage.retry_started.wait(), 1.0)
    assert consumer.commits == []
    storage.allow_success.set()
    await asyncio.wait_for(processing, 1.0)
    assert len(consumer.commits) == 1
    assert next(iter(consumer.commits[0].values())) == 9
    assert storage.persisted
    edges = {
        (
            json.loads(value)["source"],
            json.loads(value).get("target"),
            json.loads(value)["transport"],
        )
        for topic, value, _ in producer.events
        if topic == "flashdrop.traces.v1"
    }
    assert ("analytics", "postgresql", "postgresql") in edges
    assert ("postgresql", "analytics", "postgresql") in edges


@pytest.mark.asyncio
async def test_readonly_database_does_not_recover_after_successful_reads() -> None:
    pool = FakePool()
    store = PostgresStore(settings(), pool_factory=pool.create)
    await store.start()
    try:
        pool.failure = RuntimeError("read-only transaction")
        with pytest.raises(RuntimeError, match="read-only"):
            await store.upsert_order(incoming_order())
        assert store.ready is False
        pool.failure = None
        pool.writable = False
        assert await store.count_projections() == 0
        assert store.ready is False
        await store._probe_connectivity()
        assert store.ready is False
        pool.writable = True
        await store._probe_connectivity()
        assert store.ready is True
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_database_without_write_access_cannot_become_ready_at_startup() -> None:
    pool = FakePool()
    pool.writable = False
    store = PostgresStore(settings(), pool_factory=pool.create)
    with pytest.raises(RuntimeError, match="not writable"):
        await store.start()
    assert store.ready is False
    assert pool.closed is True
