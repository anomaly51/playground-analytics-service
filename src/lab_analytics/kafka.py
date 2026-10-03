from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]
from aiokafka.structs import (  # type: ignore[import-untyped]
    ConsumerRecord,
    TopicPartition,
)

from .config import Settings
from .domain import (
    AnalyticsStats,
    IncomingOrderEvent,
    InvalidOrderEvent,
    decode_order_event,
)
from .events import encode_event, trace_event
from .metrics import KAFKA_READY, MESSAGES, PROCESSING_DURATION, PUBLISHED, RETRIES
from .storage import AnalyticsStore, ProjectionWrite

LOGGER = logging.getLogger(__name__)


class KafkaRuntime:
    def __init__(
        self,
        settings: Settings,
        stats: AnalyticsStats,
        storage: AnalyticsStore,
    ) -> None:
        self._settings = settings
        self._stats = stats
        self._storage = storage
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._probe_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._started = False
        self._producer_ready = False
        self._consumer_ready = False
        self._connectivity_ready = False

    @property
    def ready(self) -> bool:
        if not self._started or self._stopping.is_set():
            return False
        if not self._settings.kafka_enabled:
            return True
        consumer_task = self._consumer_task
        probe_task = self._probe_task
        return (
            self._producer_ready
            and self._consumer_ready
            and self._connectivity_ready
            and self._producer is not None
            and self._consumer is not None
            and consumer_task is not None
            and not consumer_task.done()
            and probe_task is not None
            and not probe_task.done()
        )

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Kafka analytics runtime is already started")
        self._stopping.clear()
        if not self._settings.kafka_enabled:
            self._started = True
            self._refresh_ready_metric()
            LOGGER.warning("Kafka analytics runtime is disabled")
            return

        producer = self._new_producer()
        consumer = self._new_consumer()
        await producer.start()
        try:
            await consumer.start()
        except Exception:
            with contextlib.suppress(Exception):
                await consumer.stop()
            await producer.stop()
            self._refresh_ready_metric()
            raise

        self._producer = producer
        self._consumer = consumer
        self._producer_ready = True
        self._consumer_ready = True
        self._connectivity_ready = True
        self._started = True
        self._consumer_task = asyncio.create_task(
            self._supervise_consumer(), name="analytics-kafka-consumer"
        )
        self._consumer_task.add_done_callback(self._consumer_task_done)
        self._probe_task = asyncio.create_task(
            self._probe_loop(), name="analytics-kafka-connectivity-probe"
        )
        self._probe_task.add_done_callback(self._probe_task_done)
        self._refresh_ready_metric()
        LOGGER.info(
            "Kafka analytics runtime started",
            extra={
                "inputTopic": self._settings.kafka_input_topic,
                "analyticsTopic": self._settings.kafka_analytics_topic,
                "traceTopic": self._settings.kafka_trace_topic,
                "groupId": self._settings.kafka_group_id,
            },
        )

    async def stop(self) -> None:
        self._stopping.set()
        self._started = False
        self._producer_ready = False
        self._consumer_ready = False
        self._connectivity_ready = False
        self._refresh_ready_metric()
        consumer_task, self._consumer_task = self._consumer_task, None
        probe_task, self._probe_task = self._probe_task, None
        for task in (consumer_task, probe_task):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        consumer, self._consumer = self._consumer, None
        producer, self._producer = self._producer, None
        if consumer is not None:
            await consumer.stop()
        if producer is not None:
            await producer.stop()
        LOGGER.info("Kafka analytics runtime stopped")

    async def _probe_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._settings.kafka_probe_interval_seconds,
                )
                return
            except TimeoutError:
                await self._probe_connectivity()

    async def _probe_connectivity(self) -> None:
        was_ready = self._connectivity_ready
        try:
            producer = self._require_producer()
            async with asyncio.timeout(self._settings.kafka_probe_timeout_seconds):
                await producer.client.fetch_all_metadata()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._set_connectivity_ready(False)
            if was_ready:
                LOGGER.warning(
                    "Kafka connectivity probe failed",
                    extra={"errorType": type(error).__name__},
                )
        else:
            self._set_connectivity_ready(True)
            if not was_ready:
                LOGGER.info("Kafka connectivity probe recovered")

    def _probe_task_done(self, task: asyncio.Task[None]) -> None:
        error: BaseException | None = None
        if not task.cancelled():
            error = task.exception()
        self._connectivity_ready = False
        self._refresh_ready_metric()
        if not self._stopping.is_set():
            LOGGER.error(
                "Kafka connectivity probe stopped unexpectedly",
                extra={"error": repr(error)},
            )

    async def _supervise_consumer(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._consume()
                if self._stopping.is_set():
                    return
                raise RuntimeError("Kafka consumer loop stopped unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception:
                self._set_consumer_ready(False)
                LOGGER.exception("Kafka consumer loop failed; reconnecting")
                if not await self._recover_consumer():
                    return

    async def _recover_consumer(self) -> bool:
        previous, self._consumer = self._consumer, None
        if previous is not None:
            with contextlib.suppress(Exception):
                await previous.stop()

        attempt = 0
        while not self._stopping.is_set():
            attempt += 1
            consumer = self._new_consumer()
            try:
                await consumer.start()
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                raise
            except Exception:
                with contextlib.suppress(Exception):
                    await consumer.stop()
                delay = min(
                    2 ** min(attempt - 1, 8),
                    self._settings.processing_retry_max_seconds,
                )
                LOGGER.exception(
                    "Kafka consumer reconnect failed",
                    extra={"attempt": attempt, "retryInSeconds": delay},
                )
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                except TimeoutError:
                    continue
                return False
            else:
                self._consumer = consumer
                self._set_consumer_ready(True)
                LOGGER.info("Kafka consumer recovered", extra={"attempt": attempt})
                return True
        return False

    def _consumer_task_done(self, task: asyncio.Task[None]) -> None:
        error: BaseException | None = None
        if not task.cancelled():
            error = task.exception()
        self._consumer_ready = False
        self._refresh_ready_metric()
        if not self._stopping.is_set():
            LOGGER.error(
                "Kafka consumer supervisor stopped unexpectedly",
                extra={"error": repr(error)},
            )

    async def _consume(self) -> None:
        consumer = self._require_consumer()
        async for record in consumer:
            await self._process_with_retry(record)

    async def _process_with_retry(self, record: ConsumerRecord) -> None:
        await self._stats.begin()
        finished = False
        try:
            incoming = decode_order_event(record.value)
        except InvalidOrderEvent as error:
            MESSAGES.labels(status="invalid").inc()
            LOGGER.warning(
                "Skipping invalid Kafka record",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "reason": str(error),
                },
            )
            await self._commit(record)
            await self._stats.finish(None, invalid=True)
            finished = True
            return

        attempt = 0
        try:
            while not self._stopping.is_set():
                started_at = time.monotonic()
                try:
                    write = await self._process(incoming)
                    await self._commit(record)
                    await self._stats.finish(incoming, applied=write.applied)
                    finished = True
                    MESSAGES.labels(status="succeeded").inc()
                    PROCESSING_DURATION.observe(time.monotonic() - started_at)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    MESSAGES.labels(status="retrying").inc()
                    RETRIES.inc()
                    attempt += 1
                    delay = min(
                        2 ** min(attempt - 1, 8),
                        self._settings.processing_retry_max_seconds,
                    )
                    LOGGER.exception(
                        "Kafka order processing failed; retrying",
                        extra={
                            "traceId": incoming.trace_id,
                            "orderId": incoming.order_id,
                            "eventId": incoming.event_id,
                            "attempt": attempt,
                            "retryInSeconds": delay,
                        },
                    )
                    await self._publish_retry_trace(incoming, attempt, delay, error)
                    try:
                        await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                    except TimeoutError:
                        pass
        finally:
            if not finished:
                await self._stats.abort()

    async def _process(self, incoming: IncomingOrderEvent) -> ProjectionWrite:
        identifiers = {
            "eventId": incoming.event_id,
            "orderId": incoming.order_id,
            "aggregateVersion": incoming.aggregate_version,
            **({"runId": incoming.run_id} if incoming.run_id else {}),
        }
        received = trace_event(
            trace_id=incoming.trace_id,
            source="kafka",
            target="analytics",
            transport="kafka",
            stage="order.projection.consume",
            status="started",
            summary="Analytics consumed an order event",
            payload=identifiers,
            order_id=incoming.order_id,
            run_id=incoming.run_id,
        )
        postgres_started = trace_event(
            trace_id=incoming.trace_id,
            source="analytics",
            target="postgresql",
            transport="postgresql",
            stage="order.projection.upsert",
            status="started",
            summary="Analytics started the PostgreSQL projection upsert",
            payload=identifiers,
            order_id=incoming.order_id,
            run_id=incoming.run_id,
        )
        await self._publish(
            self._settings.kafka_trace_topic, received, incoming.trace_id
        )
        await self._publish(
            self._settings.kafka_trace_topic, postgres_started, incoming.trace_id
        )

        write = await self._storage.upsert_order(incoming)
        postgres_finished = trace_event(
            trace_id=incoming.trace_id,
            source="postgresql",
            target="analytics",
            transport="postgresql",
            stage="order.projection.upsert",
            status="succeeded",
            summary=(
                "PostgreSQL applied the newest order aggregate version"
                if write.applied
                else "PostgreSQL ignored a duplicate or stale aggregate version"
            ),
            payload={**identifiers, "applied": write.applied},
            order_id=incoming.order_id,
            run_id=incoming.run_id,
        )
        await self._storage.persist_traces(
            [received, postgres_started, postgres_finished]
        )
        await self._publish(
            self._settings.kafka_trace_topic, postgres_finished, incoming.trace_id
        )

        if write.applied:
            projection_event = {
                "schemaVersion": 1,
                "eventId": incoming.event_id,
                "occurredAt": incoming.occurred_at,
                "traceId": incoming.trace_id,
                "orderId": incoming.order_id,
                "aggregateVersion": incoming.aggregate_version,
                "status": incoming.status,
                "projectionApplied": True,
            }
            await self._publish(
                self._settings.kafka_analytics_topic,
                projection_event,
                incoming.order_id,
            )

        emitted = trace_event(
            trace_id=incoming.trace_id,
            source="analytics",
            target="kafka",
            transport="kafka",
            stage="order.projection.completed",
            status="succeeded",
            summary="Analytics completed order projection processing",
            payload={**identifiers, "applied": write.applied},
            order_id=incoming.order_id,
            run_id=incoming.run_id,
        )
        await self._storage.persist_traces([emitted])
        await self._publish(
            self._settings.kafka_trace_topic, emitted, incoming.trace_id
        )
        LOGGER.info(
            "Order projection processed",
            extra={
                "traceId": incoming.trace_id,
                "orderId": incoming.order_id,
                "aggregateVersion": incoming.aggregate_version,
                "applied": write.applied,
            },
        )
        return write

    async def _publish_retry_trace(
        self,
        incoming: IncomingOrderEvent,
        attempt: int,
        delay: float,
        error: Exception,
    ) -> None:
        event = trace_event(
            trace_id=incoming.trace_id,
            source="analytics",
            target="kafka",
            transport="kafka",
            stage="order.projection.retry",
            status="retrying",
            summary="Analytics is retrying a transient processing failure",
            payload={
                "eventId": incoming.event_id,
                "orderId": incoming.order_id,
                "attempt": attempt,
                "retryInSeconds": delay,
                "errorType": type(error).__name__,
            },
            order_id=incoming.order_id,
            run_id=incoming.run_id,
        )
        with contextlib.suppress(Exception):
            await self._publish(
                self._settings.kafka_trace_topic, event, incoming.trace_id
            )

    async def _publish(self, topic: str, event: dict[str, Any], trace_id: str) -> None:
        try:
            producer = self._require_producer()
            async with asyncio.timeout(self._settings.kafka_publish_timeout_seconds):
                await producer.send_and_wait(
                    topic,
                    value=encode_event(event),
                    key=trace_id.encode("utf-8"),
                )
            PUBLISHED.labels(topic=topic, status="succeeded").inc()
            self._set_producer_ready(True)
        except Exception:
            PUBLISHED.labels(topic=topic, status="failed").inc()
            self._set_producer_ready(False)
            raise

    async def _commit(self, record: ConsumerRecord) -> None:
        consumer = self._require_consumer()
        partition = TopicPartition(record.topic, record.partition)
        await consumer.commit({partition: record.offset + 1})

    def _require_consumer(self) -> AIOKafkaConsumer:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not started")
        return self._consumer

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")
        return self._producer

    def _new_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=f"{self._settings.kafka_client_id}-producer",
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=int(self._settings.kafka_publish_timeout_seconds * 1000),
        )

    def _new_consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            self._settings.kafka_input_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=f"{self._settings.kafka_client_id}-consumer",
            group_id=self._settings.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    def _set_producer_ready(self, ready: bool) -> None:
        self._producer_ready = ready
        self._refresh_ready_metric()

    def _set_consumer_ready(self, ready: bool) -> None:
        self._consumer_ready = ready
        self._refresh_ready_metric()

    def _set_connectivity_ready(self, ready: bool) -> None:
        self._connectivity_ready = ready
        self._producer_ready = ready
        self._refresh_ready_metric()

    def _refresh_ready_metric(self) -> None:
        KAFKA_READY.set(1 if self.ready else 0)
