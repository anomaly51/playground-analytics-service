from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import asyncpg

from .config import Settings
from .domain import IncomingOrderEvent
from .events import utc_now
from .metrics import POSTGRES_READY, POSTGRES_WRITES

LOGGER = logging.getLogger(__name__)

# The separate schema keeps analytics documents apart from operational orders
# and Airflow metadata while reusing the existing PostgreSQL service.
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS flashdrop_analytics;
CREATE TABLE IF NOT EXISTS flashdrop_analytics.order_projections (
    order_id text PRIMARY KEY,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    document jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS flashdrop_analytics.trace_events (
    event_id text PRIMARY KEY,
    document jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS order_projections_trace_id
    ON flashdrop_analytics.order_projections ((document->>'traceId'));
CREATE INDEX IF NOT EXISTS order_projections_status_updated_at
    ON flashdrop_analytics.order_projections
    ((document->>'status'), (document->>'updatedAt') DESC);
CREATE INDEX IF NOT EXISTS trace_events_trace_timestamp
    ON flashdrop_analytics.trace_events
    ((document->>'traceId'), (document->>'timestamp'));
CREATE INDEX IF NOT EXISTS trace_events_order_id
    ON flashdrop_analytics.trace_events ((document->'payload'->>'orderId'));
"""

POSTGRES_ACCESS_SQL = """
SELECT current_setting('transaction_read_only') = 'off'
    AND has_schema_privilege(current_user, 'flashdrop_analytics', 'USAGE')
    AND has_table_privilege(current_user,
        'flashdrop_analytics.order_projections', 'SELECT')
    AND has_table_privilege(current_user,
        'flashdrop_analytics.order_projections', 'INSERT')
    AND has_table_privilege(current_user,
        'flashdrop_analytics.order_projections', 'UPDATE')
    AND has_table_privilege(current_user,
        'flashdrop_analytics.trace_events', 'INSERT')
    AND has_table_privilege(current_user,
        'flashdrop_analytics.trace_events', 'SELECT')
"""

UPSERT_ORDER_SQL = """
INSERT INTO flashdrop_analytics.order_projections AS current
    (order_id, aggregate_version, document)
VALUES ($1, $2, $3::jsonb)
ON CONFLICT (order_id) DO UPDATE
SET aggregate_version = EXCLUDED.aggregate_version,
    document = EXCLUDED.document || jsonb_build_object(
        'createdAt', COALESCE(current.document->'createdAt',
                              EXCLUDED.document->'createdAt'))
WHERE current.aggregate_version < EXCLUDED.aggregate_version
RETURNING aggregate_version
"""

INSERT_TRACE_SQL = """
INSERT INTO flashdrop_analytics.trace_events (event_id, document)
VALUES ($1, $2::jsonb)
ON CONFLICT (event_id) DO NOTHING
"""


@dataclass(frozen=True, slots=True)
class ProjectionWrite:
    applied: bool
    aggregate_version: int


class AnalyticsStore(Protocol):
    async def upsert_order(self, event: IncomingOrderEvent) -> ProjectionWrite: ...

    async def persist_traces(self, events: Sequence[dict[str, Any]]) -> None: ...

    async def count_projections(self) -> int: ...


class PostgresStore:
    def __init__(
        self,
        settings: Settings,
        pool_factory: Callable[..., Any] = asyncpg.create_pool,
    ) -> None:
        self._settings = settings
        self._pool_factory = pool_factory
        self._pool: Any | None = None
        self._probe_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._started = False
        self._connectivity_ready = False

    @property
    def ready(self) -> bool:
        probe_task = self._probe_task
        return (
            self._started
            and not self._stopping.is_set()
            and self._connectivity_ready
            and self._pool is not None
            and probe_task is not None
            and not probe_task.done()
        )

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("PostgreSQL analytics store is already started")
        self._stopping.clear()
        timeout = self._settings.postgres_operation_timeout_seconds
        try:
            async with asyncio.timeout(timeout):
                self._pool = await self._pool_factory(
                    self._settings.postgres_url,
                    min_size=1,
                    max_size=4,
                    timeout=timeout,
                    command_timeout=timeout,
                    server_settings={"application_name": "flashdrop-analytics"},
                )
                await self._pool.execute(SCHEMA_SQL)
                await self._check_write_access()
        except BaseException:
            await self._close_pool()
            self._connectivity_ready = False
            self._refresh_ready_metric()
            raise

        self._connectivity_ready = True
        self._started = True
        self._probe_task = asyncio.create_task(
            self._probe_loop(), name="analytics-postgres-connectivity-probe"
        )
        self._probe_task.add_done_callback(self._probe_task_done)
        self._refresh_ready_metric()
        LOGGER.info("FlashDrop PostgreSQL projection store started")

    async def stop(self) -> None:
        self._stopping.set()
        self._started = False
        self._connectivity_ready = False
        self._refresh_ready_metric()
        probe_task, self._probe_task = self._probe_task, None
        if probe_task is not None:
            probe_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await probe_task
        await self._close_pool()
        LOGGER.info("FlashDrop PostgreSQL projection store stopped")

    async def upsert_order(self, event: IncomingOrderEvent) -> ProjectionWrite:
        pool = self._require_pool()
        recorded_at = utc_now()
        document: dict[str, Any] = {
            "orderId": event.order_id,
            "traceId": event.trace_id,
            "eventId": event.event_id,
            "eventType": event.event_type,
            "aggregateVersion": event.aggregate_version,
            "sku": event.sku,
            "quantity": event.quantity,
            "currency": event.currency,
            "totalCents": event.total_cents,
            "status": event.status,
            "reason": event.reason,
            "occurredAt": event.occurred_at,
            "runId": event.run_id,
            "createdAt": recorded_at,
            "updatedAt": recorded_at,
        }
        try:
            async with asyncio.timeout(
                self._settings.postgres_operation_timeout_seconds
            ):
                version = await pool.fetchval(
                    UPSERT_ORDER_SQL,
                    event.order_id,
                    event.aggregate_version,
                    json.dumps(document),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            POSTGRES_WRITES.labels(table="order_projections", status="failed").inc()
            self._set_connectivity_ready(False)
            raise
        else:
            POSTGRES_WRITES.labels(table="order_projections", status="succeeded").inc()
            self._set_connectivity_ready(True)
            return ProjectionWrite(
                applied=version is not None, aggregate_version=event.aggregate_version
            )

    async def persist_traces(self, events: Sequence[dict[str, Any]]) -> None:
        if not events:
            return
        pool = self._require_pool()
        rows = [
            (event["id"], json.dumps({**event, "storedAt": utc_now()}))
            for event in events
        ]
        try:
            async with asyncio.timeout(
                self._settings.postgres_operation_timeout_seconds
            ):
                # asyncpg executemany is atomic: either the whole batch is durable
                # or it can be retried. Event IDs deduplicate re-delivery.
                await pool.executemany(INSERT_TRACE_SQL, rows)
        except asyncio.CancelledError:
            raise
        except Exception:
            POSTGRES_WRITES.labels(table="trace_events", status="failed").inc()
            self._set_connectivity_ready(False)
            raise
        else:
            POSTGRES_WRITES.labels(table="trace_events", status="succeeded").inc()
            self._set_connectivity_ready(True)

    async def count_projections(self) -> int:
        pool = self._require_pool()
        try:
            async with asyncio.timeout(
                self._settings.postgres_operation_timeout_seconds
            ):
                total = await pool.fetchval(
                    "SELECT count(*) FROM flashdrop_analytics.order_projections"
                )
        except Exception:
            self._set_connectivity_ready(False)
            raise
        # Read access alone cannot recover readiness after failed durable writes.
        return int(total)

    async def _probe_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._settings.postgres_probe_interval_seconds,
                )
                return
            except TimeoutError:
                await self._probe_connectivity()

    async def _probe_connectivity(self) -> None:
        was_ready = self._connectivity_ready
        try:
            async with asyncio.timeout(self._settings.postgres_probe_timeout_seconds):
                await self._check_write_access()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._set_connectivity_ready(False)
            if was_ready:
                LOGGER.warning(
                    "PostgreSQL connectivity probe failed",
                    extra={"errorType": type(error).__name__},
                )
        else:
            self._set_connectivity_ready(True)
            if not was_ready:
                LOGGER.info("PostgreSQL connectivity probe recovered")

    async def _check_write_access(self) -> None:
        writable = await self._require_pool().fetchval(POSTGRES_ACCESS_SQL)
        if writable is not True:
            raise RuntimeError("PostgreSQL analytics persistence is not writable")

    def _probe_task_done(self, task: asyncio.Task[None]) -> None:
        error: BaseException | None = None
        if not task.cancelled():
            error = task.exception()
        self._connectivity_ready = False
        self._refresh_ready_metric()
        if not self._stopping.is_set():
            LOGGER.error(
                "PostgreSQL connectivity probe stopped unexpectedly",
                extra={"error": repr(error)},
            )

    async def _close_pool(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            try:
                async with asyncio.timeout(
                    self._settings.postgres_operation_timeout_seconds
                ):
                    await pool.close()
            except BaseException:
                pool.terminate()
                raise

    def _require_pool(self) -> Any:
        if self._pool is None:
            raise RuntimeError("PostgreSQL analytics store is not started")
        return self._pool

    def _set_connectivity_ready(self, ready: bool) -> None:
        self._connectivity_ready = ready
        self._refresh_ready_metric()

    def _refresh_ready_metric(self) -> None:
        POSTGRES_READY.set(1 if self.ready else 0)
