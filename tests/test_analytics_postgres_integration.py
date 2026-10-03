from __future__ import annotations

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from lab_analytics.storage import (
    INSERT_TRACE_SQL,
    POSTGRES_ACCESS_SQL,
    SCHEMA_SQL,
    UPSERT_ORDER_SQL,
)


@pytest.mark.asyncio
async def test_real_postgres_version_guard_and_duplicate_trace_ids() -> None:
    dsn = os.getenv("ANALYTICS_TEST_POSTGRES_URL")
    if not dsn:
        pytest.skip("ANALYTICS_TEST_POSTGRES_URL is not configured")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, command_timeout=3)
    try:
        async with pool.acquire() as connection:
            transaction = connection.transaction()
            await transaction.start()
            try:
                await connection.execute(SCHEMA_SQL)
                order_id = f"test-order-{uuid4()}"
                trace_id = f"test-trace-{uuid4()}"
                original = {
                    "orderId": order_id,
                    "aggregateVersion": 1,
                    "createdAt": "2026-01-01T00:00:00Z",
                    "status": "pending",
                }
                assert (
                    await connection.fetchval(
                        UPSERT_ORDER_SQL, order_id, 1, json.dumps(original)
                    )
                    == 1
                )
                newer = {
                    **original,
                    "aggregateVersion": 3,
                    "status": "completed",
                    "createdAt": "2026-02-01T00:00:00Z",
                }
                assert (
                    await connection.fetchval(
                        UPSERT_ORDER_SQL, order_id, 3, json.dumps(newer)
                    )
                    == 3
                )
                for version in (2, 3):
                    assert (
                        await connection.fetchval(
                            UPSERT_ORDER_SQL, order_id, version, json.dumps(original)
                        )
                        is None
                    )
                document = json.loads(
                    await connection.fetchval(
                        "SELECT document FROM flashdrop_analytics.order_projections "
                        "WHERE order_id = $1",
                        order_id,
                    )
                )
                assert document["aggregateVersion"] == 3
                assert document["status"] == "completed"
                assert document["createdAt"] == original["createdAt"]
                trace = {"id": trace_id, "summary": "original"}
                await connection.executemany(
                    INSERT_TRACE_SQL,
                    [
                        (trace_id, json.dumps(trace)),
                        (trace_id, json.dumps({**trace, "summary": "duplicate"})),
                    ],
                )
                stored_trace = json.loads(
                    await connection.fetchval(
                        "SELECT document FROM flashdrop_analytics.trace_events "
                        "WHERE event_id = $1",
                        trace_id,
                    )
                )
                assert stored_trace == trace
            finally:
                await transaction.rollback()
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_real_postgres_readonly_transaction_fails_write_access_probe() -> None:
    dsn = os.getenv("ANALYTICS_TEST_POSTGRES_URL")
    if not dsn:
        pytest.skip("ANALYTICS_TEST_POSTGRES_URL is not configured")
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(SCHEMA_SQL)
        assert await connection.fetchval(POSTGRES_ACCESS_SQL) is True
        async with connection.transaction(readonly=True):
            assert await connection.fetchval("SELECT 1") == 1
            assert await connection.fetchval(POSTGRES_ACCESS_SQL) is False
        assert await connection.fetchval(POSTGRES_ACCESS_SQL) is True
    finally:
        await connection.close()
