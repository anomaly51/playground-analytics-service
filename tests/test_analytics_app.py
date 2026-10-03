from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request

from lab_analytics.app import create_app
from lab_analytics.config import Settings
from lab_analytics.domain import AnalyticsStats


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kafka_ready, postgres_ready, status",
    [
        (True, True, 200),
        (False, True, 503),
        (True, False, 503),
    ],
)
async def test_readyz_requires_both_dependencies(
    kafka_ready: bool,
    postgres_ready: bool,
    status: int,
) -> None:
    app = create_app()
    app.state.kafka = SimpleNamespace(ready=kafka_ready)
    app.state.postgres = SimpleNamespace(ready=postgres_ready)
    endpoint = next(route.endpoint for route in app.routes if route.path == "/readyz")
    response = await endpoint(Request({"type": "http", "app": app}))
    assert response.status_code == status
    assert json.loads(response.body) == {
        "status": "ready" if status == 200 else "not-ready",
        "kafka": kafka_ready,
        "postgres": postgres_ready,
    }


@pytest.mark.asyncio
async def test_operations_only_contributes_analytics_node() -> None:
    app = create_app()
    app.state.stats = AnalyticsStats()
    app.state.kafka = SimpleNamespace(ready=True)
    app.state.postgres = SimpleNamespace(
        ready=True, count_projections=AsyncMock(return_value=108)
    )
    endpoint = next(
        route.endpoint for route in app.routes if route.path == "/operations/snapshot"
    )
    response = await endpoint(Request({"type": "http", "app": app}))
    assert set(response["nodes"]) == {"analytics"}
    assert response["projectionsTotal"] == 108
    assert response["nodes"]["analytics"]["healthy"] is True


def test_postgres_url_prefers_flashdrop_then_legacy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FLASHDROP_ANALYTICS_POSTGRES_URL", raising=False)
    monkeypatch.setenv("LAB_ANALYTICS_POSTGRES_URL", "postgresql://legacy/db")
    assert Settings.from_env().postgres_url == "postgresql://legacy/db"
    monkeypatch.setenv("FLASHDROP_ANALYTICS_POSTGRES_URL", "postgresql://current/db")
    assert Settings.from_env().postgres_url == "postgresql://current/db"
