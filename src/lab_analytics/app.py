from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from .config import Settings
from .domain import AnalyticsStats
from .kafka import KafkaRuntime
from .log import configure_logging
from .storage import PostgresStore


def create_app(configured_settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = configured_settings or Settings.from_env()
        stats = AnalyticsStats()
        postgres = PostgresStore(settings)
        kafka = KafkaRuntime(settings, stats, postgres)
        app.state.settings = settings
        app.state.stats = stats
        app.state.postgres = postgres
        app.state.kafka = kafka
        await postgres.start()
        try:
            await kafka.start()
        except Exception:
            await postgres.stop()
            raise
        try:
            yield
        finally:
            try:
                await kafka.stop()
            finally:
                await postgres.stop()

    application = FastAPI(
        title="FlashDrop Order Analytics",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
        lifespan=lifespan,
    )

    @application.get("/stats")
    async def stats(request: Request) -> dict[str, object]:
        return await request.app.state.stats.snapshot()

    @application.get("/operations/snapshot")
    async def operations_snapshot(request: Request) -> dict[str, object]:
        runtime = await request.app.state.stats.snapshot()
        postgres_ready = request.app.state.postgres.ready
        try:
            projections_total: int | None = (
                await request.app.state.postgres.count_projections()
                if postgres_ready
                else None
            )
        except Exception:
            projections_total = None
        postgres_ready = request.app.state.postgres.ready
        kafka_ready = request.app.state.kafka.ready
        return {
            "projectionsTotal": projections_total,
            "nodes": {
                "analytics": {
                    "inFlight": runtime["inFlight"],
                    "total": runtime["total"],
                    "healthy": kafka_ready and postgres_ready,
                },
            },
        }

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        kafka_ready = request.app.state.kafka.ready
        postgres_ready = request.app.state.postgres.ready
        ready = kafka_ready and postgres_ready
        return JSONResponse(
            {
                "status": "ready" if ready else "not-ready",
                "kafka": kafka_ready,
                "postgres": postgres_ready,
            },
            status_code=200 if ready else 503,
        )

    @application.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(
            content=generate_latest(REGISTRY),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return application


app = create_app()


def main() -> None:
    configure_logging()
    settings = Settings.from_env()
    uvicorn.run(
        app,
        host=settings.http_host,
        port=settings.http_port,
        access_log=False,
        log_config=None,
    )


if __name__ == "__main__":
    main()
