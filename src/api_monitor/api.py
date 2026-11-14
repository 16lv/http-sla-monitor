from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException

from src.api_monitor.engine import HealthStore, TargetConfig, execute_probe


def create_app(store: HealthStore | None = None) -> FastAPI:
    owns_store = store is None
    active_store = store or HealthStore("http_sla_monitor.db")

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.http_client = httpx.AsyncClient()
        try:
            yield
        finally:
            await application.state.http_client.aclose()
            if owns_store:
                active_store.close()

    app = FastAPI(
        title="HTTP SLA Monitor",
        version="1.2.0",
        description=(
            "Endpoint availability monitor tracking HTTP status codes, JSON response keys, "
            "and p50/p95/p99 latency percentiles."
        ),
        lifespan=lifespan,
    )

    @app.get("/health")
    def service_health() -> dict[str, str]:
        return {"service": "http-sla-monitor", "status": "ok"}

    @app.post("/targets", status_code=201)
    def register_target(target: TargetConfig) -> TargetConfig:
        active_store.upsert_target(target)
        return target

    @app.get("/targets")
    def list_targets() -> list[TargetConfig]:
        return active_store.list_targets()

    @app.post("/targets/{name}/probe")
    async def trigger_probe(name: str) -> dict[str, object]:
        target = active_store.get_target(name)
        if target is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        client: httpx.AsyncClient | None = getattr(app.state, "http_client", None)
        outcome = await execute_probe(target, client=client)
        active_store.record_outcome(outcome)
        return {
            "target_name": outcome.target_name,
            "checked_at": outcome.checked_at,
            "status_code": outcome.status_code,
            "latency_ms": outcome.latency_ms,
            "healthy": outcome.healthy,
            "failure_reason": outcome.failure_reason,
            "attempt_count": outcome.attempt_count,
        }

    @app.get("/targets/{name}/summary")
    def get_summary(name: str) -> dict[str, object]:
        if active_store.get_target(name) is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return active_store.compute_target_summary(name)

    return app


app = create_app()
