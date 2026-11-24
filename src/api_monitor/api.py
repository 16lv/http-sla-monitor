from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Response

from api_monitor.engine import (
    HealthStore,
    ProbeOutcome,
    TargetConfig,
    execute_probe,
    probe_targets_concurrently,
)


def _serialize_outcome(outcome: ProbeOutcome) -> dict[str, object]:
    return {
        "target_name": outcome.target_name,
        "checked_at": outcome.checked_at,
        "status_code": outcome.status_code,
        "latency_ms": outcome.latency_ms,
        "healthy": outcome.healthy,
        "failure_reason": outcome.failure_reason,
        "attempt_count": outcome.attempt_count,
    }


def create_app(store: HealthStore | None = None, db_path: str | None = None) -> FastAPI:
    owns_store = store is None
    resolved_db_path = db_path or os.environ.get("HTTP_SLA_DB_PATH", "http_sla_monitor.db")
    state: dict[str, HealthStore | None] = {"store": store}

    def get_store() -> HealthStore:
        current = state["store"]
        if current is None:
            current = HealthStore(resolved_db_path)
            state["store"] = current
        return current

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        application.state.http_client = httpx.AsyncClient()
        try:
            yield
        finally:
            await application.state.http_client.aclose()
            if owns_store and state["store"] is not None:
                state["store"].close()
                state["store"] = None

    app = FastAPI(
        title="HTTP SLA Monitor",
        version="1.3.0",
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
        get_store().upsert_target(target)
        return target

    @app.get("/targets")
    def list_targets() -> list[TargetConfig]:
        return get_store().list_targets()

    @app.get("/targets/summary")
    def list_all_summaries(
        limit: int = Query(default=200, ge=1, le=5000),
    ) -> list[dict[str, object]]:
        return get_store().compute_all_summaries(limit=limit)

    @app.get("/targets/{name}")
    def get_target(name: str) -> TargetConfig:
        target = get_store().get_target(name)
        if target is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return target

    @app.delete("/targets/{name}", status_code=204)
    def delete_target(name: str) -> Response:
        if not get_store().delete_target(name):
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return Response(status_code=204)

    @app.post("/targets/probe-all")
    async def probe_all_targets() -> dict[str, object]:
        active_store = get_store()
        targets = active_store.list_targets()
        if not targets:
            return {"total": 0, "healthy": 0, "unhealthy": 0, "results": []}

        client: httpx.AsyncClient | None = getattr(app.state, "http_client", None)
        outcomes = await probe_targets_concurrently(targets, client=client)
        for outcome in outcomes:
            active_store.record_outcome(outcome)

        healthy_count = sum(1 for item in outcomes if item.healthy)
        return {
            "total": len(outcomes),
            "healthy": healthy_count,
            "unhealthy": len(outcomes) - healthy_count,
            "results": [_serialize_outcome(item) for item in outcomes],
        }

    @app.post("/targets/{name}/probe")
    async def trigger_probe(name: str) -> dict[str, object]:
        active_store = get_store()
        target = active_store.get_target(name)
        if target is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        client: httpx.AsyncClient | None = getattr(app.state, "http_client", None)
        outcome = await execute_probe(target, client=client)
        active_store.record_outcome(outcome)
        return _serialize_outcome(outcome)

    @app.get("/targets/{name}/history")
    def get_history(
        name: str,
        limit: int = Query(default=20, ge=1, le=500),
    ) -> list[dict[str, object]]:
        active_store = get_store()
        if active_store.get_target(name) is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return active_store.list_history(name, limit=limit)

    @app.get("/targets/{name}/summary")
    def get_summary(
        name: str,
        limit: int = Query(default=200, ge=1, le=5000),
    ) -> dict[str, object]:
        active_store = get_store()
        if active_store.get_target(name) is None:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return active_store.compute_target_summary(name, limit=limit)

    return app


app = create_app()
