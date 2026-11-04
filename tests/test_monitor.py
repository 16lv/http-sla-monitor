from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from src.api_monitor.api import create_app
from src.api_monitor.engine import HealthStore, ProbeOutcome, TargetConfig, execute_probe


@pytest.mark.asyncio
async def test_execute_probe_passes_when_status_latency_and_json_key_match() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "healthy", "region": "eu-west-2"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        target = TargetConfig(
            name="auth-service",
            url="https://api.example.com/health",
            expected_status=200,
            max_latency_ms=500.0,
            required_json_key="status",
            retries=1,
        )
        outcome = await execute_probe(target, client=client)
        assert outcome.healthy is True
        assert outcome.status_code == 200
        assert outcome.failure_reason is None
        assert outcome.attempt_count == 1


@pytest.mark.asyncio
async def test_execute_probe_retries_and_reports_missing_json_key() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"wrong_field": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        target = TargetConfig(
            name="billing-api",
            url="https://api.example.com/v1/status",
            expected_status=200,
            required_json_key="healthy",
            retries=2,
        )
        outcome = await execute_probe(target, client=client)
        assert outcome.healthy is False
        assert outcome.attempt_count == 3
        assert calls == 3
        assert outcome.failure_reason is not None
        assert "Missing required JSON key 'healthy'" in outcome.failure_reason


@pytest.mark.asyncio
async def test_execute_probe_recovers_on_retry_after_transient_503() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"error": "warming_up"})
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        target = TargetConfig(
            name="search-index",
            url="https://search.example.com/ready",
            expected_status=200,
            required_json_key="ok",
            retries=2,
        )
        outcome = await execute_probe(target, client=client)
        assert outcome.healthy is True
        assert outcome.status_code == 200
        assert outcome.attempt_count == 2


def test_fastapi_target_registration_and_sla_percentile_summary() -> None:
    store = HealthStore(":memory:")
    client = TestClient(create_app(store))

    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "ok"

    resp = client.post(
        "/targets",
        json={
            "name": "payments-gateway",
            "url": "https://payments.example.org/ping",
            "method": "GET",
            "expected_status": 200,
            "max_latency_ms": 300.0,
            "retries": 1,
        },
    )
    assert resp.status_code == 201

    list_resp = client.get("/targets")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1

    # Verify empty summary before checks run
    empty_summary = client.get("/targets/payments-gateway/summary").json()
    assert empty_summary["total_checks"] == 0
    assert empty_summary["availability_pct"] == 0.0

    # Seed deterministic probe history to verify p50/p95/p99 and availability rollup
    sample_latencies = [12.0, 15.0, 18.0, 22.0, 25.0, 30.0, 42.0, 95.0, 140.0, 210.0]
    for idx, lat in enumerate(sample_latencies):
        store.record_outcome(
            ProbeOutcome(
                target_name="payments-gateway",
                url="https://payments.example.org/ping",
                checked_at=f"2026-10-26T12:00:{idx:02d}Z",
                status_code=200 if idx < 9 else 503,
                latency_ms=lat,
                healthy=(idx < 9),
                failure_reason=None if idx < 9 else "Expected HTTP 200, received 503",
                attempt_count=1,
            )
        )

    summary_resp = client.get("/targets/payments-gateway/summary")
    assert summary_resp.status_code == 200
    body = summary_resp.json()
    assert body["total_checks"] == 10
    assert body["availability_pct"] == 90.0
    assert body["p50_latency_ms"] == 27.5
    assert body["p95_latency_ms"] > 140.0

    missing_resp = client.get("/targets/unknown-service/summary")
    assert missing_resp.status_code == 404
