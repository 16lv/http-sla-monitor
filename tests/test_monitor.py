from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from api_monitor.api import create_app
from api_monitor.engine import (
    HealthStore,
    ProbeOutcome,
    TargetConfig,
    execute_probe,
    probe_targets_concurrently,
)


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
            required_json_key="  status  ",
            retries=1,
        )
        assert target.required_json_key == "status"
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
async def test_execute_probe_recovers_on_retry_after_transient_503_and_timeout() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("Timed out reading response", request=request)
        if calls == 2:
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
        assert outcome.attempt_count == 3


def test_target_validation_normalises_blank_key_and_rejects_head_with_json_key() -> None:
    blank_key_target = TargetConfig(
        name="static-ping",
        url="https://example.org/health",
        method="HEAD",
        required_json_key="   ",
    )
    assert blank_key_target.required_json_key is None

    with pytest.raises(ValueError, match="HEAD requests have no response body"):
        TargetConfig(
            name="invalid-head",
            url="https://example.org/health",
            method="HEAD",
            required_json_key="status",
        )


def test_fastapi_target_lifecycle_history_and_sla_summary(tmp_path: Path) -> None:
    # Importing or instantiating create_app with a custom db_path must only open on request
    lazy_db = tmp_path / "lazy_monitor.db"
    lazy_app = create_app(db_path=str(lazy_db))
    assert not lazy_db.exists()
    with TestClient(lazy_app) as lazy_client:
        assert lazy_client.get("/targets").json() == []
    assert lazy_db.exists()

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

    assert len(client.get("/targets").json()) == 1

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

    summary_resp = client.get("/targets/payments-gateway/summary", params={"limit": 10})
    assert summary_resp.status_code == 200
    body = summary_resp.json()
    assert body["total_checks"] == 10
    assert body["availability_pct"] == 90.0
    assert body["p50_latency_ms"] == 27.5
    assert body["p95_latency_ms"] > 140.0

    history_resp = client.get("/targets/payments-gateway/history", params={"limit": 3})
    assert history_resp.status_code == 200
    history = history_resp.json()
    assert len(history) == 3
    assert history[0]["status_code"] == 503

    del_resp = client.delete("/targets/payments-gateway")
    assert del_resp.status_code == 204
    assert client.get("/targets/payments-gateway/summary").status_code == 404
    assert client.delete("/targets/payments-gateway").status_code == 404

    with pytest.raises(ValueError, match="positive integer"):
        store.compute_target_summary("payments-gateway", limit=0)


@pytest.mark.asyncio
async def test_concurrent_probe_batch_and_semaphore_validation() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/fail":
            return httpx.Response(502)
        return httpx.Response(200, json={"ok": True})

    targets = [
        TargetConfig(name="svc-ok", url="https://svc.example/ok", retries=0),
        TargetConfig(name="svc-bad", url="https://svc.example/fail", retries=0),
    ]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        outcomes = await probe_targets_concurrently(targets, client=client, max_concurrency=2)

    assert len(outcomes) == 2
    assert outcomes[0].healthy is True
    assert outcomes[1].healthy is False

    with pytest.raises(ValueError, match="max_concurrency must be positive"):
        await probe_targets_concurrently(targets, max_concurrency=0)


def test_list_all_target_summaries_returns_sorted_rollups() -> None:
    store = HealthStore(":memory:")
    client = TestClient(create_app(store))

    for name, url in [("zeta-api", "https://zeta.example/h"), ("alpha-api", "https://alpha.example/h")]:
        assert client.post("/targets", json={"name": name, "url": url}).status_code == 201

    store.record_outcome(
        ProbeOutcome(
            target_name="alpha-api",
            url="https://alpha.example/h",
            checked_at="2026-11-23T12:00:00Z",
            status_code=200,
            latency_ms=19.4,
            healthy=True,
            failure_reason=None,
            attempt_count=1,
        )
    )

    resp = client.get("/targets/summary")
    assert resp.status_code == 200
    body = resp.json()
    assert [row["target_name"] for row in body] == ["alpha-api", "zeta-api"]
    assert body[0]["availability_pct"] == 100.0
    assert body[1]["total_checks"] == 0


@pytest.mark.asyncio
async def test_non_json_payload_preserves_status_and_get_single_target() -> None:
    store = HealthStore(":memory:")
    client = TestClient(create_app(store))

    assert client.post(
        "/targets",
        json={"name": "web-root", "url": "https://example.org/html", "required_json_key": "ok", "retries": 0},
    ).status_code == 201
    assert client.get("/targets/web-root").json()["name"] == "web-root"
    assert client.get("/targets/missing").status_code == 404

    transport = httpx.MockTransport(lambda _: httpx.Response(200, text="<html>not json</html>"))
    async with httpx.AsyncClient(transport=transport) as mock_client:
        outcome = await execute_probe(store.get_target("web-root"), client=mock_client)  # type: ignore[arg-type]

    assert outcome.healthy is False
    assert outcome.status_code == 200
    assert "non-JSON" in (outcome.failure_reason or "")
