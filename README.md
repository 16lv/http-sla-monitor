# HTTP SLA Monitor (http-sla-monitor)

![CI Status](https://github.com/16lv/http-sla-monitor/actions/workflows/ci.yml/badge.svg)

**HTTP SLA Monitor** ($(System.Collections.Hashtable.newRepo)) is an asynchronous HTTP endpoint availability and SLA latency monitoring service built with **Python 3.12**, **FastAPI**, **httpx**, and **SQLite**.

## Why I Built It

Basic uptime scripts only check whether a server returns HTTP 200 and ignore degraded response times or broken JSON payloads. I built HTTP SLA Monitor to verify three SLA conditions on every check—expected HTTP status code, response latency ceiling (`max_latency_ms`), and a required top-level JSON key—while recording historical probe outcomes in SQLite to calculate `p50`, `p95`, and `p99` tail latencies.

## Key Features

- **Async HTTP Probing with Retries (`execute_probe`)**: Uses `httpx.AsyncClient` with per-target timeouts (`timeout_seconds`) and configurable retry attempts (`retries`) before marking a target unhealthy.
- **Contract Validation**: Checks status code equality, SLA latency threshold (`max_latency_ms`), and optional JSON payload keys (`required_json_key`).
- **Linear-Interpolated Percentile Rollups (`compute_target_summary`)**: Queries recent probe history from an indexed SQLite table (`idx_probe_target_time`) and calculates availability percentage alongside `p50`, `p95`, and `p99` response latencies.
- **Offline Deterministic Testing**: Accepts an injected `httpx.AsyncClient` so the `pytest` suite can simulate transient `503` failures, slow responses, and malformed JSON using `httpx.MockTransport` without external network calls.

## REST API Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Returns service health status (`{"service": "upwatch", "status": "ok"}`). |
| `POST` | `/targets` | Registers or updates a `TargetConfig` monitoring contract (`201 Created`). |
| `GET` | `/targets` | Lists all configured monitoring targets ordered by name. |
| `POST` | `/targets/{name}/probe` | Triggers an immediate async probe against target `{name}` and stores the outcome. |
| `GET` | `/targets/{name}/summary` | Computes availability percentage and `p50`/`p95`/`p99` latency over the last 200 checks. |

## Setup & Usage

```bash
pip install -e ".[dev]"
uvicorn src.api_monitor.api:app --reload --port 8000
```

### Example Requests

```bash
# 1. Register a target endpoint contract
curl -X POST http://localhost:8000/targets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "auth-service",
    "url": "https://httpbin.org/json",
    "method": "GET",
    "expected_status": 200,
    "max_latency_ms": 600.0,
    "required_json_key": "slideshow",
    "retries": 1
  }'

# 2. Trigger an on-demand probe
curl -X POST http://localhost:8000/targets/auth-service/probe

# 3. Query SLA availability & latency percentiles
curl http://localhost:8000/targets/auth-service/summary
```

## Running Tests & Linter

```bash
ruff check .
pytest -v
```

## Known Limitations & Future Improvements

- Probes are currently triggered on-demand via `POST /targets/{name}/probe`; adding an `asyncio` background task loop inside FastAPI's `lifespan` context manager would allow periodic scheduled polling.
