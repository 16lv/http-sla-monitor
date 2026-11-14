# http-sla-monitor

Asynchronous HTTP endpoint health monitor and SLA latency tracker built with **FastAPI**, **HTTPX**, **Pydantic v2**, and **SQLite**.

## Overview

- **Target validation**: Targets declare expected HTTP status codes, request methods (`GET`, `HEAD`, `POST`), retry budgets, timeouts, SLA latency ceilings, and optional JSON payload keys via Pydantic `TargetConfig`.
- **Async probe engine**: `execute_probe()` measures response latency with `time.perf_counter()`, checks HTTP status and SLA bounds, inspects JSON keys when configured, and accepts an injected `httpx.AsyncClient` for offline `httpx.MockTransport` testing.
- **Latency percentiles**: `HealthStore.compute_target_summary()` calculates interpolated `p50`, `p95`, and `p99` latencies alongside availability percentages over indexed probe history.
- **Thread-safe persistence**: Uses `threading.RLock`, WAL mode on file-backed SQLite databases, primary-key target lookups, and a FastAPI lifespan client.

## Structure

```text
http-sla-monitor/
├── src/api_monitor/
│   ├── engine.py       # Probe engine, percentile calculation, thread-safe HealthStore
│   └── api.py          # FastAPI endpoints and AsyncClient lifespan management
├── tests/
│   └── test_monitor.py # MockTransport retry tests and API SLA summary tests
├── pyproject.toml
└── requirements.txt
```

## Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Service liveness check (`{"service": "http-sla-monitor", "status": "ok"}`) |
| `POST` | `/targets` | Register or update a monitored target (`201`) |
| `GET` | `/targets` | List registered targets ordered by name |
| `POST` | `/targets/{name}/probe` | Execute an on-demand probe and persist the outcome |
| `GET` | `/targets/{name}/summary` | Return availability percentage and p50/p95/p99 latency summary |

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest -v
uvicorn src.api_monitor.api:app --reload
```
