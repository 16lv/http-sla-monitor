# http-sla-monitor

Asynchronous HTTP endpoint availability monitor and latency tracker built with **Python 3.12**, **FastAPI**, **HTTPX**, **Pydantic v2**, and **SQLite**.

It checks registered web services on demand, verifies expected status codes, latency thresholds, and optional JSON keys, and records every probe in SQLite to report availability and interpolated `p50` / `p95` / `p99` response speeds.

## Features

- **Strict target rules**: `TargetConfig` normalises names and optional JSON keys, rejects `HEAD` targets configured to parse a JSON body, and checks retry and timeout bounds.
- **Retry-capable async probes**: `execute_probe()` times requests with `time.perf_counter()`, retries transient HTTP errors or timeouts up to the configured budget, and accepts a custom `httpx.AsyncClient` for offline `MockTransport` testing.
- **Latency percentiles & history**: `HealthStore` tracks recent checks in SQLite (with WAL mode enabled on disk) and computes availability plus `p50`, `p95`, and `p99` latencies without loading unbounded history.
- **Lazy database opening**: Importing the FastAPI module does not create `.db` files until a request arrives; configure storage location via `HTTP_SLA_DB_PATH`.

## Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Service status check (`{"service": "http-sla-monitor", "status": "ok"}`) |
| `POST` | `/targets` | Register or update a monitored endpoint (`201`) |
| `GET` | `/targets` | List registered targets sorted by name |
| `DELETE` | `/targets/{name}` | Remove a target and its stored check history (`204`) |
| `POST` | `/targets/{name}/probe` | Run an on-demand probe and persist the result |
| `GET` | `/targets/{name}/history` | Fetch recent probe checks (`?limit=20`) |
| `GET` | `/targets/{name}/summary` | Return availability percentage and latency percentiles (`?limit=200`) |

## Usage Example

```bash
uvicorn src.api_monitor.api:app --reload
```

```bash
curl -X POST http://127.0.0.1:8000/targets \
  -H "Content-Type: application/json" \
  -d '{"name":"api-health","url":"https://example.com/health","expected_status":200,"max_latency_ms":500}'

curl -X POST http://127.0.0.1:8000/targets/api-health/probe
curl "http://127.0.0.1:8000/targets/api-health/summary?limit=100"
```

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest -v
```
