# http-sla-monitor

Asynchronous HTTP endpoint availability monitor, CLI probe tool, and latency tracker built with **Python 3.12**, **FastAPI**, **HTTPX**, **Pydantic v2**, and **SQLite**.

It checks registered web services on demand or in concurrent batches, verifies expected status codes, latency ceilings, and optional JSON keys, and records every probe in SQLite to report availability and interpolated `p50` / `p95` / `p99` response speeds.

## Features

- **Strict target rules**: `TargetConfig` normalises names and optional JSON keys, rejects `HEAD` targets configured to inspect a JSON response body, and enforces retry and timeout bounds.
- **Bounded async batch probing**: `execute_probe()` and `probe_targets_concurrently()` share an `httpx.AsyncClient` guarded by `asyncio.Semaphore`, time requests with `time.perf_counter()`, retry transient failures within budget, and work offline with `httpx.MockTransport`.
- **Latency percentiles and history**: `HealthStore` persists checks in SQLite (`WAL` mode and `busy_timeout=5000` on disk) and computes availability plus `p50`, `p95`, and `p99` latencies from bounded recent slices.
- **Lazy database opening**: Importing the FastAPI module never creates `.db` files until a request arrives; database path overrides via `HTTP_SLA_DB_PATH`.
- **Direct CLI runner**: Run one-off SLA probes directly from the command line without starting the HTTP server.

## Technology Stack

- Python 3.11+ (tested on Python 3.12)
- FastAPI & Uvicorn
- HTTPX (`AsyncClient`)
- Pydantic v2
- SQLite3 (`sqlite3` standard library)
- pytest, pytest-asyncio, Ruff

## Installation

```bash
pip install -e ".[dev]"
```

## Endpoints

| Method | Path | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Service status check (`{"service": "http-sla-monitor", "status": "ok"}`) |
| `POST` | `/targets` | Register or update a monitored endpoint (`201`) |
| `GET` | `/targets` | List registered targets sorted by name |
| `GET` | `/targets/summary` | Return availability and latency rollups across all targets |
| `DELETE` | `/targets/{name}` | Remove a target and its stored check history (`204`) |
| `POST` | `/targets/probe-all` | Probe all registered targets concurrently and persist results |
| `POST` | `/targets/{name}/probe` | Run an on-demand probe for one target |
| `GET` | `/targets/{name}/history` | Fetch recent probe checks (`?limit=20`) |
| `GET` | `/targets/{name}/summary` | Return availability percentage and latency percentiles (`?limit=200`) |

## Usage Examples

Run the API server:

```bash
uvicorn api_monitor.api:app --reload
```

Register a target, run a batch probe, and read its SLA summary:

```bash
curl -X POST http://127.0.0.1:8000/targets \
  -H "Content-Type: application/json" \
  -d '{"name":"api-health","url":"https://example.com/health","expected_status":200,"max_latency_ms":500}'

curl -X POST http://127.0.0.1:8000/targets/probe-all
curl "http://127.0.0.1:8000/targets/api-health/summary?limit=100"
```

Run a quick CLI probe without starting FastAPI:

```bash
http-sla-monitor https://example.com --expected-status 200 --max-latency-ms 500
```

## Technical Implementation Details

- `probe_history` is indexed on `(target_name, checked_at DESC)` and queried in bounded slices ordered by `id DESC`, preventing full-table scans when summaries are calculated.
- Percentiles use linear interpolation between adjacent sorted ranks rather than nearest-rank rounding so small sample counts stay smooth.
- `HealthStore` uses `threading.RLock()` around all SQLite writes and reads so FastAPI worker threads can share a store instance cleanly.

## Project Structure

```text
http-sla-monitor/
├── src/api_monitor/
│   ├── __init__.py
│   ├── api.py
│   └── engine.py
├── tests/
│   └── test_monitor.py
├── pyproject.toml
└── requirements.txt
```

## Testing

```bash
ruff check .
pytest -v
```

## Known Limitations & Future Improvements

- Probes are triggered manually or via `/targets/probe-all`; adding an optional background scheduler (`asyncio` task loop inside FastAPI lifespan with configurable intervals) is the next logical addition.
- Probe history retention is unbounded over long runs; a periodic pruning query (or SQLite trigger keeping the newest `N` samples per target) would keep database size fixed.
