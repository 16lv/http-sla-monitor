from __future__ import annotations

import argparse
import asyncio
import json
import math
import sqlite3
import threading
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

import httpx
from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class TargetConfig(BaseModel):
    name: str = Field(..., min_length=2, max_length=80)
    url: HttpUrl
    method: str = Field(default="GET", pattern="^(GET|HEAD|POST)$")
    expected_status: int = Field(default=200, ge=100, le=599)
    max_latency_ms: float = Field(default=800.0, gt=0.0)
    required_json_key: str | None = None
    timeout_seconds: float = Field(default=5.0, gt=0.1, le=30.0)
    retries: int = Field(default=1, ge=0, le=5)

    @field_validator("name")
    @classmethod
    def normalise_name(cls, value: str) -> str:
        cleaned = value.strip()
        if len(cleaned) < 2:
            raise ValueError("Target name must contain at least 2 non-whitespace characters.")
        return cleaned

    @field_validator("required_json_key")
    @classmethod
    def normalise_json_key(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @model_validator(mode="after")
    def check_head_body_contract(self) -> Self:
        if self.method == "HEAD" and self.required_json_key is not None:
            raise ValueError("HEAD requests have no response body; omit required_json_key.")
        return self


@dataclass(frozen=True)
class ProbeOutcome:
    target_name: str
    url: str
    checked_at: str
    status_code: int | None
    latency_ms: float
    healthy: bool
    failure_reason: str | None
    attempt_count: int


class HealthStore:
    """Thread-safe SQLite repository for monitored endpoints and probe history."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if self.db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("PRAGMA busy_timeout = 5000;")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS targets (
                    name TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    expected_status INTEGER NOT NULL,
                    max_latency_ms REAL NOT NULL,
                    required_json_key TEXT,
                    timeout_seconds REAL NOT NULL,
                    retries INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS probe_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    checked_at TEXT NOT NULL,
                    status_code INTEGER,
                    latency_ms REAL NOT NULL,
                    healthy INTEGER NOT NULL,
                    failure_reason TEXT,
                    attempt_count INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_probe_target_time
                    ON probe_history(target_name, checked_at DESC);
                """
            )
            self._conn.commit()

    @staticmethod
    def _row_to_target(row: sqlite3.Row) -> TargetConfig:
        return TargetConfig(
            name=row["name"],
            url=row["url"],
            method=row["method"],
            expected_status=row["expected_status"],
            max_latency_ms=row["max_latency_ms"],
            required_json_key=row["required_json_key"],
            timeout_seconds=row["timeout_seconds"],
            retries=row["retries"],
        )

    def upsert_target(self, target: TargetConfig) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO targets (
                    name, url, method, expected_status, max_latency_ms,
                    required_json_key, timeout_seconds, retries
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    url = excluded.url,
                    method = excluded.method,
                    expected_status = excluded.expected_status,
                    max_latency_ms = excluded.max_latency_ms,
                    required_json_key = excluded.required_json_key,
                    timeout_seconds = excluded.timeout_seconds,
                    retries = excluded.retries
                """,
                (
                    target.name,
                    str(target.url),
                    target.method,
                    target.expected_status,
                    target.max_latency_ms,
                    target.required_json_key,
                    target.timeout_seconds,
                    target.retries,
                ),
            )
            self._conn.commit()

    def get_target(self, name: str) -> TargetConfig | None:
        clean_name = name.strip()
        if not clean_name:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM targets WHERE name = ?", (clean_name,)
            ).fetchone()
            return self._row_to_target(row) if row is not None else None

    def list_targets(self) -> list[TargetConfig]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM targets ORDER BY name ASC").fetchall()
            return [self._row_to_target(row) for row in rows]

    def delete_target(self, name: str) -> bool:
        clean_name = name.strip()
        if not clean_name:
            return False
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM targets WHERE name = ?", (clean_name,)
            )
            if cur.rowcount == 0:
                self._conn.commit()
                return False
            self._conn.execute(
                "DELETE FROM probe_history WHERE target_name = ?", (clean_name,)
            )
            self._conn.commit()
            return True

    def record_outcome(self, outcome: ProbeOutcome) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO probe_history (
                    target_name, url, checked_at, status_code,
                    latency_ms, healthy, failure_reason, attempt_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome.target_name,
                    outcome.url,
                    outcome.checked_at,
                    outcome.status_code,
                    outcome.latency_ms,
                    1 if outcome.healthy else 0,
                    outcome.failure_reason,
                    outcome.attempt_count,
                ),
            )
            self._conn.commit()

    def list_history(self, target_name: str, limit: int = 20) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("limit must be a positive integer.")
        clean_name = target_name.strip()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, target_name, url, checked_at, status_code,
                       latency_ms, healthy, failure_reason, attempt_count
                FROM probe_history
                WHERE target_name = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (clean_name, limit),
            ).fetchall()
        return [
            {
                "id": int(r["id"]),
                "target_name": r["target_name"],
                "url": r["url"],
                "checked_at": r["checked_at"],
                "status_code": r["status_code"],
                "latency_ms": float(r["latency_ms"]),
                "healthy": bool(r["healthy"]),
                "failure_reason": r["failure_reason"],
                "attempt_count": int(r["attempt_count"]),
            }
            for r in rows
        ]

    def compute_target_summary(self, target_name: str, limit: int = 200) -> dict[str, Any]:
        if limit <= 0:
            raise ValueError("limit must be a positive integer.")
        clean_name = target_name.strip()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT status_code, latency_ms, healthy, failure_reason, checked_at
                FROM probe_history
                WHERE target_name = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (clean_name, limit),
            ).fetchall()

        if not rows:
            return {
                "target_name": clean_name,
                "total_checks": 0,
                "availability_pct": 0.0,
                "p50_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "last_checked_at": None,
                "last_healthy": None,
                "last_failure_reason": None,
            }

        latencies = sorted(float(r["latency_ms"]) for r in rows)
        healthy_count = sum(int(r["healthy"]) for r in rows)
        total = len(rows)
        latest = rows[0]

        return {
            "target_name": clean_name,
            "total_checks": total,
            "availability_pct": round((healthy_count / total) * 100.0, 2),
            "p50_latency_ms": round(_percentile(latencies, 50), 2),
            "p95_latency_ms": round(_percentile(latencies, 95), 2),
            "p99_latency_ms": round(_percentile(latencies, 99), 2),
            "last_checked_at": latest["checked_at"],
            "last_healthy": bool(latest["healthy"]),
            "last_failure_reason": latest["failure_reason"],
        }


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


async def execute_probe(
    target: TargetConfig,
    client: httpx.AsyncClient | None = None,
) -> ProbeOutcome:
    own_client = client is None
    active_client = client or httpx.AsyncClient()
    attempts = 0
    max_attempts = target.retries + 1
    last_reason: str | None = None
    last_status: int | None = None
    elapsed_ms = 0.0

    try:
        while attempts < max_attempts:
            attempts += 1
            t0 = time.perf_counter()
            try:
                response = await active_client.request(
                    method=target.method,
                    url=str(target.url),
                    timeout=target.timeout_seconds,
                )
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                last_status = response.status_code

                if response.status_code != target.expected_status:
                    last_reason = (
                        f"Expected HTTP {target.expected_status}, received {response.status_code}"
                    )
                    continue

                if elapsed_ms > target.max_latency_ms:
                    last_reason = (
                        f"Latency {elapsed_ms:.1f}ms exceeded SLA ceiling {target.max_latency_ms:.1f}ms"
                    )
                    continue

                if target.required_json_key:
                    payload = response.json()
                    if not isinstance(payload, dict) or target.required_json_key not in payload:
                        last_reason = (
                            f"Missing required JSON key '{target.required_json_key}' in response"
                        )
                        continue

                return ProbeOutcome(
                    target_name=target.name,
                    url=str(target.url),
                    checked_at=datetime.now(UTC).isoformat(),
                    status_code=response.status_code,
                    latency_ms=round(elapsed_ms, 2),
                    healthy=True,
                    failure_reason=None,
                    attempt_count=attempts,
                )
            except (httpx.HTTPError, ValueError) as exc:
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                last_status = None
                last_reason = f"{type(exc).__name__}: {exc}"

        return ProbeOutcome(
            target_name=target.name,
            url=str(target.url),
            checked_at=datetime.now(UTC).isoformat(),
            status_code=last_status,
            latency_ms=round(elapsed_ms, 2),
            healthy=False,
            failure_reason=last_reason or "Probe failed after retries",
            attempt_count=attempts,
        )
    finally:
        if own_client:
            await active_client.aclose()


async def probe_targets_concurrently(
    targets: Sequence[TargetConfig],
    client: httpx.AsyncClient | None = None,
    max_concurrency: int = 8,
) -> list[ProbeOutcome]:
    if max_concurrency <= 0:
        raise ValueError("max_concurrency must be positive.")
    if not targets:
        return []

    own_client = client is None
    active_client = client or httpx.AsyncClient()
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _run(target: TargetConfig) -> ProbeOutcome:
        async with semaphore:
            return await execute_probe(target, client=active_client)

    try:
        return list(await asyncio.gather(*(_run(t) for t in targets)))
    finally:
        if own_client:
            await active_client.aclose()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run an HTTP SLA probe against an endpoint and print structured results."
    )
    parser.add_argument("url", help="Target HTTP/HTTPS URL to probe")
    parser.add_argument("--name", default="cli-probe", help="Target label (default: cli-probe)")
    parser.add_argument("--method", default="GET", choices=("GET", "HEAD", "POST"))
    parser.add_argument("--expected-status", type=int, default=200)
    parser.add_argument("--max-latency-ms", type=float, default=800.0)
    parser.add_argument("--required-key", default=None, help="Optional JSON object key to verify")
    parser.add_argument("--retries", type=int, default=1)
    args = parser.parse_args(argv)

    target = TargetConfig(
        name=args.name,
        url=args.url,
        method=args.method,
        expected_status=args.expected_status,
        max_latency_ms=args.max_latency_ms,
        required_json_key=args.required_key,
        retries=args.retries,
    )
    outcome = asyncio.run(execute_probe(target))
    print(json.dumps(asdict(outcome), indent=2))
    return 0 if outcome.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
