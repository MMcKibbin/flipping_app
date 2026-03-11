import json
import threading
from datetime import datetime, UTC
from typing import Any

from src.config import DATA_DIR


HEALTH_FILE = DATA_DIR / "source_health.json"
FAILURE_THRESHOLD = 3
HEALTH_LOCK = threading.Lock()


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _load_health_unlocked() -> dict[str, Any]:
    if not HEALTH_FILE.exists():
        return {}

    try:
        return json.loads(HEALTH_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_health_unlocked(health: dict[str, Any]) -> None:
    HEALTH_FILE.write_text(
        json.dumps(health, indent=2),
        encoding="utf-8",
    )


def ensure_source_entry(health: dict[str, Any], source_name: str) -> None:
    if source_name not in health:
        health[source_name] = {
            "status": "unknown",
            "last_run_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error": None,
            "consecutive_failures": 0,
            "total_successes": 0,
            "total_failures": 0,
        }


def mark_success(source_name: str) -> None:
    with HEALTH_LOCK:
        health = _load_health_unlocked()
        ensure_source_entry(health, source_name)

        entry = health[source_name]
        now = _utc_now_iso()

        entry["status"] = "healthy"
        entry["last_run_at"] = now
        entry["last_success_at"] = now
        entry["last_error"] = None
        entry["consecutive_failures"] = 0
        entry["total_successes"] += 1

        _save_health_unlocked(health)


def mark_failure(source_name: str, error_message: str) -> None:
    with HEALTH_LOCK:
        health = _load_health_unlocked()
        ensure_source_entry(health, source_name)

        entry = health[source_name]
        now = _utc_now_iso()

        entry["last_run_at"] = now
        entry["last_failure_at"] = now
        entry["last_error"] = error_message
        entry["consecutive_failures"] += 1
        entry["total_failures"] += 1

        if entry["consecutive_failures"] >= FAILURE_THRESHOLD:
            entry["status"] = "unhealthy"
        else:
            entry["status"] = "degraded"

        _save_health_unlocked(health)


def is_source_healthy(source_name: str) -> bool:
    with HEALTH_LOCK:
        health = _load_health_unlocked()
        entry = health.get(source_name)

        if not entry:
            return True

        return entry.get("status") != "unhealthy"


def get_source_status(source_name: str) -> str:
    with HEALTH_LOCK:
        health = _load_health_unlocked()
        entry = health.get(source_name)

        if not entry:
            return "unknown"

        return entry.get("status", "unknown")