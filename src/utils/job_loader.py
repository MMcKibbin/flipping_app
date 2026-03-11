import json

from src.config import BASE_DIR


JOBS_FILE = BASE_DIR / "config" / "jobs.json"


def load_jobs() -> list[dict]:
    with open(JOBS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("jobs", [])