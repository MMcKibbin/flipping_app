import json
from datetime import datetime, UTC

from src.config import BASE_DIR, RAW_DIR


KEYWORDS_FILE = BASE_DIR / "config" / "demand_keywords.json"
GOOGLE_RAW_DIR = RAW_DIR / "google_trends"
GOOGLE_RAW_DIR.mkdir(parents=True, exist_ok=True)


def load_demand_keywords() -> list[str]:
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("keywords", [])


def fetch_google_demand_mock(
    keywords: list[str],
    region: str = "CA",
) -> list[dict]:
    now = datetime.now(UTC).isoformat()

    records: list[dict] = []

    for i, keyword in enumerate(keywords, start=1):
        record = {
            "source_platform": "google_trends",
            "keyword": keyword,
            "region": region,
            "observed_at": now,
            "interest_score": max(10, 100 - i * 7),
            "trend_direction": "up" if i % 2 == 0 else "flat",
            "raw_payload_json": {
                "keyword": keyword,
                "region": region,
                "mock": True,
            },
        }
        records.append(record)

    return records