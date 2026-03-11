from abc import ABC, abstractmethod
from datetime import datetime
import json
import pandas as pd

from src.db.duckdb_manager import db


class BaseListingAdapter(ABC):
    source_name = "base"

    @abstractmethod
    def fetch_raw_records(self) -> list[dict]:
        pass

    def normalize_raw_record(self, record: dict) -> dict:
        return {
            "source": self.source_name,
            "source_listing_id": str(record.get("source_listing_id", "")),
            "search_term": record.get("search_term", ""),
            "search_region": record.get("search_region", ""),
            "scraped_at": record.get("scraped_at", datetime.utcnow()),
            "listing_url": record.get("listing_url", ""),
            "raw_title": record.get("raw_title", ""),
            "raw_description": record.get("raw_description", ""),
            "raw_price_text": record.get("raw_price_text", ""),
            "raw_location_text": record.get("raw_location_text", ""),
            "raw_image_urls": json.dumps(record.get("raw_image_urls", [])),
            "raw_payload": json.dumps(record, default=str),
        }

    def store_raw_records(self, records: list[dict]) -> None:
        if not records:
            print(f"[NO DATA] No raw records for source: {self.source_name}")
            return

        normalized = [self.normalize_raw_record(r) for r in records]
        df = pd.DataFrame(normalized)
        db.insert_dataframe(df, "listings_raw")
        print(f"[OK] Stored {len(df)} raw listing rows for source: {self.source_name}")

    def run(self) -> None:
        records = self.fetch_raw_records()
        self.store_raw_records(records)