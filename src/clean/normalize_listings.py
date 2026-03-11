import json
import re
import hashlib
from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db


BRANDS = [
    "dewalt",
    "milwaukee",
    "makita",
    "bosch",
    "ridgid",
    "ryobi",
    "hilti",
]


class ListingNormalizer:
    def load_raw_listings(self) -> pd.DataFrame:
        query = """
        SELECT
            source,
            source_listing_id,
            search_term,
            search_region,
            scraped_at,
            listing_url,
            raw_title,
            raw_description,
            raw_price_text,
            raw_location_text,
            raw_image_urls,
            raw_payload
        FROM listings_raw
        """
        return db.query(query)

    def parse_price(self, raw_price_text: str) -> float | None:
        if not raw_price_text:
            return None

        raw_price_text = str(raw_price_text).strip()

        if raw_price_text.lower() in {"contact", "please contact"}:
            return None

        cleaned = re.sub(r"[^0-9.]", "", raw_price_text)
        if not cleaned:
            return None

        try:
            return float(cleaned)
        except ValueError:
            return None

    def extract_brand(self, text: str) -> str | None:
        text_lower = text.lower()
        for brand in BRANDS:
            if brand in text_lower:
                return brand
        return None

    def parse_location(self, raw_location_text: str) -> tuple[str | None, str | None, str]:
        if not raw_location_text:
            return None, None, "CA"

        raw_location_text = str(raw_location_text).strip()

        parts = [p.strip() for p in raw_location_text.split(",") if p.strip()]

        if len(parts) >= 2:
            city = parts[0]
            region = parts[1]
            return city, region, "CA"

        if len(parts) == 1:
            city = parts[0]
            return city, "AB", "CA"

        return None, None, "CA"

    def make_dedupe_key(self, source: str, title: str, price: float | None, city: str | None) -> str:
        base = f"{source}|{title.strip().lower()}|{price}|{city}"
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    def compute_data_quality_score(
        self,
        title: str,
        description: str,
        price: float | None,
        image_count: int,
    ) -> float:
        score = 0.0

        if title:
            score += 0.35
        if description:
            score += 0.25
        if price is not None:
            score += 0.25
        if image_count > 0:
            score += 0.15

        return round(score, 2)

    def normalize_row(self, row) -> dict:
        title = (row["raw_title"] or "").strip()
        description = (row["raw_description"] or "").strip()
        price = self.parse_price(row["raw_price_text"] or "")
        brand = self.extract_brand(f"{title} {description}")
        city, region, country = self.parse_location(row["raw_location_text"] or "")

        image_urls = json.loads(row["raw_image_urls"]) if row["raw_image_urls"] else []
        image_count = len(image_urls)

        dedupe_key = self.make_dedupe_key(
            source=row["source"],
            title=title,
            price=price,
            city=city,
        )

        data_quality_score = self.compute_data_quality_score(
            title=title,
            description=description,
            price=price,
            image_count=image_count,
        )

        listing_id = f'{row["source"]}_{row["source_listing_id"]}'

        return {
            "listing_id": listing_id,
            "source": row["source"],
            "source_listing_id": row["source_listing_id"],
            "listing_url": row["listing_url"],
            "scraped_at": row["scraped_at"],
            "last_seen_at": row["scraped_at"],
            "title": title,
            "description": description,
            "asking_price": price,
            "currency": "CAD",
            "category": "power_tools",
            "subcategory": "drills",
            "brand": brand,
            "model": None,
            "condition": None,
            "seller_type": "unknown",
            "city": city,
            "region": region,
            "country": country,
            "lat": None,
            "lon": None,
            "image_count": image_count,
            "image_urls": json.dumps(image_urls),
            "listing_status": "active",
            "dedupe_key": dedupe_key,
            "data_quality_score": data_quality_score,
        }

    def run(self):
        raw_df = self.load_raw_listings()

        if raw_df.empty:
            print("[NO DATA] No raw listings to normalize.")
            return

        cleaned_rows = [self.normalize_row(row) for _, row in raw_df.iterrows()]
        cleaned_df = pd.DataFrame(cleaned_rows)
        cleaned_df = cleaned_df.drop_duplicates(subset=["listing_id"], keep="last")

        db.truncate_table("listings_clean")
        db.insert_dataframe(cleaned_df, "listings_clean")

        print(f"[OK] Stored {len(cleaned_df)} cleaned listing rows.")
        print(cleaned_df[[
            "listing_id",
            "title",
            "asking_price",
            "brand",
            "city",
            "region",
            "image_count",
            "data_quality_score"
        ]])