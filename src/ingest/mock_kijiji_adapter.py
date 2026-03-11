from datetime import datetime
from src.ingest.listing_ingest_base import BaseListingAdapter


class MockKijijiAdapter(BaseListingAdapter):
    source_name = "kijiji"

    def fetch_raw_records(self) -> list[dict]:
        return [
            {
                "source_listing_id": "kijiji_001",
                "search_term": "cordless drill",
                "search_region": "Calgary",
                "scraped_at": datetime.utcnow(),
                "listing_url": "https://example.com/kijiji/001",
                "raw_title": "DeWalt cordless drill with battery",
                "raw_description": "Used drill in good condition. Charger included.",
                "raw_price_text": "$65",
                "raw_location_text": "Calgary, AB",
                "raw_image_urls": ["https://example.com/image1.jpg"],
            },
            {
                "source_listing_id": "kijiji_002",
                "search_term": "cordless drill",
                "search_region": "Calgary",
                "scraped_at": datetime.utcnow(),
                "listing_url": "https://example.com/kijiji/002",
                "raw_title": "Milwaukee drill set",
                "raw_description": "Comes with case and battery.",
                "raw_price_text": "$90",
                "raw_location_text": "Airdrie, AB",
                "raw_image_urls": ["https://example.com/image2.jpg"],
            },
        ]