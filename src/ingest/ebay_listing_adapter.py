from datetime import datetime

from src.ingest.ebay_api import EbayAPIClient
from src.ingest.listing_ingest_base import BaseListingAdapter


class EbayListingAdapter(BaseListingAdapter):
    source_name = "ebay"

    def __init__(self, search_term: str = "dewalt cordless drill", search_region: str = "canada", limit: int = 20):
        self.search_term = search_term
        self.search_region = search_region
        self.limit = limit
        self.client = EbayAPIClient()

    def fetch_raw_records(self) -> list[dict]:
        data = self.client.search_items(query=self.search_term, limit=self.limit)

        items = data.get("itemSummaries", [])
        results = []

        for item in items:
            price_block = item.get("price", {}) or {}
            item_url = item.get("itemWebUrl")
            item_id = item.get("itemId")
            title = item.get("title")
            condition = item.get("condition")
            location = item.get("itemLocation", {}) or {}

            city = location.get("city")
            state = location.get("stateOrProvince")
            country = location.get("country")

            location_text = ", ".join([x for x in [city, state, country] if x])

            price_value = price_block.get("value")
            currency = price_block.get("currency")
            raw_price_text = f"{price_value} {currency}" if price_value and currency else ""

            results.append({
                "source_listing_id": str(item_id) if item_id else "",
                "search_term": self.search_term,
                "search_region": self.search_region,
                "scraped_at": datetime.utcnow(),
                "listing_url": item_url or "",
                "raw_title": title or "",
                "raw_description": condition or "",
                "raw_price_text": raw_price_text,
                "raw_location_text": location_text,
                "raw_image_urls": [item.get("image", {}).get("imageUrl")] if item.get("image") else [],
            })

        return results