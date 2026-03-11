from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db
from src.ingest.ebay_api import EbayAPIClient


class EbayCompsIngestor:

    def __init__(self, keyword="dewalt cordless drill", limit=10):
        self.keyword = keyword
        self.limit = limit
        self.client = EbayAPIClient()

    def fetch_items(self):

        data = self.client.search_items(query=self.keyword, limit=self.limit)
        print("[DEBUG] eBay raw response keys:", data.keys())
        print("[DEBUG] eBay raw response:", data)

        items = data.get("itemSummaries", [])
        rows = []

        for item in items:

            price_block = item.get("price", {}) or {}

            rows.append({
                "keyword": self.keyword,
                "item_title": item.get("title"),
                "item_id": item.get("itemId"),
                "price": float(price_block.get("value")) if price_block.get("value") else None,
                "currency": price_block.get("currency"),
                "item_web_url": item.get("itemWebUrl"),
                "condition": item.get("condition"),
                "buying_options": ",".join(item.get("buyingOptions", [])),
                "seller_username": (item.get("seller") or {}).get("username"),
                "marketplace_id": item.get("marketplaceId"),
                "pulled_at": datetime.utcnow(),
            })

        return rows

    def run(self):

        rows = self.fetch_items()

        if not rows:
            print("[NO DATA] No eBay comps returned.")
            return

        df = pd.DataFrame(rows)

        db.insert_dataframe(df, "ebay_price_comps")

        print(f"[OK] Stored {len(df)} ebay comp rows.")
        print(df.head())