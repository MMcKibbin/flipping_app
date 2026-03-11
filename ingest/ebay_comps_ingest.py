from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db
from src.ingest.ebay_api import EbayAPIClient


class EbayCompsIngestor:
    def __init__(self, keyword: str = "dewalt cordless drill", limit: int = 10):
        self.keyword = keyword
        self.limit = limit
        self.client = EbayAPIClient()

    def fetch_items(self) -> list[dict]:
        data = self.client.search_items(query=self.keyword, limit=self.limit)

        items = data.get("itemSummaries", [])
        output = []

        for item in items:
            price_block = item.get("price", {}) or {}
            current_price = price_block.get("value")
            currency = price_block.get("currency")

            title = item.get("title")
            item_id = item.get("itemId")
            item_web_url = item.get("itemWebUrl")
            condition = item.get("condition")
            buying_options = ",".join(item.get("buyingOptions", [])) if item.get("buyingOptions") else None
            seller = item.get("seller", {}) or {}
            seller_username = seller.get("username")
            marketplace_id = item.get("marketplaceId")

            output.append({
                "keyword": self.keyword,
                "item_title": title,
                "item_id": item_id,
                "price": float(current_price) if current_price is not None else None,
                "currency": currency,
                "item_web_url": item_web_url,
                "condition": condition,
                "buying_options": buying_options,
                "seller_username": seller_username,
                "marketplace_id": marketplace_id,
                "pulled_at": datetime.utcnow(),
            })

        return output

    def run(self):
        rows = self.fetch_items()

        if not rows:
            print("[NO DATA] No eBay item summaries returned.")
            return

        df = pd.DataFrame(rows)
        db.insert_dataframe(df, "ebay_price_comps")

        print(f"[OK] Stored {len(df)} eBay comp rows.")
        print(df.head(10))