import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.ebay_api import EbayAPIClient

client = EbayAPIClient()

token = client.get_access_token()
print("[OK] Got eBay token:", token[:20] + "...")

data = client.search_items("dewalt cordless drill", limit=5)
print("[OK] Search worked.")
print(data.keys())
print(data.get("total"))