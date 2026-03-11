import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.ebay_listing_adapter import EbayListingAdapter

adapter = EbayListingAdapter(search_term="dewalt cordless drill", search_region="canada", limit=20)
adapter.run()