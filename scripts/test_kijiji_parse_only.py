import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.kijiji_adapter import KijijiAdapter

adapter = KijijiAdapter(search_term="cordless drill", search_region="calgary")
records = adapter.fetch_raw_records()

print(f"\nTOTAL RECORDS: {len(records)}\n")

for row in records[:10]:
    print({
        "title": row["raw_title"],
        "price": row["raw_price_text"],
        "location": row["raw_location_text"],
        "url": row["listing_url"],
    })