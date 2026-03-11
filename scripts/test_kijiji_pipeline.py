import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print("\nTOTAL RAW LISTINGS")
print(db.query("SELECT COUNT(*) AS count FROM listings_raw"))

print("\nTOTAL CLEAN LISTINGS")
print(db.query("SELECT COUNT(*) AS count FROM listings_clean"))

print("\nCLEAN LISTINGS WITH PRICES")
print(db.query("""
SELECT COUNT(*) AS count
FROM listings_clean
WHERE asking_price IS NOT NULL
"""))

print("\nBRAND DISTRIBUTION")
print(db.query("""
SELECT brand, COUNT(*) AS count
FROM listings_clean
GROUP BY brand
ORDER BY count DESC
"""))

print("\nSAMPLE CLEAN LISTINGS")
print(db.query("""
SELECT listing_id, title, asking_price, brand, city
FROM listings_clean
LIMIT 10
"""))