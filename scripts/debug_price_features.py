import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print("\nLISTINGS CLEAN")
print(db.query("""
SELECT listing_id, title, asking_price, brand
FROM listings_clean
LIMIT 10
"""))

print("\nLISTING FEATURES")
print(db.query("""
SELECT listing_id, estimated_resale_mid, estimated_net_profit
FROM listing_features
LIMIT 10
"""))