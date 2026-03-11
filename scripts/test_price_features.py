import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print(db.query("""
SELECT
    listing_id,
    demand_score,
    price_discount_score,
    estimated_resale_low,
    estimated_resale_mid,
    estimated_resale_high,
    estimated_net_profit
FROM listing_features
"""))