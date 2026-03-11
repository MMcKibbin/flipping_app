import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print(db.query("""
SELECT
    listing_id,
    deal_score,
    capital_efficiency_score,
    recommended_action,
    recommended_max_buy,
    recommended_list_price,
    confidence_score
FROM deal_scores
"""))