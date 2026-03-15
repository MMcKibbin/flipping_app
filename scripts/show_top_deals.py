import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import duckdb
from src.config import DB_PATH

con = duckdb.connect(str(DB_PATH))

rows = con.execute("""
SELECT
    ds.listing_id,
    lc.clean_title,
    lc.price,
    ds.recommended_list_price,
    ds.adjusted_profit,
    ds.deal_score,
    ds.recommended_action
FROM deal_scores ds
JOIN listings_clean lc
    ON ds.listing_id = lc.listing_id
ORDER BY ds.deal_score DESC
LIMIT 20
""").fetchall()

print("\nTOP DEALS\n")

for r in rows:
    print(r)

con.close()