from src.db.duckdb_manager import db

alerts_query = """
SELECT
    ds.listing_id,
    lc.title,
    lc.brand,
    lc.asking_price,
    lf.estimated_resale_mid,
    lf.estimated_net_profit,
    ds.deal_score,
    ds.capital_efficiency_score,
    ds.recommended_action
FROM deal_scores ds
LEFT JOIN listings_clean lc
ON ds.listing_id = lc.listing_id
LEFT JOIN listing_features lf
ON ds.listing_id = lf.listing_id
WHERE ds.deal_score >= 55
AND lf.estimated_net_profit >= 25
ORDER BY ds.deal_score DESC
LIMIT 20
"""

print("\n=== STRONG BUY ALERTS ===")
print(db.query(alerts_query))