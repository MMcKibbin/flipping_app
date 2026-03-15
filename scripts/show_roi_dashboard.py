from src.db.duckdb_manager import db

summary_query = """
SELECT
    COUNT(*) AS total_listings,
    ROUND(AVG(deal_score),2) AS avg_deal_score,
    ROUND(AVG(capital_efficiency_score),2) AS avg_roi,
    ROUND(SUM(lf.estimated_net_profit),2) AS total_estimated_profit,
    ROUND(SUM(lc.asking_price),2) AS total_capital_required
FROM deal_scores ds
LEFT JOIN listings_clean lc
ON ds.listing_id = lc.listing_id
LEFT JOIN listing_features lf
ON ds.listing_id = lf.listing_id
"""

roi_leaders_query = """
SELECT
    ds.listing_id,
    lc.title,
    lc.brand,
    lc.asking_price,
    lf.estimated_resale_mid,
    lf.estimated_net_profit,
    ds.capital_efficiency_score,
    ds.deal_score
FROM deal_scores ds
LEFT JOIN listings_clean lc
ON ds.listing_id = lc.listing_id
LEFT JOIN listing_features lf
ON ds.listing_id = lf.listing_id
WHERE lf.estimated_net_profit > 0
ORDER BY ds.capital_efficiency_score DESC
LIMIT 10
"""

print("\n=== ROI DASHBOARD ===")
print(db.query(summary_query))

print("\n=== TOP ROI DEALS ===")
print(db.query(roi_leaders_query))