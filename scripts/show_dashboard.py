from src.db.duckdb_manager import db

summary_query = """
SELECT
    COUNT(*) AS total_scored_listings,
    ROUND(AVG(deal_score), 2) AS avg_deal_score,
    ROUND(AVG(capital_efficiency_score), 2) AS avg_capital_efficiency,
    SUM(CASE WHEN recommended_action = 'buy' THEN 1 ELSE 0 END) AS buy_count,
    SUM(CASE WHEN recommended_action = 'watch' THEN 1 ELSE 0 END) AS watch_count,
    SUM(CASE WHEN recommended_action = 'skip' THEN 1 ELSE 0 END) AS skip_count
FROM deal_scores
"""

top_deals_query = """
SELECT
    ds.listing_id,
    lc.title,
    lc.brand,
    lc.asking_price,
    lf.estimated_resale_mid,
    lf.estimated_net_profit,
    lf.liquidity_score,
    ds.deal_score,
    ds.capital_efficiency_score,
    ds.recommended_action,
    ds.recommended_max_buy,
    ds.recommended_list_price
FROM deal_scores ds
LEFT JOIN listings_clean lc
    ON ds.listing_id = lc.listing_id
LEFT JOIN listing_features lf
    ON ds.listing_id = lf.listing_id
ORDER BY ds.deal_score DESC
LIMIT 15
"""

brand_summary_query = """
SELECT
    lc.brand,
    COUNT(*) AS listings,
    ROUND(AVG(ds.deal_score), 2) AS avg_deal_score,
    ROUND(AVG(ds.capital_efficiency_score), 2) AS avg_capital_efficiency
FROM deal_scores ds
LEFT JOIN listings_clean lc
    ON ds.listing_id = lc.listing_id
GROUP BY lc.brand
ORDER BY avg_deal_score DESC NULLS LAST
LIMIT 10
"""

print("\n=== DASHBOARD SUMMARY ===")
print(db.query(summary_query))

print("\n=== TOP 15 DEALS ===")
print(db.query(top_deals_query))

print("\n=== BRAND SUMMARY ===")
print(db.query(brand_summary_query))
