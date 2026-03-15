import duckdb
from src.config import DB_PATH

con = duckdb.connect(str(DB_PATH))

rows = con.execute("""
    SELECT
        source_platform,
        search_term,
        listing_id,
        title,
        price,
        location_city,
        listing_url
    FROM raw_listings
    WHERE source_platform = 'facebook_marketplace'
    ORDER BY observed_at DESC
    LIMIT 10
""").fetchall()

for row in rows:
    print(row)

con.close()