import duckdb
from src.config import DB_PATH

con = duckdb.connect(str(DB_PATH))

print("\nRAW LISTINGS BY SOURCE\n")

rows = con.execute("""
SELECT
    source_platform,
    COUNT(*) as count
FROM raw_listings
GROUP BY source_platform
ORDER BY count DESC
""").fetchall()

for r in rows:
    print(r)

con.close()