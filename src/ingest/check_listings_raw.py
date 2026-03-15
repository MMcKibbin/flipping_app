import duckdb
from src.config import DB_PATH

con = duckdb.connect(str(DB_PATH))

print("\nLISTINGS_RAW BY SOURCE\n")

rows = con.execute("""
SELECT
    source,
    COUNT(*) as count
FROM listings_raw
GROUP BY source
ORDER BY count DESC
""").fetchall()

for r in rows:
    print(r)

con.close()