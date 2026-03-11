import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print("\nDATABASE TABLES")
print(db.show_tables())

print("\nROW COUNTS")
for table in ["trend_data_raw", "trend_features", "demand_scores", "listings_raw", "listings_clean"]:
    if db.table_exists(table):
        print(f"{table}: {db.get_row_count(table)}")
    else:
        print(f"{table}: MISSING")