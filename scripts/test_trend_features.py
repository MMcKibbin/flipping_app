import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print("\nRAW TREND DATA")
print(db.query("SELECT * FROM trend_data_raw LIMIT 10"))

print("\nTREND FEATURES")
print(db.query("SELECT * FROM trend_features LIMIT 10"))