import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print(db.query("SELECT * FROM listing_features"))