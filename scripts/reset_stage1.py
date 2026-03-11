import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

db.reset_stage1_tables()
print("[OK] Stage 1 tables reset.")