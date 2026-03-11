import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.db.duckdb_manager import db

print(db.query("""
SELECT
    job_type,
    status,
    priority,
    started_at,
    finished_at,
    error_message
FROM jobs
ORDER BY priority
"""))