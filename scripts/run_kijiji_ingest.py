import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.kijiji_adapter import KijijiAdapter

adapter = KijijiAdapter(search_term="cordless drill", search_region="calgary")
adapter.run()