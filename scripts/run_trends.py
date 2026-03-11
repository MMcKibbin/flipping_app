import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.ingest.google_trends_api import GoogleTrendsCollector

collector = GoogleTrendsCollector()
collector.run()