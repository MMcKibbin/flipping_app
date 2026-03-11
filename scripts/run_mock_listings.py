import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.mock_kijiji_adapter import MockKijijiAdapter

adapter = MockKijijiAdapter()
adapter.run()