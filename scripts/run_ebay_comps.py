import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.ebay_comps_ingest import EbayCompsIngestor

ingestor = EbayCompsIngestor(keyword="dewalt cordless drill", limit=10)
ingestor.run()