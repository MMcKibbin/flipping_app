import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.features.listing_features import ListingFeatureBuilder

builder = ListingFeatureBuilder()
builder.run()