import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.features.demand_score import DemandScorer

scorer = DemandScorer()
scorer.run()