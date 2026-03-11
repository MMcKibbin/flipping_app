import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.scoring.opportunity_score import OpportunityScorer

scorer = OpportunityScorer()
scorer.run()