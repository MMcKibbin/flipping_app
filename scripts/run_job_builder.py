import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.jobs.job_builder import JobBuilder

JobBuilder().run()