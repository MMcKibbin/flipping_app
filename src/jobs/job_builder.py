import json
import uuid
from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db


class JobBuilder:
    def __init__(self):
        self.now = datetime.utcnow()

    def make_job(
        self,
        job_type: str,
        job_target: str = "",
        job_scope: str = "",
        priority: int = 100,
        payload: dict | None = None,
    ) -> dict:
        return {
            "job_id": str(uuid.uuid4()),
            "job_type": job_type,
            "job_target": job_target,
            "job_scope": job_scope,
            "status": "pending",
            "priority": priority,
            "payload": json.dumps(payload or {}),
            "scheduled_at": self.now,
            "started_at": None,
            "finished_at": None,
            "error_message": None,
            "created_at": self.now,
        }

    def build_stage1_pipeline_jobs(self) -> pd.DataFrame:
        jobs = [
            self.make_job("collect_trends", priority=10),
            self.make_job("build_trend_features", priority=20),
            self.make_job("build_demand_scores", priority=30),
            self.make_job("ingest_mock_listings", priority=40),
            self.make_job("normalize_listings", priority=50),
            self.make_job("build_listing_features", priority=60),
            self.make_job("build_price_features", priority=70),
            self.make_job("score_opportunities", priority=80),
        ]
        return pd.DataFrame(jobs)

    def run(self):
        job_df = self.build_stage1_pipeline_jobs()

        if job_df.empty:
            print("[NO DATA] No jobs built.")
            return

        db.truncate_table("jobs")
        db.insert_dataframe(job_df, "jobs")
        print(f"[OK] Stored {len(job_df)} jobs.")
        print(job_df[["job_type", "priority", "status"]])