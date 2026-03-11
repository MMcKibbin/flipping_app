from datetime import datetime

from src.db.duckdb_manager import db
from src.ingest.google_trends_api import GoogleTrendsCollector
from src.features.demand_features import DemandFeatureEngineer
from src.features.demand_score import DemandScorer
from src.ingest.mock_kijiji_adapter import MockKijijiAdapter
from src.clean.normalize_listings import ListingNormalizer
from src.features.listing_features import ListingFeatureBuilder
from src.features.price_features import PriceFeatureBuilder
from src.scoring.opportunity_score import OpportunityScorer


class JobScheduler:
    def load_pending_jobs(self):
        query = """
        SELECT *
        FROM jobs
        WHERE status = 'pending'
        ORDER BY priority ASC, created_at ASC
        """
        return db.query(query)

    def mark_started(self, job_id: str):
        started_at = datetime.utcnow()
        db.execute(f"""
        UPDATE jobs
        SET status = 'running',
            started_at = TIMESTAMP '{started_at}'
        WHERE job_id = '{job_id}'
        """)

    def mark_finished(self, job_id: str):
        finished_at = datetime.utcnow()
        db.execute(f"""
        UPDATE jobs
        SET status = 'completed',
            finished_at = TIMESTAMP '{finished_at}'
        WHERE job_id = '{job_id}'
        """)

    def mark_failed(self, job_id: str, error_message: str):
        finished_at = datetime.utcnow()
        safe_error = error_message.replace("'", "''")
        db.execute(f"""
        UPDATE jobs
        SET status = 'failed',
            finished_at = TIMESTAMP '{finished_at}',
            error_message = '{safe_error}'
        WHERE job_id = '{job_id}'
        """)

    def run_job(self, job_type: str):
        if job_type == "collect_trends":
            GoogleTrendsCollector().run()
        elif job_type == "build_trend_features":
            DemandFeatureEngineer().run()
        elif job_type == "build_demand_scores":
            DemandScorer().run()
        elif job_type == "ingest_mock_listings":
            MockKijijiAdapter().run()
        elif job_type == "normalize_listings":
            ListingNormalizer().run()
        elif job_type == "build_listing_features":
            ListingFeatureBuilder().run()
        elif job_type == "build_price_features":
            PriceFeatureBuilder().run()
        elif job_type == "score_opportunities":
            OpportunityScorer().run()
        else:
            raise ValueError(f"Unknown job type: {job_type}")

    def run(self):
        jobs_df = self.load_pending_jobs()

        if jobs_df.empty:
            print("[NO DATA] No pending jobs found.")
            return

        for _, row in jobs_df.iterrows():
            job_id = row["job_id"]
            job_type = row["job_type"]

            print(f"\n[RUNNING] {job_type} ({job_id})")

            try:
                self.mark_started(job_id)
                self.run_job(job_type)
                self.mark_finished(job_id)
                print(f"[DONE] {job_type}")
            except Exception as e:
                self.mark_failed(job_id, str(e))
                print(f"[FAILED] {job_type}: {e}")
                break