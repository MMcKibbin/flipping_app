import pandas as pd
from datetime import datetime

from src.db.duckdb_manager import db


class DemandScorer:

    def load_trend_features(self) -> pd.DataFrame:

        query = """
        SELECT
            keyword,
            geo,
            as_of_date,
            trend_level,
            trend_velocity,
            trend_acceleration
        FROM trend_features
        """

        return db.query(query)

    def compute_score(self, row):

        level = float(row["trend_level"])
        velocity = float(row["trend_velocity"])
        acceleration = float(row["trend_acceleration"])

        score = (
            0.6 * level +
            0.25 * max(0, velocity) * 10 +
            0.15 * max(0, acceleration) * 10
        )

        return round(score, 2)

    def run(self):

        df = self.load_trend_features()

        if df.empty:
            print("[NO DATA] No trend features found.")
            return

        df["demand_score"] = df.apply(self.compute_score, axis=1)
        df["computed_at"] = datetime.utcnow()

        output = df[[
            "keyword",
            "geo",
            "as_of_date",
            "demand_score",
            "computed_at"
        ]]

        db.conn.execute("DELETE FROM demand_scores")

        db.insert_dataframe(output, "demand_scores")

        print("\n[OK] Demand scores stored:")
        print(output)