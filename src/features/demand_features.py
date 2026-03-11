import pandas as pd
from datetime import datetime

from src.db.duckdb_manager import db


class DemandFeatureEngineer:
    def __init__(self):
        pass

    def load_raw_trends(self) -> pd.DataFrame:
        query = """
        SELECT
            keyword,
            geo,
            trend_date,
            interest_value,
            is_partial,
            pulled_at
        FROM trend_data_raw
        WHERE is_partial = FALSE OR is_partial IS NULL
        ORDER BY keyword, geo, trend_date
        """
        return db.query(query)

    def compute_features_for_keyword(self, df_keyword: pd.DataFrame) -> dict | None:
        df_keyword = df_keyword.sort_values("trend_date").copy()

        if len(df_keyword) < 3:
            return None

        latest = df_keyword.iloc[-1]
        prev_1 = df_keyword.iloc[-2]
        prev_2 = df_keyword.iloc[-3]

        trend_level = float(latest["interest_value"])

        prev_val = float(prev_1["interest_value"])
        prev2_val = float(prev_2["interest_value"])
        latest_val = float(latest["interest_value"])

        trend_velocity = latest_val - prev_val
        prev_velocity = prev_val - prev2_val
        trend_acceleration = trend_velocity - prev_velocity

        return {
            "keyword": latest["keyword"],
            "geo": latest["geo"],
            "as_of_date": latest["trend_date"],
            "trend_level": trend_level,
            "trend_velocity": trend_velocity,
            "trend_acceleration": trend_acceleration,
            "computed_at": datetime.utcnow(),
        }

    def build_feature_table(self) -> pd.DataFrame:
        raw_df = self.load_raw_trends()

        if raw_df.empty:
            return pd.DataFrame()

        feature_rows = []

        grouped = raw_df.groupby(["keyword", "geo"], dropna=False)

        for (keyword, geo), group_df in grouped:
            feature_row = self.compute_features_for_keyword(group_df)
            if feature_row is not None:
                feature_rows.append(feature_row)

        if not feature_rows:
            return pd.DataFrame()

        return pd.DataFrame(feature_rows)

    def run(self):
        feature_df = self.build_feature_table()

        if feature_df.empty:
            print("[NO DATA] No trend features computed.")
            return

        db.conn.execute("DELETE FROM trend_features")

        db.insert_dataframe(feature_df, "trend_features")
        print(f"[OK] Stored {len(feature_df)} trend feature rows.")