from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db


class ListingFeatureBuilder:
    def load_joined_data(self) -> pd.DataFrame:
        query = """
        SELECT
            lc.listing_id,
            lc.title,
            lc.asking_price,
            lc.brand,
            lc.city,
            lc.region,
            lr.search_term,
            ds.as_of_date,
            ds.demand_score
        FROM listings_clean lc
        LEFT JOIN listings_raw lr
            ON lc.source = lr.source
           AND lc.source_listing_id = lr.source_listing_id
        LEFT JOIN demand_scores ds
            ON LOWER(lr.search_term) = LOWER(ds.keyword)
        """
        return db.query(query)

    def build_features(self) -> pd.DataFrame:
        df = self.load_joined_data()

        if df.empty:
            return pd.DataFrame()

        output = pd.DataFrame({
            "listing_id": df["listing_id"],
            "as_of_date": df["as_of_date"],
            "demand_score": df["demand_score"].fillna(0.0),
            "price_discount_score": 0.0,
            "estimated_resale_low": None,
            "estimated_resale_mid": None,
            "estimated_resale_high": None,
            "estimated_net_profit": None,
            "estimated_days_to_sell": None,
            "liquidity_score": 0.0,
            "seller_confidence_score": 0.0,
            "seasonality_score": 0.0,
            "photo_confidence_score": 0.0,
            "arbitrage_spread_score": 0.0,
            "computed_at": datetime.utcnow(),
        })

        return output

    def run(self):
        feature_df = self.build_features()

        if feature_df.empty:
            print("[NO DATA] No listing features built.")
            return

        db.truncate_table("listing_features")
        db.insert_dataframe(feature_df, "listing_features")

        print(f"[OK] Stored {len(feature_df)} listing feature rows.")
        print(feature_df[[
            "listing_id",
            "as_of_date",
            "demand_score"
        ]])