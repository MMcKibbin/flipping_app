from datetime import datetime, UTC
import duckdb
import pandas as pd

from src.config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def initialize_feature_table() -> None:
    con = get_connection()

    con.execute("DROP TABLE IF EXISTS listing_features")

    con.execute(
        """
        CREATE TABLE listing_features (
            listing_id VARCHAR,
            observed_at TIMESTAMP,
            search_term VARCHAR,

            clean_title VARCHAR,
            brand VARCHAR,
            model VARCHAR,
            category VARCHAR,

            price DOUBLE,
            estimated_market_price DOUBLE,
            discount_vs_market DOUBLE,
            price_percentile DOUBLE,
            sample_size INTEGER,

            location_city VARCHAR,
            location_state_province VARCHAR,
            quality_flag VARCHAR,

            demand_score DOUBLE,
            liquidity_score DOUBLE,
            estimated_resale_low DOUBLE,
            estimated_resale_mid DOUBLE,
            estimated_resale_high DOUBLE,
            estimated_net_profit DOUBLE,

            computed_at TIMESTAMP
        )
        """
    )

    con.close()


class ListingFeatureBuilder:
    def load_cleaned_data(self) -> pd.DataFrame:
        con = get_connection()

        query = """
        SELECT
            listing_id,
            observed_at,
            search_term,
            clean_title,
            brand,
            model,
            category,
            price,
            location_city,
            location_state_province,
            quality_flag
        FROM listings_clean
        """

        df = con.execute(query).df()
        con.close()
        return df

    def compute_market_reference(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        First-pass market reference:
        group by brand + category + province.
        This is still broad, but works for Phase 1.
        """
        ref = (
            df.groupby(
                ["brand", "category", "location_state_province"],
                dropna=False
            )["price"]
            .agg(
                estimated_market_price="median",
                sample_size="count"
            )
            .reset_index()
        )

        return ref

    def attach_market_features(self, df: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
        merged = df.merge(
            ref,
            on=["brand", "category", "location_state_province"],
            how="left"
        )

        def calc_discount(row):
            market = row["estimated_market_price"]
            price = row["price"]

            if pd.isna(market) or market in [0, None]:
                return None
            if pd.isna(price):
                return None

            return round((market - price) / market, 4)

        merged["discount_vs_market"] = merged.apply(calc_discount, axis=1)

        def calc_percentile(row, full_df):
            subset = full_df[
                (full_df["brand"] == row["brand"]) &
                (full_df["category"] == row["category"]) &
                (full_df["location_state_province"] == row["location_state_province"])
            ]["price"].dropna()

            if subset.empty or pd.isna(row["price"]):
                return None

            pct = (subset <= row["price"]).mean()
            return round(float(pct), 4)

        merged["price_percentile"] = merged.apply(
            lambda row: calc_percentile(row, df),
            axis=1
        )

        return merged

    def compute_demand_score(self, row) -> float:
        title = str(row["clean_title"]).lower() if pd.notna(row["clean_title"]) else ""
        brand = str(row["brand"]).lower() if pd.notna(row["brand"]) else ""

        score = 50.0

        if brand in ["milwaukee", "dewalt", "makita", "hilti", "bosch"]:
            score += 10.0

        if "kit" in title or "combo" in title:
            score += 8.0

        if "fuel" in title or "brushless" in title:
            score += 6.0

        if "m18" in title or "m12" in title:
            score += 5.0

        if "new" in title or "sealed" in title or "brand new" in title:
            score += 4.0

        return round(min(score, 100.0), 2)

    def compute_liquidity_score(self, row) -> float:
        """
        Simple Phase-1 liquidity heuristic.
        """
        title = str(row["clean_title"]).lower() if pd.notna(row["clean_title"]) else ""
        brand = str(row["brand"]).lower() if pd.notna(row["brand"]) else ""
        discount = row["discount_vs_market"]

        score = 40.0

        if brand in ["milwaukee", "dewalt", "makita", "hilti"]:
            score += 15.0

        if "drill" in title or "impact" in title or "hammer" in title:
            score += 10.0

        if discount is not None and pd.notna(discount):
            if discount >= 0.30:
                score += 20.0
            elif discount >= 0.15:
                score += 10.0

        return round(min(score, 100.0), 2)

    def build_features(self) -> pd.DataFrame:
        df = self.load_cleaned_data()

        if df.empty:
            return pd.DataFrame()

        ref = self.compute_market_reference(df)
        feature_df = self.attach_market_features(df, ref)

        feature_df["demand_score"] = feature_df.apply(self.compute_demand_score, axis=1)
        feature_df["liquidity_score"] = feature_df.apply(self.compute_liquidity_score, axis=1)

        feature_df["estimated_resale_mid"] = feature_df["estimated_market_price"]
        feature_df["estimated_resale_low"] = feature_df["estimated_market_price"] * 0.90
        feature_df["estimated_resale_high"] = feature_df["estimated_market_price"] * 1.10

        feature_df["estimated_net_profit"] = (
            feature_df["estimated_resale_mid"] - feature_df["price"]
        )

        feature_df["computed_at"] = datetime.now(UTC)

        return feature_df[
            [
                "listing_id",
                "observed_at",
                "search_term",
                "clean_title",
                "brand",
                "model",
                "category",
                "price",
                "estimated_market_price",
                "discount_vs_market",
                "price_percentile",
                "sample_size",
                "location_city",
                "location_state_province",
                "quality_flag",
                "demand_score",
                "liquidity_score",
                "estimated_resale_low",
                "estimated_resale_mid",
                "estimated_resale_high",
                "estimated_net_profit",
                "computed_at",
            ]
        ]

    def run(self) -> None:
        initialize_feature_table()
        feature_df = self.build_features()

        if feature_df.empty:
            print("[NO DATA] No listing features built.")
            return

        con = get_connection()
        con.register("feature_view", feature_df)

        con.execute(
            """
            INSERT INTO listing_features
            SELECT
                listing_id,
                observed_at,
                search_term,
                clean_title,
                brand,
                model,
                category,
                price,
                estimated_market_price,
                discount_vs_market,
                price_percentile,
                sample_size,
                location_city,
                location_state_province,
                quality_flag,
                demand_score,
                liquidity_score,
                estimated_resale_low,
                estimated_resale_mid,
                estimated_resale_high,
                estimated_net_profit,
                computed_at
            FROM feature_view
            """
        )

        preview = con.execute(
            """
            SELECT
                listing_id,
                clean_title,
                brand,
                price,
                estimated_market_price,
                discount_vs_market,
                demand_score,
                liquidity_score,
                estimated_net_profit
            FROM listing_features
            ORDER BY discount_vs_market DESC NULLS LAST
            LIMIT 20
            """
        ).fetchall()

        con.close()

        print(f"[OK] Stored {len(feature_df)} listing feature rows.")
        for row in preview:
            print(row)


if __name__ == "__main__":
    builder = ListingFeatureBuilder()
    builder.run()