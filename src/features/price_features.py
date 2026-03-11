from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db


BRAND_RESALE_MULTIPLIERS = {
    "dewalt": 1.75,
    "milwaukee": 1.85,
    "makita": 1.70,
    "bosch": 1.55,
    "ridgid": 1.45,
    "ryobi": 1.30,
    "hilti": 2.10,
}


class PriceFeatureBuilder:
    def load_listing_data(self) -> pd.DataFrame:
        query = """
        SELECT
            lc.listing_id,
            lc.title,
            lc.asking_price,
            lc.brand
        FROM listings_clean lc
        ORDER BY lc.listing_id
        """
        return db.query(query)

    def estimate_resale(self, asking_price: float, brand) -> tuple[float, float, float]:
        brand_str = ""
        if brand is not None and not pd.isna(brand):
            brand_str = str(brand).strip().lower()

        multiplier = BRAND_RESALE_MULTIPLIERS.get(brand_str, 1.50)

        est_mid = round(asking_price * multiplier, 2)
        est_low = round(est_mid * 0.90, 2)
        est_high = round(est_mid * 1.10, 2)

        return est_low, est_mid, est_high

    def compute_price_discount_score(self, asking_price: float, est_mid: float) -> float:
        if est_mid <= 0:
            return 0.0

        discount_ratio = (est_mid - asking_price) / est_mid
        score = max(0.0, min(100.0, discount_ratio * 100))
        return round(score, 2)

    def compute_estimated_net_profit(self, asking_price: float, est_mid: float) -> float:
        estimated_fees_and_costs = est_mid * 0.15
        est_profit = est_mid - asking_price - estimated_fees_and_costs
        return round(est_profit, 2)

    def build_price_features(self) -> pd.DataFrame:
        df = self.load_listing_data()

        if df.empty:
            return pd.DataFrame()

        rows = []

        for _, row in df.iterrows():
            listing_id = row["listing_id"]
            asking_price = row["asking_price"]
            brand = row["brand"]

            # Missing price → keep null resale/profit
            if asking_price is None or pd.isna(asking_price):
                rows.append({
                    "listing_id": listing_id,
                    "estimated_resale_low": None,
                    "estimated_resale_mid": None,
                    "estimated_resale_high": None,
                    "price_discount_score": 0.0,
                    "estimated_net_profit": None,
                })
                continue

            asking_price = float(asking_price)

            est_low, est_mid, est_high = self.estimate_resale(asking_price, brand)
            price_discount_score = self.compute_price_discount_score(asking_price, est_mid)
            estimated_net_profit = self.compute_estimated_net_profit(asking_price, est_mid)

            rows.append({
                "listing_id": listing_id,
                "estimated_resale_low": est_low,
                "estimated_resale_mid": est_mid,
                "estimated_resale_high": est_high,
                "price_discount_score": price_discount_score,
                "estimated_net_profit": estimated_net_profit,
            })

        return pd.DataFrame(rows)

    def run(self):
        price_df = self.build_price_features()

        if price_df.empty:
            print("[NO DATA] No price features built.")
            return

        # Reset these columns first so reruns are clean
        db.execute("""
        UPDATE listing_features
        SET
            estimated_resale_low = NULL,
            estimated_resale_mid = NULL,
            estimated_resale_high = NULL,
            price_discount_score = 0.0,
            estimated_net_profit = NULL
        """)

        temp_name = "temp_price_features"
        db.conn.register(temp_name, price_df)

        try:
            db.conn.execute(f"""
            UPDATE listing_features AS lf
            SET
                estimated_resale_low = t.estimated_resale_low,
                estimated_resale_mid = t.estimated_resale_mid,
                estimated_resale_high = t.estimated_resale_high,
                price_discount_score = t.price_discount_score,
                estimated_net_profit = t.estimated_net_profit
            FROM {temp_name} AS t
            WHERE lf.listing_id = t.listing_id
            """)
        finally:
            db.conn.unregister(temp_name)

        print("[OK] Updated listing_features with price intelligence.")
        print(price_df.head(15))