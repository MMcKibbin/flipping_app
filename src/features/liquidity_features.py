import pandas as pd

from src.db.duckdb_manager import db


BRAND_LIQUIDITY = {
    "milwaukee": 0.90,
    "dewalt": 0.88,
    "makita": 0.84,
    "ridgid": 0.74,
    "ryobi": 0.68,
}


class LiquidityFeatureBuilder:
    def load_data(self) -> pd.DataFrame:
        query = """
        SELECT
            lc.listing_id,
            lc.brand,
            lc.asking_price,
            lf.demand_score,
            lf.estimated_resale_mid,
            lf.price_discount_score
        FROM listings_clean lc
        LEFT JOIN listing_features lf
            ON lc.listing_id = lf.listing_id
        """
        return db.query(query)
    def compute_liquidity_score(self, row) -> float:

        demand = row["demand_score"]
        if demand is None or pd.isna(demand):
            demand = 0.0
        else:
            demand = float(demand)

        discount = row["price_discount_score"]
        if discount is None or pd.isna(discount):
            discount = 0.0
        else:
            discount = float(discount)

        brand = row["brand"]
        brand_key = ""
        if brand is not None and not pd.isna(brand):
            brand_key = str(brand).strip().lower()

        brand_factor = BRAND_LIQUIDITY.get(brand_key, 0.60)

        score = min(
            100.0,
            (0.15 * min(demand, 100.0))
            + (0.45 * (brand_factor * 100.0))
            + (0.40 * min(discount, 100.0))
        )

        return round(score, 2)

    def compute_days_to_sell(self, liquidity_score: float) -> float:
        if liquidity_score >= 80:
            return 5.0
        if liquidity_score >= 65:
            return 10.0
        if liquidity_score >= 50:
            return 18.0
        return 30.0

    def run(self):
        df = self.load_data()
        if df.empty:
            print("[NO DATA] No liquidity features built.")
            return

        rows = []
        for _, row in df.iterrows():
            score = self.compute_liquidity_score(row)
            days = self.compute_days_to_sell(score)
            rows.append({
                "listing_id": row["listing_id"],
                "liquidity_score": score,
                "estimated_days_to_sell": days,
            })

        out = pd.DataFrame(rows)
        db.conn.register("temp_liquidity", out)
        try:
            db.conn.execute("""
            UPDATE listing_features AS lf
            SET
                liquidity_score = t.liquidity_score,
                estimated_days_to_sell = t.estimated_days_to_sell
            FROM temp_liquidity AS t
            WHERE lf.listing_id = t.listing_id
            """)
        finally:
            db.conn.unregister("temp_liquidity")

        print("[OK] Updated listing_features with liquidity.")
        print(out.head(10))