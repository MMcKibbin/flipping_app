from datetime import datetime, UTC
import pandas as pd
import duckdb

from src.config import DB_PATH


def get_connection():
    return duckdb.connect(str(DB_PATH))


class OpportunityScorer:

    def load_features(self) -> pd.DataFrame:
        con = get_connection()

        query = """
        SELECT
            listing_id,
            price,
            demand_score,
            discount_vs_market,
            estimated_resale_mid,
            estimated_net_profit,
            liquidity_score
        FROM listing_features
        """

        df = con.execute(query).df()
        con.close()

        return df

    def compute_logistics_cost(self, price):

        if price is None or pd.isna(price):
            return 0.0

        price = float(price)

        if price < 100:
            return 15.0
        elif price < 300:
            return 25.0
        elif price < 700:
            return 35.0
        else:
            return 50.0

    def compute_adjusted_profit(self, row):

        profit = row["estimated_net_profit"]

        if profit is None or pd.isna(profit):
            profit = 0.0

        logistics = self.compute_logistics_cost(row["price"])

        return round(profit - logistics, 2)

    def compute_deal_score(self, row):

        demand = float(row["demand_score"] or 0)
        discount = float(row["discount_vs_market"] or 0)
        liquidity = float(row["liquidity_score"] or 0)

        price = float(row["price"] or 0)

        adjusted_profit = self.compute_adjusted_profit(row)

        if price > 0:
            profit_score = (adjusted_profit / price) * 100
        else:
            profit_score = 0

        profit_score = min(max(profit_score, 0), 100)

        demand_component = demand * 0.10
        discount_component = discount * 100 * 0.30
        profit_component = profit_score * 0.40
        liquidity_component = liquidity * 0.20

        score = (
            demand_component
            + discount_component
            + profit_component
            + liquidity_component
        )

        return round(score, 2)

    def compute_capital_efficiency(self, row):

        price = row["price"]
        if price is None or price <= 0:
            return 0

        adjusted_profit = self.compute_adjusted_profit(row)

        score = (adjusted_profit / price) * 100

        return round(max(0, min(score, 100)), 2)

    def compute_recommended_max_buy(self, row):

        est_mid = row["estimated_resale_mid"]

        if est_mid is None or pd.isna(est_mid):
            return 0

        return round(est_mid * 0.65, 2)

    def compute_recommended_list_price(self, row):

        est_mid = row["estimated_resale_mid"]

        if est_mid is None or pd.isna(est_mid):
            return 0

        return round(est_mid, 2)

    def compute_action(self, deal_score, adjusted_profit):

        if deal_score >= 70 and adjusted_profit >= 25:
            return "buy"

        if deal_score >= 50 and adjusted_profit >= 10:
            return "watch"

        return "skip"

    def build_scores(self):

        df = self.load_features()

        if df.empty:
            return pd.DataFrame()

        rows = []

        for _, row in df.iterrows():

            deal_score = self.compute_deal_score(row)
            adjusted_profit = self.compute_adjusted_profit(row)

            rows.append({
                "listing_id": row["listing_id"],
                "deal_score": deal_score,
                "capital_efficiency_score": self.compute_capital_efficiency(row),
                "recommended_action": self.compute_action(deal_score, adjusted_profit),
                "recommended_max_buy": self.compute_recommended_max_buy(row),
                "recommended_list_price": self.compute_recommended_list_price(row),
                "adjusted_profit": adjusted_profit,
                "computed_at": datetime.now(UTC)
            })

        return pd.DataFrame(rows)

    def run(self):

        df = self.build_scores()

        if df.empty:
            print("[NO DATA] No deal scores built.")
            return

        con = get_connection()

        con.execute("""
        CREATE TABLE IF NOT EXISTS deal_scores (
            listing_id VARCHAR,
            deal_score DOUBLE,
            capital_efficiency_score DOUBLE,
            recommended_action VARCHAR,
            recommended_max_buy DOUBLE,
            recommended_list_price DOUBLE,
            adjusted_profit DOUBLE,
            computed_at TIMESTAMP
        )
        """)

        con.execute("DELETE FROM deal_scores")

        con.register("score_view", df)

        con.execute("""
        INSERT INTO deal_scores
        SELECT * FROM score_view
        """)

        preview = con.execute("""
        SELECT *
        FROM deal_scores
        ORDER BY deal_score DESC
        LIMIT 20
        """).fetchall()

        con.close()

        print(f"[OK] Stored {len(df)} deal score rows.")

        for r in preview:
            print(r)


if __name__ == "__main__":
    OpportunityScorer().run()