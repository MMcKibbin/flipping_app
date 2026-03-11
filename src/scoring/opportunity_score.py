from datetime import datetime
import pandas as pd

from src.db.duckdb_manager import db


class OpportunityScorer:
    def load_features(self) -> pd.DataFrame:
        query = """
        SELECT
            lc.listing_id,
            lc.asking_price,
            lc.data_quality_score,
            lf.as_of_date,
            lf.demand_score,
            lf.price_discount_score,
            lf.estimated_resale_mid,
            lf.estimated_net_profit
        FROM listing_features lf
        LEFT JOIN listings_clean lc
            ON lf.listing_id = lc.listing_id
            SELECT
    lc.listing_id,
    lc.asking_price,
    lc.data_quality_score,
    lf.as_of_date,
    lf.demand_score,
    lf.price_discount_score,
    lf.estimated_resale_mid,
    lf.estimated_net_profit,
    lf.liquidity_score
        """
        return db.query(query)

    def compute_deal_score(self, row) -> float:
        demand_score = row["demand_score"]
        if demand_score is None or pd.isna(demand_score):
            demand_score = 0.0
        else:
            demand_score = float(demand_score)

        price_discount_score = row["price_discount_score"]
        if price_discount_score is None or pd.isna(price_discount_score):
            price_discount_score = 0.0
        else:
            price_discount_score = float(price_discount_score)

        estimated_net_profit = row["estimated_net_profit"]
        if estimated_net_profit is None or pd.isna(estimated_net_profit):
            estimated_net_profit = 0.0
        else:
            estimated_net_profit = float(estimated_net_profit)

        data_quality_score = row["data_quality_score"]
        if data_quality_score is None or pd.isna(data_quality_score):
            data_quality_score = 0.0
        else:
            data_quality_score = float(data_quality_score)

        demand_component = min(demand_score, 100.0) * 0.35
        discount_component = min(price_discount_score, 100.0) * 0.30
        profit_component = min(max(estimated_net_profit, 0.0), 100.0) * 0.25
        quality_component = min(data_quality_score * 100.0, 100.0) * 0.10

        deal_score = demand_component + discount_component + profit_component + quality_component
        return round(deal_score, 2)

    def compute_capital_efficiency_score(self, row) -> float:
        asking_price = row["asking_price"]
        if asking_price is None or pd.isna(asking_price):
            return 0.0

        estimated_net_profit = row["estimated_net_profit"]
        if estimated_net_profit is None or pd.isna(estimated_net_profit):
            estimated_net_profit = 0.0
        else:
            estimated_net_profit = float(estimated_net_profit)

        asking_price = float(asking_price)

        if asking_price <= 0:
            return 0.0

        score = (estimated_net_profit / asking_price) * 100
        return round(max(0.0, min(score, 100.0)), 2)

    def compute_recommended_max_buy(self, row) -> float:
        est_mid = row["estimated_resale_mid"]
        if est_mid is None or pd.isna(est_mid):
            return 0.0

        est_mid = float(est_mid)
        if est_mid <= 0:
            return 0.0

        max_buy = est_mid * 0.65
        return round(max_buy, 2)

    def compute_recommended_list_price(self, row) -> float:
        est_mid = row["estimated_resale_mid"]
        if est_mid is None or pd.isna(est_mid):
            return 0.0

        return round(float(est_mid), 2)

    def compute_confidence_score(self, row) -> float:
        quality = row["data_quality_score"]
        if quality is None or pd.isna(quality):
            quality = 0.0
        else:
            quality = float(quality)

        quality = quality * 100.0
        return round(min(max(quality, 0.0), 100.0), 2)

    def compute_action(self, deal_score, est_profit):
        if deal_score is None or pd.isna(deal_score):
            return "skip"

        if est_profit is None or pd.isna(est_profit):
            est_profit = 0.0
        else:
            est_profit = float(est_profit)

        if deal_score >= 70 and est_profit >= 25:
            return "buy"

        if deal_score >= 50 and est_profit >= 10:
            return "watch"

        return "skip"

    def build_scores(self) -> pd.DataFrame:
        df = self.load_features()

        if df.empty:
            return pd.DataFrame()

        rows = []

        for _, row in df.iterrows():
            deal_score = self.compute_deal_score(row)
            capital_efficiency_score = self.compute_capital_efficiency_score(row)
            recommended_max_buy = self.compute_recommended_max_buy(row)
            recommended_list_price = self.compute_recommended_list_price(row)
            confidence_score = self.compute_confidence_score(row)

            est_profit = row["estimated_net_profit"]
            if est_profit is None or pd.isna(est_profit):
                est_profit = 0.0
            else:
                est_profit = float(est_profit)

            action = self.compute_action(deal_score, est_profit)

            rows.append({
                "listing_id": row["listing_id"],
                "as_of_date": row["as_of_date"],
                "deal_score": deal_score,
                "capital_efficiency_score": capital_efficiency_score,
                "recommended_action": action,
                "recommended_max_buy": recommended_max_buy,
                "recommended_list_price": recommended_list_price,
                "confidence_score": confidence_score,
                "computed_at": datetime.utcnow(),
            })

        return pd.DataFrame(rows)

    def run(self):
        score_df = self.build_scores()

        if score_df.empty:
            print("[NO DATA] No deal scores built.")
            return

        db.truncate_table("deal_scores")
        db.insert_dataframe(score_df, "deal_scores")

        print(f"[OK] Stored {len(score_df)} deal score rows.")
        print(score_df[[
            "listing_id",
            "deal_score",
            "capital_efficiency_score",
            "recommended_action",
            "recommended_max_buy",
            "recommended_list_price",
            "confidence_score",
        ]])