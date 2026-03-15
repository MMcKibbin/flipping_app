from pathlib import Path
from contextlib import contextmanager
import duckdb
import pandas as pd

from src.config import DB_PATH


class DuckDBManager:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._configure()
        self.initialize_tables()

    def _configure(self) -> None:
        self.conn.execute("PRAGMA threads=4")

    def initialize_tables(self) -> None:
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS trend_data_raw (
            keyword TEXT,
            geo TEXT,
            trend_date DATE,
            interest_value INTEGER,
            is_partial BOOLEAN,
            pulled_at TIMESTAMP
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS trend_features (
            keyword TEXT,
            geo TEXT,
            as_of_date DATE,
            trend_level DOUBLE,
            trend_velocity DOUBLE,
            trend_acceleration DOUBLE,
            computed_at TIMESTAMP
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS demand_scores (
            keyword TEXT,
            geo TEXT,
            as_of_date DATE,
            demand_score DOUBLE,
            computed_at TIMESTAMP
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS listings_raw (
            source TEXT,
            source_listing_id TEXT,
            search_term TEXT,
            search_region TEXT,
            scraped_at TIMESTAMP,
            listing_url TEXT,
            raw_title TEXT,
            raw_description TEXT,
            raw_price_text TEXT,
            raw_location_text TEXT,
            raw_image_urls TEXT,
            raw_payload TEXT
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS listings_clean (
            listing_id TEXT,
            source TEXT,
            source_listing_id TEXT,
            listing_url TEXT,
            scraped_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            title TEXT,
            description TEXT,
            asking_price DOUBLE,
            currency TEXT,
            category TEXT,
            subcategory TEXT,
            brand TEXT,
            model TEXT,
            condition TEXT,
            seller_type TEXT,
            city TEXT,
            region TEXT,
            country TEXT,
            lat DOUBLE,
            lon DOUBLE,
            image_count INTEGER,
            image_urls TEXT,
            listing_status TEXT,
            dedupe_key TEXT,
            data_quality_score DOUBLE
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS listing_features (
            listing_id TEXT,
            as_of_date DATE,
            demand_score DOUBLE,
            price_discount_score DOUBLE,
            estimated_resale_low DOUBLE,
            estimated_resale_mid DOUBLE,
            estimated_resale_high DOUBLE,
            estimated_net_profit DOUBLE,
            estimated_days_to_sell DOUBLE,
            liquidity_score DOUBLE,
            seller_confidence_score DOUBLE,
            seasonality_score DOUBLE,
            photo_confidence_score DOUBLE,
            arbitrage_spread_score DOUBLE,
            computed_at TIMESTAMP
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS deal_scores (
            listing_id TEXT,
            as_of_date DATE,
            deal_score DOUBLE,
            capital_efficiency_score DOUBLE,
            recommended_action TEXT,
            recommended_max_buy DOUBLE,
            recommended_list_price DOUBLE,
            confidence_score DOUBLE,
            computed_at TIMESTAMP
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT,
            job_type TEXT,
            job_target TEXT,
            job_scope TEXT,
            status TEXT,
            priority INTEGER,
            payload TEXT,
            scheduled_at TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            error_message TEXT,
            created_at TIMESTAMP
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS ebay_price_comps (
            keyword TEXT,
            item_title TEXT,
            item_id TEXT,
            price DOUBLE,
            currency TEXT,
            item_web_url TEXT,
            condition TEXT,
            buying_options TEXT,
            seller_username TEXT,
            marketplace_id TEXT,
            pulled_at TIMESTAMP
        )
        """)

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        if df.empty:
            print(f"[SKIP] Empty dataframe, nothing inserted into {table_name}.")
            return

        temp_name = "temp_df"
        self.conn.register(temp_name, df)
        try:
            self.conn.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_name}
            """)
        finally:
            self.conn.unregister(temp_name)

    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).fetchdf()

    def execute(self, sql: str) -> None:
        self.conn.execute(sql)

    def table_exists(self, table_name: str) -> bool:
        result = self.conn.execute(f"""
        SELECT COUNT(*) AS count
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
        """).fetchone()
        return bool(result[0])

    def get_row_count(self, table_name: str) -> int:
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(result[0])

    def truncate_table(self, table_name: str) -> None:
        self.conn.execute(f"DELETE FROM {table_name}")

    def show_tables(self) -> pd.DataFrame:
        return self.conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """).fetchdf()

    def reset_stage1_tables(self) -> None:
        for table in [
            "trend_data_raw",
            "trend_features",
            "demand_scores",
            "listings_raw",
            "listings_clean",
        ]:
        
            if self.table_exists(table):
                self.truncate_table(table)
    def close(self) -> None:
        self.conn.close()


db = DuckDBManager()