import duckdb
import pandas as pd

from src.config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


import duckdb
import pandas as pd

from src.config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def initialize_database() -> None:
    con = get_connection()

    con.execute("DROP TABLE IF EXISTS raw_listings")
    con.execute("DROP TABLE IF EXISTS google_demand_raw")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_listings (
            source_platform VARCHAR,
            search_term VARCHAR,
            listing_id VARCHAR,
            observed_at TIMESTAMP,
            title VARCHAR,
            description VARCHAR,
            category VARCHAR,
            brand VARCHAR,
            model VARCHAR,
            condition VARCHAR,
            price DOUBLE,
            currency VARCHAR,
            location_country VARCHAR,
            location_state_province VARCHAR,
            location_city VARCHAR,
            shipping_available BOOLEAN,
            shipping_cost_est DOUBLE,
            pickup_only BOOLEAN,
            listing_url VARCHAR,
            raw_payload_json JSON
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS google_demand_raw (
            source_platform VARCHAR,
            keyword VARCHAR,
            region VARCHAR,
            observed_at TIMESTAMP,
            interest_score DOUBLE,
            trend_direction VARCHAR,
            raw_payload_json JSON
        )
        """
    )

    con.close()


def insert_records(records: list[dict]) -> None:
    if not records:
        print("[WARN] No records to insert.")
        return

    initialize_database()

    df = pd.DataFrame(records)
    con = get_connection()
    con.register("incoming_records", df)
    con.execute(
        """
        INSERT INTO raw_listings
        SELECT
            source_platform,
            search_term,
            listing_id,
            observed_at,
            title,
            description,
            category,
            brand,
            model,
            condition,
            price,
            currency,
            location_country,
            location_state_province,
            location_city,
            shipping_available,
            shipping_cost_est,
            pickup_only,
            listing_url,
            raw_payload_json
        FROM incoming_records
        """
    )
    con.close()
    print(f"[OK] Inserted {len(records)} records into raw_listings")


def insert_google_demand_records(records: list[dict]) -> None:
    if not records:
        print("[WARN] No Google demand records to insert.")
        return

    initialize_database()

    df = pd.DataFrame(records)
    con = get_connection()
    con.register("google_demand_view", df)
    con.execute(
        """
        INSERT INTO google_demand_raw
        SELECT
            source_platform,
            keyword,
            region,
            observed_at,
            interest_score,
            trend_direction,
            raw_payload_json
        FROM google_demand_view
        """
    )
    con.close()
    print(f"[OK] Inserted {len(records)} records into google_demand_raw")