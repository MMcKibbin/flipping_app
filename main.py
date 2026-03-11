from ingest.google_trends_api import (
    fetch_google_demand_mock,
    load_demand_keywords,
)

# Import typing support for defining scraper function types
from collections.abc import Callable

# Thread pool tools that allow multiple scrapers to run simultaneously
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import the eBay scraper (first ingestion source)
from src.ingest.ebay_api import search_ebay

# Data model used for all listings collected by the app
from src.schemas import ListingRecord

# Database initialization and insertion helpers
from src.storage.duckdb_store import (
    initialize_database,
    insert_records,
    insert_google_demand_records,
)

# Load scraping jobs from config/jobs.json
from src.utils.job_loader import load_jobs

# Source health monitoring utilities
from src.utils.source_health import (
    get_source_status,
    is_source_healthy,
    mark_failure,
    mark_success,
)

# Define a type alias for ingestion functions.
# Every scraper must return a list of ListingRecord objects.
Ingestor = Callable[..., list[ListingRecord]]

# Registry mapping source names to their ingestion functions.
SOURCE_REGISTRY: dict[str, Ingestor] = {
    "ebay": search_ebay,
    "google_trends": fetch_google_demand_mock,
    # Future sources:
    # "facebook": scrape_facebook_marketplace,
    # "amazon": search_amazon,
    # "kijiji": search_kijiji,
}


def run_source(source_name: str, **kwargs) -> list[ListingRecord]:
    """
    Run a single ingestion source.

    This function:
    1. Checks if the source is currently healthy
    2. Executes the scraper
    3. Marks success or failure
    4. Returns collected records
    """

    if not is_source_healthy(source_name):
        status = get_source_status(source_name)
        print(f"Skipping source: {source_name} (status={status})")
        return source_name, []

    source_func = SOURCE_REGISTRY[source_name]

    print(f"Running source: {source_name}")

    try:
        if source_name == "google_trends":
            keywords = load_demand_keywords()
            records = source_func(keywords=keywords, region="CA")
        else:
            records = source_func(**kwargs)

        print(f"{source_name}: fetched {len(records)} records")
        mark_success(source_name)
        return source_name, records

    except Exception as exc:
        error_message = str(exc)
        print(f"{source_name}: failed with error: {error_message}")
        mark_failure(source_name, error_message)
        return source_name, []


def main() -> None:
    """
    Main application entry point.

    Responsibilities:
    - initialize database
    - load jobs
    - run jobs in parallel
    - combine results
    - store results in DuckDB
    """

    initialize_database()

    jobs = load_jobs()

    if not jobs:
        print("No jobs found in config/jobs.json")
        return

    all_records: list[ListingRecord] = []

    max_workers = min(8, len(jobs)) if jobs else 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_job = {
            executor.submit(
                run_source,
                job["source_name"],
                **job["kwargs"],
            ): job
            for job in jobs
        }

        for future in as_completed(future_to_job):
            job = future_to_job[future]
            source_name = job["source_name"]

            try:
                source_name, records = future.result()

                if source_name == "google_trends":
                    if records:
                        insert_google_demand_records(records)
                        print("Saved Google demand records to DuckDB")
                else:
                    all_records.extend(records)

            except Exception as exc:
                print(f"{source_name}: unexpected thread error: {exc}")
                mark_failure(source_name, f"thread error: {exc}")
    print(f"Total records fetched: {len(all_records)}")

    if not all_records:
        print("No records found. Nothing inserted.")
        return

    insert_records([record.model_dump(mode="json") for record in all_records])
    print("Saved records to DuckDB")


if __name__ == "__main__":
    main()