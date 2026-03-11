from datetime import datetime


def fetch_google_demand_mock(**kwargs):
    """
    Temporary mock Google demand scraper.
    This simulates Google Trends data until the real API is added.
    """

    return [
        {
            "source_platform": "google_trends",
            "keyword": "dewalt drill",
            "region": "CA",
            "observed_at": datetime.utcnow(),
            "interest_score": 72,
            "trend_direction": "up",
            "raw_payload_json": {}
        },
        {
            "source_platform": "google_trends",
            "keyword": "milwaukee impact driver",
            "region": "CA",
            "observed_at": datetime.utcnow(),
            "interest_score": 81,
            "trend_direction": "up",
            "raw_payload_json": {}
        }
    ]