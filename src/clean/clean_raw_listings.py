"""
clean_raw_listings.py

Purpose:
Read standardized raw scraper records from listings_raw, parse and normalize them,
and write a machine-friendly cleaned dataset to listings_clean.

Unified pipeline:
scrapers -> listings_raw -> listings_clean -> listing_features -> deal_scores
"""

import json
import re
from typing import Optional

import duckdb
import pandas as pd

from src.config import DB_PATH


# Matches simple location strings like:
# Calgary, AB
# Edmonton, Alberta
LOCATION_PATTERN = re.compile(
    r"^[A-Za-z .'\-&]+,\s*(AB|Alberta|BC|British Columbia|SK|Saskatchewan|ON|Ontario)$",
    flags=re.IGNORECASE,
)

BRAND_KEYWORDS = [
    "milwaukee",
    "dewalt",
    "makita",
    "hilti",
    "bosch",
    "ridgid",
    "ryobi",
    "craftsman",
    "metabo",
    "mastercraft",
    "maximum",
]

CATEGORY_RULES = [
    ("battery", "tool_batteries"),
    ("impact", "power_tools"),
    ("hammer drill", "power_tools"),
    ("hammerdrill", "power_tools"),
    ("drill", "power_tools"),
    ("driver", "power_tools"),
    ("sawzall", "power_tools"),
    ("rotary hammer", "power_tools"),
    ("sds", "power_tools"),
    ("socket set", "tool_accessories"),
    ("packout", "tool_storage"),
    ("level", "measuring_tools"),
]


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def initialize_clean_table() -> None:
    """
    Create the unified cleaned table if it does not already exist.
    """
    con = get_connection()

    # TEMPORARY: reset table so schema matches the new cleaner
    con.execute("DROP TABLE IF EXISTS listings_clean")

    con.execute(
        """
        CREATE TABLE listings_clean (
            listing_id VARCHAR,
            source VARCHAR,
            source_listing_id VARCHAR,
            search_term VARCHAR,
            search_region VARCHAR,
            observed_at TIMESTAMP,

            raw_title VARCHAR,
            raw_description VARCHAR,
            raw_price_text VARCHAR,
            raw_location_text VARCHAR,

            clean_title VARCHAR,
            clean_description VARCHAR,

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
            raw_image_urls VARCHAR,
            raw_payload JSON,

            title_was_location BOOLEAN,
            quality_flag VARCHAR
        )
        """
    )

    con.close()


def safe_json_loads(value) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


def is_location_like(text: Optional[str]) -> bool:
    if not text:
        return False
    return bool(LOCATION_PATTERN.match(text.strip()))


def parse_price_text(raw_price_text: Optional[str]) -> Optional[float]:
    """
    Convert raw price text into numeric price.
    Examples:
    $120 -> 120.0
    CA$190 -> 190.0
    1,250 -> 1250.0
    CONTACT / Please Contact -> None
    """
    if not raw_price_text:
        return None

    text = str(raw_price_text).strip().lower()

    if not text:
        return None

    if "contact" in text:
        return None

    match = re.search(r"(\d[\d,]*\.?\d*)", text)
    if not match:
        return None

    numeric = match.group(1).replace(",", "")

    try:
        return float(numeric)
    except Exception:
        return None


def extract_brand(text: Optional[str]) -> Optional[str]:
    if not text:
        return None

    lower_text = text.lower()

    for brand in BRAND_KEYWORDS:
        if brand in lower_text:
            return brand.title()

    return None


def extract_model(text: Optional[str]) -> Optional[str]:
    """
    First-pass model extraction.
    Examples:
    M12
    M18
    2407
    2953-20
    DCD999
    48-89-9221
    """
    if not text:
        return None

    patterns = [
        r"\bM12\b",
        r"\bM18\b",
        r"\b\d{4,5}(?:-\d{2,4})?\b",
        r"\b[A-Z]{2,5}\d{2,5}(?:-\d{2,4})?\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(0).upper()

    return None


def infer_category(search_term: Optional[str], title: Optional[str]) -> Optional[str]:
    combined = f"{search_term or ''} {title or ''}".lower()

    for keyword, category in CATEGORY_RULES:
        if keyword in combined:
            return category

    return "unknown"


def normalize_province(province: Optional[str]) -> Optional[str]:
    if not province:
        return None

    p = province.strip().lower()

    mapping = {
        "ab": "AB",
        "alberta": "AB",
        "bc": "BC",
        "british columbia": "BC",
        "sk": "SK",
        "saskatchewan": "SK",
        "on": "ON",
        "ontario": "ON",
    }

    return mapping.get(p, province.strip().upper())


def split_location(raw_location_text: Optional[str], fallback_region: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Parse raw location text like:
    Calgary, AB
    Edmonton, Alberta
    """
    if raw_location_text and "," in raw_location_text:
        parts = [p.strip() for p in raw_location_text.split(",", 1)]
        city = parts[0] if len(parts) >= 1 else None
        province = normalize_province(parts[1]) if len(parts) >= 2 else None
        return city, province

    # Fallback if no clean location was scraped
    if fallback_region:
        return fallback_region.title(), "AB"

    return None, None


def clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(text)).strip()

    if not cleaned:
        return None

    return cleaned


def recover_title_from_payload(
    raw_payload: dict,
    raw_title: Optional[str],
    raw_price_text: Optional[str],
    raw_location_text: Optional[str],
) -> Optional[str]:
    """
    Recover a better title when the raw title is missing or looks like location/price noise.
    """
    if raw_title and not is_location_like(raw_title):
        title_candidate = clean_text(raw_title)
        if title_candidate and title_candidate != raw_price_text:
            return title_candidate[:500]

    # Try payload lines if present
    lines = raw_payload.get("lines", [])
    if isinstance(lines, list):
        for line in lines:
            if not isinstance(line, str):
                continue

            cleaned = clean_text(line)
            if not cleaned:
                continue

            lower_cleaned = cleaned.lower()

            if cleaned == raw_price_text:
                continue

            if cleaned == raw_location_text:
                continue

            if cleaned.startswith("$") or lower_cleaned.startswith("ca$"):
                continue

            if "hours ago" in lower_cleaned or "days ago" in lower_cleaned or "listed" in lower_cleaned:
                continue

            if is_location_like(cleaned):
                continue

            return cleaned[:500]

    # Fallback to description first line
    raw_text = raw_payload.get("raw_text")
    if isinstance(raw_text, str):
        for line in raw_text.splitlines():
            cleaned = clean_text(line)
            if not cleaned:
                continue
            if cleaned == raw_price_text or cleaned == raw_location_text:
                continue
            if is_location_like(cleaned):
                continue
            if cleaned.startswith("$") or cleaned.lower().startswith("ca$"):
                continue
            return cleaned[:500]

    return clean_text(raw_title) or "UNKNOWN"


def infer_condition(title: Optional[str], description: Optional[str]) -> Optional[str]:
    text = f"{title or ''} {description or ''}".lower()

    if "brand new" in text or "new sealed" in text or "new in box" in text or "new" in text:
        return "new"

    if "like new" in text:
        return "like_new"

    if "used" in text:
        return "used"

    return None


def assign_quality_flag(
    clean_title: Optional[str],
    price: Optional[float],
    brand: Optional[str],
) -> str:
    if not clean_title or clean_title == "UNKNOWN":
        return "bad_title"

    if is_location_like(clean_title):
        return "title_is_location"

    if price is None or price <= 0:
        return "missing_price"

    if brand is None:
        return "missing_brand"

    return "ok"


def build_listing_id(source: Optional[str], source_listing_id: Optional[str]) -> str:
    return f"{source or 'unknown'}::{source_listing_id or 'missing'}"


def clean_raw_listings() -> None:
    """
    Main cleaning routine:
    - read raw records from listings_raw
    - normalize fields
    - write cleaned output to listings_clean
    """
    initialize_clean_table()

    con = get_connection()

    raw_df = con.execute(
        """
        SELECT
            source,
            source_listing_id,
            search_term,
            search_region,
            scraped_at,
            listing_url,
            raw_title,
            raw_description,
            raw_price_text,
            raw_location_text,
            raw_image_urls,
            raw_payload
        FROM listings_raw
        """
    ).df()

    if raw_df.empty:
        print("[WARN] listings_raw is empty. Nothing to clean.")
        con.close()
        return

    cleaned_rows = []

    for _, row in raw_df.iterrows():
        source = row["source"]
        source_listing_id = row["source_listing_id"]
        search_term = row["search_term"]
        search_region = row["search_region"]
        observed_at = row["scraped_at"]
        listing_url = row["listing_url"]

        raw_title = clean_text(row["raw_title"])
        raw_description = clean_text(row["raw_description"])
        raw_price_text = clean_text(row["raw_price_text"])
        raw_location_text = clean_text(row["raw_location_text"])
        raw_image_urls = row["raw_image_urls"]
        raw_payload = safe_json_loads(row["raw_payload"])

        title_was_location = is_location_like(raw_title)

        clean_title = recover_title_from_payload(
            raw_payload=raw_payload,
            raw_title=raw_title,
            raw_price_text=raw_price_text,
            raw_location_text=raw_location_text,
        )

        clean_description = raw_description
        price = parse_price_text(raw_price_text)
        city, province = split_location(raw_location_text, search_region)

        brand = extract_brand(clean_title)
        model = extract_model(clean_title)
        category = infer_category(search_term, clean_title)
        condition = infer_condition(clean_title, clean_description)

        quality_flag = assign_quality_flag(
            clean_title=clean_title,
            price=price,
            brand=brand,
        )

        cleaned_rows.append(
            {
                "listing_id": build_listing_id(source, source_listing_id),
                "source": source,
                "source_listing_id": source_listing_id,
                "search_term": search_term,
                "search_region": search_region,
                "observed_at": observed_at,

                "raw_title": raw_title,
                "raw_description": raw_description,
                "raw_price_text": raw_price_text,
                "raw_location_text": raw_location_text,

                "clean_title": clean_title,
                "clean_description": clean_description,

                "category": category,
                "brand": brand,
                "model": model,
                "condition": condition,

                "price": price,
                "currency": "CAD",

                "location_country": "Canada",
                "location_state_province": province,
                "location_city": city,

                "shipping_available": None,
                "shipping_cost_est": None,
                "pickup_only": True,

                "listing_url": listing_url,
                "raw_image_urls": raw_image_urls,
                "raw_payload": json.dumps(raw_payload),

                "title_was_location": title_was_location,
                "quality_flag": quality_flag,
            }
        )

    cleaned_df = pd.DataFrame(cleaned_rows)

    con.execute("DELETE FROM listings_clean")
    con.register("cleaned_view", cleaned_df)

    con.execute(
        """
        INSERT INTO listings_clean
        SELECT
            listing_id,
            source,
            source_listing_id,
            search_term,
            search_region,
            observed_at,

            raw_title,
            raw_description,
            raw_price_text,
            raw_location_text,

            clean_title,
            clean_description,

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
            raw_image_urls,
            raw_payload,

            title_was_location,
            quality_flag
        FROM cleaned_view
        """
    )

    preview = con.execute(
        """
        SELECT
            listing_id,
            source,
            raw_title,
            clean_title,
            brand,
            model,
            category,
            price,
            location_city,
            location_state_province,
            title_was_location,
            quality_flag
        FROM listings_clean
        ORDER BY observed_at DESC
        LIMIT 20
        """
    ).fetchall()

    con.close()

    print(f"[OK] Cleaned {len(cleaned_rows)} raw listings into listings_clean")
    for row in preview:
        print(row)


if __name__ == "__main__":
    clean_raw_listings()