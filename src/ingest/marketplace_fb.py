import json
from pathlib import Path

from playwright.sync_api import sync_playwright

from src.config import RAW_DIR
from src.schemas import ListingRecord
from src.utils.parsing import parse_price


def scrape_facebook_marketplace(query: str, city: str) -> list[ListingRecord]:
    records: list[ListingRecord] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        url = f"https://www.facebook.com/marketplace/search/?query={query}"
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        # Replace these selectors after inspecting the page in DevTools.
        card_selector = "a[href*='/marketplace/item']"

        cards = page.locator(card_selector)
        count = min(cards.count(), 20)

        for i in range(count):
            try:
                card = cards.nth(i)
                href = card.get_attribute("href")
                title = card.inner_text(timeout=2000).strip()

                record = ListingRecord(
                    source_platform="facebook_marketplace",
                    search_term=query,
                    listing_id=f"{city}-{query}-{i}",
                    title=title[:500] if title else "UNKNOWN",
                    price=parse_price(title),
                    location_country="Canada",
                    location_state_province="Alberta",
                    location_city=city,
                    listing_url=href if href else None,
                    raw_payload_json={
                        "raw_text": title,
                        "href": href,
                    },
                )
                records.append(record)

            except Exception as exc:
                print(f"Skipping card {i}: {exc}")

        browser.close()

    save_raw_snapshot(query=query, city=city, records=records)
    return records


def save_raw_snapshot(query: str, city: str, records: list[ListingRecord]) -> None:
    safe_query = query.replace(" ", "_").lower()
    safe_city = city.replace(" ", "_").lower()
    out_path = RAW_DIR / f"{safe_city}_{safe_query}.json"

    payload = [record.model_dump(mode="json") for record in records]
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")