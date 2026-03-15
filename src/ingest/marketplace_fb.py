import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from src.config import RAW_DIR
from src.schemas import ListingRecord
from src.storage.duckdb_store import insert_records
from src.utils.parsing import parse_price

BASE_URL = "https://www.facebook.com"
SESSION_PATH = Path("data/sessions/facebook_state.json")


def normalize_url(href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(BASE_URL, href)


def extract_listing_id(href: str | None) -> str | None:
    if not href:
        return None

    href = normalize_url(href)
    if not href:
        return None

    match = re.search(r"/marketplace/item/(\d+)", href)
    if match:
        return match.group(1)

    return None


def split_lines(raw_text: str) -> list[str]:
    if not raw_text:
        return []
    return [line.strip() for line in raw_text.splitlines() if line.strip()]


def guess_fields_from_lines(lines: list[str]) -> tuple[str, float | None, str | None]:
    title = "UNKNOWN"
    price = None
    raw_location = None

    for line in lines[:4]:
        parsed = parse_price(line)
        if parsed is not None:
            price = parsed
            break

    for line in lines:
        if parse_price(line) is None and len(line) > 2:
            title = line[:500]
            break

    if len(lines) >= 2:
        raw_location = lines[-1][:200]

    return title, price, raw_location


def scroll_results(page, max_scrolls: int = 10, pause_ms: int = 1800) -> None:
    last_height = 0

    for _ in range(max_scrolls):
        page.mouse.wheel(0, 9000)
        page.wait_for_timeout(pause_ms)

        try:
            current_height = page.evaluate("document.body.scrollHeight")
        except Exception:
            current_height = 0

        if current_height == last_height:
            break

        last_height = current_height


def ensure_marketplace_loaded(page) -> None:
    page.wait_for_timeout(4000)

    current_url = page.url.lower()
    page_text = page.locator("body").inner_text(timeout=5000).lower()

    if "login" in current_url:
        raise RuntimeError(
            "Facebook redirected to login. Run: python -m src.ingest.fb_login_once"
        )

    login_signals = [
        "log in",
        "sign up",
        "create new account",
    ]

    if any(signal in page_text for signal in login_signals):
        raise RuntimeError(
            "Facebook appears to be showing a login wall. Run fb_login_once first."
        )


def scrape_facebook_marketplace(
    query: str,
    city: str,
    max_items: int = 40,
) -> list[ListingRecord]:
    records: list[ListingRecord] = []
    seen_listing_ids: set[str] = set()

    search_url = f"{BASE_URL}/marketplace/search/?query={quote_plus(query)}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)

        if SESSION_PATH.exists():
            context = browser.new_context(storage_state=str(SESSION_PATH))
        else:
            context = browser.new_context()

        page = context.new_page()

        print(f"[INFO] Opening: {search_url}")
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)

        ensure_marketplace_loaded(page)
        scroll_results(page, max_scrolls=10, pause_ms=1800)

        card_selector = "a[href*='/marketplace/item/']"
        cards = page.locator(card_selector)

        try:
            total_cards = cards.count()
        except PlaywrightTimeoutError:
            total_cards = 0

        print(f"[DEBUG] Found {total_cards} candidate cards")
        count = min(total_cards, max_items)

        for i in range(count):
            try:
                card = cards.nth(i)

                href = card.get_attribute("href")
                full_url = normalize_url(href)
                listing_id = extract_listing_id(href)

                if not listing_id:
                    print(f"[DEBUG] Skipping card {i}: no listing_id")
                    continue

                if listing_id in seen_listing_ids:
                    continue
                seen_listing_ids.add(listing_id)

                raw_text = card.inner_text(timeout=2000).strip()
                lines = split_lines(raw_text)
                title, price, raw_location = guess_fields_from_lines(lines)

                record = ListingRecord(
                    source_platform="facebook_marketplace",
                    search_term=query,
                    listing_id=listing_id,
                    observed_at=datetime.now(timezone.utc),
                    title=title,
                    description=None,
                    category=None,
                    brand=None,
                    model=None,
                    condition=None,
                    price=price,
                    currency="CAD",
                    location_country="Canada",
                    location_state_province="Alberta",
                    location_city=city,
                    shipping_available=False,
                    shipping_cost_est=None,
                    pickup_only=True,
                    listing_url=full_url,
                    raw_payload_json={
                        "raw_text": raw_text,
                        "lines": lines,
                        "href": full_url,
                        "raw_location": raw_location,
                    },
                )

                records.append(record)

            except Exception as exc:
                print(f"[WARN] Skipping card {i}: {exc}")

        browser.close()

    save_raw_snapshot(query=query, city=city, records=records)
    print(f"[OK] Scraped {len(records)} Facebook Marketplace records")
    return records


def save_raw_snapshot(query: str, city: str, records: list[ListingRecord]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    safe_query = query.replace(" ", "_").lower()
    safe_city = city.replace(" ", "_").lower()
    out_path = RAW_DIR / f"{safe_city}_{safe_query}.json"

    payload = [record.model_dump(mode="json") for record in records]
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"[OK] Saved raw snapshot to: {out_path}")


if __name__ == "__main__":
    records = scrape_facebook_marketplace("milwaukee drill", "Calgary", max_items=40)
    print(f"[INFO] Final record count: {len(records)}")

    db_records = [record.model_dump(mode="json") for record in records]
    insert_records(db_records)