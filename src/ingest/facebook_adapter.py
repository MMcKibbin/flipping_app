from datetime import datetime, UTC
from pathlib import Path
from urllib.parse import quote_plus, urljoin
import re

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from src.ingest.listing_ingest_base import BaseListingAdapter

BASE_URL = "https://www.facebook.com"
SESSION_PATH = Path("data/sessions/facebook_state.json")


class FacebookAdapter(BaseListingAdapter):
    """
    Facebook Marketplace scraper that feeds the unified raw pipeline.

    Output target:
        listings_raw
    """

    source_name = "facebook_marketplace"

    def __init__(self, search_term="cordless drill", search_region="calgary"):
        self.search_term = search_term
        self.search_region = search_region

    def build_search_url(self) -> str:
        return f"{BASE_URL}/marketplace/search/?query={quote_plus(self.search_term)}"

    def normalize_url(self, href: str | None) -> str | None:
        if not href:
            return None
        return urljoin(BASE_URL, href)

    def extract_listing_id(self, href: str | None) -> str | None:
        href = self.normalize_url(href)
        if not href:
            return None

        match = re.search(r"/marketplace/item/(\d+)", href)
        if match:
            return match.group(1)

        return None

    def split_lines(self, raw_text: str) -> list[str]:
        if not raw_text:
            return []
        return [line.strip() for line in raw_text.splitlines() if line.strip()]

    def is_price_line(self, line: str) -> bool:
        if not line:
            return False

        text = line.strip()
        lower = text.lower()

        if lower in {"free", "contact for price", "please contact"}:
            return True

        if re.fullmatch(r"(?:CA\$|\$)\s?\d[\d,]*(?:\.\d{2})?", text, flags=re.IGNORECASE):
            return True

        return False

    def is_location_line(self, line: str) -> bool:
        if not line:
            return False

        text = line.strip()

        return bool(
            re.fullmatch(
                r"[A-Za-z .'\-&]+,\s*(AB|Alberta|BC|British Columbia|SK|Saskatchewan|ON|Ontario)",
                text,
                flags=re.IGNORECASE,
            )
        )

    def guess_raw_fields(self, lines: list[str]) -> tuple[str, str, str]:
        raw_title = ""
        raw_price_text = ""
        raw_location_text = ""

        # Pull price/location out first, even if the card is a single-line blob.
        joined_text = " | ".join(lines)

        price_matches = re.findall(
            r"(?:CA\$|\$)\s?\d[\d,]*(?:\.\d{2})?",
            joined_text,
            flags=re.IGNORECASE,
        )
        if price_matches:
            raw_price_text = price_matches[0]

        location_match = re.search(
            r"([A-Za-z .'\-&]+,\s*(?:AB|Alberta|BC|British Columbia|SK|Saskatchewan|ON|Ontario))",
            joined_text,
            flags=re.IGNORECASE,
        )
        if location_match:
            raw_location_text = location_match.group(1).strip()

        # Remove price and location from the joined text to recover title.
        title_candidate = joined_text
        if raw_price_text:
            title_candidate = title_candidate.replace(raw_price_text, " ")
        if raw_location_text:
            title_candidate = title_candidate.replace(raw_location_text, " ")

        # Remove common Facebook noise.
        title_candidate = re.sub(r"\bJust listed\b", " ", title_candidate, flags=re.IGNORECASE)
        title_candidate = re.sub(r"\s*\|\s*", " ", title_candidate)
        title_candidate = re.sub(r"\s+", " ", title_candidate).strip(" -–|,")

        # If title is still empty, fall back to first usable line.
        if not title_candidate:
            for line in lines:
                lower = line.lower().strip()
                if not line:
                    continue
                if line == raw_price_text:
                    continue
                if line == raw_location_text:
                    continue
                if "hours ago" in lower or "days ago" in lower or "weeks ago" in lower:
                    continue
                title_candidate = line
                break

        raw_title = title_candidate if title_candidate else "UNKNOWN"

        return raw_title, raw_price_text, raw_location_text

    def scroll_results(self, page) -> None:
        last_height = 0

        for _ in range(10):
            page.mouse.wheel(0, 9000)
            page.wait_for_timeout(1800)

            try:
                current_height = page.evaluate("document.body.scrollHeight")
            except Exception:
                current_height = 0

            if current_height == last_height:
                break

            last_height = current_height

    def ensure_marketplace_loaded(self, page) -> None:
        page.wait_for_timeout(4000)

        current_url = page.url.lower()
        page_text = page.locator("body").inner_text(timeout=5000).lower()

        if "login" in current_url:
            raise RuntimeError(
                "Facebook redirected to login. Run the Facebook login-state save flow first."
            )

        login_signals = [
            "log in",
            "sign up",
            "create new account",
        ]

        if any(signal in page_text for signal in login_signals):
            raise RuntimeError(
                "Facebook appears to be showing a login wall. Save a valid session first."
            )

    def fetch_raw_records(self) -> list[dict]:
        records = []
        seen_ids = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, slow_mo=50)

            if SESSION_PATH.exists():
                context = browser.new_context(storage_state=str(SESSION_PATH))
            else:
                context = browser.new_context()

            page = context.new_page()

            search_url = self.build_search_url()
            print(f"[INFO] Opening: {search_url}")

            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            self.ensure_marketplace_loaded(page)
            self.scroll_results(page)

            cards = page.locator("a[href*='/marketplace/item/']")

            try:
                total_cards = cards.count()
            except PlaywrightTimeoutError:
                total_cards = 0

            print(f"[DEBUG] Found {total_cards} candidate Facebook cards")

            for i in range(total_cards):
                try:
                    card = cards.nth(i)

                    href = card.get_attribute("href")
                    listing_url = self.normalize_url(href)
                    listing_id = self.extract_listing_id(href)

                    if not listing_id:
                        continue

                    if listing_id in seen_ids:
                        continue
                    seen_ids.add(listing_id)

                    raw_text = card.inner_text(timeout=2000).strip()
                    lines = self.split_lines(raw_text)
                    raw_title, raw_price_text, raw_location_text = self.guess_raw_fields(lines)

                    records.append(
                        {
                            "source_listing_id": listing_id,
                            "search_term": self.search_term,
                            "search_region": self.search_region,
                            "scraped_at": datetime.now(UTC),
                            "listing_url": listing_url,
                            "raw_title": raw_title,
                            "raw_description": raw_text[:2000],
                            "raw_price_text": raw_price_text,
                            "raw_location_text": raw_location_text,
                            "raw_image_urls": [],
                        }
                    )

                except Exception as exc:
                    print(f"[WARN] skipping Facebook card {i}: {exc}")

            browser.close()

        print(f"[OK] Collected {len(records)} Facebook records")
        return records