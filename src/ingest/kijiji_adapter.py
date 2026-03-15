from datetime import datetime, UTC
import re
import random
import time
import requests
from bs4 import BeautifulSoup

from src.ingest.listing_ingest_base import BaseListingAdapter


class KijijiAdapter(BaseListingAdapter):
    source_name = "kijiji"

    def __init__(self, search_term: str = "cordless drill", search_region: str = "calgary"):
        self.search_term = search_term
        self.search_region = search_region
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        ]

    def build_search_url(self, page: int = 1) -> str:
        from urllib.parse import quote_plus

        query = quote_plus(self.search_term)

        if self.search_region.lower() == "calgary":
            region_path = "b-calgary"
            region_code = "l1700199"
        else:
            region_path = f"b-{self.search_region.lower()}"
            region_code = "l1700000"

        if page == 1:
            return f"https://www.kijiji.ca/{region_path}/{query}/k0{region_code}"

        return f"https://www.kijiji.ca/{region_path}/{query}/page-{page}/k0{region_code}"

    def fetch_page(self, page: int = 1) -> str:
        url = self.build_search_url(page=page)

        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-CA,en;q=0.9",
            "Referer": "https://www.kijiji.ca/",
            "Cache-Control": "no-cache",
        }

        time.sleep(random.uniform(1.5, 4.0))

        response = self.session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    def _safe_text(self, element) -> str:
        return element.get_text(" ", strip=True) if element else ""

    def _extract_price(self, container) -> str:
        selectors = [
            '[data-testid="listing-price"]',
            '[data-testid="price"]',
            '[class*="price"]',
            'p[class*="price"]',
            'span[class*="price"]',
        ]

        for selector in selectors:
            tags = container.select(selector)
            for tag in tags:
                text = self._safe_text(tag)
                if text and "$" in text:
                    return text

        full_text = self._safe_text(container)

        price_match = re.search(r"\$\s?\d[\d,]*(?:\.\d{2})?", full_text)
        if price_match:
            return price_match.group(0)

        if "please contact" in full_text.lower():
            return "CONTACT"

        return ""

    def _extract_location(self, container) -> str:
        selectors = [
            '[data-testid="listing-location"]',
            '[data-testid="location"]',
            '[class*="location"]',
            'span[class*="location"]',
            'p[class*="location"]',
        ]

        for selector in selectors:
            tags = container.select(selector)
            for tag in tags:
                text = self._safe_text(tag)
                if text:
                    return text

        full_text = self._safe_text(container)

        loc_match = re.search(
            r"\b([A-Z][a-zA-Z\s\-'&]+,\s*(?:AB|Alberta|BC|British Columbia|SK|Saskatchewan|ON|Ontario))\b",
            full_text
        )
        if loc_match:
            return loc_match.group(1)

        return ""

    def _find_card_container(self, link_tag):
        current = link_tag

        for _ in range(8):
            if current is None:
                break

            current = current.parent
            if current is None:
                break

            text = self._safe_text(current)
            href_count = len(current.select("a[href]"))

            if 40 <= len(text) <= 4000 and href_count <= 25:
                return current

        return link_tag.parent if link_tag.parent else link_tag

    def parse_listings(self, html: str) -> list[dict]:
        debug_no_price_count = 0
        soup = BeautifulSoup(html, "lxml")
        results = []
        seen_urls = set()

        link_tags = soup.select('a[data-testid="listing-link"]')

        if not link_tags:
            link_tags = soup.select('a[href*="/v-"]')

        for link_tag in link_tags:
            href = link_tag.get("href")
            if not href:
                continue

            if href.startswith("/"):
                listing_url = f"https://www.kijiji.ca{href}"
            else:
                listing_url = href

            if listing_url in seen_urls:
                continue
            seen_urls.add(listing_url)

            title = self._safe_text(link_tag)
            if not title or len(title) < 4:
                continue

            container = self._find_card_container(link_tag)

            price_text = self._extract_price(container)
            location_text = self._extract_location(container)
            description_text = self._safe_text(container)[:2000]

            if not price_text and debug_no_price_count < 10:
                debug_no_price_count += 1
                print(f"\n[DEBUG NO PRICE #{debug_no_price_count}]")
                print({
                    "title": title,
                    "url": listing_url,
                    "location": location_text,
                    "card_text_sample": description_text[:500]
                })

            source_listing_id = listing_url.rstrip("/").split("/")[-1]

            results.append({
                "source_listing_id": source_listing_id,
                "search_term": self.search_term,
                "search_region": self.search_region,
                "scraped_at": datetime.now(UTC),
                "listing_url": listing_url,
                "raw_title": title,
                "raw_description": description_text,
                "raw_price_text": price_text,
                "raw_location_text": location_text,
                "raw_image_urls": [],
            })

        return results

    def fetch_raw_records(self) -> list[dict]:
        all_records = []
        seen_urls = set()
        max_pages = 5

        for page in range(1, max_pages + 1):
            print(f"[DEBUG] Fetching Kijiji page {page}...")

            html = self.fetch_page(page=page)
            records = self.parse_listings(html)

            if not records:
                print(f"[DEBUG] No records found on page {page}, stopping.")
                break

            new_records = []
            for record in records:
                url = record["listing_url"]
                if url not in seen_urls:
                    seen_urls.add(url)
                    new_records.append(record)

            print(f"[DEBUG] Page {page}: parsed {len(records)}, new unique {len(new_records)}")

            if not new_records:
                print(f"[DEBUG] No new unique records on page {page}, stopping.")
                break

            all_records.extend(new_records)

            sleep_time = random.uniform(3, 7)
            print(f"[DEBUG] Sleeping {sleep_time:.1f}s before next page...")
            time.sleep(sleep_time)

        print(f"[DEBUG] Total unique Kijiji records collected: {len(all_records)}")
        return all_records