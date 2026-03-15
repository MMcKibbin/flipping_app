from __future__ import annotations

import re
import time
from dataclasses import dataclass, asdict
from typing import List, Optional

import pandas as pd
from playwright.sync_api import sync_playwright, Page


@dataclass
class FBListing:
    source: str
    search_term: str
    title: Optional[str]
    price: Optional[float]
    location_text: Optional[str]
    url: Optional[str]
    source_listing_id: Optional[str]


def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"\$\s*([\d,]+(?:\.\d{1,2})?)", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def extract_listing_id(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/marketplace/item/(\d+)", url)
    return m.group(1) if m else None


def build_search_url(query: str, city_slug: str = "calgary") -> str:
    q = query.replace(" ", "%20")
    return f"https://www.facebook.com/marketplace/{city_slug}/search?query={q}&exact=false"


def scroll_results(page: Page, rounds: int = 8, pause_sec: float = 2.0) -> None:
    for _ in range(rounds):
        page.mouse.wheel(0, 7000)
        time.sleep(pause_sec)


def scrape_search(page: Page, search_term: str) -> List[FBListing]:
    cards = page.locator('a[href*="/marketplace/item/"]')
    listings: List[FBListing] = []
    seen = set()

    count = cards.count()
    for i in range(count):
        try:
            card = cards.nth(i)
            href = card.get_attribute("href")
            if not href:
                continue

            if href.startswith("/"):
                href = "https://www.facebook.com" + href

            listing_id = extract_listing_id(href)
            if href in seen:
                continue
            seen.add(href)

            text = card.inner_text(timeout=1000)
            lines = [x.strip() for x in text.split("\n") if x.strip()]

            title = None
            price = None
            location_text = None

            for line in lines:
                if price is None:
                    price = parse_price(line)
                    if price is not None:
                        continue
                if title is None and "$" not in line:
                    title = line
                    continue
                location_text = line

            listings.append(
                FBListing(
                    source="facebook_marketplace",
                    search_term=search_term,
                    title=title,
                    price=price,
                    location_text=location_text,
                    url=href,
                    source_listing_id=listing_id,
                )
            )
        except Exception:
            continue

    return listings


def run_marketplace_scrape(search_term: str, city_slug: str = "calgary") -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        url = build_search_url(search_term, city_slug=city_slug)
        page.goto(url, wait_until="domcontentloaded")

        print("Log into Facebook in the opened browser if needed, then let results load.")
        input("Press Enter here after the marketplace results page is visible...")

        scroll_results(page)
        rows = scrape_search(page, search_term)

        browser.close()

    df = pd.DataFrame([asdict(x) for x in rows])
    if not df.empty:
        df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df


if __name__ == "__main__":
    search_term = "milwaukee drill"
    df = run_marketplace_scrape(search_term=search_term, city_slug="calgary")
    print(df.head(20))
    print(f"Scraped rows: {len(df)}")
    df.to_csv("data/facebook_marketplace_raw_preview.csv", index=False)
    print("Saved preview to data/facebook_marketplace_raw_preview.csv")
