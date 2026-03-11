import json
import time
import random
from datetime import datetime

import pandas as pd
from pytrends.request import TrendReq
from pytrends import exceptions

from src.db.duckdb_manager import db

CONFIG_PATH = "config/demand_keywords.json"


class GoogleTrendsCollector:
    def __init__(self):
        self.pytrends = TrendReq(hl="en-US", tz=360)
        self.keywords = self.load_keywords()

    def load_keywords(self):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

        if not isinstance(config, dict):
            raise ValueError("demand_keywords.json must be a dictionary at the top level.")

        keywords = []

        for category_name, category in config.items():
            if not isinstance(category, dict):
                raise ValueError(
                    f"Category '{category_name}' must be an object/dict, got {type(category).__name__}."
                )

            subcategories = category.get("subcategories", {})
            if not isinstance(subcategories, dict):
                raise ValueError(
                    f"Category '{category_name}' has invalid 'subcategories'. Expected object/dict."
                )

            for sub_name, sub in subcategories.items():
                if not isinstance(sub, dict):
                    raise ValueError(
                        f"Subcategory '{sub_name}' in '{category_name}' must be an object/dict."
                    )

                keywords.extend(sub.get("broad_keywords", []))
                keywords.extend(sub.get("brand_keywords", []))
                keywords.extend(sub.get("model_keywords", []))

        clean_keywords = sorted(set(
            k.strip() for k in keywords if isinstance(k, str) and k.strip()
        ))

        return clean_keywords

    def fetch_trends(self, keyword, max_retries=5):
        for attempt in range(1, max_retries + 1):
            try:
                self.pytrends.build_payload(
                    [keyword],
                    timeframe="today 12-m",
                    geo="CA"
                )

                data = self.pytrends.interest_over_time()

                if data.empty:
                    return None

                data = data.reset_index()

                data = data.rename(columns={
                    keyword: "interest_value",
                    "date": "trend_date"
                })

                data["keyword"] = keyword
                data["geo"] = "CA"
                data["pulled_at"] = datetime.utcnow()

                data = data[[
                    "keyword",
                    "geo",
                    "trend_date",
                    "interest_value",
                    "isPartial",
                    "pulled_at"
                ]]

                data = data.rename(columns={"isPartial": "is_partial"})
                return data

            except exceptions.TooManyRequestsError:
                wait_time = min(60, (2 ** attempt) + random.uniform(1, 3))
                print(f"[429] Rate limited on '{keyword}'. Waiting {wait_time:.1f}s before retry {attempt}/{max_retries}...")
                time.sleep(wait_time)

            except Exception as e:
                print(f"[ERROR] Failed for '{keyword}' on attempt {attempt}: {e}")
                wait_time = min(30, attempt * 2)
                time.sleep(wait_time)

        print(f"[SKIP] Giving up on keyword: {keyword}")
        return None

    def run(self):
        for keyword in self.keywords:
            print(f"Collecting trend data: {keyword}")

            df = self.fetch_trends(keyword)

            if df is not None:
                db.insert_dataframe(df, "trend_data_raw")
                print(f"[OK] Stored trend data for: {keyword}")
            else:
                print(f"[NO DATA] Nothing stored for: {keyword}")

            sleep_time = random.uniform(8, 15)
            print(f"Sleeping {sleep_time:.1f}s before next keyword...")
            time.sleep(sleep_time)