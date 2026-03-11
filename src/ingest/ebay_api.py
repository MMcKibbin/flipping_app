import os
import base64
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv(Path(".env"))


class EbayAPIClient:
    def __init__(self):
        self.client_id = os.getenv("EBAY_CLIENT_ID")
        self.client_secret = os.getenv("EBAY_CLIENT_SECRET")
        self.env = os.getenv("EBAY_ENV", "production").lower()

        if not self.client_id or not self.client_secret:
            raise ValueError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET in .env")

        if self.env == "sandbox":
            self.oauth_url = "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
            self.api_base = "https://api.sandbox.ebay.com"
        else:
            self.oauth_url = "https://api.ebay.com/identity/v1/oauth2/token"
            self.api_base = "https://api.ebay.com"

        self._token = None
        self._token_expiry = None

    def _basic_auth_header(self) -> str:
        raw = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        encoded = base64.b64encode(raw).decode("utf-8")
        return f"Basic {encoded}"

    def get_access_token(self) -> str:
        if self._token and self._token_expiry and datetime.utcnow() < self._token_expiry:
            return self._token

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self._basic_auth_header(),
        }

        data = {
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        }

        response = requests.post(self.oauth_url, headers=headers, data=data, timeout=30)
        response.raise_for_status()

        payload = response.json()

        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 7200))
        self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in - 60)

        return self._token

    def search_items(self, query: str, limit: int = 10, offset: int = 0, category_ids: str | None = None):
        token = self.get_access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_CA",
        }

        params = {
            "q": query,
            "limit": limit,
            "offset": offset,
        }

        if category_ids:
            params["category_ids"] = category_ids

        url = f"{self.api_base}/buy/browse/v1/item_summary/search"
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()