import base64
import requests
from typing import Dict

from src.config import EBAY_CLIENT_ID, EBAY_CLIENT_SECRET, EBAY_OAUTH_URL


def get_ebay_access_token() -> str:
    """
    Request an OAuth token from eBay.
    """

    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise ValueError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET in .env")

    credentials = f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}",
    }

    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    response = requests.post(
        EBAY_OAUTH_URL,
        headers=headers,
        data=data,
        timeout=30
    )

    response.raise_for_status()

    payload: Dict = response.json()

    return payload["access_token"]