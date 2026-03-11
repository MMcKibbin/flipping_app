import re
from typing import Optional


def parse_price(price_text: str | None) -> Optional[float]:
    if not price_text:
        return None

    cleaned = price_text.strip()
    cleaned = cleaned.replace("$", "")
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("CAD", "")
    cleaned = cleaned.replace("CA", "")
    cleaned = cleaned.strip()

    match = re.search(r"\d+(\.\d+)?", cleaned)
    if not match:
        return None

    try:
        return float(match.group())
    except ValueError:
        return None