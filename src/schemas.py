from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class ListingRecord(BaseModel):
    source_platform: str
    search_term: str
    listing_id: str
    observed_at: datetime = Field(default_factory=datetime.utcnow)

    title: str
    description: Optional[str] = None

    category: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    condition: Optional[str] = None

    price: Optional[float] = None
    currency: str = "CAD"

    location_country: Optional[str] = None
    location_state_province: Optional[str] = None
    location_city: Optional[str] = None

    shipping_available: Optional[bool] = None
    shipping_cost_est: Optional[float] = None
    pickup_only: Optional[bool] = None

    listing_url: Optional[HttpUrl] = None
    raw_payload_json: dict = Field(default_factory=dict)