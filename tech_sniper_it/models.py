from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ProductCategory(str, Enum):
    PHOTOGRAPHY = "photography"
    APPLE_PHONE = "apple_phone"
    GENERAL_TECH = "general_tech"

    @classmethod
    def from_raw(cls, raw: str) -> "ProductCategory":
        value = (raw or "").strip().lower()
        if any(token in value for token in ("foto", "camera", "photography", "fotografia")):
            return cls.PHOTOGRAPHY
        if any(token in value for token in ("apple", "iphone", "phone", "telefono", "smartphone")):
            return cls.APPLE_PHONE
        return cls.GENERAL_TECH


@dataclass(slots=True)
class AmazonProduct:
    title: str
    price_eur: float
    category: ProductCategory
    ean: str | None = None
    url: str | None = None


@dataclass(slots=True)
class ValuationResult:
    platform: str
    normalized_name: str
    offer_eur: float | None
    condition: str = "grade_a"
    currency: str = "EUR"
    source_url: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def is_valid(self) -> bool:
        return self.error is None and self.offer_eur is not None


@dataclass(slots=True)
class ArbitrageDecision:
    product: AmazonProduct
    normalized_name: str
    offers: list[ValuationResult]
    best_offer: ValuationResult | None
    spread_eur: float | None
    should_notify: bool
    ai_provider: str | None = None
    ai_model: str | None = None
    ai_mode: str | None = None
    ai_used: bool = False
