from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Any


class ProductCategory(str, Enum):
    PHOTOGRAPHY = "photography"
    APPLE_PHONE = "apple_phone"
    GENERAL_TECH = "general_tech"

    @classmethod
    def from_raw(cls, raw: str) -> "ProductCategory":
        value = (raw or "").strip().lower()
        if cls._is_photography(value):
            return cls.PHOTOGRAPHY
        if cls._is_apple_phone(value):
            return cls.APPLE_PHONE
        return cls.GENERAL_TECH

    @staticmethod
    def _is_photography(value: str) -> bool:
        tokens = (
            "fotografia",
            "photography",
            "fotocamera",
            "camera",
            "mirrorless",
            "dslr",
            "obiettivo",
            "lens",
            "canon eos",
            "sony alpha",
            "nikon z",
            "fujifilm x",
            "lumix",
        )
        return any(token in value for token in tokens)

    @staticmethod
    def _is_apple_phone(value: str) -> bool:
        # iPhone-focused category: avoid routing generic Apple laptops/tablets to phone-only valuators.
        if "iphone" in value:
            return True
        apple_token = "apple" in value
        phone_tokens = (
            "phone",
            "telefono",
            "smartphone",
            "cellulare",
            "ios",
            "sim",
        )
        if apple_token and any(token in value for token in phone_tokens):
            return True
        if re.search(r"\biphone\s*\d+\b", value):
            return True
        return False


@dataclass(slots=True)
class AmazonProduct:
    title: str
    price_eur: float
    category: ProductCategory
    ean: str | None = None
    url: str | None = None
    source_marketplace: str | None = None


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
