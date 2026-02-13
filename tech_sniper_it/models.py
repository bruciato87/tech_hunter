from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Any


class ProductCategory(str, Enum):
    PHOTOGRAPHY = "photography"
    APPLE_PHONE = "apple_phone"
    SMARTWATCH = "smartwatch"
    DRONE = "drone"
    HANDHELD_CONSOLE = "handheld_console"
    GENERAL_TECH = "general_tech"

    @classmethod
    def from_raw(cls, raw: str) -> "ProductCategory":
        value = (raw or "").strip().lower()
        if cls._is_apple_phone(value):
            return cls.APPLE_PHONE
        if cls._is_smartwatch(value):
            return cls.SMARTWATCH
        if cls._is_drone(value):
            return cls.DRONE
        if cls._is_handheld_console(value):
            return cls.HANDHELD_CONSOLE
        if cls._is_photography(value):
            return cls.PHOTOGRAPHY
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

    @staticmethod
    def _is_smartwatch(value: str) -> bool:
        tokens = (
            "smartwatch",
            "apple watch",
            "watch ultra",
            "garmin fenix",
            "garmin epix",
            "garmin forerunner",
            "galaxy watch",
            "suunto",
            "ticwatch",
        )
        return any(token in value for token in tokens)

    @staticmethod
    def _is_drone(value: str) -> bool:
        tokens = (
            "drone",
            "dji mini",
            "dji air",
            "dji mavic",
            "dji avata",
            "mavic",
            "quadcopter",
        )
        return any(token in value for token in tokens)

    @staticmethod
    def _is_handheld_console(value: str) -> bool:
        tokens = (
            "steam deck",
            "rog ally",
            "legion go",
            "handheld console",
            "console portatile",
            "portable console",
        )
        return any(token in value for token in tokens)


def to_legacy_storage_category(category: ProductCategory) -> str:
    # Supabase check constraints may still accept only legacy categories.
    if category in {ProductCategory.PHOTOGRAPHY, ProductCategory.APPLE_PHONE, ProductCategory.GENERAL_TECH}:
        return category.value
    return ProductCategory.GENERAL_TECH.value


@dataclass(slots=True)
class AmazonProduct:
    title: str
    price_eur: float
    category: ProductCategory
    ean: str | None = None
    url: str | None = None
    source_marketplace: str | None = None
    amazon_condition: str | None = None
    amazon_condition_confidence: float = 0.0
    amazon_packaging_only: bool = False


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
    spread_gross_eur: float | None = None
    operating_cost_eur: float = 0.0
    risk_buffer_eur: float = 0.0
    strategy_profile: str | None = None
    ai_provider: str | None = None
    ai_model: str | None = None
    ai_mode: str | None = None
    ai_used: bool = False
