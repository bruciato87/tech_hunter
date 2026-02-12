from __future__ import annotations

import pytest

from tech_sniper_it.ai_balancer import SmartAIBalancer
from tech_sniper_it.manager import ArbitrageManager
from tech_sniper_it.models import AmazonProduct, ProductCategory, ValuationResult


class FakeBalancer(SmartAIBalancer):
    async def normalize_product_name(self, title: str) -> str:
        return "iPhone 14 Pro 128GB"


class StaticValuator:
    def __init__(self, platform: str, offer: float | None, error: str | None = None) -> None:
        self.platform = platform
        self.offer = offer
        self.error = error

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        return ValuationResult(
            platform=self.platform,
            normalized_name=normalized_name,
            offer_eur=self.offer,
            error=self.error,
        )


class FakeStorage:
    def __init__(self) -> None:
        self.saved = []

    async def save_opportunity(self, decision) -> None:  # noqa: ANN001
        self.saved.append(decision)


class FakeNotifier:
    def __init__(self) -> None:
        self.notified = []

    async def notify(self, decision) -> None:  # noqa: ANN001
        self.notified.append(decision)


class ManagerUnderTest(ArbitrageManager):
    def __init__(self, valuators, *args, **kwargs) -> None:  # noqa: ANN001
        super().__init__(*args, **kwargs)
        self._valuators = valuators

    def _build_valuators(self, category: ProductCategory):  # noqa: ANN001
        return self._valuators


@pytest.mark.asyncio
async def test_manager_selects_best_offer_and_notifies() -> None:
    storage = FakeStorage()
    notifier = FakeNotifier()
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("trenddevice", 610.0),
            StaticValuator("rebuy", 590.0),
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        storage=storage,
        notifier=notifier,
        min_spread_eur=80.0,
    )
    product = AmazonProduct(title="Apple iPhone 14 Pro 128GB", price_eur=500.0, category=ProductCategory.APPLE_PHONE)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "trenddevice"
    assert decision.spread_eur == 110.0
    assert decision.should_notify is True
    assert len(storage.saved) == 1
    assert len(notifier.notified) == 1


@pytest.mark.asyncio
async def test_manager_does_not_notify_when_spread_is_low() -> None:
    storage = FakeStorage()
    notifier = FakeNotifier()
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("trenddevice", 560.0),
            StaticValuator("rebuy", 555.0),
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        storage=storage,
        notifier=notifier,
        min_spread_eur=80.0,
    )
    product = AmazonProduct(title="Apple iPhone 14 Pro 128GB", price_eur=500.0, category=ProductCategory.APPLE_PHONE)

    decision = await manager.evaluate_product(product)

    assert decision.should_notify is False
    assert len(storage.saved) == 0
    assert len(notifier.notified) == 0


@pytest.mark.asyncio
async def test_manager_handles_all_invalid_offers() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("trenddevice", None, error="timeout"),
            StaticValuator("rebuy", None, error="not_found"),
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=80.0,
    )
    product = AmazonProduct(title="Apple iPhone 14 Pro 128GB", price_eur=500.0, category=ProductCategory.APPLE_PHONE)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.spread_eur is None
    assert decision.should_notify is False
