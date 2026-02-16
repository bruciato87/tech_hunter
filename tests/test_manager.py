from __future__ import annotations

import asyncio

import pytest

from tech_sniper_it.ai_balancer import SmartAIBalancer
from tech_sniper_it.manager import ArbitrageManager, build_default_manager
from tech_sniper_it.models import AmazonProduct, ProductCategory, ValuationResult


class FakeBalancer(SmartAIBalancer):
    async def normalize_product_name(self, title: str) -> str:
        return "iPhone 14 Pro 128GB"


class TitleBalancer(SmartAIBalancer):
    async def normalize_product_name(self, title: str) -> str:
        return title


class StaticValuator:
    def __init__(
        self,
        platform: str,
        offer: float | None,
        error: str | None = None,
        *,
        source_url: str | None = None,
        raw_payload: dict | None = None,
    ) -> None:
        self.platform = platform
        self.offer = offer
        self.error = error
        self.source_url = source_url
        self.raw_payload = raw_payload or {}

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        return ValuationResult(
            platform=self.platform,
            normalized_name=normalized_name,
            offer_eur=self.offer,
            error=self.error,
            source_url=self.source_url,
            raw_payload=dict(self.raw_payload),
        )


class QueryRetryValuator:
    def __init__(self, platform: str) -> None:
        self.platform_name = platform
        self.queries: list[str] = []

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        self.queries.append(normalized_name)
        if "valve" not in normalized_name.lower():
            return ValuationResult(
                platform=self.platform_name,
                normalized_name=normalized_name,
                offer_eur=None,
                error=f"{self.platform_name} low-confidence match (low-token-similarity); discarded to prevent false-positive.",
                raw_payload={
                    "price_source": "dom",
                    "match_quality": {"ok": False, "reason": "low-token-similarity"},
                },
            )
        if self.platform_name == "rebuy":
            source_url = "https://www.rebuy.it/comprare/valve-steam-deck-oled-1tb/123456"
        else:
            source_url = "https://www.trendevice.com/vendi/valutazione/prodotto/valve-steam-deck-oled-1tb"
        return ValuationResult(
            platform=self.platform_name,
            normalized_name=normalized_name,
            offer_eur=320.0,
            source_url=source_url,
            raw_payload={
                "price_text": "Ti paghiamo 320,00 €",
                "price_source": "dom",
                "match_quality": {"ok": True, "reason": "ok"},
            },
        )


class SlowValuator:
    def __init__(self, platform: str, delay_seconds: float) -> None:
        self.platform_name = platform
        self.delay_seconds = delay_seconds

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        await asyncio.sleep(self.delay_seconds)
        return ValuationResult(
            platform=self.platform_name,
            normalized_name=normalized_name,
            offer_eur=300.0,
            source_url="https://example.test/sell/item/123",
            raw_payload={"price_text": "300,00 €", "price_source": "dom"},
        )


class FakeStorage:
    def __init__(self) -> None:
        self.saved = []

    async def save_opportunity(self, decision) -> None:  # noqa: ANN001
        self.saved.append(decision)


class FakeCacheStorage(FakeStorage):
    def __init__(self, cached_quote: dict | None) -> None:
        super().__init__()
        self.cached_quote = cached_quote
        self.cache_calls: list[dict[str, object]] = []

    async def get_recent_platform_quote_cache(self, **kwargs):  # noqa: ANN003
        self.cache_calls.append(kwargs)
        return self.cached_quote


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


class RetryMetaBalancer(SmartAIBalancer):
    async def normalize_with_meta(self, title: str) -> tuple[str, dict]:  # type: ignore[override]
        normalized = title.replace("Valve ", "", 1)
        return normalized, {
            "provider": "test",
            "model": "mock",
            "mode": "live",
            "ai_used": True,
        }


class BrokenWatchBalancer(SmartAIBalancer):
    async def normalize_with_meta(self, title: str) -> tuple[str, dict]:  # type: ignore[override]
        return "iPhone 13 Pro 256GB", {
            "provider": "openrouter",
            "model": "mock",
            "mode": "live",
            "ai_used": True,
        }


class HallucinatedStorageWatchBalancer(SmartAIBalancer):
    async def normalize_with_meta(self, title: str) -> tuple[str, dict]:  # type: ignore[override]
        return "Apple Watch Ultra GPS + Cellular 49mm 32GB", {
            "provider": "openrouter",
            "model": "mock",
            "mode": "live",
            "ai_used": True,
        }


class HallucinatedGenerationWatchBalancer(SmartAIBalancer):
    async def normalize_with_meta(self, title: str) -> tuple[str, dict]:  # type: ignore[override]
        return "Apple Watch Ultra 3 GPS + Cellular 49mm", {
            "provider": "openrouter",
            "model": "mock",
            "mode": "live",
            "ai_used": True,
        }


class CaptureQueryValuator:
    def __init__(self, platform: str = "trenddevice") -> None:
        self.platform_name = platform
        self.queries: list[str] = []

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        self.queries.append(normalized_name)
        return ValuationResult(
            platform=self.platform_name,
            normalized_name=normalized_name,
            offer_eur=150.0,
            source_url="https://www.trendevice.com/vendi/valutazione/?model=800&request=1234",
            raw_payload={
                "price_text": "Ti paghiamo 150,00 €",
                "price_source": "api",
                "match_quality": {"ok": True, "reason": "ok", "has_model_step": True, "token_ratio": 0.9},
            },
        )


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


@pytest.mark.asyncio
async def test_manager_applies_condition_risk_and_operating_cost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STRATEGY_PROFILE", "conservative")
    manager = ManagerUnderTest(
        valuators=[StaticValuator("rebuy", 640.0)],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=95.0,
    )
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
        amazon_condition="acceptable",
        amazon_condition_confidence=1.0,
    )

    decision = await manager.evaluate_product(product)

    assert decision.spread_gross_eur == 140.0
    assert decision.operating_cost_eur == 8.0
    assert decision.risk_buffer_eur == 34.0
    assert decision.spread_eur == 98.0
    assert decision.should_notify is True
    assert decision.strategy_profile == "conservative"


@pytest.mark.asyncio
async def test_manager_invalid_profile_falls_back_to_balanced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STRATEGY_PROFILE", "invalid-profile")
    manager = ManagerUnderTest(
        valuators=[StaticValuator("rebuy", 640.0)],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=100.0,
    )
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
        amazon_condition="acceptable",
        amazon_condition_confidence=1.0,
    )

    decision = await manager.evaluate_product(product)

    assert decision.operating_cost_eur == 0.0
    assert decision.risk_buffer_eur == 26.0
    assert decision.spread_eur == 114.0
    assert decision.strategy_profile == "balanced"


def test_manager_build_valuators_routes_new_categories() -> None:
    manager = ArbitrageManager(
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )

    assert [v.platform_name for v in manager._build_valuators(ProductCategory.SMARTWATCH)] == ["trenddevice", "rebuy"]
    assert [v.platform_name for v in manager._build_valuators(ProductCategory.DRONE)] == ["mpb", "rebuy"]
    assert [v.platform_name for v in manager._build_valuators(ProductCategory.HANDHELD_CONSOLE)] == ["rebuy"]


@pytest.mark.asyncio
async def test_manager_runtime_filter_skips_trenddevice_for_non_apple_smartwatch() -> None:
    trend = CaptureQueryValuator("trenddevice")
    manager = ManagerUnderTest(
        valuators=[
            trend,
            StaticValuator(
                "rebuy",
                126.92,
                source_url="https://www.rebuy.it/vendere/smartwatch/garmin-forerunner-955-schwarz-am-silikonarmband-schwarz_13303170",
                raw_payload={
                    "price_text": "Pagamento Diretto 126,92 €",
                    "price_source": "ry-inject",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.9},
                },
            ),
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Garmin Forerunner 955 Solar WH",
        price_eur=100.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert trend.queries == []
    assert decision.best_offer is not None
    assert decision.best_offer.platform == "rebuy"
    assert [offer.platform for offer in decision.offers] == ["rebuy"]


def test_build_default_manager_uses_fallback_supabase_table(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    class FakeStorage:
        def __init__(self, url: str, key: str, table: str) -> None:
            captured["url"] = url
            captured["key"] = key
            captured["table"] = table

    monkeypatch.setattr("tech_sniper_it.manager.SupabaseStorage", FakeStorage)
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "service-role-key")
    monkeypatch.setenv("SUPABASE_TABLE", "")
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("GEMINI_API_KEYS", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEYS", raising=False)
    monkeypatch.delenv("MIN_SPREAD_EUR", raising=False)
    monkeypatch.delenv("HEADLESS", raising=False)
    monkeypatch.delenv("PLAYWRIGHT_NAV_TIMEOUT_MS", raising=False)

    manager = build_default_manager()

    assert manager.storage is not None
    assert captured["table"] == "arbitrage_opportunities"


@pytest.mark.asyncio
async def test_manager_circuit_breaker_skips_mpb_for_queued_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALUATOR_CIRCUIT_BREAKER_ENABLED", "true")
    monkeypatch.setenv("VALUATOR_BACKOFF_MPB_ERRORS", "1")

    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("mpb", None, error="MPB blocked by anti-bot challenge (turnstile/cloudflare)."),
            StaticValuator("rebuy", 120.0),
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    items = [
        AmazonProduct(title="Canon EOS R7", price_eur=500.0, category=ProductCategory.PHOTOGRAPHY),
        AmazonProduct(title="Nikon Z fc", price_eur=500.0, category=ProductCategory.PHOTOGRAPHY),
    ]

    decisions = await manager.evaluate_many(items, max_parallel_products=1)

    assert len(decisions) == 2
    first_platforms = [offer.platform for offer in decisions[0].offers]
    second_platforms = [offer.platform for offer in decisions[1].offers]
    assert "mpb" in first_platforms
    assert second_platforms == ["rebuy"]


@pytest.mark.asyncio
async def test_manager_quote_verification_rejects_rebuy_generic_category_url() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                220.0,
                source_url="https://www.rebuy.it/comprare/apple",
                raw_payload={
                    "price_text": "Ti paghiamo 220,00 €",
                    "match_quality": {"ok": True, "reason": "ok"},
                    "price_source": "dom",
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Apple iPad Air 13 M3 256GB", price_eur=120.0, category=ProductCategory.GENERAL_TECH)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)
    assert "generic-source-url" in str(decision.offers[0].error)


@pytest.mark.asyncio
async def test_manager_quote_verification_accepts_rebuy_specific_product_url() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                220.0,
                source_url="https://www.rebuy.it/comprare/apple-iphone-14-pro-128gb-nero/123456",
                raw_payload={
                    "price_text": "Ti paghiamo 220,00 €",
                    "match_quality": {"ok": True, "reason": "ok"},
                    "price_source": "dom",
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Apple iPhone 14 Pro 128GB", price_eur=100.0, category=ProductCategory.APPLE_PHONE)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "rebuy"
    assert decision.best_offer.offer_eur == 220.0
    assert decision.should_notify is True


@pytest.mark.asyncio
async def test_manager_quote_verification_rejects_rebuy_variant_storage_mismatch() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                618.9,
                source_url="https://www.rebuy.it/vendere/tablet-e-ebook-reader/apple-ipad-air-6-13-1tb-wifi-plus-cellulare-blu_15426786",
                raw_payload={
                    "price_text": "Pagamento Diretto 618,90 €",
                    "price_source": "ry-inject",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.78},
                },
            )
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title='Apple iPad Air 13" M3 256GB', price_eur=121.8, category=ProductCategory.GENERAL_TECH)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)
    assert "variant-storage-mismatch" in str(decision.offers[0].error)


@pytest.mark.asyncio
async def test_manager_quote_verification_rejects_rebuy_variant_display_size_mismatch() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                796.74,
                source_url="https://www.rebuy.it/vendere/tablet-e-ebook-reader/apple-ipad-pro-13-512gb-wifi-modello-2024-nero-siderale_15426695",
                raw_payload={
                    "price_text": "Pagamento Diretto 796,74 €",
                    "price_source": "ry-inject",
                    "query": "Apple iPad Pro 10.5 512GB Wi-Fi",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.86},
                },
            )
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Apple iPad Pro 10.5 512GB Wi-Fi", price_eur=349.0, category=ProductCategory.GENERAL_TECH)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)
    assert "variant-display-size-mismatch" in str(decision.offers[0].error)


@pytest.mark.asyncio
async def test_manager_quote_verification_rejects_rebuy_phone_vs_tablet_brand_mismatch() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                361.44,
                source_url=(
                    "https://www.rebuy.it/vendere/tablet-e-ebook-reader/"
                    "microsoft-surface-pro-8-13-intel-evo-i5-256gb-ssd-8gb-ram-wifi-platino_12283088"
                ),
                raw_payload={
                    "price_text": "Pagamento Diretto 361,44 €",
                    "price_source": "ry-inject",
                    "query": "Xiaomi Redmi Note 12 Pro+ 5G 8GB RAM 256GB Midnight",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.72},
                },
            )
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Xiaomi Redmi Note 12 Pro+ 5G 8GB RAM 256GB Midnight Black",
        price_eur=279.9,
        category=ProductCategory.GENERAL_TECH,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)
    assert (
        "source-brand-mismatch" in str(decision.offers[0].error)
        or "source-device-class-mismatch" in str(decision.offers[0].error)
    )


@pytest.mark.asyncio
async def test_manager_quote_verification_accepts_rebuy_generic_url_with_strong_override() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                520.0,
                source_url="https://www.rebuy.it/vendere/smartwatch",
                raw_payload={
                    "price_text": "Pagamento Diretto 520,00 €",
                    "price_source": "dom-cash",
                    "wizard_states": [{"attempt": 1, "state": "step1"}, {"attempt": 2, "state": "offer"}],
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.81, "generic_override": True},
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Apple Watch Series 9 GPS + Cellular 45mm",
        price_eur=240.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "rebuy"
    assert decision.best_offer.offer_eur == 520.0
    assert decision.should_notify is True


@pytest.mark.asyncio
async def test_manager_quote_verification_rejects_mpb_generic_search_url() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "mpb",
                480.0,
                source_url="https://www.mpb.com/it-it/cerca?q=canon+eos+r7",
                raw_payload={
                    "price_text": "Ti paghiamo 480,00 €",
                    "price_source": "direct_search",
                    "match_quality": {"ok": False, "reason": "generic-url"},
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Canon EOS R7 Body", price_eur=200.0, category=ProductCategory.PHOTOGRAPHY)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)


@pytest.mark.asyncio
async def test_manager_quote_verification_accepts_mpb_specific_sell_url() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "mpb",
                480.0,
                source_url="https://www.mpb.com/it-it/sell/product/canon-eos-r7/12345",
                raw_payload={
                    "price_text": "Ti paghiamo 480,00 €",
                    "price_source": "direct_sell_link",
                    "match_quality": {"ok": True, "reason": "ok"},
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Canon EOS R7 Body", price_eur=200.0, category=ProductCategory.PHOTOGRAPHY)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "mpb"
    assert decision.best_offer.offer_eur == 480.0
    assert decision.should_notify is True


@pytest.mark.asyncio
async def test_manager_quote_verification_accepts_mpb_api_generic_url_with_model_evidence() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "mpb",
                510.0,
                source_url="https://www.mpb.com/it-it/cerca?q=canon+eos+r7",
                raw_payload={
                    "price_text": "purchase_value=510.00 EUR condition=excellent",
                    "price_source": "api_purchase_price",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.83},
                    "api_purchase_price_result": {
                        "model_id": "69474",
                        "model_name": "Canon EOS R7 Body",
                    },
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Canon EOS R7 Body", price_eur=200.0, category=ProductCategory.PHOTOGRAPHY)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "mpb"
    assert decision.best_offer.offer_eur == 510.0
    assert decision.should_notify is True


@pytest.mark.asyncio
async def test_manager_quote_verification_accepts_mpb_api_when_match_quality_is_false() -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "mpb",
                685.0,
                source_url="https://www.mpb.com/it-it/sell/product/canon-eos-r7/77097",
                raw_payload={
                    "price_text": "purchase_value=685.00 EUR condition=excellent",
                    "price_source": "api_purchase_price",
                    "match_quality": {"ok": False, "reason": "score-too-low", "token_ratio": 0.5},
                    "api_purchase_price_result": {
                        "model_id": "77097",
                        "model_name": "Canon EOS R7 Body",
                    },
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(title="Canon EOS R7 Body", price_eur=200.0, category=ProductCategory.PHOTOGRAPHY)

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "mpb"
    assert decision.best_offer.offer_eur == 685.0
    assert decision.should_notify is True


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["rebuy", "trenddevice"])
async def test_manager_query_variant_retry_recovers_rebuy_and_trenddevice(platform: str) -> None:
    valuator = QueryRetryValuator(platform)
    manager = ManagerUnderTest(
        valuators=[valuator],
        ai_balancer=RetryMetaBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Valve Steam Deck OLED 1TB",
        price_eur=100.0,
        category=ProductCategory.GENERAL_TECH,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == platform
    assert decision.best_offer.offer_eur == 320.0
    assert decision.should_notify is True
    assert len(valuator.queries) >= 2
    assert any("Valve" in query for query in valuator.queries)


@pytest.mark.asyncio
async def test_manager_evaluate_many_clamps_invalid_parallel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALUATOR_TIMEOUT_REBUY_SECONDS", "5")
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "rebuy",
                120.0,
                source_url="https://www.rebuy.it/vendere/smartphone/apple-iphone-14_123",
                raw_payload={
                    "price_text": "Pagamento Diretto 120,00 €",
                    "price_source": "dom-cash",
                    "wizard_states": [{"state": "offer"}],
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.91},
                },
            )
        ],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=10.0,
    )
    product = AmazonProduct(title="Apple iPhone 14 128GB", price_eur=100.0, category=ProductCategory.APPLE_PHONE)

    decisions = await asyncio.wait_for(manager.evaluate_many([product], max_parallel_products=0), timeout=2.0)
    assert len(decisions) == 1


@pytest.mark.asyncio
async def test_manager_applies_valuator_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tech_sniper_it.manager._valuator_timeout_seconds", lambda _platform: 0.01)
    manager = ManagerUnderTest(
        valuators=[SlowValuator("rebuy", delay_seconds=0.06)],
        ai_balancer=FakeBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Valve Steam Deck OLED 1TB",
        price_eur=220.0,
        category=ProductCategory.GENERAL_TECH,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.offers
    assert "timeout" in (decision.offers[0].error or "").lower()


@pytest.mark.asyncio
async def test_manager_does_not_circuit_break_trenddevice_after_single_email_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALUATOR_CIRCUIT_BREAKER_ENABLED", "true")
    monkeypatch.setenv("VALUATOR_BACKOFF_TRENDDEVICE_ERRORS", "1")

    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("trenddevice", None, error="TrendDevice price not found after wizard (email-gate-submitted)."),
            StaticValuator("rebuy", 150.0),
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    items = [
        AmazonProduct(title="Apple Watch Series 9 GPS + Cellular 45mm", price_eur=300.0, category=ProductCategory.SMARTWATCH),
        AmazonProduct(title="Apple Watch Ultra 2 GPS + Cellular 49mm", price_eur=500.0, category=ProductCategory.SMARTWATCH),
    ]

    decisions = await manager.evaluate_many(items, max_parallel_products=1)

    assert len(decisions) == 2
    assert "trenddevice" in [offer.platform for offer in decisions[0].offers]
    assert "trenddevice" in [offer.platform for offer in decisions[1].offers]


@pytest.mark.asyncio
async def test_manager_ai_safeguard_rewrites_watch_phone_category_mismatch() -> None:
    valuator = CaptureQueryValuator("trenddevice")
    manager = ManagerUnderTest(
        valuators=[valuator],
        ai_balancer=BrokenWatchBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Apple Watch Ultra (GPS + Cellular, 49mm)",
        price_eur=350.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert "watch" in decision.normalized_name.lower()
    assert "iphone" not in decision.normalized_name.lower()
    assert valuator.queries
    assert "watch" in valuator.queries[0].lower()
    assert "iphone" not in valuator.queries[0].lower()


@pytest.mark.asyncio
async def test_manager_ai_safeguard_removes_untrusted_watch_storage() -> None:
    valuator = CaptureQueryValuator("trenddevice")
    manager = ManagerUnderTest(
        valuators=[valuator],
        ai_balancer=HallucinatedStorageWatchBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Apple Watch Ultra (GPS + Cellular, 49mm)",
        price_eur=350.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert "32gb" not in decision.normalized_name.lower()
    assert valuator.queries
    assert "32gb" not in valuator.queries[0].lower()


@pytest.mark.asyncio
async def test_manager_ai_safeguard_removes_untrusted_watch_generation() -> None:
    valuator = CaptureQueryValuator("trenddevice")
    manager = ManagerUnderTest(
        valuators=[valuator],
        ai_balancer=HallucinatedGenerationWatchBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Apple Watch Ultra (GPS + Cellular, 49mm)",
        price_eur=350.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert "ultra 3" not in decision.normalized_name.lower()
    assert "apple watch ultra" in decision.normalized_name.lower()
    assert valuator.queries
    assert "ultra 3" not in valuator.queries[0].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("platform", "source_url", "payload"),
    [
        (
            "rebuy",
            "https://www.rebuy.it/vendere/smartwatch/apple-watch-ultra-3-49-mm-cassa-in-titanio-nero-wifi-plus-cellulare_21513678",
            {
                "price_text": "Pagamento Diretto 512,08 €",
                "price_source": "ry-inject",
                "query": "Apple Watch Ultra GPS + Cellular 49mm",
                "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.84},
            },
        ),
        (
            "trenddevice",
            "https://www.trendevice.com/vendi/valutazione/",
            {
                "price_text": "Ti paghiamo 520,00 €",
                "price_source": "api",
                "query": "Apple Watch Ultra GPS + Cellular 49mm",
                "match_quality": {"ok": True, "reason": "ok", "has_model_step": True, "token_ratio": 0.91},
                "wizard_steps": [
                    {"name": "Modello", "selected": "Apple Watch Ultra 3 49mm"},
                    {"name": "Condizione", "selected": "Grado A"},
                ],
            },
        ),
    ],
)
async def test_manager_quote_verification_rejects_watch_generation_mismatch(
    platform: str,
    source_url: str,
    payload: dict,
) -> None:
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                platform,
                512.08,
                source_url=source_url,
                raw_payload=payload,
            )
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        min_spread_eur=40.0,
    )
    product = AmazonProduct(
        title="Apple Watch Ultra GPS + Cellular 49mm",
        price_eur=379.0,
        category=ProductCategory.SMARTWATCH,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is None
    assert decision.should_notify is False
    assert decision.offers[0].error is not None
    assert "quote verification failed" in str(decision.offers[0].error)
    assert "source-watch-generation-mismatch" in str(decision.offers[0].error)


@pytest.mark.asyncio
async def test_manager_applies_mpb_cache_fallback_on_transient_failure() -> None:
    storage = FakeCacheStorage(
        {
            "offer_eur": 640.0,
            "platform": "mpb",
            "source_url": "https://www.mpb.com/it-it/sell/model/123",
            "condition": "ottimo",
            "currency": "EUR",
            "created_at": "2026-02-14T10:00:00+00:00",
            "origin": "offers_payload",
        }
    )
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator(
                "mpb",
                None,
                error="MPB blocked by anti-bot challenge (turnstile/cloudflare).",
                raw_payload={"price_source": "api"},
            ),
            StaticValuator(
                "rebuy",
                590.0,
                source_url="https://www.rebuy.it/vendere/p/dji-mini-4-pro/12345",
                raw_payload={
                    "price_text": "Pagamento Diretto 590,00 €",
                    "price_source": "ry-inject",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.95},
                },
            ),
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        storage=storage,
        min_spread_eur=20.0,
    )
    product = AmazonProduct(
        title="DJI Mini 4 Pro",
        price_eur=500.0,
        category=ProductCategory.DRONE,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "mpb"
    assert decision.best_offer.offer_eur == 640.0
    assert storage.cache_calls
    mpb_offer = next(offer for offer in decision.offers if offer.platform == "mpb")
    assert mpb_offer.error is None
    assert mpb_offer.raw_payload.get("price_source") == "mpb-cache"


@pytest.mark.asyncio
async def test_manager_does_not_apply_mpb_cache_for_non_transient_error() -> None:
    storage = FakeCacheStorage(
        {
            "offer_eur": 640.0,
            "platform": "mpb",
            "source_url": "https://www.mpb.com/it-it/sell/model/123",
            "condition": "ottimo",
            "currency": "EUR",
            "created_at": "2026-02-14T10:00:00+00:00",
            "origin": "offers_payload",
        }
    )
    manager = ManagerUnderTest(
        valuators=[
            StaticValuator("mpb", None, error="mpb low-confidence match (token mismatch)."),
            StaticValuator(
                "rebuy",
                580.0,
                source_url="https://www.rebuy.it/vendere/p/canon-eos-r7/12345",
                raw_payload={
                    "price_text": "Pagamento Diretto 580,00 €",
                    "price_source": "ry-inject",
                    "match_quality": {"ok": True, "reason": "ok", "token_ratio": 0.95},
                },
            ),
        ],
        ai_balancer=TitleBalancer(gemini_keys=[], openrouter_keys=[]),
        storage=storage,
        min_spread_eur=20.0,
    )
    product = AmazonProduct(
        title="Canon EOS R7",
        price_eur=500.0,
        category=ProductCategory.PHOTOGRAPHY,
    )

    decision = await manager.evaluate_product(product)

    assert decision.best_offer is not None
    assert decision.best_offer.platform == "rebuy"
    assert storage.cache_calls == []
    mpb_offer = next(offer for offer in decision.offers if offer.platform == "mpb")
    assert mpb_offer.offer_eur is None
