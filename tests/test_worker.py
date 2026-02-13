from __future__ import annotations

import json
from pathlib import Path

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.worker import (
    _ai_usage_label,
    _ai_usage_stats,
    _amazon_search_url,
    _build_dynamic_warehouse_queries,
    _build_prioritization_context,
    _chunk_telegram_text,
    _coerce_product,
    _daily_exclusion_since_iso,
    _dedupe_products,
    _exclude_non_profitable_candidates,
    _filter_non_core_device_candidates,
    _format_scan_summary,
    _is_truthy_env,
    _normalize_http_url,
    _offer_log_payload,
    _parse_last_limit,
    _prioritize_products,
    _resolve_command,
    _run_scan_command,
    _run_status_command,
    _select_balanced_candidates,
    _save_non_profitable_decisions,
    _safe_error_details,
    _send_telegram_message,
    _split_complete_quote_decisions,
    load_products,
    run_worker,
)


def test_coerce_product_valid() -> None:
    product = _coerce_product({"title": "iPhone", "price_eur": 100, "category": "apple iphone"})
    assert product.title == "iPhone"
    assert product.category == ProductCategory.APPLE_PHONE


def test_coerce_product_detects_smartwatch_and_condition() -> None:
    product = _coerce_product(
        {
            "title": "Garmin Fenix 7 Pro Usato - Condizioni accettabili",
            "price_eur": 399.0,
            "category": "",
        }
    )
    assert product.category == ProductCategory.SMARTWATCH
    assert product.amazon_condition == "acceptable"


def test_coerce_product_keeps_source_marketplace() -> None:
    product = _coerce_product(
        {
            "title": "Canon EOS R50",
            "price_eur": 700,
            "category": "photography",
            "source_marketplace": "DE",
        }
    )
    assert product.source_marketplace == "de"


def test_filter_non_core_device_candidates_drops_macbook_cover() -> None:
    cover = AmazonProduct(
        title='Soonjet compatible avec MacBook Air 13" A2337 A2179 A1932 coque',
        price_eur=24.99,
        category=ProductCategory.GENERAL_TECH,
        source_marketplace="fr",
    )
    device = AmazonProduct(
        title="Apple MacBook Air 13 M1 8GB 256GB",
        price_eur=699.0,
        category=ProductCategory.GENERAL_TECH,
        source_marketplace="it",
    )
    kept, dropped = _filter_non_core_device_candidates([cover, device])
    assert len(kept) == 1
    assert kept[0].title == "Apple MacBook Air 13 M1 8GB 256GB"
    assert len(dropped) == 1
    assert "accessory" in dropped[0]


def test_filter_non_core_device_candidates_keeps_device_with_cover_included() -> None:
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB con cover inclusa",
        price_eur=699.0,
        category=ProductCategory.APPLE_PHONE,
        source_marketplace="it",
    )
    kept, dropped = _filter_non_core_device_candidates([product])
    assert len(kept) == 1
    assert len(dropped) == 0


def test_filter_non_core_device_candidates_keeps_high_price_compatible_bundle() -> None:
    product = AmazonProduct(
        title='Apple iPhone 14 Pro 256GB compatible with MagSafe, case included',
        price_eur=799.0,
        category=ProductCategory.APPLE_PHONE,
        source_marketplace="fr",
    )
    kept, dropped = _filter_non_core_device_candidates([product])
    assert len(kept) == 1
    assert len(dropped) == 0


def test_coerce_product_invalid_missing_title() -> None:
    with pytest.raises(ValueError):
        _coerce_product({"price_eur": 100})


def test_resolve_command_priority() -> None:
    assert _resolve_command({}, {"command": "status"}) == "status"
    assert _resolve_command({"action": "last"}, {}) == "last"
    assert _resolve_command({"action": "unknown"}, {}) == "scan"


def test_parse_last_limit_bounds() -> None:
    assert _parse_last_limit({"limit": 0}) == 1
    assert _parse_last_limit({"limit": 999}) == 10
    assert _parse_last_limit({"limit": "x"}) == 5


def test_chunk_telegram_text_splits_large_payload() -> None:
    payload = "x" * 9001
    chunks = _chunk_telegram_text(payload, limit=4000)
    assert len(chunks) == 3
    assert len(chunks[0]) == 4000
    assert len(chunks[1]) == 4000
    assert len(chunks[2]) == 1001


def test_safe_error_details_truncates() -> None:
    err = RuntimeError("boom " + ("x" * 500))
    details = _safe_error_details(err, max_len=80)
    assert len(details) == 80
    assert details.endswith("...")


def test_offer_log_payload_truncates_error() -> None:
    class DummyOffer:
        platform = "rebuy"
        offer_eur = 123.45
        condition = "come_nuovo"
        currency = "EUR"
        error = "timeout " + ("x" * 400)
        source_url = "https://rebuy.it/item"
        is_valid = False

    payload = _offer_log_payload(DummyOffer())
    assert payload["platform"] == "rebuy"
    assert payload["offer_eur"] == 123.45
    assert payload["valid"] is False
    assert payload["error"] is not None
    assert len(payload["error"]) <= 220


def test_offer_log_payload_includes_ui_probe_summary() -> None:
    class DummyOffer:
        platform = "trenddevice"
        offer_eur = None
        condition = "grado_a"
        currency = "EUR"
        error = "price missing"
        source_url = None
        is_valid = False
        raw_payload = {
            "ui_probes": [{"drift_suspected": True}, {"drift_suspected": False}],
            "adaptive_fallbacks": {"search_semantic": True},
        }

    payload = _offer_log_payload(DummyOffer())
    assert payload["ui_drift"] is True
    assert payload["ui_probe_count"] == 2
    assert payload["adaptive_fallbacks"] == {"search_semantic": True}


def test_split_complete_quote_decisions_rejects_missing_required_platform_quotes() -> None:
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
    )
    complete = type(
        "Decision",
        (),
        {
            "product": product,
            "offers": [
                type(
                    "Offer",
                    (),
                    {
                        "platform": "trenddevice",
                        "offer_eur": 620.0,
                        "error": None,
                        "source_url": "https://www.trendevice.com/vendi/valutazione/iphone-14-pro-128",
                        "raw_payload": {"price_source": "dom", "quote_verification": {"ok": True}},
                    },
                )(),
                type(
                    "Offer",
                    (),
                    {
                        "platform": "rebuy",
                        "offer_eur": 590.0,
                        "error": None,
                        "source_url": "https://www.rebuy.it/comprare/apple-iphone-14-pro-128gb-nero/123",
                        "raw_payload": {"price_source": "dom", "quote_verification": {"ok": True}},
                    },
                )(),
            ],
        },
    )()
    incomplete = type(
        "Decision",
        (),
        {
            "product": product,
            "offers": [
                type(
                    "Offer",
                    (),
                    {
                        "platform": "rebuy",
                        "offer_eur": 590.0,
                        "error": None,
                        "source_url": "https://www.rebuy.it/comprare/apple-iphone-14-pro-128gb-nero/123",
                        "raw_payload": {"price_source": "dom", "quote_verification": {"ok": True}},
                    },
                )()
            ],
        },
    )()

    accepted, rejected = _split_complete_quote_decisions([complete, incomplete])

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert rejected[0][1] == ["trenddevice"]


def test_format_scan_summary_includes_ui_drift_counter() -> None:
    class DummyDecision:
        def __init__(self) -> None:
            self.normalized_name = "A"
            self.spread_eur = 10.0
            self.should_notify = False
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.product = type("Product", (), {"price_eur": 100.0, "url": None, "title": "A"})()
            self.best_offer = None
            self.offers = [
                type(
                    "Offer",
                    (),
                    {
                        "platform": "rebuy",
                        "offer_eur": None,
                        "error": "missing",
                        "raw_payload": {"ui_probes": [{"drift_suspected": True}]},
                    },
                )(),
            ]

    summary = _format_scan_summary([DummyDecision()], threshold=40.0)
    assert "🧩 UI drift rilevati: 1/1" in summary


def test_normalize_http_url_adds_scheme() -> None:
    assert _normalize_http_url("amazon.it/dp/B0ABC123") == "https://amazon.it/dp/B0ABC123"


def test_amazon_search_url_builder() -> None:
    assert _amazon_search_url("iPhone 14 Pro 128GB") == "https://www.amazon.it/s?k=iPhone+14+Pro+128GB"


def test_load_products_from_env_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "AMAZON_PRODUCTS_JSON",
        json.dumps([{"title": "Camera", "price_eur": 200, "category": "photography"}]),
    )
    monkeypatch.setattr("tech_sniper_it.worker._load_from_github_event", lambda event_data=None: [])
    monkeypatch.setattr("tech_sniper_it.worker._load_from_file", lambda: [])

    products = load_products()
    assert len(products) == 1
    assert products[0].title == "Camera"


def test_is_truthy_env_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X_FLAG", "false")
    assert _is_truthy_env("X_FLAG", "true") is False
    monkeypatch.setenv("X_FLAG", "true")
    assert _is_truthy_env("X_FLAG", "false") is True


def test_dedupe_products_prefers_unique_url() -> None:
    p1 = type(
        "P",
        (),
        {
            "title": "Apple iPhone 14 Pro 128GB",
            "price_eur": 500.0,
            "category": ProductCategory.APPLE_PHONE,
            "url": "https://www.amazon.it/dp/B0TEST",
        },
    )()
    p2 = type(
        "P",
        (),
        {
            "title": "Apple iPhone 14 Pro 128GB",
            "price_eur": 500.0,
            "category": ProductCategory.APPLE_PHONE,
            "url": "https://www.amazon.it/dp/B0TEST",
        },
    )()
    items = _dedupe_products([p1, p2])
    assert len(items) == 1


def test_prioritize_products_orders_by_price_then_category() -> None:
    p1 = type("P", (), {"title": "Camera", "price_eur": 320.0, "category": ProductCategory.PHOTOGRAPHY})()
    p2 = type("P", (), {"title": "iPhone", "price_eur": 320.0, "category": ProductCategory.APPLE_PHONE})()
    p3 = type("P", (), {"title": "Laptop", "price_eur": 500.0, "category": ProductCategory.GENERAL_TECH})()
    ordered = _prioritize_products([p1, p3, p2])
    assert ordered[0].title == "iPhone"
    assert ordered[1].title == "Camera"
    assert ordered[2].title == "Laptop"


def test_prioritize_products_prefers_expected_spread_with_scoring_context() -> None:
    iphone = AmazonProduct(
        title="Apple iPhone 15 Pro 128GB",
        price_eur=700.0,
        category=ProductCategory.APPLE_PHONE,
        source_marketplace="it",
    )
    camera = AmazonProduct(
        title="Canon EOS 2000D",
        price_eur=450.0,
        category=ProductCategory.PHOTOGRAPHY,
        source_marketplace="de",
    )
    context = {
        "enabled": True,
        "exact_offer_median": {},
        "exact_confidence": {},
        "category_offer_median": {
            ProductCategory.APPLE_PHONE.value: 920.0,
            ProductCategory.PHOTOGRAPHY.value: 280.0,
        },
        "category_spread_median": {},
        "platform_health": {
            "rebuy": {"rate": 1.0, "samples": 12},
            "trenddevice": {"rate": 0.9, "samples": 12},
            "mpb": {"rate": 0.9, "samples": 12},
        },
    }
    ordered = _prioritize_products([camera, iphone], scoring_context=context)
    assert ordered[0].title == "Apple iPhone 15 Pro 128GB"


def test_build_dynamic_warehouse_queries_prefers_trend_models(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_DYNAMIC_QUERIES_ENABLED", "true")
    monkeypatch.setenv("SCAN_DYNAMIC_QUERY_LIMIT", "8")
    monkeypatch.setenv("SCAN_DYNAMIC_EXPLORATION_RATIO", "0.25")
    monkeypatch.setenv("SCAN_DYNAMIC_TREND_MIN_SCORE", "0")
    monkeypatch.delenv("AMAZON_WAREHOUSE_QUERIES", raising=False)
    context = {
        "category_spread_median": {
            ProductCategory.APPLE_PHONE.value: 65.0,
            ProductCategory.PHOTOGRAPHY.value: 25.0,
            ProductCategory.GENERAL_TECH.value: -10.0,
        },
        "platform_health": {
            "trenddevice": {"rate": 0.8, "samples": 20},
            "rebuy": {"rate": 1.0, "samples": 20},
            "mpb": {"rate": 0.75, "samples": 20},
        },
        "trend_models": [
            {
                "model": "Apple iPhone 15 Pro 256GB",
                "category": "apple_phone",
                "trend_score": 120.0,
                "positive_rate": 0.8,
                "threshold_rate": 0.5,
                "max_spread": 110.0,
            },
            {
                "model": "Apple iPhone 14 Pro 128GB",
                "category": "apple_phone",
                "trend_score": 95.0,
                "positive_rate": 0.6,
                "threshold_rate": 0.3,
                "max_spread": 72.0,
            },
            {
                "model": "Canon EOS R6",
                "category": "photography",
                "trend_score": 70.0,
                "positive_rate": 0.4,
                "threshold_rate": 0.1,
                "max_spread": 50.0,
            },
        ],
    }
    queries, meta = _build_dynamic_warehouse_queries(scoring_context=context, target_count=6)
    assert meta["mode"] == "dynamic"
    assert len(queries) == 6
    assert any("iphone 15 pro 256gb amazon warehouse" in item for item in queries)
    assert any("canon eos r6 amazon warehouse" in item for item in queries)
    assert meta["source_breakdown"]["trend"] >= 2


def test_build_dynamic_warehouse_queries_disabled_uses_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_DYNAMIC_QUERIES_ENABLED", "false")
    monkeypatch.setenv(
        "AMAZON_WAREHOUSE_QUERIES",
        "iphone 13 128gb amazon warehouse,sony alpha amazon warehouse",
    )
    queries, meta = _build_dynamic_warehouse_queries(scoring_context={}, target_count=4)
    assert meta["mode"] == "disabled"
    assert queries == [
        "iphone 13 128gb amazon warehouse",
        "sony alpha amazon warehouse",
    ]


def test_build_dynamic_warehouse_queries_filters_accessory_like_trend_models(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_DYNAMIC_QUERIES_ENABLED", "true")
    monkeypatch.setenv("SCAN_DYNAMIC_QUERY_LIMIT", "6")
    monkeypatch.setenv("SCAN_DYNAMIC_EXPLORATION_RATIO", "0.2")
    context = {
        "trend_models": [
            {
                "model": "Soonjet MacBook Air A2337 A2179 A1932",
                "category": "general_tech",
                "trend_score": 140.0,
                "positive_rate": 0.7,
                "threshold_rate": 0.6,
                "max_spread": 120.0,
            },
            {
                "model": "Apple iPhone 14 Pro 256GB",
                "category": "apple_phone",
                "trend_score": 80.0,
                "positive_rate": 0.5,
                "threshold_rate": 0.2,
                "max_spread": 60.0,
            },
        ],
        "platform_health": {"rebuy": {"rate": 1.0, "samples": 10}, "trenddevice": {"rate": 0.8, "samples": 10}},
    }
    queries, _meta = _build_dynamic_warehouse_queries(scoring_context=context, target_count=4)
    joined = " | ".join(queries).lower()
    assert "soonjet" not in joined
    assert "a2337" not in joined
    assert "iphone 14 pro 256gb amazon warehouse" in joined


@pytest.mark.asyncio
async def test_build_prioritization_context_generates_trend_models(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyStorage:
        async def get_recent_scoring_rows(self, *, lookback_days: int, limit: int):  # noqa: ANN201
            return [
                {
                    "normalized_name": "Apple iPhone 15 Pro 256GB",
                    "category": "apple_phone",
                    "best_offer_eur": 780.0,
                    "spread_eur": 105.0,
                    "offers_payload": [
                        {"platform": "trenddevice", "offer_eur": 760.0, "error": None},
                        {"platform": "rebuy", "offer_eur": 740.0, "error": None},
                    ],
                    "created_at": "2026-02-11T10:00:00Z",
                },
                {
                    "normalized_name": "Canon EOS R6",
                    "category": "photography",
                    "best_offer_eur": 650.0,
                    "spread_eur": 48.0,
                    "offers_payload": [
                        {"platform": "mpb", "offer_eur": 650.0, "error": None},
                        {"platform": "rebuy", "offer_eur": 520.0, "error": None},
                    ],
                    "created_at": "2026-02-10T10:00:00Z",
                },
            ]

    manager = type("M", (), {"storage": DummyStorage()})()
    monkeypatch.setenv("SCORING_ENABLE", "true")
    monkeypatch.setenv("SCORING_LOOKBACK_DAYS", "30")
    context = await _build_prioritization_context(manager)
    assert context["rows_count"] == 2
    assert len(context["trend_models"]) >= 2
    assert context["trend_models"][0]["model"]
    assert context["trend_models"][0]["trend_score"] >= context["trend_models"][1]["trend_score"]


def test_select_balanced_candidates_respects_it_eu_quota(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_IT_QUOTA", "3")
    monkeypatch.setenv("SCAN_EU_QUOTA", "3")
    products: list[AmazonProduct] = []
    for idx in range(6):
        products.append(
            AmazonProduct(
                title=f"IT-{idx}",
                price_eur=100.0 + idx,
                category=ProductCategory.GENERAL_TECH,
                source_marketplace="it",
            )
        )
    for idx in range(4):
        products.append(
            AmazonProduct(
                title=f"EU-{idx}",
                price_eur=200.0 + idx,
                category=ProductCategory.GENERAL_TECH,
                source_marketplace="de",
            )
        )
    selected = _select_balanced_candidates(products, target=6)
    it_count = sum(1 for item in selected if item.source_marketplace == "it")
    eu_count = sum(1 for item in selected if item.source_marketplace == "de")
    assert len(selected) == 6
    assert it_count == 3
    assert eu_count == 3


def test_ai_usage_helpers() -> None:
    decision = type(
        "D",
        (),
        {"ai_provider": "openrouter", "ai_model": "perplexity/sonar", "ai_mode": "live"},
    )()
    label = _ai_usage_label(decision)
    assert "openrouter" in label
    counts = _ai_usage_stats([decision, type("D2", (), {"ai_provider": "heuristic"})()])
    assert counts == (1, 1)


def test_format_scan_summary() -> None:
    class DummyDecision:
        def __init__(self, name, spread, should_notify, platform):  # noqa: ANN001
            self.normalized_name = name
            self.spread_eur = spread
            self.should_notify = should_notify
            self.ai_provider = "gemini"
            self.ai_model = "gemini-2.0-flash"
            self.ai_mode = "live"
            self.product = type("Product", (), {"price_eur": 500.0, "url": "https://amazon.it/item"})()
            self.best_offer = type(
                "Best",
                (),
                {
                    "platform": platform,
                    "offer_eur": 620.0,
                    "source_url": "https://rebuy.it/item",
                },
            )()
            self.offers = [
                type("Offer", (), {"platform": platform, "offer_eur": 620.0, "error": None})(),
            ]

    summary = _format_scan_summary(
        [
            DummyDecision("A", 50.0, False, "rebuy"),
            DummyDecision("B", 120.0, True, "trenddevice"),
        ],
        threshold=80.0,
    )
    assert "📦 Analizzati: 2 | ✅ Over soglia: 1 | 🗑️ Scartati: 1" in summary
    assert "🔥 Opportunita (ordinate per spread netto):" in summary
    assert "💶 Buy 500.00 EUR → 📱 trenddevice 620.00 EUR | netto +120.00 EUR" in summary
    assert "🔗 Link migliore offerta: https://rebuy.it/item" in summary


def test_format_scan_summary_falls_back_to_amazon_search_link() -> None:
    class DummyDecision:
        def __init__(self) -> None:
            self.normalized_name = "Apple iPhone 14 Pro 128GB"
            self.spread_eur = 10.0
            self.should_notify = False
            self.product = type("Product", (), {"price_eur": 500.0, "url": None, "title": "Apple iPhone 14 Pro 128GB"})()
            self.best_offer = type("Best", (), {"platform": "rebuy", "offer_eur": 510.0, "source_url": None})()
            self.offers = [type("Offer", (), {"platform": "rebuy", "offer_eur": 510.0, "error": None})()]

    summary = _format_scan_summary([DummyDecision()], threshold=40.0)
    assert "✅ Over soglia: 0" in summary
    assert "😴 Nessuna opportunita sopra soglia in questa run." in summary
    assert "🛒 Amazon link:" not in summary


@pytest.mark.asyncio
async def test_run_worker_routes_status_command(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"status": False}

    async def fake_status(payload):  # noqa: ANN001
        called["status"] = True
        assert payload["command"] == "status"
        return 0

    monkeypatch.setattr("tech_sniper_it.worker.load_dotenv", lambda: None)
    monkeypatch.setattr(
        "tech_sniper_it.worker._load_github_event_data",
        lambda: {"client_payload": {"command": "status"}},
    )
    monkeypatch.setattr("tech_sniper_it.worker._run_status_command", fake_status)

    exit_code = await run_worker()
    assert exit_code == 0
    assert called["status"] is True


@pytest.mark.asyncio
async def test_run_worker_defaults_to_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"scan": False}

    async def fake_scan(payload):  # noqa: ANN001
        called["scan"] = True
        return 0

    monkeypatch.setattr("tech_sniper_it.worker.load_dotenv", lambda: None)
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr("tech_sniper_it.worker._run_scan_command", fake_scan)

    exit_code = await run_worker()
    assert exit_code == 0
    assert called["scan"] is True


@pytest.mark.asyncio
async def test_send_telegram_message_splits_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = []

    class FakeBot:
        def __init__(self, token: str) -> None:
            self.token = token

        async def send_message(self, chat_id: str, text: str, disable_web_page_preview: bool) -> None:
            sent.append((chat_id, text, disable_web_page_preview))

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42")
    monkeypatch.setattr("tech_sniper_it.worker.Bot", FakeBot)

    await _send_telegram_message("z" * 9001, chat_id=None)

    assert len(sent) == 3
    assert all(item[0] == "42" for item in sent)
    assert max(len(item[1]) for item in sent) <= 4000


@pytest.mark.asyncio
async def test_run_status_command_includes_emojis(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyStorage:
        async def get_recent_opportunities(self, limit: int = 1, min_spread_eur: float | None = None):  # noqa: ANN201
            return []

    class DummyManager:
        def __init__(self) -> None:
            self.min_spread_eur = 40.0
            self.storage = DummyStorage()
            self.notifier = object()

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append((chat_id, text))

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)
    monkeypatch.delenv("OPENROUTER_API_KEYS", raising=False)

    exit_code = await _run_status_command({"chat_id": "123"})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert sent_messages[0][0] == "123"
    text = sent_messages[0][1]
    assert "🤖 Tech_Sniper_IT status:" in text
    assert "⚙️ worker: online" in text
    assert "🎯 threshold spread netto: 40.00 EUR" in text
    assert "🧭 strategy profile: balanced" in text
    assert "🧠 ai: openrouter=off" in text
    assert "🗄️ supabase: on" in text
    assert "💬 telegram alerts default chat: on" in text
    assert "📌 last opportunity: none" in text


@pytest.mark.asyncio
async def test_run_status_command_reports_last_opportunity(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyStorage:
        async def get_recent_opportunities(self, limit: int = 1, min_spread_eur: float | None = None):  # noqa: ANN201
            return [{"normalized_name": "iPhone 15 128GB", "spread_eur": 52.4, "best_platform": "rebuy"}]

    class DummyManager:
        def __init__(self) -> None:
            self.min_spread_eur = 40.0
            self.storage = DummyStorage()
            self.notifier = object()

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append(text)

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_status_command({})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert "📌 last opportunity: iPhone 15 128GB | spread 52.4 EUR | rebuy" in sent_messages[0]


@pytest.mark.asyncio
async def test_run_status_command_handles_storage_read_error(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyStorage:
        async def get_recent_opportunities(self, limit: int = 1, min_spread_eur: float | None = None):  # noqa: ANN201
            raise RuntimeError("db unavailable")

    class DummyManager:
        def __init__(self) -> None:
            self.min_spread_eur = 40.0
            self.storage = DummyStorage()
            self.notifier = object()

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append(text)

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_status_command({})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert "📌 last opportunity: read error (db unavailable)" in sent_messages[0]


@pytest.mark.asyncio
async def test_run_scan_command_sends_summary_for_manual_debug(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type(
                "P",
                (),
                {
                    "title": "iPhone",
                    "price_eur": 679.0,
                },
            )()
            self.normalized_name = "iPhone 14 Pro 128GB"
            self.best_offer = type("B", (), {"offer_eur": 650.0, "platform": "rebuy"})()
            self.spread_eur = -29.0
            self.should_notify = False
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            return [DummyDecision()]

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append((chat_id, text))

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr(
        "tech_sniper_it.worker.load_products",
        lambda event_data=None: [
            type("Product", (), {"title": "iPhone", "price_eur": 679.0, "category": ProductCategory.APPLE_PHONE})()
        ],
    )
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_scan_command({"source": "manual_debug"})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert sent_messages[0][0] is None
    assert "🔎 Scan completata" in sent_messages[0][1]
    assert "✅ Over soglia: 0" in sent_messages[0][1]
    assert "🔥 Opportunita (ordinate per spread netto):" not in sent_messages[0][1]


@pytest.mark.asyncio
async def test_run_scan_command_disables_individual_notifier_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {"notifier_disabled": False}

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type("P", (), {"title": "iPhone", "price_eur": 679.0, "url": None})()
            self.normalized_name = "iPhone 14 Pro 128GB"
            self.best_offer = type("B", (), {"offer_eur": 720.0, "platform": "rebuy", "source_url": None})()
            self.spread_eur = 41.0
            self.should_notify = True
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0
        notifier = object()

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            captured["notifier_disabled"] = self.notifier is None
            return [DummyDecision()]

    async def fake_send(_text: str, _chat_id: str | None) -> None:
        return None

    manager = DummyManager()
    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: manager)
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr(
        "tech_sniper_it.worker.load_products",
        lambda event_data=None: [
            type("Product", (), {"title": "iPhone", "price_eur": 679.0, "category": ProductCategory.APPLE_PHONE})()
        ],
    )
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)
    monkeypatch.delenv("SCAN_TELEGRAM_INDIVIDUAL_ALERTS", raising=False)

    exit_code = await _run_scan_command({"source": "manual_debug"})

    assert exit_code == 0
    assert captured["notifier_disabled"] is True
    assert manager.notifier is not None


@pytest.mark.asyncio
async def test_run_scan_command_applies_cart_net_pricing_before_valuation(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {"cart_called": False, "evaluated_price": None}

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type("P", (), {"title": "iPhone", "price_eur": 579.0, "url": None})()
            self.normalized_name = "iPhone 14 Pro 128GB"
            self.best_offer = type("B", (), {"offer_eur": 600.0, "platform": "rebuy", "source_url": None})()
            self.spread_eur = 21.0
            self.should_notify = False
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            captured["evaluated_price"] = products[0].price_eur
            return [DummyDecision()]

    async def fake_send(_text: str, _chat_id: str | None) -> None:
        return None

    async def fake_apply_cart_net_pricing(products, *, headless: bool, nav_timeout_ms: int):  # noqa: ANN201
        captured["cart_called"] = True
        assert headless is True
        assert nav_timeout_ms == 45000
        products[0].price_eur = 579.0
        return {"checked": 1, "updated": 1, "skipped": 0}

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr(
        "tech_sniper_it.worker.load_products",
        lambda event_data=None: [
            AmazonProduct(
                title="Apple iPhone 14 Pro 128GB",
                price_eur=649.0,
                category=ProductCategory.APPLE_PHONE,
                url="https://www.amazon.it/dp/B0TEST1234",
            )
        ],
    )
    monkeypatch.setattr("tech_sniper_it.worker.apply_cart_net_pricing", fake_apply_cart_net_pricing)
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_scan_command({"source": "manual_debug"})

    assert exit_code == 0
    assert captured["cart_called"] is True
    assert captured["evaluated_price"] == 579.0


@pytest.mark.asyncio
async def test_run_scan_command_uses_warehouse_fallback_when_no_input(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type(
                "P",
                (),
                {
                    "title": "Apple iPhone 14 Pro 128GB",
                    "price_eur": 679.0,
                    "url": "https://www.amazon.it/dp/B0TEST",
                },
            )()
            self.normalized_name = "Apple iPhone 14 Pro 128GB"
            self.best_offer = type(
                "B",
                (),
                {
                    "offer_eur": 700.0,
                    "platform": "rebuy",
                    "source_url": "https://www.rebuy.it/item/123",
                },
            )()
            self.spread_eur = 21.0
            self.should_notify = False
            self.ai_provider = "gemini"
            self.ai_model = "gemini-2.0-flash"
            self.ai_mode = "live"
            self.offers = [type("Offer", (), {"platform": "rebuy", "offer_eur": 700.0, "error": None})()]

    class DummyManager:
        min_spread_eur = 40.0

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            return [DummyDecision()]

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append((chat_id, text))

    async def fake_fetch_warehouse_products(*, headless: bool, nav_timeout_ms: int):  # noqa: ANN201
        return [
            {
                "title": "Apple iPhone 14 Pro 128GB",
                "price_eur": 679,
                "category": "apple_phone",
                "url": "https://www.amazon.it/dp/B0TEST",
            }
        ]

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr("tech_sniper_it.worker.load_products", lambda event_data=None: [])
    monkeypatch.setattr("tech_sniper_it.worker.fetch_amazon_warehouse_products", fake_fetch_warehouse_products)
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_scan_command({"source": "manual_debug"})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert "📦 Analizzati: 1" in sent_messages[0][1]
    assert "✅ Over soglia: 0" in sent_messages[0][1]


@pytest.mark.asyncio
async def test_run_scan_command_passes_dynamic_queries_to_warehouse_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type("P", (), {"title": "iPhone", "price_eur": 500.0, "url": None})()
            self.normalized_name = "iPhone 15 Pro 256GB"
            self.best_offer = type("B", (), {"offer_eur": 520.0, "platform": "rebuy", "source_url": None})()
            self.spread_eur = 20.0
            self.should_notify = False
            self.ai_provider = "openrouter"
            self.ai_model = "perplexity/sonar"
            self.ai_mode = "live"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0
        storage = None

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            return [DummyDecision()]

    async def fake_fetch_warehouse_products(**kwargs):  # noqa: ANN201
        captured.update(kwargs)
        return [
            {
                "title": "Apple iPhone 15 Pro 256GB",
                "price_eur": 500.0,
                "category": "apple_phone",
                "url": "https://www.amazon.it/dp/B0TEST",
            }
        ]

    async def fake_send(_text: str, _chat_id: str | None) -> None:
        return None

    async def fake_scoring_context(_manager):  # noqa: ANN001, ANN201
        return {"enabled": False, "trend_models": []}

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr("tech_sniper_it.worker.load_products", lambda event_data=None: [])
    monkeypatch.setattr("tech_sniper_it.worker._build_prioritization_context", fake_scoring_context)
    monkeypatch.setattr(
        "tech_sniper_it.worker._build_dynamic_warehouse_queries",
        lambda **kwargs: (["iphone 15 pro 256gb amazon warehouse"], {"mode": "dynamic", "selected": 1, "target": 1}),
    )
    monkeypatch.setattr("tech_sniper_it.worker.fetch_amazon_warehouse_products", fake_fetch_warehouse_products)
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)

    exit_code = await _run_scan_command({"source": "manual_debug"})

    assert exit_code == 0
    assert captured["search_queries"] == ["iphone 15 pro 256gb amazon warehouse"]
    assert isinstance(captured["max_products"], int)
    assert captured["max_products"] > 0


@pytest.mark.asyncio
async def test_run_scan_command_sends_summary_when_default_telegram_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type(
                "P",
                (),
                {
                    "title": "iPhone",
                    "price_eur": 679.0,
                    "url": None,
                },
            )()
            self.normalized_name = "iPhone 14 Pro 128GB"
            self.best_offer = type("B", (), {"offer_eur": 650.0, "platform": "rebuy", "source_url": None})()
            self.spread_eur = -29.0
            self.should_notify = False
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            return [DummyDecision()]

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append((chat_id, text))

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr(
        "tech_sniper_it.worker.load_products",
        lambda event_data=None: [
            type("Product", (), {"title": "iPhone", "price_eur": 679.0, "category": ProductCategory.APPLE_PHONE})()
        ],
    )
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "42")

    exit_code = await _run_scan_command({})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert "🚀 Tech_Sniper_IT | Scan Report" in sent_messages[0][1]


@pytest.mark.asyncio
async def test_run_scan_command_skips_summary_without_telegram_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    sent_messages = []

    class DummyDecision:
        def __init__(self) -> None:
            self.product = type("P", (), {"title": "iPhone", "price_eur": 679.0, "url": None})()
            self.normalized_name = "iPhone 14 Pro 128GB"
            self.best_offer = type("B", (), {"offer_eur": 650.0, "platform": "rebuy", "source_url": None})()
            self.spread_eur = -29.0
            self.should_notify = False
            self.ai_provider = "heuristic"
            self.ai_model = None
            self.ai_mode = "fallback"
            self.offers = []

    class DummyManager:
        min_spread_eur = 40.0

        async def evaluate_many(self, products, max_parallel_products=3):  # noqa: ANN001, ANN201
            return [DummyDecision()]

    async def fake_send(text: str, chat_id: str | None) -> None:
        sent_messages.append((chat_id, text))

    monkeypatch.setattr("tech_sniper_it.worker.build_default_manager", lambda: DummyManager())
    monkeypatch.setattr("tech_sniper_it.worker._load_github_event_data", lambda: {})
    monkeypatch.setattr(
        "tech_sniper_it.worker.load_products",
        lambda event_data=None: [
            type("Product", (), {"title": "iPhone", "price_eur": 679.0, "category": ProductCategory.APPLE_PHONE})()
        ],
    )
    monkeypatch.setattr("tech_sniper_it.worker._send_telegram_message", fake_send)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    exit_code = await _run_scan_command({})

    assert exit_code == 0
    assert len(sent_messages) == 0


@pytest.mark.asyncio
async def test_exclude_non_profitable_candidates_filters_by_url(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyStorage:
        async def get_excluded_source_urls(self, *, max_spread_eur: float, lookback_days: int, limit: int):  # noqa: ANN201
            return {"https://www.amazon.it/dp/B0SKIP"}

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    monkeypatch.setenv("EXCLUDE_NON_PROFITABLE", "true")

    keep = type(
        "P1",
        (),
        {"title": "Keep", "price_eur": 100.0, "category": ProductCategory.GENERAL_TECH, "url": "https://www.amazon.it/dp/B0KEEP"},
    )()
    skip = type(
        "P2",
        (),
        {"title": "Skip", "price_eur": 100.0, "category": ProductCategory.GENERAL_TECH, "url": "https://www.amazon.it/dp/B0SKIP"},
    )()
    filtered = await _exclude_non_profitable_candidates(manager, [keep, skip])
    assert len(filtered) == 1
    assert filtered[0].title == "Keep"


@pytest.mark.asyncio
async def test_exclude_non_profitable_candidates_relaxes_to_min_keep(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyStorage:
        async def get_excluded_source_urls(self, *, max_spread_eur: float, lookback_days: int, limit: int):  # noqa: ANN201
            return {"https://www.amazon.it/dp/B0A", "https://www.amazon.it/dp/B0B"}

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    monkeypatch.setenv("EXCLUDE_NON_PROFITABLE", "true")
    monkeypatch.setenv("EXCLUDE_MIN_KEEP", "1")

    removed_a = type(
        "P1",
        (),
        {"title": "A", "price_eur": 100.0, "category": ProductCategory.GENERAL_TECH, "url": "https://www.amazon.it/dp/B0A"},
    )()
    removed_b = type(
        "P2",
        (),
        {"title": "B", "price_eur": 100.0, "category": ProductCategory.GENERAL_TECH, "url": "https://www.amazon.it/dp/B0B"},
    )()

    filtered = await _exclude_non_profitable_candidates(manager, [removed_a, removed_b])
    assert len(filtered) == 1
    assert filtered[0].title == "A"


@pytest.mark.asyncio
async def test_exclude_non_profitable_candidates_filters_by_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyStorage:
        async def get_excluded_source_urls(self, *, max_spread_eur: float, lookback_days: int, limit: int):  # noqa: ANN201
            return set()

        async def get_recent_scoring_rows(self, *, lookback_days: int, limit: int):  # noqa: ANN201
            return [
                {
                    "normalized_name": "Steam Deck OLED 512GB",
                    "category": "general_tech",
                    "amazon_price_eur": 420.0,
                    "spread_eur": 10.0,
                    "created_at": "2026-02-12T08:00:00Z",
                }
            ]

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    monkeypatch.setenv("EXCLUDE_NON_PROFITABLE", "true")

    skip = type(
        "P1",
        (),
        {
            "title": "Steam Deck OLED 512GB",
            "price_eur": 420.0,
            "category": ProductCategory.HANDHELD_CONSOLE,
            "url": "https://www.amazon.it/dp/B0SKIP",
            "amazon_condition": None,
            "amazon_packaging_only": False,
        },
    )()
    keep = type(
        "P2",
        (),
        {
            "title": "ASUS ROG Ally Z1 Extreme",
            "price_eur": 520.0,
            "category": ProductCategory.HANDHELD_CONSOLE,
            "url": "https://www.amazon.it/dp/B0KEEP",
            "amazon_condition": None,
            "amazon_packaging_only": False,
        },
    )()

    filtered = await _exclude_non_profitable_candidates(manager, [skip, keep])
    assert len(filtered) == 1
    assert filtered[0].title == "ASUS ROG Ally Z1 Extreme"


@pytest.mark.asyncio
async def test_exclude_non_profitable_candidates_daily_reset_uses_since_iso(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {"since_iso": None, "lookback_days": None}

    class DummyStorage:
        async def get_excluded_source_urls(  # noqa: ANN201
            self,
            *,
            max_spread_eur: float,
            lookback_days: int,
            limit: int,
            since_iso: str | None = None,
        ):
            captured["since_iso"] = since_iso
            captured["lookback_days"] = lookback_days
            return set()

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    monkeypatch.setenv("EXCLUDE_NON_PROFITABLE", "true")
    monkeypatch.setenv("EXCLUDE_DAILY_RESET", "true")
    monkeypatch.setenv("EXCLUDE_RESET_TIMEZONE", "Europe/Rome")
    monkeypatch.setenv("EXCLUDE_LOOKBACK_DAYS", "1")

    keep = type(
        "P1",
        (),
        {"title": "Keep", "price_eur": 100.0, "category": ProductCategory.GENERAL_TECH, "url": "https://www.amazon.it/dp/B0KEEP"},
    )()
    filtered = await _exclude_non_profitable_candidates(manager, [keep])
    assert len(filtered) == 1
    assert isinstance(captured["since_iso"], str) and "T" in str(captured["since_iso"])
    assert captured["lookback_days"] == 1


def test_daily_exclusion_since_iso_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXCLUDE_DAILY_RESET", "false")
    since_iso, timezone_name = _daily_exclusion_since_iso()
    assert since_iso is None
    assert timezone_name is None


@pytest.mark.asyncio
async def test_save_non_profitable_decisions_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = []

    class DummyStorage:
        async def save_non_profitable(self, decision, threshold: float):  # noqa: ANN001
            saved.append((decision, threshold))

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    best_offer = type("Best", (), {"offer_eur": 100.0, "platform": "rebuy"})()
    decision_low = type("D1", (), {"spread_eur": 10.0, "best_offer": best_offer})()
    decision_high = type("D2", (), {"spread_eur": 60.0, "best_offer": best_offer})()
    decision_none = type("D3", (), {"spread_eur": None, "best_offer": best_offer})()

    count = await _save_non_profitable_decisions(manager, [decision_low, decision_high, decision_none])
    assert count == 1
    assert len(saved) == 1


@pytest.mark.asyncio
async def test_save_non_profitable_decisions_ignores_storage_transient_errors() -> None:
    class DummyStorage:
        async def save_non_profitable(self, decision, threshold: float):  # noqa: ANN001
            if getattr(decision, "tag", "") == "fail":
                raise RuntimeError("temporary supabase disconnect")

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    best_offer = type("Best", (), {"offer_eur": 100.0, "platform": "rebuy"})()
    decision_ok = type("D1", (), {"spread_eur": 10.0, "tag": "ok", "best_offer": best_offer})()
    decision_fail = type("D2", (), {"spread_eur": 20.0, "tag": "fail", "best_offer": best_offer})()
    decision_skip = type("D3", (), {"spread_eur": 60.0, "tag": "skip", "best_offer": best_offer})()

    count = await _save_non_profitable_decisions(manager, [decision_ok, decision_fail, decision_skip])
    assert count == 1
