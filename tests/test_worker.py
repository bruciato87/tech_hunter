from __future__ import annotations

import json
from pathlib import Path

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.worker import (
    _ai_usage_label,
    _ai_usage_stats,
    _amazon_search_url,
    _chunk_telegram_text,
    _coerce_product,
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
    load_products,
    run_worker,
)


def test_coerce_product_valid() -> None:
    product = _coerce_product({"title": "iPhone", "price_eur": 100, "category": "apple iphone"})
    assert product.title == "iPhone"
    assert product.category == ProductCategory.APPLE_PHONE


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
        {"ai_provider": "gemini", "ai_model": "gemini-2.0-flash", "ai_mode": "live"},
    )()
    label = _ai_usage_label(decision)
    assert "gemini" in label
    counts = _ai_usage_stats([decision, type("D2", (), {"ai_provider": "heuristic"})()])
    assert counts == (1, 0, 1)


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
    assert "📦 Prodotti analizzati: 2" in summary
    assert "✅ Opportunita sopra soglia: 1" in summary
    assert "🏆 Best offer: 620.00 EUR (trenddevice)" in summary
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
    assert "✅ Opportunita sopra soglia: 0" in summary
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
    monkeypatch.setenv("GEMINI_API_KEYS", "k1")
    monkeypatch.delenv("OPENROUTER_API_KEYS", raising=False)

    exit_code = await _run_status_command({"chat_id": "123"})

    assert exit_code == 0
    assert len(sent_messages) == 1
    assert sent_messages[0][0] == "123"
    text = sent_messages[0][1]
    assert "🤖 Tech_Sniper_IT status:" in text
    assert "⚙️ worker: online" in text
    assert "🎯 threshold spread (offer-amazon): 40.00 EUR" in text
    assert "🧠 ai: gemini=on, openrouter=off" in text
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
    assert "✅ Opportunita sopra soglia: 0" in sent_messages[0][1]
    assert "🏆 Best offer:" not in sent_messages[0][1]


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
    assert "Prodotti analizzati: 1" in sent_messages[0][1]
    assert "✅ Opportunita sopra soglia: 0" in sent_messages[0][1]


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
async def test_save_non_profitable_decisions_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = []

    class DummyStorage:
        async def save_non_profitable(self, decision, threshold: float):  # noqa: ANN001
            saved.append((decision, threshold))

    manager = type("M", (), {"storage": DummyStorage(), "min_spread_eur": 40.0})()
    decision_low = type("D1", (), {"spread_eur": 10.0})()
    decision_high = type("D2", (), {"spread_eur": 60.0})()
    decision_none = type("D3", (), {"spread_eur": None})()

    count = await _save_non_profitable_decisions(manager, [decision_low, decision_high, decision_none])
    assert count == 1
    assert len(saved) == 3
