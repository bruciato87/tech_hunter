from __future__ import annotations

import json
from pathlib import Path

import pytest

from tech_sniper_it.models import ProductCategory
from tech_sniper_it.worker import (
    _chunk_telegram_text,
    _coerce_product,
    _format_scan_summary,
    _offer_log_payload,
    _parse_last_limit,
    _resolve_command,
    _run_scan_command,
    _run_status_command,
    _safe_error_details,
    _send_telegram_message,
    load_products,
    run_worker,
)


def test_coerce_product_valid() -> None:
    product = _coerce_product({"title": "iPhone", "price_eur": 100, "category": "apple"})
    assert product.title == "iPhone"
    assert product.category == ProductCategory.APPLE_PHONE


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


def test_format_scan_summary() -> None:
    class DummyDecision:
        def __init__(self, name, spread, should_notify, platform):  # noqa: ANN001
            self.normalized_name = name
            self.spread_eur = spread
            self.should_notify = should_notify
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
        async def get_recent_opportunities(self, limit: int = 1):  # noqa: ANN201
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
    assert "🏆 Best offer: 650.00 EUR (rebuy)" in sent_messages[0][1]
