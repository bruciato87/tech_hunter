from __future__ import annotations

import json
from pathlib import Path

import pytest

from tech_sniper_it.models import ProductCategory
from tech_sniper_it.worker import (
    _chunk_telegram_text,
    _coerce_product,
    _format_scan_summary,
    _parse_last_limit,
    _resolve_command,
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
            self.best_offer = type("Best", (), {"platform": platform})()

    summary = _format_scan_summary(
        [
            DummyDecision("A", 50.0, False, "rebuy"),
            DummyDecision("B", 120.0, True, "trenddevice"),
        ],
        threshold=80.0,
    )
    assert "Prodotti analizzati: 2" in summary
    assert "Opportunita > 80.00 EUR: 1" in summary
    assert "Top: B" in summary


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
