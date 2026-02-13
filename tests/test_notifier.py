from __future__ import annotations

import pytest

from tech_sniper_it.models import AmazonProduct, ArbitrageDecision, ProductCategory, ValuationResult
from tech_sniper_it.notifier import TelegramNotifier


class FakeBot:
    def __init__(self, token: str) -> None:
        self.token = token
        self.messages = []

    async def send_message(self, chat_id: str, text: str) -> None:
        self.messages.append({"chat_id": chat_id, "text": text})


@pytest.mark.asyncio
async def test_notifier_sends_message(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_bot = FakeBot("token")

    def fake_bot_factory(token: str) -> FakeBot:
        fake_bot.token = token
        return fake_bot

    monkeypatch.setattr("tech_sniper_it.notifier.Bot", fake_bot_factory)
    notifier = TelegramNotifier(bot_token="token", chat_id="123")

    decision = ArbitrageDecision(
        product=AmazonProduct(
            title="Apple iPhone 14 Pro 128GB",
            price_eur=500.0,
            category=ProductCategory.APPLE_PHONE,
            url="https://amazon.it/item",
        ),
        normalized_name="iPhone 14 Pro 128GB",
        offers=[],
        best_offer=ValuationResult(platform="trenddevice", normalized_name="iPhone 14 Pro 128GB", offer_eur=640.0),
        spread_eur=140.0,
        should_notify=True,
    )

    await notifier.notify(decision)

    assert len(fake_bot.messages) == 1
    assert fake_bot.messages[0]["chat_id"] == "123"
    text = fake_bot.messages[0]["text"]
    assert "🚨 Tech_Sniper_IT | Opportunita trovata" in text
    assert "✅ Spread netto: +140.00 EUR" in text
    assert "lordo +140.00" in text
    assert "🧠 AI: heuristic/rule-based (fallback)" in text
    assert "🛒 Amazon link: https://amazon.it/item" in text


@pytest.mark.asyncio
async def test_notifier_skips_invalid_decision(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_bot = FakeBot("token")
    monkeypatch.setattr("tech_sniper_it.notifier.Bot", lambda token: fake_bot)
    notifier = TelegramNotifier(bot_token="token", chat_id="123")

    decision = ArbitrageDecision(
        product=AmazonProduct(title="Test", price_eur=100.0, category=ProductCategory.GENERAL_TECH),
        normalized_name="Test",
        offers=[],
        best_offer=None,
        spread_eur=None,
        should_notify=False,
    )
    await notifier.notify(decision)
    assert fake_bot.messages == []
