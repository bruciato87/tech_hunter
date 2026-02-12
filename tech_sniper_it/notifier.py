from __future__ import annotations

from telegram import Bot

from tech_sniper_it.models import ArbitrageDecision


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id

    async def notify(self, decision: ArbitrageDecision) -> None:
        if not decision.best_offer or decision.spread_eur is None:
            return
        product = decision.product
        best = decision.best_offer
        lines = [
            "Tech_Sniper_IT ALERT",
            f"Prodotto: {decision.normalized_name}",
            f"Amazon Warehouse: {product.price_eur:.2f} EUR",
            f"Miglior offerta: {best.offer_eur:.2f} EUR ({best.platform})",
            f"Spread: {decision.spread_eur:.2f} EUR",
        ]
        if product.url:
            lines.append(product.url)
        await self.bot.send_message(chat_id=self.chat_id, text="\n".join(lines))

