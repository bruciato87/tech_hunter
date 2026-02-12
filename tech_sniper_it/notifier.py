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
        platform = best.platform or "n/d"
        product_name = decision.normalized_name or product.title
        lines = [
            "🚨 Tech_Sniper_IT | Opportunita trovata",
            "━━━━━━━━━━━━━━━━━━━━",
            f"📦 Prodotto: {product_name}",
            f"💶 Amazon Warehouse: {product.price_eur:.2f} EUR",
            f"🏆 Miglior cash-out: {best.offer_eur:.2f} EUR ({platform})",
            f"📈 Spread netto: +{decision.spread_eur:.2f} EUR",
            f"🧠 AI match: {decision.ai_provider or 'heuristic'} ({decision.ai_mode or 'fallback'})",
            "⚡ Azione consigliata: verifica disponibilita e prezzo in tempo reale.",
        ]
        if product.url:
            lines.append(f"🛒 Amazon link: {product.url}")
        if best.source_url:
            lines.append(f"🔗 Link reseller: {best.source_url}")
        await self.bot.send_message(chat_id=self.chat_id, text="\n".join(lines))
