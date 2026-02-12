from __future__ import annotations

import asyncio
import os
from typing import Any

from supabase import Client, create_client

from tech_sniper_it.models import ArbitrageDecision


class SupabaseStorage:
    def __init__(self, url: str, key: str, table: str = "arbitrage_opportunities") -> None:
        self.client: Client = create_client(url, key)
        self.table = table
        self.scanner_user_id = os.getenv("SUPABASE_SCANNER_USER_ID")

    async def save_opportunity(self, decision: ArbitrageDecision) -> None:
        if not decision.best_offer or decision.spread_eur is None:
            return
        payload: dict[str, Any] = {
            "product_title": decision.product.title,
            "normalized_name": decision.normalized_name,
            "amazon_price_eur": decision.product.price_eur,
            "category": decision.product.category.value,
            "best_platform": decision.best_offer.platform,
            "best_offer_eur": decision.best_offer.offer_eur,
            "spread_eur": decision.spread_eur,
            "condition_target": decision.best_offer.condition,
            "offers_payload": [result.raw_payload | {"platform": result.platform, "error": result.error} for result in decision.offers],
            "source_url": decision.product.url,
            "ean": decision.product.ean,
        }
        if self.scanner_user_id:
            payload["scanner_user_id"] = self.scanner_user_id

        def _insert() -> None:
            self.client.table(self.table).insert(payload).execute()

        await asyncio.to_thread(_insert)
