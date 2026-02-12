from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
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

    async def save_non_profitable(self, decision: ArbitrageDecision, *, threshold: float) -> None:
        if not decision.best_offer or decision.spread_eur is None:
            return
        if decision.spread_eur > threshold:
            return
        await self.save_opportunity(decision)

    async def get_recent_opportunities(self, limit: int = 5, *, min_spread_eur: float | None = None) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 20))

        def _select() -> list[dict[str, Any]]:
            query = self.client.table(self.table).select("*")
            if min_spread_eur is not None:
                query = query.gt("spread_eur", float(min_spread_eur))
            response = query.order("created_at", desc=True).limit(safe_limit).execute()
            data = getattr(response, "data", None)
            return data if isinstance(data, list) else []

        return await asyncio.to_thread(_select)

    async def get_excluded_source_urls(
        self,
        *,
        max_spread_eur: float,
        lookback_days: int = 14,
        limit: int = 1000,
    ) -> set[str]:
        safe_limit = max(10, min(limit, 5000))
        cutoff_iso: str | None = None
        if lookback_days > 0:
            cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
            cutoff_iso = cutoff.isoformat()

        def _select() -> set[str]:
            query = self.client.table(self.table).select("source_url,spread_eur").lte("spread_eur", float(max_spread_eur))
            if cutoff_iso:
                query = query.gte("created_at", cutoff_iso)
            response = query.order("created_at", desc=True).limit(safe_limit).execute()
            data = getattr(response, "data", None)
            if not isinstance(data, list):
                return set()
            urls: set[str] = set()
            for row in data:
                if not isinstance(row, dict):
                    continue
                value = row.get("source_url")
                if isinstance(value, str) and value.strip():
                    urls.add(value.strip())
            return urls

        return await asyncio.to_thread(_select)
