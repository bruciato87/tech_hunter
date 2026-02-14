from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from supabase import Client, create_client

from tech_sniper_it.models import ArbitrageDecision, ProductCategory, to_legacy_storage_category


class SupabaseStorage:
    def __init__(self, url: str, key: str, table: str = "arbitrage_opportunities") -> None:
        self.client: Client = create_client(url, key)
        self.table = table
        self.scanner_user_id = os.getenv("SUPABASE_SCANNER_USER_ID")

    def _write_max_attempts(self) -> int:
        raw = (os.getenv("SUPABASE_WRITE_MAX_ATTEMPTS") or "").strip()
        try:
            value = int(raw) if raw else 3
        except ValueError:
            value = 3
        return max(1, min(value, 8))

    def _write_base_delay_ms(self) -> int:
        raw = (os.getenv("SUPABASE_WRITE_RETRY_DELAY_MS") or "").strip()
        try:
            value = int(raw) if raw else 250
        except ValueError:
            value = 250
        return max(50, min(value, 5000))

    def _is_retryable_write_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        retryable_markers = (
            "server disconnected",
            "connection reset",
            "connection aborted",
            "connection refused",
            "read timed out",
            "timed out",
            "timeout",
            "temporarily unavailable",
            "temporary failure",
            "too many requests",
            "429",
            "500",
            "502",
            "503",
            "504",
        )
        return any(marker in text for marker in retryable_markers)

    async def _insert_payload(self, payload: dict[str, Any]) -> None:
        max_attempts = self._write_max_attempts()
        base_delay_ms = self._write_base_delay_ms()
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.to_thread(self.client.table(self.table).insert(payload).execute)
                return
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts or not self._is_retryable_write_error(exc):
                    raise
                delay = min(6.0, (base_delay_ms / 1000) * (2 ** (attempt - 1)))
                print(
                    "[storage] Write retry scheduled | "
                    f"attempt={attempt}/{max_attempts} delay_s={delay:.2f} error='{type(exc).__name__}: {exc}'"
                )
                await asyncio.sleep(delay)

        if last_error is not None:
            raise last_error

    async def save_opportunity(self, decision: ArbitrageDecision) -> None:
        if not decision.best_offer or decision.spread_eur is None:
            return
        offers_payload: list[dict[str, Any]] = []
        for result in decision.offers:
            raw_payload = result.raw_payload if isinstance(result.raw_payload, dict) else {}
            row = dict(raw_payload)
            row.update(
                {
                    "platform": result.platform,
                    "error": result.error,
                    "offer_eur": result.offer_eur,
                    "condition": result.condition,
                    "currency": result.currency,
                    "source_url": result.source_url,
                }
            )
            offers_payload.append(row)
        payload: dict[str, Any] = {
            "product_title": decision.product.title,
            "normalized_name": decision.normalized_name,
            "amazon_price_eur": decision.product.price_eur,
            "category": to_legacy_storage_category(decision.product.category),
            "best_platform": decision.best_offer.platform,
            "best_offer_eur": decision.best_offer.offer_eur,
            "spread_eur": decision.spread_eur,
            "condition_target": decision.best_offer.condition,
            "offers_payload": offers_payload,
            "source_url": decision.product.url,
            "ean": decision.product.ean,
        }
        if self.scanner_user_id:
            payload["scanner_user_id"] = self.scanner_user_id

        await self._insert_payload(payload)

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
        since_iso: str | None = None,
    ) -> set[str]:
        safe_limit = max(10, min(limit, 5000))
        cutoff_iso: str | None = (since_iso or "").strip() or None
        if cutoff_iso is None and lookback_days > 0:
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

    async def get_recent_scoring_rows(
        self,
        *,
        lookback_days: int = 30,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        safe_limit = max(100, min(limit, 5000))
        cutoff_iso: str | None = None
        if lookback_days > 0:
            cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
            cutoff_iso = cutoff.isoformat()

        def _select() -> list[dict[str, Any]]:
            query = self.client.table(self.table).select(
                "normalized_name,category,best_offer_eur,spread_eur,amazon_price_eur,offers_payload,source_url,created_at"
            )
            if cutoff_iso:
                query = query.gte("created_at", cutoff_iso)
            response = query.order("created_at", desc=True).limit(safe_limit).execute()
            data = getattr(response, "data", None)
            return data if isinstance(data, list) else []

        return await asyncio.to_thread(_select)

    async def get_recent_platform_quote_cache(
        self,
        *,
        platform: str,
        normalized_name: str,
        category: str | None = None,
        max_age_hours: int = 24,
        limit: int = 80,
    ) -> dict[str, Any] | None:
        platform_name = (platform or "").strip().lower()
        name = (normalized_name or "").strip()
        if not platform_name or not name:
            return None
        safe_limit = max(5, min(limit, 200))
        safe_max_age = max(1, min(max_age_hours, 168))
        cutoff_iso = (datetime.now(UTC) - timedelta(hours=safe_max_age)).isoformat()
        category_filter = ""
        raw_category = (category or "").strip()
        if raw_category:
            try:
                parsed_category = ProductCategory.from_raw(raw_category)
                legacy = to_legacy_storage_category(parsed_category)
            except Exception:
                legacy = ""
            if legacy in {"photography", "apple_phone", "general_tech"}:
                category_filter = legacy

        def _as_float(value: Any) -> float | None:
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                return None
            if parsed <= 0:
                return None
            return round(parsed, 2)

        def _extract_from_payload(row: dict[str, Any], created_at: str) -> dict[str, Any] | None:
            offers_payload = row.get("offers_payload")
            if not isinstance(offers_payload, list):
                return None
            for item in offers_payload:
                if not isinstance(item, dict):
                    continue
                if str(item.get("platform") or "").strip().lower() != platform_name:
                    continue
                if item.get("error"):
                    continue
                offer = _as_float(item.get("offer_eur"))
                if offer is None:
                    continue
                source_url = str(item.get("source_url") or "").strip() or None
                condition = str(item.get("condition") or "grade_a").strip() or "grade_a"
                currency = str(item.get("currency") or "EUR").strip().upper() or "EUR"
                return {
                    "offer_eur": offer,
                    "platform": platform_name,
                    "source_url": source_url,
                    "condition": condition,
                    "currency": currency,
                    "created_at": created_at,
                    "origin": "offers_payload",
                }
            return None

        def _select() -> dict[str, Any] | None:
            query = self.client.table(self.table).select(
                "created_at,normalized_name,category,best_platform,best_offer_eur,condition_target,offers_payload"
            )
            query = query.eq("normalized_name", name).gte("created_at", cutoff_iso)
            if category_filter:
                query = query.eq("category", category_filter)
            response = query.order("created_at", desc=True).limit(safe_limit).execute()
            data = getattr(response, "data", None)
            if not isinstance(data, list):
                return None
            for row in data:
                if not isinstance(row, dict):
                    continue
                created_at = str(row.get("created_at") or "").strip()
                payload_hit = _extract_from_payload(row, created_at)
                if payload_hit is not None:
                    return payload_hit
                row_platform = str(row.get("best_platform") or "").strip().lower()
                if row_platform != platform_name:
                    continue
                offer = _as_float(row.get("best_offer_eur"))
                if offer is None:
                    continue
                condition = str(row.get("condition_target") or "grade_a").strip() or "grade_a"
                return {
                    "offer_eur": offer,
                    "platform": platform_name,
                    "source_url": None,
                    "condition": condition,
                    "currency": "EUR",
                    "created_at": created_at,
                    "origin": "best_offer",
                }
            return None

        return await asyncio.to_thread(_select)
