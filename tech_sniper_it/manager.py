from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from typing import Any

from tech_sniper_it.ai_balancer import SmartAIBalancer
from tech_sniper_it.models import AmazonProduct, ArbitrageDecision, ProductCategory, ValuationResult
from tech_sniper_it.notifier import TelegramNotifier
from tech_sniper_it.storage import SupabaseStorage
from tech_sniper_it.valuators import MPBValuator, RebuyValuator, TrendDeviceValuator


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _valuator_platform_name(valuator: Any) -> str:
    value = getattr(valuator, "platform_name", None) or getattr(valuator, "platform", None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return valuator.__class__.__name__.strip().lower()


def _valuator_backoff_threshold(platform: str) -> int:
    default_threshold = max(1, int(_env_or_default("VALUATOR_BACKOFF_DEFAULT_ERRORS", "2")))
    defaults = {
        "mpb": 1,
        "trenddevice": 2,
    }
    platform_name = platform.strip().lower()
    platform_default = defaults.get(platform_name, default_threshold)
    env_name = f"VALUATOR_BACKOFF_{platform_name.upper()}_ERRORS"
    return max(1, int(_env_or_default(env_name, str(platform_default))))


def _should_backoff_result(result: ValuationResult) -> bool:
    platform = (result.platform or "").strip().lower()
    if not platform:
        return False
    error_text = (result.error or "").lower()
    if not error_text:
        return False
    if platform == "mpb":
        return any(
            marker in error_text
            for marker in (
                "anti-bot challenge",
                "turnstile",
                "cloudflare",
                "search input not found",
                "price not found after retries",
            )
        )
    if platform == "trenddevice":
        return "email-gate" in error_text
    return False


class ArbitrageManager:
    def __init__(
        self,
        ai_balancer: SmartAIBalancer,
        *,
        storage: SupabaseStorage | None = None,
        notifier: TelegramNotifier | None = None,
        min_spread_eur: float = 40.0,
        headless: bool = True,
        nav_timeout_ms: int = 45000,
    ) -> None:
        self.ai_balancer = ai_balancer
        self.storage = storage
        self.notifier = notifier
        self.min_spread_eur = min_spread_eur
        self.headless = headless
        self.nav_timeout_ms = nav_timeout_ms

    async def evaluate_product(self, product: AmazonProduct) -> ArbitrageDecision:
        print(
            f"[scan] Evaluating product | title='{product.title}' | category={product.category.value} | amazon_price={product.price_eur:.2f}"
        )
        normalized_name, ai_usage = await self.ai_balancer.normalize_with_meta(product.title)
        self._log_ai_usage(normalized_name, ai_usage)
        offers = await self._evaluate_offers(product.category, product, normalized_name)
        decision = self._build_decision(product, normalized_name, offers, ai_usage)

        if decision.should_notify:
            await self._persist_and_notify(decision)
        return decision

    async def evaluate_many(self, products: Iterable[AmazonProduct], max_parallel_products: int = 3) -> list[ArbitrageDecision]:
        items = list(products)
        print(f"[scan] Parallel evaluation start | products={len(items)} | max_parallel={max_parallel_products}")
        if not items:
            return []

        semaphore = asyncio.Semaphore(max_parallel_products)
        backoff_enabled = _env_or_default("VALUATOR_CIRCUIT_BREAKER_ENABLED", "true").lower() != "false"
        disabled_platforms: set[str] = set()
        platform_failures: dict[str, int] = defaultdict(int)
        backoff_lock = asyncio.Lock()

        async def _normalize_title(title: str) -> tuple[str, str, dict[str, Any]]:
            async with semaphore:
                normalized_name, ai_usage = await self.ai_balancer.normalize_with_meta(title)
                self._log_ai_usage(normalized_name, ai_usage)
                return title, normalized_name, ai_usage

        unique_titles = list(dict.fromkeys(item.title for item in items))
        print(f"[scan] Normalization stage | unique_titles={len(unique_titles)}")
        normalized_rows = await asyncio.gather(*(_normalize_title(title) for title in unique_titles))
        title_map: dict[str, tuple[str, dict[str, Any]]] = {
            title: (normalized_name, ai_usage)
            for title, normalized_name, ai_usage in normalized_rows
        }

        grouped: dict[tuple[str, str], list[AmazonProduct]] = {}
        for item in items:
            normalized_name, _usage = title_map[item.title]
            key = (item.category.value, normalized_name)
            grouped.setdefault(key, []).append(item)
        print(f"[scan] Valuation stage | unique_model_groups={len(grouped)}")

        async def _valuate_group(
            key: tuple[str, str],
            group_items: list[AmazonProduct],
        ) -> tuple[tuple[str, str], list[ValuationResult]]:
            category_value, normalized_name = key
            category = ProductCategory(category_value)
            sample = group_items[0]
            all_valuators = self._build_valuators(category)

            async with semaphore:
                # Filter valuators only when the task actually starts to run, so queued tasks
                # can observe circuit-breaker updates from earlier failures.
                if backoff_enabled:
                    async with backoff_lock:
                        blocked = set(disabled_platforms)
                    allowed_valuators = [item for item in all_valuators if _valuator_platform_name(item) not in blocked]
                    skipped = [item for item in all_valuators if _valuator_platform_name(item) in blocked]
                    if skipped:
                        skipped_names = [_valuator_platform_name(item) for item in skipped]
                        print(
                            "[scan] Valuator skipped by circuit breaker | "
                            f"platforms={skipped_names} | category={category.value}"
                        )
                else:
                    allowed_valuators = all_valuators
                offers = await self._evaluate_with_valuators(allowed_valuators, sample, normalized_name)

            if backoff_enabled:
                async with backoff_lock:
                    for offer in offers:
                        if not _should_backoff_result(offer):
                            continue
                        platform = (offer.platform or "").strip().lower()
                        platform_failures[platform] += 1
                        threshold = _valuator_backoff_threshold(platform)
                        if platform_failures[platform] >= threshold and platform not in disabled_platforms:
                            disabled_platforms.add(platform)
                            print(
                                "[scan] Valuator circuit breaker triggered | "
                                f"platform={platform} hits={platform_failures[platform]} threshold={threshold} "
                                f"last_error={offer.error}"
                            )
            return key, offers

        grouped_rows = await asyncio.gather(*(_valuate_group(key, values) for key, values in grouped.items()))
        offers_by_key = {key: offers for key, offers in grouped_rows}

        decisions: list[ArbitrageDecision] = []
        for item in items:
            normalized_name, ai_usage = title_map[item.title]
            offers = offers_by_key[(item.category.value, normalized_name)]
            decision = self._build_decision(item, normalized_name, offers, ai_usage)
            decisions.append(decision)

        notify_tasks = [self._persist_and_notify(item) for item in decisions if item.should_notify]
        if notify_tasks:
            await asyncio.gather(*notify_tasks)
        if backoff_enabled and disabled_platforms:
            print(
                "[scan] Valuator circuit breaker summary | "
                f"disabled={sorted(disabled_platforms)} failures={dict(platform_failures)}"
            )
        print("[scan] Parallel evaluation completed.")
        return decisions

    def _build_valuators(self, category: ProductCategory) -> list:
        common = {"headless": self.headless, "nav_timeout_ms": self.nav_timeout_ms}
        if category == ProductCategory.PHOTOGRAPHY:
            return [MPBValuator(**common), RebuyValuator(**common)]
        if category == ProductCategory.APPLE_PHONE:
            return [TrendDeviceValuator(**common), RebuyValuator(**common)]
        return [RebuyValuator(**common)]

    async def _persist_and_notify(self, decision: ArbitrageDecision) -> None:
        tasks = []
        if self.storage:
            tasks.append(self.storage.save_opportunity(decision))
        if self.notifier:
            tasks.append(self.notifier.notify(decision))
        if tasks:
            await asyncio.gather(*tasks)

    def _build_decision(
        self,
        product: AmazonProduct,
        normalized_name: str,
        offers: list[ValuationResult],
        ai_usage: dict[str, Any] | None = None,
    ) -> ArbitrageDecision:
        cloned_offers = [self._clone_offer(item) for item in offers]
        valid_offers = [item for item in cloned_offers if item.is_valid and item.offer_eur is not None]
        best_offer = max(valid_offers, key=lambda item: item.offer_eur) if valid_offers else None
        spread = round(best_offer.offer_eur - product.price_eur, 2) if best_offer and best_offer.offer_eur is not None else None
        should_notify = spread is not None and spread > self.min_spread_eur
        print(
            "[scan] Decision -> "
            f"best_platform={best_offer.platform if best_offer else None} | "
            f"best_offer={best_offer.offer_eur if best_offer else None} | "
            f"spread={spread} | should_notify={should_notify}"
        )
        usage = ai_usage or {}
        return ArbitrageDecision(
            product=product,
            normalized_name=normalized_name,
            offers=cloned_offers,
            best_offer=best_offer,
            spread_eur=spread,
            should_notify=should_notify,
            ai_provider=str(usage.get("provider") or "heuristic"),
            ai_model=str(usage.get("model")) if usage.get("model") else None,
            ai_mode=str(usage.get("mode") or "fallback"),
            ai_used=bool(usage.get("ai_used", False)),
        )

    async def _evaluate_offers(
        self,
        category: ProductCategory,
        product: AmazonProduct,
        normalized_name: str,
    ) -> list[ValuationResult]:
        valuators = self._build_valuators(category)
        return await self._evaluate_with_valuators(valuators, product, normalized_name)

    async def _evaluate_with_valuators(
        self,
        valuators: list[Any],
        product: AmazonProduct,
        normalized_name: str,
    ) -> list[ValuationResult]:
        if not valuators:
            print("[scan] No valuators available for this product after runtime filters.")
            return []
        valuator_names = [getattr(valuator, "platform_name", valuator.__class__.__name__) for valuator in valuators]
        print(f"[scan] Selected valuators -> {valuator_names}")

        tasks = [valuator.valuate(product, normalized_name) for valuator in valuators]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        offers: list[ValuationResult] = []
        for raw in raw_results:
            if isinstance(raw, Exception):
                print(f"[scan] Valuator exception -> {type(raw).__name__}: {raw}")
                offers.append(
                    ValuationResult(
                        platform="unknown",
                        normalized_name=normalized_name,
                        offer_eur=None,
                        error=str(raw),
                    )
                )
                continue
            offers.append(raw)
            print(
                "[scan] Offer result -> "
                f"platform={raw.platform} | offer={raw.offer_eur} | valid={raw.is_valid} | error={raw.error}"
            )
        return offers

    def _log_ai_usage(self, normalized_name: str, usage: dict[str, Any] | None) -> None:
        meta = usage or {}
        provider = str(meta.get("provider") or "heuristic")
        model = str(meta.get("model") or "n/a")
        mode = str(meta.get("mode") or "fallback")
        ai_used = bool(meta.get("ai_used", False))
        print(
            "[scan] AI normalization -> "
            f"provider={provider} | model={model} | mode={mode} | ai_used={ai_used} | normalized='{normalized_name}'"
        )

    def _clone_offer(self, offer: ValuationResult) -> ValuationResult:
        return ValuationResult(
            platform=offer.platform,
            normalized_name=offer.normalized_name,
            offer_eur=offer.offer_eur,
            condition=offer.condition,
            currency=offer.currency,
            source_url=offer.source_url,
            raw_payload=deepcopy(offer.raw_payload),
            error=offer.error,
        )


def build_default_manager() -> ArbitrageManager:
    ai_balancer = SmartAIBalancer()
    storage = None
    notifier = None

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    supabase_table = _env_or_default("SUPABASE_TABLE", "arbitrage_opportunities")
    if supabase_url and supabase_key:
        storage = SupabaseStorage(url=supabase_url, key=supabase_key, table=supabase_table)

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if telegram_bot_token and telegram_chat_id:
        notifier = TelegramNotifier(bot_token=telegram_bot_token, chat_id=telegram_chat_id)

    min_spread_eur = float(_env_or_default("MIN_SPREAD_EUR", "40"))
    headless = _env_or_default("HEADLESS", "true").lower() != "false"
    nav_timeout_ms = int(_env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000"))

    return ArbitrageManager(
        ai_balancer=ai_balancer,
        storage=storage,
        notifier=notifier,
        min_spread_eur=min_spread_eur,
        headless=headless,
        nav_timeout_ms=nav_timeout_ms,
    )
