from __future__ import annotations

import asyncio
import os
import re
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from typing import Any
from urllib.parse import urlparse

from tech_sniper_it.ai_balancer import SmartAIBalancer
from tech_sniper_it.models import AmazonProduct, ArbitrageDecision, ProductCategory, ValuationResult
from tech_sniper_it.notifier import TelegramNotifier
from tech_sniper_it.storage import SupabaseStorage
from tech_sniper_it.valuators import MPBValuator, RebuyValuator, TrendDeviceValuator


STRATEGY_PROFILE_DEFAULT = "balanced"
STRATEGY_PROFILE_ENV = "STRATEGY_PROFILE"

_STRATEGY_PROFILES: dict[str, dict[str, Any]] = {
    "conservative": {
        "operating_cost_eur": 8.0,
        "risk_buffers": {
            "acceptable": 34.0,
            "good": 20.0,
            "very_good": 14.0,
            "like_new": 8.0,
            "unknown": 4.0,
        },
        "packaging_only_factor": 0.70,
        "uncertainty_floor": 0.80,
        "uncertainty_scale": 15.0,
    },
    "balanced": {
        "operating_cost_eur": 0.0,
        "risk_buffers": {
            "acceptable": 26.0,
            "good": 14.0,
            "very_good": 9.0,
            "like_new": 5.0,
            "unknown": 0.0,
        },
        "packaging_only_factor": 0.45,
        "uncertainty_floor": 0.70,
        "uncertainty_scale": 12.0,
    },
    "aggressive": {
        "operating_cost_eur": 0.0,
        "risk_buffers": {
            "acceptable": 16.0,
            "good": 9.0,
            "very_good": 6.0,
            "like_new": 3.0,
            "unknown": 0.0,
        },
        "packaging_only_factor": 0.30,
        "uncertainty_floor": 0.60,
        "uncertainty_scale": 7.0,
    },
}

_CAPACITY_TOKEN_PATTERN = re.compile(r"\b\d{1,4}\s*(?:gb|tb)\b", re.IGNORECASE)


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def get_strategy_profile_name() -> str:
    raw = _env_or_default(STRATEGY_PROFILE_ENV, STRATEGY_PROFILE_DEFAULT).strip().lower()
    if raw in _STRATEGY_PROFILES:
        return raw
    return STRATEGY_PROFILE_DEFAULT


def get_strategy_profile_snapshot() -> dict[str, Any]:
    profile_name = get_strategy_profile_name()
    raw = _STRATEGY_PROFILES.get(profile_name, _STRATEGY_PROFILES[STRATEGY_PROFILE_DEFAULT])
    return {
        "profile": profile_name,
        "operating_cost_eur": float(raw.get("operating_cost_eur", 0.0)),
        "risk_buffers": dict(raw.get("risk_buffers", {})),
        "packaging_only_factor": float(raw.get("packaging_only_factor", 0.45)),
        "uncertainty_floor": float(raw.get("uncertainty_floor", 0.70)),
        "uncertainty_scale": float(raw.get("uncertainty_scale", 12.0)),
    }


def _condition_key(condition: str | None) -> str:
    value = (condition or "").strip().lower()
    if value in {"acceptable", "accettabile"}:
        return "acceptable"
    if value in {"good", "buono"}:
        return "good"
    if value in {"very_good", "ottimo"}:
        return "very_good"
    if value in {"like_new", "come_nuovo"}:
        return "like_new"
    if "acceptable" in value or "accett" in value:
        return "acceptable"
    if "good" in value or "buon" in value:
        return "good"
    if "very" in value or "ottim" in value:
        return "very_good"
    if "like" in value or "nuovo" in value:
        return "like_new"
    return "unknown"


def _capacity_tokens(value: str | None) -> set[str]:
    raw = (value or "").lower()
    tokens: set[str] = set()
    for match in _CAPACITY_TOKEN_PATTERN.finditer(raw):
        token = match.group(0).replace(" ", "").strip()
        if token:
            tokens.add(token)
    return tokens


def _build_rebuy_candidate_context(*, payload: dict[str, Any], source_url: str) -> str:
    parts: list[str] = [source_url]
    for key in ("resolved_source_url", "price_text", "query"):
        value = str(payload.get(key) or "").strip()
        if value:
            parts.append(value)
    for key in ("result_pick", "deep_link_pick"):
        row = payload.get(key)
        if not isinstance(row, dict):
            continue
        for field in ("text", "href", "url"):
            value = str(row.get(field) or "").strip()
            if value:
                parts.append(value)
    return " ".join(parts)


def _valuator_platform_name(valuator: Any) -> str:
    value = getattr(valuator, "platform_name", None) or getattr(valuator, "platform", None)
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return valuator.__class__.__name__.strip().lower()


def _valuator_backoff_threshold(platform: str) -> int:
    default_threshold = max(1, int(_env_or_default("VALUATOR_BACKOFF_DEFAULT_ERRORS", "2")))
    defaults = {
        # Do not disable platforms too aggressively: a single transient reseller failure
        # should not silence quotes for the rest of the scan.
        "mpb": 2,
        "trenddevice": 3,
    }
    platform_name = platform.strip().lower()
    platform_default = defaults.get(platform_name, default_threshold)
    env_name = f"VALUATOR_BACKOFF_{platform_name.upper()}_ERRORS"
    return max(1, int(_env_or_default(env_name, str(platform_default))))


def _valuator_parallel_limit(platform: str) -> int:
    platform_name = (platform or "").strip().lower()
    defaults = {
        "mpb": 1,
        "trenddevice": 2,
    }
    default_limit = defaults.get(platform_name, 4)
    env_name = f"VALUATOR_MAX_PARALLEL_{platform_name.upper()}"
    try:
        value = int(_env_or_default(env_name, str(default_limit)))
    except ValueError:
        value = default_limit
    return max(1, min(value, 12))


def _valuator_query_variant_limit(platform: str) -> int:
    platform_name = (platform or "").strip().lower()
    defaults = {
        "rebuy": 2,
        "trenddevice": 2,
        "mpb": 1,
    }
    default_limit = defaults.get(platform_name, 1)
    env_name = f"VALUATOR_QUERY_VARIANTS_{platform_name.upper()}_MAX"
    try:
        value = int(_env_or_default(env_name, str(default_limit)))
    except ValueError:
        value = default_limit
    return max(1, min(value, 6))


def _valuator_timeout_seconds(platform: str) -> float:
    platform_name = (platform or "").strip().lower()
    defaults = {
        "rebuy": 45.0,
        "trenddevice": 45.0,
        "mpb": 28.0,
    }
    default_timeout = defaults.get(platform_name, 45.0)
    env_name = f"VALUATOR_TIMEOUT_{platform_name.upper()}_SECONDS"
    raw = _env_or_default(env_name, _env_or_default("VALUATOR_TIMEOUT_DEFAULT_SECONDS", str(default_timeout)))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = default_timeout
    return max(12.0, min(value, 180.0))


def _trim_query_variant(value: str) -> str:
    cleaned = re.sub(r"[\[\]\(\)\|,;/]+", " ", value or "")
    cleaned = re.sub(
        r"\b(warehouse|ricondizionat[oa]?|renewed|reconditioned|usato|used|pack|bundle|combo)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    tokens = cleaned.split(" ")
    if len(tokens) > 8:
        cleaned = " ".join(tokens[:8])
    return cleaned


def _build_query_variants_for_valuator(product: AmazonProduct, normalized_name: str, platform: str) -> list[str]:
    max_variants = _valuator_query_variant_limit(platform)
    variants: list[str] = []
    seen: set[str] = set()

    def _push(raw: str | None) -> None:
        value = re.sub(r"\s+", " ", (raw or "").strip())
        if len(value) < 3:
            return
        marker = value.casefold()
        if marker in seen:
            return
        seen.add(marker)
        variants.append(value)

    if platform in {"rebuy", "mpb"}:
        ean = (product.ean or "").strip()
        if ean and re.fullmatch(r"[0-9\-\s]{8,20}", ean):
            _push(ean)
    _push(normalized_name)
    _push(_trim_query_variant(normalized_name))
    _push(_trim_query_variant(product.title))
    _push(product.title)
    if not variants:
        variants.append(normalized_name)
    return variants[:max_variants]


def _has_quote_verification(result: ValuationResult) -> bool:
    payload = result.raw_payload if isinstance(result.raw_payload, dict) else {}
    verification = payload.get("quote_verification")
    return isinstance(verification, dict) and "ok" in verification


def _should_retry_valuator_result(platform: str, result: ValuationResult, *, attempt: int, max_attempts: int) -> bool:
    if attempt >= max_attempts:
        return False
    if result.is_valid and result.offer_eur is not None:
        return False
    error_text = (result.error or "").strip().lower()
    if not error_text:
        return True
    hard_markers = (
        "storage_state missing/invalid",
        "temporarily paused after anti-bot challenge",
        "blocked by anti-bot challenge",
        "turnstile/cloudflare",
    )
    if any(marker in error_text for marker in hard_markers):
        return False
    soft_markers = (
        "low-confidence",
        "price not found",
        "generic-source-url",
        "generic-url",
        "quote verification failed",
        "search input not found",
        "email-gate",
        "stagnant-options",
        "wizard",
    )
    if any(marker in error_text for marker in soft_markers):
        return True
    # Conservative default for TrendDevice/Rebuy/MPB: one extra query variant before giving up.
    return platform in {"trenddevice", "rebuy", "mpb"}


def _operating_cost_eur() -> float:
    profile = get_strategy_profile_snapshot()
    value = profile.get("operating_cost_eur", 0.0)
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = 0.0
    return max(0.0, min(value, 400.0))


def _condition_risk_base(condition: str | None) -> float:
    profile = get_strategy_profile_snapshot()
    risk_buffers = profile.get("risk_buffers", {})
    if not isinstance(risk_buffers, dict):
        return 0.0
    key = _condition_key(condition)
    try:
        value = float(risk_buffers.get(key, risk_buffers.get("unknown", 0.0)))
    except (TypeError, ValueError):
        value = 0.0
    return max(0.0, value)


def _condition_risk_buffer(product: AmazonProduct) -> float:
    base = _condition_risk_base(getattr(product, "amazon_condition", None))
    if base <= 0:
        return 0.0
    profile = get_strategy_profile_snapshot()
    if bool(getattr(product, "amazon_packaging_only", False)):
        try:
            multiplier = float(profile.get("packaging_only_factor", 0.45))
        except (TypeError, ValueError):
            multiplier = 0.45
        base *= max(0.1, min(multiplier, 1.0))
    confidence = float(getattr(product, "amazon_condition_confidence", 0.0) or 0.0)
    confidence = max(0.0, min(confidence, 1.0))
    if confidence <= 0:
        return round(base, 2)
    try:
        uncertainty_floor = float(profile.get("uncertainty_floor", 0.70))
    except (TypeError, ValueError):
        uncertainty_floor = 0.70
    try:
        uncertainty_scale = float(profile.get("uncertainty_scale", 12.0))
    except (TypeError, ValueError):
        uncertainty_scale = 12.0
    # Lower confidence means larger safety buffer.
    uncertainty_bonus = max(0.0, (uncertainty_floor - confidence) * uncertainty_scale)
    return round(base + uncertainty_bonus, 2)


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
                "storage_state missing/invalid",
            )
        )
    if platform == "trenddevice":
        return "storage_state missing/invalid" in error_text
    return False


def _is_live_quote_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict) or not payload:
        return False
    return any(
        key in payload
        for key in (
            "price_text",
            "price_source",
            "match_quality",
            "attempts",
            "adaptive_fallbacks",
            "storage_state",
        )
    )


def _is_generic_rebuy_offer_url(url: str | None) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    if path.startswith("comprare/search") or path.startswith("vendere/cerca") or path in {"vendi", "vendere"}:
        return True
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) <= 1:
        return True
    if segments[0] == "vendere":
        # Accept both:
        # - /vendere/<category>/<slug>_<id>
        # - /vendere/p/<slug>/<id>
        if len(segments) >= 4 and segments[1] == "p" and segments[-1].isdigit():
            return False
        if "_" in segments[-1] and segments[-1].split("_")[-1].isdigit():
            return False
        return True
    if len(segments) == 2 and segments[0] == "comprare":
        return True
    return False


def _is_generic_trenddevice_offer_url(url: str | None) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    return path in {"vendi", "vendi/valutazione"}


def _is_generic_mpb_offer_url(url: str | None) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    if path.startswith("it-it/cerca"):
        return True
    return path in {"it-it/sell", "sell"}


def _verified_offer(result: ValuationResult, *, payload: dict[str, Any], checks: dict[str, Any]) -> ValuationResult:
    payload_copy = deepcopy(payload)
    payload_copy["quote_verification"] = {"ok": True, "checks": checks}
    return ValuationResult(
        platform=result.platform,
        normalized_name=result.normalized_name,
        offer_eur=result.offer_eur,
        condition=result.condition,
        currency=result.currency,
        source_url=result.source_url,
        raw_payload=payload_copy,
        error=None,
    )


def _rejected_offer(result: ValuationResult, *, payload: dict[str, Any], checks: dict[str, Any], reason: str) -> ValuationResult:
    payload_copy = deepcopy(payload)
    payload_copy["quote_verification"] = {"ok": False, "checks": checks, "reason": reason}
    platform = (result.platform or "valuator").strip().lower()
    return ValuationResult(
        platform=result.platform,
        normalized_name=result.normalized_name,
        offer_eur=None,
        condition=result.condition,
        currency=result.currency,
        source_url=result.source_url,
        raw_payload=payload_copy,
        error=f"{platform} quote verification failed ({reason}); discarded to prevent false-positive.",
    )


def _verify_real_resale_quote(result: ValuationResult) -> ValuationResult:
    if not result.is_valid or result.offer_eur is None:
        return result
    platform = (result.platform or "").strip().lower()
    if platform not in {"rebuy", "trenddevice", "mpb"}:
        return result

    payload = result.raw_payload if isinstance(result.raw_payload, dict) else {}
    if not _is_live_quote_payload(payload):
        return result

    source_url = (result.source_url or "").strip()
    reasons: list[str] = []
    checks: dict[str, Any] = {
        "platform": platform,
        "offer_eur": result.offer_eur,
        "source_url": source_url or None,
    }
    if result.offer_eur <= 0 or result.offer_eur > 10000:
        reasons.append("offer-out-of-range")

    match_quality = payload.get("match_quality")
    if isinstance(match_quality, dict):
        checks["match_ok"] = bool(match_quality.get("ok"))
        checks["match_reason"] = match_quality.get("reason")
        if not checks["match_ok"]:
            reasons.append("match-quality")

    price_text = str(payload.get("price_text") or "").strip()
    checks["price_text_present"] = bool(price_text)
    checks["price_source"] = str(payload.get("price_source") or "").strip() or None

    if platform == "rebuy":
        checks["generic_url"] = _is_generic_rebuy_offer_url(source_url)
        if checks["generic_url"]:
            generic_override = False
            token_ratio = 0.0
            if isinstance(match_quality, dict):
                try:
                    token_ratio = float(match_quality.get("token_ratio") or 0.0)
                except (TypeError, ValueError):
                    token_ratio = 0.0
                generic_override = bool(match_quality.get("generic_override")) or (
                    bool(match_quality.get("ok")) and token_ratio >= 0.72
                )
            wizard_states = payload.get("wizard_states")
            has_offer_state = False
            if isinstance(wizard_states, list):
                has_offer_state = any(
                    isinstance(item, dict) and str(item.get("state", "")).lower() == "offer"
                    for item in wizard_states[-4:]
                )
            price_source = str(payload.get("price_source") or "").strip().lower()
            checks["token_ratio"] = round(token_ratio, 3)
            checks["generic_override"] = generic_override
            checks["price_source"] = price_source or checks["price_source"]
            checks["has_offer_state"] = has_offer_state
            if not generic_override and not (price_source == "dom-cash" and has_offer_state):
                reasons.append("generic-source-url")
        query_storage_tokens = _capacity_tokens(result.normalized_name)
        if not query_storage_tokens:
            query_storage_tokens = _capacity_tokens(str(payload.get("query") or ""))
        candidate_context = _build_rebuy_candidate_context(payload=payload, source_url=source_url)
        candidate_storage_tokens = _capacity_tokens(candidate_context)
        checks["query_storage_tokens"] = sorted(query_storage_tokens)
        checks["candidate_storage_tokens"] = sorted(candidate_storage_tokens)
        if query_storage_tokens and candidate_storage_tokens and not query_storage_tokens.intersection(candidate_storage_tokens):
            reasons.append("variant-storage-mismatch")
        if not price_text:
            reasons.append("missing-price-context")
    elif platform == "trenddevice":
        checks["generic_url"] = _is_generic_trenddevice_offer_url(source_url)
        if checks["generic_url"]:
            has_model_step = False
            token_ratio = 0.0
            if isinstance(match_quality, dict):
                has_model_step = bool(match_quality.get("has_model_step"))
                try:
                    token_ratio = float(match_quality.get("token_ratio") or 0.0)
                except (TypeError, ValueError):
                    token_ratio = 0.0
            checks["has_model_step"] = has_model_step
            checks["token_ratio"] = round(token_ratio, 3)
            # TrendDevice wizard often stays on /vendi/valutazione; allow generic URL if the wizard
            # selection coverage is strong enough to prevent false positives.
            if not has_model_step:
                reasons.append("generic-url-no-model-step")
            elif token_ratio < 0.55:
                reasons.append("generic-url-low-coverage")
        if not price_text:
            reasons.append("missing-price-context")
    elif platform == "mpb":
        checks["generic_url"] = _is_generic_mpb_offer_url(source_url)
        if checks["generic_url"]:
            reasons.append("generic-source-url")
        if not checks["price_source"] and not price_text:
            reasons.append("missing-price-context")

    if reasons:
        unique_reasons = ",".join(sorted(set(reasons)))
        return _rejected_offer(result, payload=payload, checks=checks, reason=unique_reasons)
    return _verified_offer(result, payload=payload, checks=checks)


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
        self._platform_semaphores: dict[str, asyncio.Semaphore] = {}

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
        if category == ProductCategory.SMARTWATCH:
            return [TrendDeviceValuator(**common), RebuyValuator(**common)]
        if category == ProductCategory.DRONE:
            return [MPBValuator(**common), RebuyValuator(**common)]
        if category == ProductCategory.HANDHELD_CONSOLE:
            return [RebuyValuator(**common)]
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
        gross_spread = (
            round(best_offer.offer_eur - product.price_eur, 2) if best_offer and best_offer.offer_eur is not None else None
        )
        operating_cost = _operating_cost_eur()
        risk_buffer = _condition_risk_buffer(product)
        strategy_profile = get_strategy_profile_name()
        spread = round(gross_spread - operating_cost - risk_buffer, 2) if gross_spread is not None else None
        should_notify = spread is not None and spread > self.min_spread_eur
        print(
            "[scan] Decision -> "
            f"best_platform={best_offer.platform if best_offer else None} | "
            f"best_offer={best_offer.offer_eur if best_offer else None} | "
            f"strategy={strategy_profile} | "
            f"spread_gross={gross_spread} | operating_cost={operating_cost} | risk_buffer={risk_buffer} | "
            f"spread_net={spread} | should_notify={should_notify}"
        )
        usage = ai_usage or {}
        return ArbitrageDecision(
            product=product,
            normalized_name=normalized_name,
            offers=cloned_offers,
            best_offer=best_offer,
            spread_eur=spread,
            should_notify=should_notify,
            spread_gross_eur=gross_spread,
            operating_cost_eur=operating_cost,
            risk_buffer_eur=risk_buffer,
            strategy_profile=strategy_profile,
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

        async def _run_once(valuator: Any, query_name: str) -> ValuationResult:
            platform = _valuator_platform_name(valuator)
            limit = _valuator_parallel_limit(platform)
            timeout_seconds = _valuator_timeout_seconds(platform)

            async def _execute() -> ValuationResult:
                return await asyncio.wait_for(
                    valuator.valuate(product, query_name),
                    timeout=timeout_seconds,
                )

            async def _timeout_result() -> ValuationResult:
                return ValuationResult(
                    platform=platform,
                    normalized_name=normalized_name,
                    offer_eur=None,
                    error=(
                        f"{platform} valuation timeout after {timeout_seconds:.0f}s "
                        f"(query='{query_name}')"
                    ),
                    raw_payload={
                        "timeout_s": timeout_seconds,
                        "query": query_name,
                        "platform": platform,
                    },
                )

            if limit <= 1:
                semaphore = self._platform_semaphores.get(platform)
                if semaphore is None:
                    semaphore = asyncio.Semaphore(limit)
                    self._platform_semaphores[platform] = semaphore
                async with semaphore:
                    try:
                        return await _execute()
                    except asyncio.TimeoutError:
                        return await _timeout_result()
            try:
                return await _execute()
            except asyncio.TimeoutError:
                return await _timeout_result()

        async def _run_valuator(valuator: Any) -> ValuationResult:
            platform = _valuator_platform_name(valuator)
            query_variants = _build_query_variants_for_valuator(product, normalized_name, platform)
            last_result: ValuationResult | None = None

            for attempt_index, query_name in enumerate(query_variants, start=1):
                raw = await _run_once(valuator, query_name)
                verified = raw if _has_quote_verification(raw) else _verify_real_resale_quote(raw)
                payload = deepcopy(verified.raw_payload) if isinstance(verified.raw_payload, dict) else {}
                payload["query_retry"] = {
                    "enabled": len(query_variants) > 1,
                    "attempt": attempt_index,
                    "max_attempts": len(query_variants),
                    "query": query_name,
                    "platform": platform,
                }
                verified = ValuationResult(
                    platform=verified.platform,
                    normalized_name=verified.normalized_name,
                    offer_eur=verified.offer_eur,
                    condition=verified.condition,
                    currency=verified.currency,
                    source_url=verified.source_url,
                    raw_payload=payload,
                    error=verified.error,
                )
                last_result = verified
                if verified.is_valid and verified.offer_eur is not None:
                    if attempt_index > 1:
                        print(
                            "[scan] Query retry recovered quote | "
                            f"platform={platform} attempt={attempt_index}/{len(query_variants)} query='{query_name}' "
                            f"offer={verified.offer_eur}"
                        )
                    return verified
                if not _should_retry_valuator_result(
                    platform,
                    verified,
                    attempt=attempt_index,
                    max_attempts=len(query_variants),
                ):
                    return verified
                print(
                    "[scan] Query retry next variant | "
                    f"platform={platform} attempt={attempt_index}/{len(query_variants)} "
                    f"query='{query_name}' error={verified.error}"
                )
            if last_result is not None:
                return last_result
            return ValuationResult(
                platform=platform,
                normalized_name=normalized_name,
                offer_eur=None,
                error=f"{platform} valuation failed before query attempts.",
            )

        tasks = [_run_valuator(valuator) for valuator in valuators]
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
            verified = raw if _has_quote_verification(raw) else _verify_real_resale_quote(raw)
            offers.append(verified)
            print(
                "[scan] Offer result -> "
                f"platform={verified.platform} | offer={verified.offer_eur} | valid={verified.is_valid} | error={verified.error}"
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
