from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
from statistics import median
from typing import Any
from urllib.parse import quote_plus, urlparse

from dotenv import load_dotenv
from telegram import Bot

from tech_sniper_it.manager import build_default_manager
from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.sources import fetch_amazon_warehouse_products


MAX_LAST_LIMIT = 10
TELEGRAM_TEXT_LIMIT = 4000
SCORING_DEFAULT_LOOKBACK_DAYS = 30
SCORING_DEFAULT_HISTORY_LIMIT = 2000

CATEGORY_REQUIRED_PLATFORMS: dict[ProductCategory, tuple[str, ...]] = {
    ProductCategory.APPLE_PHONE: ("trenddevice", "rebuy"),
    ProductCategory.PHOTOGRAPHY: ("mpb", "rebuy"),
    ProductCategory.GENERAL_TECH: ("rebuy",),
}

CATEGORY_FALLBACK_OFFER_RATIO: dict[ProductCategory, float] = {
    ProductCategory.APPLE_PHONE: 0.38,
    ProductCategory.PHOTOGRAPHY: 0.30,
    ProductCategory.GENERAL_TECH: 0.22,
}

LIQUIDITY_PATTERNS: tuple[tuple[str, float], ...] = (
    (r"\biphone\b", 65.0),
    (r"\biphone\s*(14|15|16)\b", 35.0),
    (r"\bpro\s*max\b", 25.0),
    (r"\bmacbook\s*(air|pro)\b", 48.0),
    (r"\bcanon\s*eos\b", 40.0),
    (r"\bsony\s*alpha\b", 40.0),
    (r"\bplaystation\s*5\b|\bps5\b", 42.0),
    (r"\bxbox\s*series\s*x\b", 35.0),
)

ACCESSORY_KEYWORDS: tuple[str, ...] = (
    "custodia",
    "coque",
    "hulle",
    "hülle",
    "funda",
    "cover",
    "case",
    "bumper",
    "sleeve",
    "shell",
    "pellicola",
    "screen protector",
    "vetro temperato",
    "protezione schermo",
    "caricatore",
    "charger",
    "cavo",
    "cable",
    "adattatore",
    "adapter",
    "alimentatore",
    "mouse",
    "tastiera",
    "keyboard",
    "supporto",
    "stand",
    "dock",
    "hub usb",
    "borsa",
    "bag",
    "zaino",
)

COMPATIBILITY_MARKERS: tuple[str, ...] = (
    "compatibile con",
    "compatible with",
    "compatible avec",
    "compatible",
    "pour ",
    "for ",
    "per ",
    "für ",
)

CORE_DEVICE_MARKERS: tuple[str, ...] = (
    "smartphone",
    "telefono",
    "cellulare",
    "notebook",
    "laptop",
    "fotocamera",
    "mirrorless",
    "dslr",
    "console",
    "ricondizionato",
    "refurbished",
    "renewed",
    "usato",
)

BUNDLE_INCLUDED_MARKERS: tuple[str, ...] = (
    "con cover",
    "cover inclusa",
    "cover incluso",
    "custodia inclusa",
    "custodia incluso",
    "with case",
    "case included",
    "avec coque",
    "coque incluse",
    "inklusive hülle",
)

DEVICE_ANCHOR_PRICE_FLOOR: tuple[tuple[str, float], ...] = (
    ("iphone", 120.0),
    ("macbook", 180.0),
    ("ipad", 100.0),
    ("canon eos", 180.0),
    ("sony alpha", 180.0),
    ("mirrorless", 180.0),
    ("playstation", 120.0),
    ("xbox", 120.0),
)


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _is_truthy_env(name: str, default: str) -> bool:
    return _env_or_default(name, default).lower() not in {"0", "false", "no", "off"}


def _load_github_event_data() -> dict[str, Any]:
    event_path = os.getenv("GITHUB_EVENT_PATH")
    if not event_path or not Path(event_path).exists():
        return {}
    try:
        data = json.loads(Path(event_path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _get_client_payload(event_data: dict[str, Any]) -> dict[str, Any]:
    payload = event_data.get("client_payload")
    return payload if isinstance(payload, dict) else {}


def _resolve_command(event_data: dict[str, Any], payload: dict[str, Any]) -> str:
    command = str(payload.get("command", "")).strip().lower()
    if command:
        return command
    action = str(event_data.get("action", "")).strip().lower()
    if action in {"scan", "status", "last"}:
        return action
    return "scan"


def _telegram_target_chat(payload: dict[str, Any]) -> str | None:
    raw = payload.get("chat_id")
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


async def _send_telegram_message(text: str, chat_id: str | None) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    target_chat = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not target_chat:
        print("Telegram not configured for command response; skipping message.")
        return
    bot = Bot(token=token)
    chunks = _chunk_telegram_text(text)
    for chunk in chunks:
        await bot.send_message(chat_id=target_chat, text=chunk, disable_web_page_preview=True)


def _chunk_telegram_text(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> list[str]:
    if limit <= 0:
        return [text]

    payload = text or ""
    if len(payload) <= limit:
        return [payload]

    lines = payload.splitlines(keepends=True)
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    def flush_current() -> None:
        nonlocal current, current_len
        if not current:
            return
        chunks.append("".join(current).rstrip("\n"))
        current = []
        current_len = 0

    for line in lines:
        segments = [line]
        if len(line) > limit:
            segments = [line[i : i + limit] for i in range(0, len(line), limit)]

        for segment in segments:
            if current and current_len + len(segment) > limit:
                flush_current()
            current.append(segment)
            current_len += len(segment)

    flush_current()
    return chunks if chunks else [payload[:limit]]


def _safe_error_details(exc: Exception, max_len: int = 220) -> str:
    raw = " ".join(str(exc).split())
    if not raw:
        return exc.__class__.__name__
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3] + "..."


def _safe_text(value: str | None, max_len: int = 220) -> str | None:
    raw = " ".join((value or "").split())
    if not raw:
        return None
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3] + "..."


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _normalize_for_scoring(value: str) -> str:
    raw = (value or "").lower()
    raw = re.sub(r"\([^)]*\)", " ", raw)
    raw = re.sub(
        r"\b(warehouse|ricondizionato|ottime condizioni|come nuovo|grado a|usato|amazon|qwerty|italiano)\b",
        " ",
        raw,
    )
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw[:120]


def _marketplace_from_url(url: str | None) -> str | None:
    normalized = _normalize_http_url(url)
    if not normalized:
        return None
    host = (urlparse(normalized).hostname or "").lower()
    if host.endswith(".it"):
        return "it"
    if host.endswith(".de"):
        return "de"
    if host.endswith(".fr"):
        return "fr"
    if host.endswith(".es"):
        return "es"
    return None


def _candidate_marketplace(product: AmazonProduct) -> str:
    explicit = (getattr(product, "source_marketplace", None) or "").strip().lower()
    if explicit in {"it", "de", "fr", "es"}:
        return explicit
    from_url = _marketplace_from_url(getattr(product, "url", None))
    if from_url:
        return from_url
    return "unknown"


def _candidate_region(product: AmazonProduct) -> str:
    marketplace = _candidate_marketplace(product)
    if marketplace == "it":
        return "it"
    if marketplace in {"de", "fr", "es"}:
        return "eu"
    return "other"


def _required_platforms_for_category(category: ProductCategory) -> tuple[str, ...]:
    return CATEGORY_REQUIRED_PLATFORMS.get(category, ("rebuy",))


def _liquidity_bonus(title: str) -> float:
    lowered = (title or "").lower()
    score = 0.0
    for pattern, bonus in LIQUIDITY_PATTERNS:
        if re.search(pattern, lowered):
            score += bonus
    return score


def _has_storage_token(text: str) -> bool:
    return bool(re.search(r"\b\d{2,4}\s?(gb|tb)\b", text.lower()))


def _contains_token(text: str, token: str) -> bool:
    lowered = text.lower()
    normalized_token = token.lower()
    if normalized_token.strip() == "for":
        return bool(re.search(r"\bfor\b", lowered))
    if normalized_token.strip() == "per":
        return bool(re.search(r"\bper\b", lowered))
    if normalized_token.strip() == "pour":
        return bool(re.search(r"\bpour\b", lowered))
    if normalized_token.strip() == "für":
        return "für" in lowered or "fur " in lowered
    return normalized_token in lowered


def _device_anchor_floor(title: str) -> float | None:
    floor: float | None = None
    for anchor, threshold in DEVICE_ANCHOR_PRICE_FLOOR:
        if anchor in title:
            floor = max(floor or 0.0, float(threshold))
    return floor


def _is_core_device_sale_context(
    title: str,
    *,
    price_eur: float,
    storage_hit: bool,
    anchor_floor: float | None,
) -> bool:
    has_anchor = anchor_floor is not None
    has_device_marker = any(_contains_token(title, token) for token in CORE_DEVICE_MARKERS)
    bundle_included = any(_contains_token(title, token) for token in BUNDLE_INCLUDED_MARKERS)
    if not has_anchor:
        return False
    floor = float(anchor_floor or 0.0)
    if price_eur < floor:
        return False
    if storage_hit or has_device_marker:
        return True
    if bundle_included and price_eur >= max(120.0, floor):
        return True
    return False


def _accessory_guardrail_reasons(product: AmazonProduct) -> list[str]:
    title = (product.title or "").lower()
    reasons: list[str] = []
    accessory_hit = any(_contains_token(title, token) for token in ACCESSORY_KEYWORDS)
    compatibility_hit = any(_contains_token(title, token) for token in COMPATIBILITY_MARKERS)
    storage_hit = _has_storage_token(title)
    anchor_floor = _device_anchor_floor(title)
    low_price_anchor = bool(anchor_floor is not None and float(product.price_eur) < float(anchor_floor))
    if low_price_anchor:
        reasons.append(f"low-price-anchor<{float(anchor_floor):.0f}")

    if accessory_hit and _is_core_device_sale_context(
        title,
        price_eur=float(product.price_eur),
        storage_hit=storage_hit,
        anchor_floor=anchor_floor,
    ):
        return []

    if accessory_hit and compatibility_hit:
        reasons.append("accessory+compatibility")
    if accessory_hit and low_price_anchor:
        reasons.append("accessory+low-price")
    if accessory_hit and (not storage_hit) and float(product.price_eur) < 80.0:
        reasons.append("accessory-no-storage-low-price")
    return reasons


def _filter_non_core_device_candidates(products: list[AmazonProduct]) -> tuple[list[AmazonProduct], list[str]]:
    if not _is_truthy_env("SCAN_FILTER_ACCESSORIES", "true"):
        return products, []
    kept: list[AmazonProduct] = []
    dropped_logs: list[str] = []
    for item in products:
        reasons = _accessory_guardrail_reasons(item)
        if reasons:
            dropped_logs.append(
                f"title='{_safe_text(item.title, max_len=90)}' price={item.price_eur:.2f} reasons={','.join(reasons)}"
            )
            continue
        kept.append(item)
    return kept, dropped_logs


def _offer_log_payload(offer) -> dict[str, Any]:  # noqa: ANN001
    valid_value = getattr(offer, "is_valid", None)
    if callable(valid_value):
        try:
            valid_value = bool(valid_value())
        except Exception:
            valid_value = None
    raw_payload = getattr(offer, "raw_payload", {}) or {}
    ui_probes = raw_payload.get("ui_probes")
    probe_rows = ui_probes if isinstance(ui_probes, list) else []
    ui_drift = any(isinstance(item, dict) and bool(item.get("drift_suspected")) for item in probe_rows)
    adaptive_fallbacks = raw_payload.get("adaptive_fallbacks", {})
    network_candidates = raw_payload.get("network_price_candidates")
    return {
        "platform": getattr(offer, "platform", "unknown"),
        "offer_eur": getattr(offer, "offer_eur", None),
        "condition": getattr(offer, "condition", None),
        "currency": getattr(offer, "currency", "EUR"),
        "valid": valid_value,
        "error": _safe_text(getattr(offer, "error", None)),
        "source_url": getattr(offer, "source_url", None),
        "ui_drift": ui_drift,
        "ui_probe_count": len(probe_rows),
        "adaptive_fallbacks": adaptive_fallbacks if isinstance(adaptive_fallbacks, dict) else {},
        "price_source": raw_payload.get("price_source"),
        "network_candidates_count": len(network_candidates) if isinstance(network_candidates, list) else 0,
        "storage_state_used": bool(raw_payload.get("storage_state", False)),
    }


def _offer_has_ui_drift(offer) -> bool:  # noqa: ANN001
    raw_payload = getattr(offer, "raw_payload", {}) or {}
    probes = raw_payload.get("ui_probes")
    if not isinstance(probes, list):
        return False
    return any(isinstance(item, dict) and bool(item.get("drift_suspected")) for item in probes)


def _ui_drift_stats(decisions: list) -> tuple[int, int]:  # noqa: ANN001
    drift_count = 0
    total_offers = 0
    for decision in decisions:
        for offer in getattr(decision, "offers", []):
            total_offers += 1
            if _offer_has_ui_drift(offer):
                drift_count += 1
    return drift_count, total_offers


def _format_eur(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value:.2f} EUR"


def _format_signed_eur(value: float | None) -> str:
    if value is None:
        return "n/d"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f} EUR"


def _platform_icon(platform: str | None) -> str:
    mapping = {
        "rebuy": "♻️",
        "trenddevice": "📱",
        "mpb": "📸",
    }
    return mapping.get((platform or "").lower(), "🏷️")


def _format_offers_compact(decision) -> str:  # noqa: ANN001
    items: list[str] = []
    for offer in decision.offers:
        icon = _platform_icon(getattr(offer, "platform", None))
        if offer.offer_eur is not None:
            items.append(f"{icon} {offer.platform}: {_format_eur(offer.offer_eur)}")
        else:
            error = _safe_text(offer.error, max_len=60)
            suffix = f" ({error})" if error else ""
            items.append(f"{icon} {offer.platform}: n/d{suffix}")
    return " | ".join(items) if items else "n/d"


def _ai_usage_label(decision) -> str:  # noqa: ANN001
    provider = str(getattr(decision, "ai_provider", None) or "heuristic")
    model = getattr(decision, "ai_model", None)
    mode = str(getattr(decision, "ai_mode", None) or "fallback")
    if model:
        return f"provider={provider} | model={model} | mode={mode}"
    return f"provider={provider} | model=rule-based | mode={mode}"


def _ai_usage_stats(decisions: list) -> tuple[int, int, int]:  # noqa: ANN001
    gemini = 0
    openrouter = 0
    heuristic = 0
    for decision in decisions:
        provider = str(getattr(decision, "ai_provider", "")).lower()
        if provider == "gemini":
            gemini += 1
        elif provider == "openrouter":
            openrouter += 1
        else:
            heuristic += 1
    return gemini, openrouter, heuristic


def _ai_model_overview(decisions: list) -> str:  # noqa: ANN001
    counters: dict[str, int] = {}
    for decision in decisions:
        provider = str(getattr(decision, "ai_provider", None) or "heuristic")
        model = str(getattr(decision, "ai_model", None) or "rule-based")
        key = f"{provider}:{model}"
        counters[key] = counters.get(key, 0) + 1
    if not counters:
        return "n/d"
    ordered = sorted(counters.items(), key=lambda item: (-item[1], item[0]))
    return " | ".join(f"{key} x{count}" for key, count in ordered)


def _spread_status_badge(spread_eur: float | None, threshold: float) -> tuple[str, str]:
    if spread_eur is None:
        return "⚪", "Valutazione incompleta"
    if spread_eur >= threshold:
        return "🟢", "OPPORTUNITA"
    if spread_eur >= 0:
        return "🟡", "Margine basso"
    return "🔴", "Sotto costo"


def _normalize_http_url(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None

    candidate = raw
    if raw.startswith("//"):
        candidate = f"https:{raw}"
    elif not raw.startswith(("http://", "https://")):
        if "." in raw and " " not in raw:
            candidate = f"https://{raw}"
        else:
            return None

    parsed = urlparse(candidate)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return candidate
    return None


def _amazon_search_url(query: str) -> str:
    return f"https://www.amazon.it/s?k={quote_plus(query)}"


def _parse_last_limit(payload: dict[str, Any]) -> int:
    raw = payload.get("limit", 5)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 5
    return max(1, min(value, MAX_LAST_LIMIT))


def _coerce_product(raw: dict[str, Any]) -> AmazonProduct:
    title = str(raw.get("title", "")).strip()
    if not title:
        raise ValueError("Product missing title")
    price_raw = raw.get("price_eur", raw.get("price"))
    if price_raw is None:
        raise ValueError(f"Product '{title}' missing price")
    price = float(price_raw)
    category = ProductCategory.from_raw(str(raw.get("category", "")))
    source_marketplace_raw = raw.get("source_marketplace")
    source_marketplace = str(source_marketplace_raw).strip().lower() if source_marketplace_raw is not None else ""
    return AmazonProduct(
        title=title,
        price_eur=price,
        category=category,
        ean=raw.get("ean"),
        url=raw.get("url"),
        source_marketplace=source_marketplace or None,
    )


def _load_from_github_event(event_data: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    data = event_data or _load_github_event_data()
    payload = _get_client_payload(data)
    products = payload.get("products")
    if isinstance(products, list):
        return [item for item in products if isinstance(item, dict)]
    single = payload.get("product")
    if isinstance(single, dict):
        return [single]
    return []


def _load_from_file() -> list[dict[str, Any]]:
    file_path = os.getenv("AMAZON_PRODUCTS_FILE")
    if not file_path:
        return []
    path = Path(file_path)
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        products = data.get("products")
        if isinstance(products, list):
            return [item for item in products if isinstance(item, dict)]
    return []


def _load_from_env_json() -> list[dict[str, Any]]:
    raw = os.getenv("AMAZON_PRODUCTS_JSON", "[]")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def load_products(event_data: dict[str, Any] | None = None) -> list[AmazonProduct]:
    raw_items = _load_from_github_event(event_data) or _load_from_file() or _load_from_env_json()
    products: list[AmazonProduct] = []
    for item in raw_items:
        try:
            products.append(_coerce_product(item))
        except Exception as exc:
            print(f"Skipping invalid product payload: {exc}")
    return products


def _product_dedupe_key(product: AmazonProduct) -> str:
    normalized_url = _normalize_http_url(getattr(product, "url", None))
    if normalized_url:
        return f"url:{normalized_url}"
    title = str(getattr(product, "title", ""))
    price = float(getattr(product, "price_eur", 0.0))
    category = getattr(getattr(product, "category", None), "value", "general_tech")
    title_key = " ".join(title.lower().split())
    return f"title:{title_key}|price:{price:.2f}|cat:{category}"


def _dedupe_products(products: list[AmazonProduct]) -> list[AmazonProduct]:
    deduped: list[AmazonProduct] = []
    seen: set[str] = set()
    for product in products:
        key = _product_dedupe_key(product)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(product)
    return deduped


def _legacy_priority_key(product: AmazonProduct) -> tuple[float, int, int]:
    category_weight = {
        ProductCategory.APPLE_PHONE: 0,
        ProductCategory.PHOTOGRAPHY: 1,
        ProductCategory.GENERAL_TECH: 2,
    }
    return (
        float(product.price_eur),
        category_weight.get(product.category, 9),
        len(product.title),
    )


async def _build_prioritization_context(manager) -> dict[str, Any]:  # noqa: ANN001
    context: dict[str, Any] = {
        "enabled": _is_truthy_env("SCORING_ENABLE", "true"),
        "rows_count": 0,
        "exact_offer_median": {},
        "exact_confidence": {},
        "category_offer_median": {},
        "category_spread_median": {},
        "platform_health": {},
    }
    if not context["enabled"]:
        return context

    storage = getattr(manager, "storage", None)
    get_rows = getattr(storage, "get_recent_scoring_rows", None)
    if storage is None or not callable(get_rows):
        return context

    try:
        lookback_days = max(1, int(_env_or_default("SCORING_LOOKBACK_DAYS", str(SCORING_DEFAULT_LOOKBACK_DAYS))))
    except ValueError:
        lookback_days = SCORING_DEFAULT_LOOKBACK_DAYS
    try:
        limit = max(100, int(_env_or_default("SCORING_HISTORY_LIMIT", str(SCORING_DEFAULT_HISTORY_LIMIT))))
    except ValueError:
        limit = SCORING_DEFAULT_HISTORY_LIMIT
    try:
        rows = await get_rows(lookback_days=lookback_days, limit=limit)
    except Exception as exc:
        print(f"[scan] Scoring context unavailable: {_safe_error_details(exc)}")
        return context
    if not rows:
        return context

    exact_offer_samples: dict[str, list[float]] = {}
    category_offer_samples: dict[str, list[float]] = {}
    category_spread_samples: dict[str, list[float]] = {}
    platform_totals: dict[str, int] = {}
    platform_successes: dict[str, int] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized_name = _normalize_for_scoring(str(row.get("normalized_name", "")))
        category = ProductCategory.from_raw(str(row.get("category", ""))).value
        best_offer = _to_float(row.get("best_offer_eur"))
        spread = _to_float(row.get("spread_eur"))

        if normalized_name and best_offer is not None:
            exact_offer_samples.setdefault(normalized_name, []).append(best_offer)
        if best_offer is not None:
            category_offer_samples.setdefault(category, []).append(best_offer)
        if spread is not None:
            category_spread_samples.setdefault(category, []).append(spread)

        offers_payload = row.get("offers_payload")
        if not isinstance(offers_payload, list):
            continue
        for offer_item in offers_payload:
            if not isinstance(offer_item, dict):
                continue
            platform = str(offer_item.get("platform", "")).strip().lower()
            if not platform:
                continue
            platform_totals[platform] = platform_totals.get(platform, 0) + 1
            error = offer_item.get("error")
            if error in (None, ""):
                platform_successes[platform] = platform_successes.get(platform, 0) + 1

    exact_offer_median: dict[str, float] = {}
    exact_confidence: dict[str, float] = {}
    for key, sample in exact_offer_samples.items():
        score = _median_or_none(sample)
        if score is None:
            continue
        exact_offer_median[key] = score
        exact_confidence[key] = min(1.0, len(sample) / 6.0)

    category_offer_median: dict[str, float] = {}
    for key, sample in category_offer_samples.items():
        score = _median_or_none(sample)
        if score is not None:
            category_offer_median[key] = score

    category_spread_median: dict[str, float] = {}
    for key, sample in category_spread_samples.items():
        score = _median_or_none(sample)
        if score is not None:
            category_spread_median[key] = score

    platform_health: dict[str, dict[str, float | int]] = {}
    for platform, total in platform_totals.items():
        successes = platform_successes.get(platform, 0)
        rate = successes / total if total > 0 else 0.0
        platform_health[platform] = {"rate": round(rate, 3), "samples": total}

    context.update(
        {
            "rows_count": len(rows),
            "exact_offer_median": exact_offer_median,
            "exact_confidence": exact_confidence,
            "category_offer_median": category_offer_median,
            "category_spread_median": category_spread_median,
            "platform_health": platform_health,
        }
    )
    return context


def _valuator_health_adjustment(category: ProductCategory, scoring_context: dict[str, Any]) -> tuple[float, float]:
    platform_health = scoring_context.get("platform_health", {})
    if not isinstance(platform_health, dict):
        return 0.0, 0.6
    required = _required_platforms_for_category(category)
    if not required:
        return 0.0, 0.6

    rates: list[float] = []
    samples = 0
    for platform in required:
        state = platform_health.get(platform, {})
        if not isinstance(state, dict):
            rates.append(0.6)
            continue
        rate = _to_float(state.get("rate"))
        sample_count = int(_to_float(state.get("samples")) or 0)
        samples += sample_count
        rates.append(rate if rate is not None else 0.6)
    avg_rate = sum(rates) / len(rates)
    if samples < 6:
        return 0.0, avg_rate
    if avg_rate < 0.20:
        return -180.0, avg_rate
    if avg_rate < 0.35:
        return -120.0, avg_rate
    if avg_rate < 0.50:
        return -70.0, avg_rate
    if avg_rate > 0.85:
        return 20.0, avg_rate
    return 0.0, avg_rate


def _estimate_offer_and_spread(product: AmazonProduct, scoring_context: dict[str, Any]) -> tuple[float, float, str, float]:
    exact_offer_median = scoring_context.get("exact_offer_median", {})
    exact_confidence = scoring_context.get("exact_confidence", {})
    category_offer_median = scoring_context.get("category_offer_median", {})
    category_spread_median = scoring_context.get("category_spread_median", {})

    normalized_title = _normalize_for_scoring(product.title)
    source = "fallback_ratio"
    confidence = 0.25

    exact_offer = _to_float(exact_offer_median.get(normalized_title)) if isinstance(exact_offer_median, dict) else None
    if exact_offer is not None:
        source = "exact_model_history"
        confidence = _to_float(exact_confidence.get(normalized_title)) if isinstance(exact_confidence, dict) else None
        confidence = confidence if confidence is not None else 0.8
        expected_offer = exact_offer
    else:
        category_key = product.category.value
        category_offer = _to_float(category_offer_median.get(category_key)) if isinstance(category_offer_median, dict) else None
        if category_offer is not None:
            source = "category_history"
            confidence = 0.55
            expected_offer = category_offer
        else:
            ratio = CATEGORY_FALLBACK_OFFER_RATIO.get(product.category, 0.20)
            expected_offer = round(float(product.price_eur) * ratio, 2)

    expected_spread = round(expected_offer - float(product.price_eur), 2)
    category_spread = (
        _to_float(category_spread_median.get(product.category.value))
        if isinstance(category_spread_median, dict)
        else None
    )
    if category_spread is not None:
        expected_spread = round((expected_spread * 0.75) + (category_spread * 0.25), 2)
    return expected_offer, expected_spread, source, confidence


def _score_product_candidate(product: AmazonProduct, scoring_context: dict[str, Any]) -> dict[str, Any]:
    if not scoring_context.get("enabled"):
        return {
            "score": 0.0,
            "expected_offer": None,
            "expected_spread": None,
            "liquidity_bonus": 0.0,
            "valuator_health_adjustment": 0.0,
            "valuator_health_rate": 0.6,
            "source": "disabled",
            "confidence": 0.0,
            "region": _candidate_region(product),
        }

    expected_offer, expected_spread, source, confidence = _estimate_offer_and_spread(product, scoring_context)
    liquidity = _liquidity_bonus(product.title)
    health_adjustment, health_rate = _valuator_health_adjustment(product.category, scoring_context)
    score = expected_spread + liquidity + health_adjustment + (confidence * 20.0)
    return {
        "score": round(score, 2),
        "expected_offer": expected_offer,
        "expected_spread": expected_spread,
        "liquidity_bonus": round(liquidity, 2),
        "valuator_health_adjustment": round(health_adjustment, 2),
        "valuator_health_rate": round(health_rate, 3),
        "source": source,
        "confidence": round(confidence, 3),
        "region": _candidate_region(product),
    }


def _prioritize_products(
    products: list[AmazonProduct],
    *,
    scoring_context: dict[str, Any] | None = None,
) -> list[AmazonProduct]:
    context = scoring_context or {}
    if not context.get("enabled"):
        return sorted(products, key=_legacy_priority_key)

    rows: list[tuple[AmazonProduct, dict[str, Any]]] = []
    for item in products:
        score_row = _score_product_candidate(item, context)
        rows.append((item, score_row))

    rows.sort(
        key=lambda pair: (
            float(pair[1].get("score", 0.0)),
            float(pair[1].get("expected_spread") or -10_000.0),
            -float(pair[0].price_eur),
        ),
        reverse=True,
    )
    return [item for item, _score in rows]


def _priority_preview(products: list[AmazonProduct], scoring_context: dict[str, Any], limit: int = 8) -> list[str]:
    preview: list[str] = []
    top = products[: max(0, limit)]
    for index, item in enumerate(top, start=1):
        row = _score_product_candidate(item, scoring_context)
        preview.append(
            f"#{index} score={row['score']:.2f} spread_est={row['expected_spread']} "
            f"region={row['region']} source={row['source']} "
            f"health={row['valuator_health_rate']} liquidity={row['liquidity_bonus']:.1f} "
            f"title='{_safe_text(item.title, max_len=70)}'"
        )
    return preview


def _region_counts(products: list[AmazonProduct]) -> dict[str, int]:
    counts = {"it": 0, "eu": 0, "other": 0}
    for item in products:
        region = _candidate_region(item)
        counts[region] = counts.get(region, 0) + 1
    return counts


def _select_balanced_candidates(products: list[AmazonProduct], target: int) -> list[AmazonProduct]:
    if len(products) <= target:
        return products

    queues = {"it": [], "eu": [], "other": []}
    for item in products:
        queues[_candidate_region(item)].append(item)

    it_quota = max(0, int(_env_or_default("SCAN_IT_QUOTA", str(target // 2))))
    eu_quota = max(0, int(_env_or_default("SCAN_EU_QUOTA", str(target // 2))))
    if it_quota + eu_quota > target:
        total_quota = max(1, it_quota + eu_quota)
        it_quota = int((it_quota / total_quota) * target)
        eu_quota = min(target - it_quota, eu_quota)

    selected: list[AmazonProduct] = []
    selected_ids: set[int] = set()

    def _take(region: str, amount: int) -> None:
        while amount > 0 and queues[region] and len(selected) < target:
            item = queues[region].pop(0)
            marker = id(item)
            if marker in selected_ids:
                continue
            selected.append(item)
            selected_ids.add(marker)
            amount -= 1

    _take("it", it_quota)
    _take("eu", eu_quota)

    if len(selected) < target:
        for item in products:
            marker = id(item)
            if marker in selected_ids:
                continue
            selected.append(item)
            selected_ids.add(marker)
            if len(selected) >= target:
                break

    return selected


async def _exclude_non_profitable_candidates(manager, products: list[AmazonProduct]) -> list[AmazonProduct]:  # noqa: ANN001
    storage = getattr(manager, "storage", None)
    if not storage or not _is_truthy_env("EXCLUDE_NON_PROFITABLE", "true"):
        return products

    lookback_days = max(1, int(_env_or_default("EXCLUDE_LOOKBACK_DAYS", "14")))
    max_rows = max(50, int(_env_or_default("EXCLUDE_MAX_ROWS", "1500")))
    excluded_urls = await storage.get_excluded_source_urls(
        max_spread_eur=manager.min_spread_eur,
        lookback_days=lookback_days,
        limit=max_rows,
    )
    if not excluded_urls:
        print("[scan] Exclusion cache: no historical under-threshold urls.")
        return products

    filtered: list[AmazonProduct] = []
    removed_products: list[AmazonProduct] = []
    removed = 0
    for product in products:
        normalized_url = _normalize_http_url(product.url)
        if normalized_url and normalized_url in excluded_urls:
            removed += 1
            removed_products.append(product)
            continue
        filtered.append(product)
    print(
        "[scan] Exclusion cache applied | "
        f"removed={removed} kept={len(filtered)} lookback_days={lookback_days} rows={len(excluded_urls)}"
    )
    min_keep = max(0, int(_env_or_default("EXCLUDE_MIN_KEEP", "0")))
    if min_keep > 0 and len(filtered) < min_keep and removed_products:
        restore_count = min(min_keep - len(filtered), len(removed_products))
        filtered.extend(removed_products[:restore_count])
        print(
            "[scan] Exclusion cache relaxed | "
            f"restored={restore_count} min_keep={min_keep} final={len(filtered)}"
        )
    return filtered


async def _save_non_profitable_decisions(manager, decisions: list) -> int:  # noqa: ANN001
    storage = getattr(manager, "storage", None)
    if not storage:
        return 0
    tasks = [storage.save_non_profitable(decision, threshold=manager.min_spread_eur) for decision in decisions]
    if not tasks:
        return 0
    await asyncio.gather(*tasks)
    saved = sum(1 for decision in decisions if decision.spread_eur is not None and decision.spread_eur <= manager.min_spread_eur)
    print(f"[scan] Stored non-profitable records for exclusion cache: {saved}")
    return saved


def _format_scan_summary(decisions: list, threshold: float) -> str:
    profitable = [item for item in decisions if item.should_notify and item.spread_eur is not None]
    best_spread = max((item.spread_eur for item in decisions if item.spread_eur is not None), default=None)
    gemini_count, openrouter_count, heuristic_count = _ai_usage_stats(decisions)
    ai_live_count = gemini_count + openrouter_count
    ai_models = _ai_model_overview(decisions)
    ui_drift_count, ui_drift_total = _ui_drift_stats(decisions)
    lines = [
        "🚀 Tech_Sniper_IT | Scan Report",
        "━━━━━━━━━━━━━━━━━━━━",
        "🔎 Scan completata",
        "💡 Formula spread: offerta reseller - prezzo Amazon",
        f"📦 Prodotti analizzati: {len(decisions)}",
        f"🎯 Soglia spread: {threshold:.2f} EUR",
        f"✅ Opportunita sopra soglia: {len(profitable)}",
        f"🗑️ Scartati sotto soglia: {len(decisions) - len(profitable)}",
        f"🧠 AI usata: {ai_live_count}/{len(decisions)} | gemini={gemini_count} openrouter={openrouter_count} fallback={heuristic_count}",
        f"🧠 Modelli AI: {ai_models}",
        f"🧩 UI drift rilevati: {ui_drift_count}/{ui_drift_total}",
        f"🏁 Miglior spread trovato: {_format_signed_eur(best_spread)}",
    ]

    if not profitable:
        lines.append("😴 Nessuna opportunita sopra soglia in questa run.")
        return "\n".join(lines)

    ranked = sorted(profitable, key=lambda item: item.spread_eur or 0.0, reverse=True)
    for index, decision in enumerate(ranked, start=1):
        best_offer = decision.best_offer
        spread = _format_signed_eur(decision.spread_eur)
        status_icon, status_text = _spread_status_badge(decision.spread_eur, threshold)
        product_url = _normalize_http_url(getattr(decision.product, "url", None))
        if not product_url:
            product_url = _amazon_search_url(decision.normalized_name or decision.product.title)
        best_offer_url = _normalize_http_url(getattr(best_offer, "source_url", None) if best_offer else None)
        platform_name = best_offer.platform if best_offer else "n/d"
        platform_icon = _platform_icon(platform_name)
        decision_label = "🔥 SI"
        display_name = decision.normalized_name or getattr(decision.product, "title", "n/d")
        category = getattr(getattr(decision.product, "category", None), "value", None) or "n/d"
        lines.extend(
            [
                "",
                f"{status_icon} Prodotto {index}: {display_name}",
                f"🧾 Esito: {status_text}",
                f"🏷️ Categoria: {category}",
                f"💶 Amazon: {_format_eur(decision.product.price_eur)}",
                f"🏆 Best offer: {_format_eur(best_offer.offer_eur if best_offer else None)} ({platform_name})",
                f"{platform_icon} Reseller top: {platform_name}",
                f"📈 Spread netto: {spread}",
                f"🚨 Opportunita: {decision_label}",
                f"🧠 AI match: {_ai_usage_label(decision)}",
                f"📊 Offerte: {_format_offers_compact(decision)}",
                f"🛒 Amazon link: {product_url}",
                f"🔗 Link migliore offerta: {best_offer_url or 'n/d'}",
            ]
        )
    return "\n".join(lines)


async def _run_scan_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    print("[scan] Starting worker scan command.")
    strategy_getter = getattr(getattr(manager, "ai_balancer", None), "get_strategy_snapshot", None)
    if callable(strategy_getter):
        try:
            snapshot = strategy_getter()
            print(f"[scan] AI strategy -> {json.dumps(snapshot, ensure_ascii=False)}")
        except Exception as exc:
            print(f"[scan] AI strategy unavailable: {_safe_error_details(exc)}")
    products = load_products(_load_github_event_data())
    command_chat = _telegram_target_chat(payload)
    scan_target_products = max(1, int(_env_or_default("SCAN_TARGET_PRODUCTS", _env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "12"))))
    candidate_multiplier = max(1, int(_env_or_default("SCAN_CANDIDATE_MULTIPLIER", "4")))
    candidate_budget = scan_target_products * candidate_multiplier
    if not products:
        print("[scan] No explicit products provided. Trying Amazon Warehouse automatic source (IT+EU).")
        try:
            fetch_kwargs = {
                "headless": _env_or_default("HEADLESS", "true").lower() != "false",
                "nav_timeout_ms": int(_env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000")),
                "max_products": candidate_budget,
            }
            try:
                warehouse_items = await fetch_amazon_warehouse_products(**fetch_kwargs)
            except TypeError:
                fetch_kwargs.pop("max_products", None)
                warehouse_items = await fetch_amazon_warehouse_products(**fetch_kwargs)
        except Exception as exc:  # pragma: no cover - defensive fallback
            warehouse_items = []
            print(f"[scan] Amazon Warehouse source error: {_safe_error_details(exc)}")

        for item in warehouse_items:
            try:
                products.append(_coerce_product(item))
            except Exception as exc:
                print(f"[scan] Skipping invalid warehouse product: {exc}")

        if not products:
            message = (
                "Nessun prodotto disponibile per lo scan (payload/file/env + Amazon Warehouse IT/EU). "
                "Puoi passare JSON a /scan o regolare la configurazione warehouse."
            )
            print(message)
            if payload.get("source") == "telegram":
                await _send_telegram_message(message, command_chat)
            return 0

    deduped = _dedupe_products(products)
    if len(deduped) != len(products):
        print(f"[scan] Deduplicated products: {len(products)} -> {len(deduped)}")
    products = deduped
    products, accessory_drops = _filter_non_core_device_candidates(products)
    if accessory_drops:
        print(
            "[scan] Accessory guardrail applied | "
            f"dropped={len(accessory_drops)} kept={len(products)}"
        )
        preview = accessory_drops[:5]
        for row in preview:
            print(f"[scan] Accessory drop -> {row}")
        if len(accessory_drops) > len(preview):
            print(f"[scan] Accessory drop -> ... and {len(accessory_drops) - len(preview)} more.")
    products = await _exclude_non_profitable_candidates(manager, products)
    scoring_context = await _build_prioritization_context(manager)
    if scoring_context.get("enabled"):
        health_snapshot = scoring_context.get("platform_health", {})
        print(
            "[scan] Scoring context | "
            f"rows={scoring_context.get('rows_count', 0)} "
            f"exact_models={len(scoring_context.get('exact_offer_median', {}))} "
            f"category_models={len(scoring_context.get('category_offer_median', {}))} "
            f"platform_health={json.dumps(health_snapshot, ensure_ascii=False)}"
        )
    else:
        print("[scan] Scoring context disabled; using legacy priority.")

    products = _prioritize_products(products, scoring_context=scoring_context)
    preview_rows = _priority_preview(products, scoring_context, limit=min(len(products), 8))
    if preview_rows:
        print("[scan] Priority preview:")
        for row in preview_rows:
            print(f"[scan]   {row}")

    if len(products) > scan_target_products:
        selected = _select_balanced_candidates(products, scan_target_products)
        selected_region_counts = _region_counts(selected)
        total_region_counts = _region_counts(products)
        print(
            "[scan] Candidate selection | "
            f"target={scan_target_products} budget={candidate_budget} selected={len(selected)} "
            f"total_after_filter={len(products)} total_regions={total_region_counts} selected_regions={selected_region_counts}"
        )
        products = selected

    if not products:
        message = "Nessun candidato disponibile dopo i filtri di esclusione storica."
        print(message)
        if payload.get("source") in {"telegram", "vercel_scan_api", "manual_debug"}:
            await _send_telegram_message(message, command_chat)
        return 0

    max_parallel_products = int(_env_or_default("MAX_PARALLEL_PRODUCTS", "3"))
    print(f"[scan] Loaded products: {len(products)} | max_parallel_products={max_parallel_products}")
    decisions = await manager.evaluate_many(products, max_parallel_products=max_parallel_products)
    await _save_non_profitable_decisions(manager, decisions)
    profitable = [item for item in decisions if item.should_notify]
    print(f"Scanned: {len(decisions)} | Profitable: {len(profitable)}")
    for decision in decisions:
        best = decision.best_offer.offer_eur if decision.best_offer else None
        print(
            json.dumps(
                {
                    "title": decision.product.title,
                    "normalized": decision.normalized_name,
                    "ai_provider": getattr(decision, "ai_provider", None),
                    "ai_model": getattr(decision, "ai_model", None),
                    "ai_mode": getattr(decision, "ai_mode", None),
                    "ai_used": getattr(decision, "ai_used", False),
                    "amazon_price": decision.product.price_eur,
                    "best_offer": best,
                    "best_platform": decision.best_offer.platform if decision.best_offer else None,
                    "spread_eur": decision.spread_eur,
                    "should_notify": decision.should_notify,
                    "offers": [_offer_log_payload(item) for item in decision.offers],
                },
                ensure_ascii=False,
            )
        )

    default_chat_configured = bool(os.getenv("TELEGRAM_BOT_TOKEN")) and bool(os.getenv("TELEGRAM_CHAT_ID"))
    should_send_summary = (
        bool(command_chat)
        or default_chat_configured
        or payload.get("source") in {"telegram", "vercel_scan_api", "manual_debug"}
    )
    if should_send_summary:
        summary = _format_scan_summary(decisions, manager.min_spread_eur)
        print(
            "[scan] Sending Telegram summary "
            f"(target={'explicit_chat' if command_chat else 'default_chat'})."
        )
        await _send_telegram_message(summary, command_chat)
    else:
        print("[scan] Telegram summary skipped (chat not configured).")
    return 0


async def _run_status_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    chat_id = _telegram_target_chat(payload)
    gemini_present = bool(os.getenv("GEMINI_API_KEYS", "").strip())
    openrouter_present = bool(os.getenv("OPENROUTER_API_KEYS", "").strip())

    lines = [
        "🤖 Tech_Sniper_IT status:",
        "⚙️ worker: online",
        f"🎯 threshold spread (offer-amazon): {manager.min_spread_eur:.2f} EUR",
        f"🧠 ai: gemini={'on' if gemini_present else 'off'}, openrouter={'on' if openrouter_present else 'off'}",
        f"🗄️ supabase: {'on' if manager.storage else 'off'}",
        f"💬 telegram alerts default chat: {'on' if manager.notifier else 'off'}",
    ]

    if manager.storage:
        try:
            try:
                recent = await manager.storage.get_recent_opportunities(limit=1, min_spread_eur=manager.min_spread_eur)
            except TypeError:
                recent = await manager.storage.get_recent_opportunities(limit=1)
            if recent:
                row = recent[0]
                lines.append(
                    "📌 last opportunity: "
                    f"{row.get('normalized_name', 'n/a')} | spread {row.get('spread_eur', 'n/a')} EUR | {row.get('best_platform', 'n/a')}"
                )
            else:
                lines.append("📌 last opportunity: none")
        except Exception as exc:
            lines.append(f"📌 last opportunity: read error ({_safe_error_details(exc)})")

    message = "\n".join(lines)
    print(message)
    await _send_telegram_message(message, chat_id)
    return 0


async def _run_last_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    chat_id = _telegram_target_chat(payload)
    if not manager.storage:
        message = "Supabase non configurato nel worker: comando /last non disponibile."
        print(message)
        await _send_telegram_message(message, chat_id)
        return 0

    limit = _parse_last_limit(payload)
    try:
        try:
            rows = await manager.storage.get_recent_opportunities(limit=limit, min_spread_eur=manager.min_spread_eur)
        except TypeError:
            rows = await manager.storage.get_recent_opportunities(limit=limit)
    except Exception as exc:
        message = f"Errore lettura Supabase: {_safe_error_details(exc)}"
        print(message)
        await _send_telegram_message(message, chat_id)
        return 0

    if not rows:
        message = "Nessuna opportunita salvata."
        print(message)
        await _send_telegram_message(message, chat_id)
        return 0

    lines = [f"Ultime opportunita ({len(rows)}):"]
    for idx, row in enumerate(rows, start=1):
        name = str(row.get("normalized_name", "n/a"))
        spread = row.get("spread_eur", "n/a")
        platform = str(row.get("best_platform", "n/a"))
        lines.append(f"{idx}. {name} | spread {spread} EUR | {platform}")

    message = "\n".join(lines)
    print(message)
    await _send_telegram_message(message, chat_id)
    return 0


async def run_worker() -> int:
    load_dotenv()
    event_data = _load_github_event_data()
    payload = _get_client_payload(event_data)
    command = _resolve_command(event_data, payload)

    if command == "status":
        return await _run_status_command(payload)
    if command == "last":
        return await _run_last_command(payload)
    return await _run_scan_command(payload)


def main() -> None:
    raise SystemExit(asyncio.run(run_worker()))


if __name__ == "__main__":
    main()
