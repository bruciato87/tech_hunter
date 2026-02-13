from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
from statistics import median
from typing import Any
from urllib.parse import quote_plus, urlparse
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import Bot

from tech_sniper_it.manager import build_default_manager, get_strategy_profile_snapshot
from tech_sniper_it.models import AmazonProduct, ProductCategory, to_legacy_storage_category
from tech_sniper_it.sources import apply_cart_net_pricing, fetch_amazon_warehouse_products
from tech_sniper_it.utils import infer_amazon_warehouse_condition


MAX_LAST_LIMIT = 10
TELEGRAM_TEXT_LIMIT = 4000
SCORING_DEFAULT_LOOKBACK_DAYS = 30
SCORING_DEFAULT_HISTORY_LIMIT = 2000

CATEGORY_REQUIRED_PLATFORMS: dict[ProductCategory, tuple[str, ...]] = {
    ProductCategory.APPLE_PHONE: ("trenddevice", "rebuy"),
    ProductCategory.SMARTWATCH: ("trenddevice", "rebuy"),
    ProductCategory.DRONE: ("mpb", "rebuy"),
    ProductCategory.HANDHELD_CONSOLE: ("rebuy",),
    ProductCategory.PHOTOGRAPHY: ("mpb", "rebuy"),
    ProductCategory.GENERAL_TECH: ("rebuy",),
}

CATEGORY_FALLBACK_OFFER_RATIO: dict[ProductCategory, float] = {
    ProductCategory.APPLE_PHONE: 0.38,
    ProductCategory.SMARTWATCH: 0.34,
    ProductCategory.DRONE: 0.33,
    ProductCategory.HANDHELD_CONSOLE: 0.35,
    ProductCategory.PHOTOGRAPHY: 0.30,
    ProductCategory.GENERAL_TECH: 0.22,
}

LIQUIDITY_PATTERNS: tuple[tuple[str, float], ...] = (
    (r"\biphone\b", 65.0),
    (r"\biphone\s*(14|15|16)\b", 35.0),
    (r"\bpro\s*max\b", 25.0),
    (r"\bapple watch ultra\b", 45.0),
    (r"\bgarmin\s*(fenix|epix)\b", 45.0),
    (r"\bforerunner\b", 28.0),
    (r"\bdji\s*(mini|air|mavic|avata)\b", 42.0),
    (r"\bdrone\b", 26.0),
    (r"\bsteam deck\b", 44.0),
    (r"\brog ally\b", 46.0),
    (r"\blegion go\b", 44.0),
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
    "cinturino",
    "strap",
    "band",
    "elica",
    "propeller",
    "battery pack",
    "batteria esterna",
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
    "console portatile",
    "smartwatch",
    "watch ultra",
    "garmin",
    "drone",
    "steam deck",
    "rog ally",
    "legion go",
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
    ("apple watch ultra", 280.0),
    ("garmin fenix", 220.0),
    ("garmin epix", 260.0),
    ("forerunner", 140.0),
    ("dji mini", 220.0),
    ("dji air", 300.0),
    ("dji mavic", 450.0),
    ("drone", 200.0),
    ("steam deck", 220.0),
    ("rog ally", 300.0),
    ("legion go", 340.0),
    ("macbook", 180.0),
    ("ipad", 100.0),
    ("canon eos", 180.0),
    ("sony alpha", 180.0),
    ("mirrorless", 180.0),
    ("playstation", 120.0),
    ("xbox", 120.0),
)

WAREHOUSE_QUERY_FALLBACKS: tuple[str, ...] = (
    "apple watch ultra 2 amazon warehouse",
    "garmin fenix 7x amazon warehouse",
    "dji mini 4 pro amazon warehouse",
    "steam deck oled 512gb amazon warehouse",
    "iphone 15 pro 256gb amazon warehouse",
    "canon eos r7 amazon warehouse",
)

DYNAMIC_QUERY_STOPWORDS: tuple[str, ...] = (
    "amazon",
    "warehouse",
    "ricondizionato",
    "ricondizionata",
    "reacondicionado",
    "renewed",
    "usato",
    "used",
    "grade",
    "ottime",
    "condizioni",
    "come",
    "nuovo",
)

DYNAMIC_DISCOVERY_QUERY_CATALOG: dict[str, tuple[str, ...]] = {
    ProductCategory.APPLE_PHONE.value: (
        "iphone 16 pro 256gb amazon warehouse",
        "iphone 16 pro max 256gb amazon warehouse",
        "iphone 15 pro 256gb amazon warehouse",
        "iphone 15 pro max 256gb amazon warehouse",
        "iphone 15 128gb amazon warehouse",
        "iphone 14 pro 256gb amazon warehouse",
        "iphone 14 pro max 256gb amazon warehouse",
        "iphone 14 128gb amazon warehouse",
        "iphone 13 pro 256gb amazon warehouse",
        "iphone 13 128gb amazon warehouse",
        "iphone 12 pro 256gb amazon warehouse",
        "iphone se 2022 128gb amazon warehouse",
    ),
    ProductCategory.PHOTOGRAPHY.value: (
        "sony alpha a7 iii amazon warehouse",
        "sony alpha a7 iv amazon warehouse",
        "sony alpha a6400 amazon warehouse",
        "canon eos r6 amazon warehouse",
        "canon eos r7 amazon warehouse",
        "canon eos r50 amazon warehouse",
        "nikon z6 ii amazon warehouse",
        "nikon z fc amazon warehouse",
        "fujifilm x s20 amazon warehouse",
        "fujifilm x t4 amazon warehouse",
        "lumix s5 amazon warehouse",
        "lumix g9 amazon warehouse",
    ),
    ProductCategory.SMARTWATCH.value: (
        "apple watch ultra 2 amazon warehouse",
        "apple watch ultra amazon warehouse",
        "garmin fenix 7x pro sapphire amazon warehouse",
        "garmin fenix 7 pro amazon warehouse",
        "garmin epix pro gen 2 amazon warehouse",
        "garmin forerunner 965 amazon warehouse",
        "garmin forerunner 955 amazon warehouse",
        "apple watch series 9 45mm amazon warehouse",
        "apple watch series 10 46mm amazon warehouse",
    ),
    ProductCategory.DRONE.value: (
        "dji mini 4 pro amazon warehouse",
        "dji mini 3 pro amazon warehouse",
        "dji air 3 amazon warehouse",
        "dji mavic 3 classic amazon warehouse",
        "dji avata 2 amazon warehouse",
        "dji mini 3 amazon warehouse",
        "dji air 2s amazon warehouse",
    ),
    ProductCategory.HANDHELD_CONSOLE.value: (
        "steam deck oled 512gb amazon warehouse",
        "steam deck 1tb amazon warehouse",
        "asus rog ally z1 extreme amazon warehouse",
        "rog ally x amazon warehouse",
        "lenovo legion go amazon warehouse",
        "msi claw a1m amazon warehouse",
    ),
    ProductCategory.GENERAL_TECH.value: (
        "macbook air m1 256gb amazon warehouse",
        "macbook air m2 256gb amazon warehouse",
        "macbook air m3 256gb amazon warehouse",
        "macbook pro m1 14 amazon warehouse",
        "macbook pro m2 14 amazon warehouse",
        "ipad air m1 256gb amazon warehouse",
        "ipad pro 11 m2 256gb amazon warehouse",
        "playstation 5 amazon warehouse",
        "xbox series x amazon warehouse",
        "nintendo switch oled amazon warehouse",
        "steam deck 512gb amazon warehouse",
        "dji mini 3 pro amazon warehouse",
    ),
}


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


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


def _parse_datetime_utc(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _stable_hash(text: str) -> int:
    total = 0
    for index, char in enumerate(text):
        total = (total + ((index + 1) * ord(char))) % 1_000_003
    return total


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


def _condition_bucket_from_product(product: AmazonProduct) -> str:
    raw = str(getattr(product, "amazon_condition", "") or "").strip().lower()
    if raw in {"like_new", "very_good", "good", "acceptable"}:
        return raw
    if "like" in raw or "nuovo" in raw:
        return "like_new"
    if "acceptable" in raw or "accett" in raw:
        return "acceptable"
    if "good" in raw or "buon" in raw:
        return "good"
    if "very" in raw or "ottim" in raw:
        return "very_good"
    return "unknown"


def _price_bucket_eur(value: float) -> int:
    step = 25.0
    if value <= 0:
        return 0
    return int((value // step) * step)


def _candidate_signature(
    *,
    normalized_name: str,
    category: str,
    price_eur: float,
    condition: str,
) -> str:
    model = _normalize_for_scoring(normalized_name) or "n/a"
    price_bucket = _price_bucket_eur(float(price_eur))
    return f"{category}|{condition}|{price_bucket}|{model}"


def _required_platforms_for_category(category: ProductCategory) -> tuple[str, ...]:
    return CATEGORY_REQUIRED_PLATFORMS.get(category, ("rebuy",))


def _offer_has_real_reseller_quote(offer) -> bool:  # noqa: ANN001
    if offer is None:
        return False
    if getattr(offer, "offer_eur", None) is None:
        return False
    if getattr(offer, "error", None):
        return False
    platform = str(getattr(offer, "platform", "") or "").strip().lower()
    payload = getattr(offer, "raw_payload", {}) or {}
    if not isinstance(payload, dict):
        payload = {}
    if platform in {"rebuy", "trenddevice", "mpb"}:
        quote_verification = payload.get("quote_verification")
        if isinstance(quote_verification, dict) and quote_verification.get("ok") is False:
            return False
        source_url = str(getattr(offer, "source_url", "") or "").strip()
        price_source = str(payload.get("price_source", "") or "").strip()
        if not source_url or not price_source:
            return False
    return True


def _missing_required_reseller_quotes(decision, *, optional_platforms: set[str] | None = None) -> list[str]:  # noqa: ANN001
    optional = optional_platforms or set()
    required = [
        platform
        for platform in _required_platforms_for_category(getattr(decision.product, "category", ProductCategory.GENERAL_TECH))
        if platform not in optional
    ]
    valid_platforms: set[str] = set()
    for offer in getattr(decision, "offers", []):
        platform = str(getattr(offer, "platform", "") or "").strip().lower()
        if not platform:
            continue
        if _offer_has_real_reseller_quote(offer):
            valid_platforms.add(platform)
    return [platform for platform in required if platform not in valid_platforms]


def _split_complete_quote_decisions(
    decisions: list,
    *,
    optional_platforms: set[str] | None = None,
) -> tuple[list, list[tuple[Any, list[str]]]]:  # noqa: ANN001
    accepted: list[Any] = []
    rejected: list[tuple[Any, list[str]]] = []
    for decision in decisions:
        missing = _missing_required_reseller_quotes(decision, optional_platforms=optional_platforms)
        if missing:
            rejected.append((decision, missing))
        else:
            accepted.append(decision)
    return accepted, rejected


def _detect_outage_optional_platforms(decisions: list) -> set[str]:  # noqa: ANN001
    """If a required platform returns zero real quotes in this run, treat it as temporarily optional.

    This prevents infinite refill loops when a reseller is down / blocked.
    """

    required: set[str] = set()
    for decision in decisions:
        category = getattr(getattr(decision, "product", None), "category", ProductCategory.GENERAL_TECH)
        required.update(_required_platforms_for_category(category))

    if not required:
        return set()

    quote_counts: dict[str, int] = {platform: 0 for platform in required}
    for decision in decisions:
        for offer in getattr(decision, "offers", []) or []:
            platform = str(getattr(offer, "platform", "") or "").strip().lower()
            if not platform or platform not in quote_counts:
                continue
            if _offer_has_real_reseller_quote(offer):
                quote_counts[platform] += 1

    return {platform for platform, count in quote_counts.items() if count <= 0}


def _liquidity_bonus(title: str) -> float:
    lowered = (title or "").lower()
    score = 0.0
    for pattern, bonus in LIQUIDITY_PATTERNS:
        if re.search(pattern, lowered):
            score += bonus
    return score


def _normalize_query_terms(value: str, max_tokens: int = 8) -> str | None:
    raw = (value or "").lower()
    raw = re.sub(r"[\(\)\[\]\{\}\|,;:'\"`]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw)
    raw = re.sub(r"[^a-z0-9+\- ]+", " ", raw)
    tokens = [item for item in raw.split() if item and item not in DYNAMIC_QUERY_STOPWORDS]
    if not tokens:
        return None
    compact = " ".join(tokens[: max(1, max_tokens)]).strip()
    return compact or None


def _trend_query_from_model_name(model_name: str) -> str | None:
    terms = _normalize_query_terms(model_name, max_tokens=8)
    if not terms:
        return None
    return f"{terms} amazon warehouse"


def _is_low_quality_trend_model(model_name: str) -> bool:
    lowered = (model_name or "").lower()
    if not lowered.strip():
        return True
    accessory_hit = any(_contains_token(lowered, token) for token in ACCESSORY_KEYWORDS)
    compatibility_hit = any(_contains_token(lowered, marker) for marker in COMPATIBILITY_MARKERS)
    compatibility_codes = len(re.findall(r"\b[a-z]?\d{4}\b", lowered)) >= 2 and not _has_storage_token(lowered)
    return (accessory_hit and compatibility_hit) or compatibility_codes


def _rotating_pick(values: list[str], count: int, *, salt: str) -> list[str]:
    if count <= 0 or not values:
        return []
    unique: list[str] = []
    for item in values:
        cleaned = item.strip()
        if not cleaned or cleaned in unique:
            continue
        unique.append(cleaned)
    if not unique:
        return []
    offset = _stable_hash(salt) % len(unique)
    picked: list[str] = []
    for index in range(len(unique)):
        candidate = unique[(offset + index) % len(unique)]
        picked.append(candidate)
        if len(picked) >= count:
            break
    return picked


def _weighted_slot_allocation(weights: dict[str, float], total_slots: int) -> dict[str, int]:
    if total_slots <= 0:
        return {key: 0 for key in weights}
    normalized = {key: max(0.05, float(value)) for key, value in weights.items()}
    total_weight = sum(normalized.values())
    if total_weight <= 0:
        keys = list(normalized.keys())
        if not keys:
            return {}
        base = total_slots // len(keys)
        slots = {key: base for key in keys}
        remainder = total_slots - sum(slots.values())
        for key in keys[:remainder]:
            slots[key] += 1
        return slots

    raw_slots: dict[str, float] = {
        key: (value / total_weight) * float(total_slots)
        for key, value in normalized.items()
    }
    allocated = {key: int(raw_value) for key, raw_value in raw_slots.items()}
    remainder = total_slots - sum(allocated.values())
    if remainder <= 0:
        return allocated

    fractional = sorted(
        ((key, raw_slots[key] - float(allocated[key])) for key in raw_slots),
        key=lambda row: row[1],
        reverse=True,
    )
    for index in range(remainder):
        key = fractional[index % len(fractional)][0]
        allocated[key] += 1
    return allocated


def _category_trend_weights(scoring_context: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {
        ProductCategory.SMARTWATCH.value: 1.22,
        ProductCategory.DRONE.value: 1.18,
        ProductCategory.HANDHELD_CONSOLE.value: 1.12,
        ProductCategory.APPLE_PHONE.value: 1.08,
        ProductCategory.PHOTOGRAPHY.value: 0.88,
        ProductCategory.GENERAL_TECH.value: 0.84,
    }
    category_spread = scoring_context.get("category_spread_median", {})
    trend_models = scoring_context.get("trend_models", [])

    for category_value in list(weights):
        spread = _to_float(category_spread.get(category_value)) if isinstance(category_spread, dict) else None
        if spread is not None:
            weights[category_value] += max(-80.0, min(140.0, spread)) / 160.0

        category = ProductCategory.from_raw(category_value)
        _health_adjustment, health_rate = _valuator_health_adjustment(category, scoring_context)
        weights[category_value] += max(0.0, health_rate - 0.35)

        if isinstance(trend_models, list):
            category_trends = [
                _to_float(item.get("trend_score"))
                for item in trend_models
                if isinstance(item, dict) and str(item.get("category")) == category_value
            ]
            category_trends = [item for item in category_trends if item is not None]
            if category_trends:
                top_score = max(category_trends)
                weights[category_value] += max(0.0, min(220.0, top_score)) / 260.0

        weights[category_value] = max(0.10, round(weights[category_value], 3))

    return weights


def _build_dynamic_warehouse_queries(
    *,
    scoring_context: dict[str, Any],
    target_count: int,
) -> tuple[list[str], dict[str, Any]]:
    fallback_queries = _split_csv(os.getenv("AMAZON_WAREHOUSE_QUERIES")) or list(WAREHOUSE_QUERY_FALLBACKS)
    fallback_queries = [query.strip() for query in fallback_queries if query and query.strip()]
    fallback_queries = list(dict.fromkeys(fallback_queries))

    if not _is_truthy_env("SCAN_DYNAMIC_QUERIES_ENABLED", "true"):
        selected = fallback_queries[: max(1, target_count)]
        return selected, {
            "mode": "disabled",
            "target": target_count,
            "selected": len(selected),
            "trend_slots": 0,
            "exploration_slots": len(selected),
        }

    query_cap = max(4, int(_env_or_default("SCAN_DYNAMIC_QUERY_LIMIT", str(max(8, target_count)))))
    requested = max(4, min(target_count, query_cap))
    exploration_ratio = _to_float(_env_or_default("SCAN_DYNAMIC_EXPLORATION_RATIO", "0.35")) or 0.35
    exploration_ratio = min(0.75, max(0.15, exploration_ratio))
    trend_min_score = _to_float(_env_or_default("SCAN_DYNAMIC_TREND_MIN_SCORE", "-35")) or -35.0
    trend_slots = max(1, int(round(requested * (1.0 - exploration_ratio))))
    exploration_slots = max(1, requested - trend_slots)

    trend_models = scoring_context.get("trend_models", [])
    trend_candidates: list[dict[str, Any]] = []
    if isinstance(trend_models, list):
        for row in trend_models:
            if not isinstance(row, dict):
                continue
            score = _to_float(row.get("trend_score"))
            model = str(row.get("model", "")).strip()
            if not model:
                continue
            if _is_low_quality_trend_model(model):
                continue
            if score is None or score < trend_min_score:
                continue
            positive_rate = _to_float(row.get("positive_rate")) or 0.0
            threshold_rate = _to_float(row.get("threshold_rate")) or 0.0
            max_spread = _to_float(row.get("max_spread"))
            has_profit_signal = bool(threshold_rate > 0.0 or positive_rate >= 0.25 or (max_spread is not None and max_spread > 0))
            if not has_profit_signal:
                continue
            trend_candidates.append(row)

    queries: list[str] = []
    query_sources: dict[str, str] = {}

    for row in trend_candidates:
        if len(queries) >= trend_slots:
            break
        model_name = str(row.get("model", "")).strip()
        query = _trend_query_from_model_name(model_name)
        if not query or query in query_sources:
            continue
        queries.append(query)
        query_sources[query] = "trend"

    timestamp_bucket = datetime.now(UTC).strftime("%Y%m%d%H")
    category_weights = _category_trend_weights(scoring_context)
    exploration_allocation = _weighted_slot_allocation(category_weights, exploration_slots)
    for category, slots in exploration_allocation.items():
        if slots <= 0:
            continue
        catalog = list(DYNAMIC_DISCOVERY_QUERY_CATALOG.get(category, ()))
        for query in _rotating_pick(catalog, slots, salt=f"{timestamp_bucket}:{category}"):
            if query in query_sources:
                continue
            queries.append(query)
            query_sources[query] = f"catalog:{category}"
            if len(queries) >= requested:
                break
        if len(queries) >= requested:
            break

    if len(queries) < requested:
        combined_catalog: list[str] = []
        for items in DYNAMIC_DISCOVERY_QUERY_CATALOG.values():
            combined_catalog.extend(items)
        needed = requested - len(queries)
        for query in _rotating_pick(combined_catalog, needed, salt=f"{timestamp_bucket}:all"):
            if query in query_sources:
                continue
            queries.append(query)
            query_sources[query] = "catalog:all"
            if len(queries) >= requested:
                break

    if len(queries) < requested:
        for query in fallback_queries:
            if query in query_sources:
                continue
            queries.append(query)
            query_sources[query] = "fallback"
            if len(queries) >= requested:
                break

    if not queries:
        queries = fallback_queries[: max(1, target_count)]
        query_sources = {query: "fallback" for query in queries}

    meta = {
        "mode": "dynamic",
        "target": requested,
        "selected": len(queries),
        "trend_slots": trend_slots,
        "exploration_slots": exploration_slots,
        "trend_candidates": len(trend_candidates),
        "category_weights": category_weights,
        "source_breakdown": {
            "trend": sum(1 for source in query_sources.values() if source == "trend"),
            "catalog": sum(1 for source in query_sources.values() if source.startswith("catalog:")),
            "fallback": sum(1 for source in query_sources.values() if source == "fallback"),
        },
    }
    return queries, meta


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


def _ai_usage_stats(decisions: list) -> tuple[int, int]:  # noqa: ANN001
    openrouter = 0
    heuristic = 0
    for decision in decisions:
        provider = str(getattr(decision, "ai_provider", "")).lower()
        if provider == "openrouter":
            openrouter += 1
        else:
            heuristic += 1
    return openrouter, heuristic


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
    category_raw = str(raw.get("category", "")).strip()
    category = ProductCategory.from_raw(category_raw if category_raw else title)
    source_marketplace_raw = raw.get("source_marketplace")
    source_marketplace = str(source_marketplace_raw).strip().lower() if source_marketplace_raw is not None else ""
    amazon_condition_raw = raw.get("amazon_condition")
    amazon_condition = str(amazon_condition_raw).strip().lower() if amazon_condition_raw is not None else ""
    amazon_condition_conf_raw = raw.get("amazon_condition_confidence")
    try:
        amazon_condition_confidence = float(amazon_condition_conf_raw) if amazon_condition_conf_raw is not None else 0.0
    except (TypeError, ValueError):
        amazon_condition_confidence = 0.0
    amazon_packaging_only = bool(raw.get("amazon_packaging_only", False))
    if not amazon_condition:
        inferred_condition, inferred_confidence, inferred_packaging = infer_amazon_warehouse_condition(title)
        if inferred_condition:
            amazon_condition = inferred_condition
            amazon_condition_confidence = inferred_confidence
        if inferred_packaging:
            amazon_packaging_only = True

    return AmazonProduct(
        title=title,
        price_eur=price,
        category=category,
        ean=raw.get("ean"),
        url=raw.get("url"),
        source_marketplace=source_marketplace or None,
        amazon_condition=amazon_condition or None,
        amazon_condition_confidence=max(0.0, min(float(amazon_condition_confidence), 1.0)),
        amazon_packaging_only=amazon_packaging_only,
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
        ProductCategory.SMARTWATCH: 0,
        ProductCategory.DRONE: 1,
        ProductCategory.HANDHELD_CONSOLE: 2,
        ProductCategory.APPLE_PHONE: 0,
        ProductCategory.PHOTOGRAPHY: 3,
        ProductCategory.GENERAL_TECH: 4,
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
        "lookback_days": 0,
        "exact_offer_median": {},
        "exact_confidence": {},
        "category_offer_median": {},
        "category_spread_median": {},
        "platform_health": {},
        "trend_models": [],
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

    context["lookback_days"] = lookback_days
    exact_offer_samples: dict[str, list[float]] = {}
    category_offer_samples: dict[str, list[float]] = {}
    category_spread_samples: dict[str, list[float]] = {}
    platform_totals: dict[str, int] = {}
    platform_successes: dict[str, int] = {}
    trend_samples: dict[str, dict[str, Any]] = {}
    now = datetime.now(UTC)
    threshold = _to_float(_env_or_default("MIN_SPREAD_EUR", "40")) or 40.0

    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized_raw = str(row.get("normalized_name", "")).strip()
        normalized_name = _normalize_for_scoring(normalized_raw)
        category = ProductCategory.from_raw(str(row.get("category", ""))).value
        best_offer = _to_float(row.get("best_offer_eur"))
        spread = _to_float(row.get("spread_eur"))

        if normalized_name and best_offer is not None:
            exact_offer_samples.setdefault(normalized_name, []).append(best_offer)
        if best_offer is not None:
            category_offer_samples.setdefault(category, []).append(best_offer)
        if spread is not None:
            category_spread_samples.setdefault(category, []).append(spread)

        valid_platforms: set[str] = set()
        offers_payload = row.get("offers_payload")
        if not isinstance(offers_payload, list):
            offers_payload = []
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
                if _to_float(offer_item.get("offer_eur")) is not None:
                    valid_platforms.add(platform)

        if normalized_name:
            created_at = _parse_datetime_utc(row.get("created_at"))
            age_days = float(max(0.0, (now - created_at).total_seconds() / 86_400.0)) if created_at else float(lookback_days)
            recency = max(0.15, min(1.0, 1.0 - (age_days / (lookback_days * 1.35))))
            liquidity = min(100.0, _liquidity_bonus(normalized_raw or normalized_name))
            spread_component = max(-180.0, min(220.0, spread if spread is not None else -35.0))
            platform_bonus = float(min(3, len(valid_platforms)) * 16.0)
            row_score = ((spread_component * 0.72) + platform_bonus + (liquidity * 0.34)) * recency
            if spread is not None and spread >= 0:
                row_score += 10.0 * recency
            if spread is not None and spread >= threshold:
                row_score += 18.0 * recency
            if spread is not None and spread <= -120.0:
                row_score -= 12.0 * recency

            bucket = trend_samples.setdefault(
                normalized_name,
                {
                    "model": normalized_raw or normalized_name,
                    "category": category,
                    "score_sum": 0.0,
                    "samples": 0,
                    "positive_hits": 0,
                    "threshold_hits": 0,
                    "max_spread": None,
                    "platforms": set(),
                    "recency_max": 0.0,
                },
            )
            bucket["score_sum"] += row_score
            bucket["samples"] += 1
            bucket["recency_max"] = max(float(bucket["recency_max"]), recency)
            if spread is not None and spread >= 0:
                bucket["positive_hits"] += 1
            if spread is not None and spread > threshold:
                bucket["threshold_hits"] += 1
            if spread is not None:
                current_max_spread = _to_float(bucket.get("max_spread"))
                bucket["max_spread"] = spread if current_max_spread is None else max(current_max_spread, spread)
            if valid_platforms:
                bucket["platforms"].update(valid_platforms)

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

    trend_models: list[dict[str, Any]] = []
    for bucket in trend_samples.values():
        samples = int(bucket.get("samples") or 0)
        if samples <= 0:
            continue
        avg_score = float(bucket.get("score_sum") or 0.0) / float(samples)
        positive_rate = float(bucket.get("positive_hits") or 0) / float(samples)
        threshold_rate = float(bucket.get("threshold_hits") or 0) / float(samples)
        max_spread = _to_float(bucket.get("max_spread"))
        recency_max = float(bucket.get("recency_max") or 0.0)
        platform_count = len(bucket.get("platforms", set()))
        trend_score = avg_score + (positive_rate * 34.0) + (threshold_rate * 52.0) + (min(samples, 8) * 2.8)
        trend_score += recency_max * 8.0 + (platform_count * 3.0)
        if max_spread is not None:
            trend_score += max(0.0, min(180.0, max_spread)) * 0.16
        trend_models.append(
            {
                "model": str(bucket.get("model") or ""),
                "category": str(bucket.get("category") or ProductCategory.GENERAL_TECH.value),
                "trend_score": round(trend_score, 2),
                "samples": samples,
                "positive_rate": round(positive_rate, 3),
                "threshold_rate": round(threshold_rate, 3),
                "max_spread": round(max_spread, 2) if max_spread is not None else None,
                "platform_count": platform_count,
            }
        )
    trend_models.sort(
        key=lambda row: (
            _to_float(row.get("trend_score")) or -9999.0,
            _to_float(row.get("threshold_rate")) or 0.0,
            int(row.get("samples") or 0),
        ),
        reverse=True,
    )

    context.update(
        {
            "rows_count": len(rows),
            "lookback_days": lookback_days,
            "exact_offer_median": exact_offer_median,
            "exact_confidence": exact_confidence,
            "category_offer_median": category_offer_median,
            "category_spread_median": category_spread_median,
            "platform_health": platform_health,
            "trend_models": trend_models[:120],
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


def _daily_exclusion_since_iso() -> tuple[str | None, str | None]:
    if not _is_truthy_env("EXCLUDE_DAILY_RESET", "true"):
        return None, None
    timezone_name = _env_or_default("EXCLUDE_RESET_TIMEZONE", "Europe/Rome")
    try:
        tzinfo = ZoneInfo(timezone_name)
    except Exception:
        tzinfo = UTC
        timezone_name = "UTC"
    now_tz = datetime.now(tzinfo)
    start_of_day_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_of_day_tz.astimezone(UTC).isoformat(), timezone_name


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

    lookback_days = max(1, int(_env_or_default("EXCLUDE_LOOKBACK_DAYS", "1")))
    max_rows = max(50, int(_env_or_default("EXCLUDE_MAX_ROWS", "1500")))
    since_iso, reset_timezone = _daily_exclusion_since_iso()
    try:
        excluded_urls = await storage.get_excluded_source_urls(
            max_spread_eur=manager.min_spread_eur,
            lookback_days=lookback_days,
            limit=max_rows,
            since_iso=since_iso,
        )
    except TypeError:
        excluded_urls = await storage.get_excluded_source_urls(
            max_spread_eur=manager.min_spread_eur,
            lookback_days=lookback_days,
            limit=max_rows,
        )

    excluded_signatures: set[str] = set()
    get_rows = getattr(storage, "get_recent_scoring_rows", None)
    if callable(get_rows):
        try:
            rows = await get_rows(lookback_days=lookback_days, limit=max_rows)
        except Exception:
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            spread = _to_float(row.get("spread_eur"))
            if spread is None or spread > manager.min_spread_eur:
                continue
            normalized_name = str(row.get("normalized_name", "")).strip()
            if not normalized_name:
                continue
            category = ProductCategory.from_raw(str(row.get("category", ""))).value
            amazon_price = _to_float(row.get("amazon_price_eur"))
            if amazon_price is None or amazon_price <= 0:
                continue
            raw_condition = str(row.get("amazon_condition", "") or "").strip().lower()
            condition = raw_condition if raw_condition in {"like_new", "very_good", "good", "acceptable"} else "unknown"
            excluded_signatures.add(
                _candidate_signature(
                    normalized_name=normalized_name,
                    category=category,
                    price_eur=amazon_price,
                    condition=condition,
                )
            )

    if not excluded_urls and not excluded_signatures:
        print("[scan] Exclusion cache: no historical under-threshold urls/signatures.")
        return products

    filtered: list[AmazonProduct] = []
    removed_products: list[AmazonProduct] = []
    removed = 0
    removed_url = 0
    removed_signature = 0
    for product in products:
        normalized_url = _normalize_http_url(product.url)
        if normalized_url and normalized_url in excluded_urls:
            removed += 1
            removed_url += 1
            removed_products.append(product)
            continue
        signature = _candidate_signature(
            normalized_name=product.title,
            category=to_legacy_storage_category(product.category),
            price_eur=float(product.price_eur),
            condition=_condition_bucket_from_product(product),
        )
        if signature in excluded_signatures:
            removed += 1
            removed_signature += 1
            removed_products.append(product)
            continue
        filtered.append(product)
    window_label = f"daily({reset_timezone})" if since_iso else f"lookback_days={lookback_days}"
    print(
        "[scan] Exclusion cache applied | "
        f"removed={removed} kept={len(filtered)} window={window_label} "
        f"url_rows={len(excluded_urls)} signature_rows={len(excluded_signatures)} "
        f"removed_by_url={removed_url} removed_by_signature={removed_signature}"
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
    eligible: list[tuple[int, Any]] = [
        (index, decision)
        for index, decision in enumerate(decisions)
        if decision.best_offer is not None and decision.spread_eur is not None and decision.spread_eur <= manager.min_spread_eur
    ]
    if not eligible:
        return 0

    try:
        max_parallel = max(1, int(_env_or_default("NON_PROFITABLE_SAVE_MAX_PARALLEL", "3")))
    except ValueError:
        max_parallel = 3
    semaphore = asyncio.Semaphore(max_parallel)

    async def _persist(_index: int, decision: Any) -> Exception | None:
        async with semaphore:
            try:
                await storage.save_non_profitable(decision, threshold=manager.min_spread_eur)
                return None
            except Exception as exc:  # pragma: no cover - defensive
                return exc

    persisted = await asyncio.gather(*(_persist(index, decision) for index, decision in eligible))
    saved = sum(1 for item in persisted if item is None)
    failed = [item for item in persisted if isinstance(item, Exception)]
    print(f"[scan] Stored non-profitable records for exclusion cache: {saved}")
    if failed:
        first_error = _safe_error_details(failed[0], max_len=160)
        print(
            "[scan] Non-profitable cache persistence warnings | "
            f"failed={len(failed)} first_error='{first_error}'"
        )
    return saved


def _format_scan_summary(decisions: list, threshold: float) -> str:
    strategy = get_strategy_profile_snapshot()
    profitable = [item for item in decisions if item.should_notify and item.spread_eur is not None]
    best_spread = max((item.spread_eur for item in decisions if item.spread_eur is not None), default=None)
    openrouter_count, heuristic_count = _ai_usage_stats(decisions)
    ai_live_count = openrouter_count
    ai_models = _ai_model_overview(decisions)
    ui_drift_count, ui_drift_total = _ui_drift_stats(decisions)
    lines = [
        "🚀 Tech_Sniper_IT | Scan Report",
        "━━━━━━━━━━━━━━━━━━━━",
        "🔎 Scan completata",
        f"🧭 Profilo strategia: {strategy.get('profile', 'balanced')}",
        f"📦 Analizzati: {len(decisions)} | ✅ Over soglia: {len(profitable)} | 🗑️ Scartati: {len(decisions) - len(profitable)}",
        f"🎯 Soglia spread netto: {threshold:.2f} EUR",
        f"🧠 AI usata: {ai_live_count}/{len(decisions)} | openrouter={openrouter_count} fallback={heuristic_count}",
        f"🧠 Modelli AI: {ai_models}",
        f"🧩 UI drift rilevati: {ui_drift_count}/{ui_drift_total}",
        f"🏁 Miglior spread trovato: {_format_signed_eur(best_spread)}",
    ]

    if not profitable:
        lines.append("😴 Nessuna opportunita sopra soglia in questa run.")
        return "\n".join(lines)

    ranked = sorted(profitable, key=lambda item: item.spread_eur or 0.0, reverse=True)
    lines.extend(["", "🔥 Opportunita (ordinate per spread netto):"])
    for index, decision in enumerate(ranked, start=1):
        best_offer = decision.best_offer
        spread_net = _format_signed_eur(decision.spread_eur)
        product_url = _normalize_http_url(getattr(decision.product, "url", None))
        if not product_url:
            product_url = _amazon_search_url(decision.normalized_name or decision.product.title)
        best_offer_url = _normalize_http_url(getattr(best_offer, "source_url", None) if best_offer else None)
        platform_name = best_offer.platform if best_offer else "n/d"
        platform_icon = _platform_icon(platform_name)
        display_name = decision.normalized_name or getattr(decision.product, "title", "n/d")
        amazon_condition = getattr(decision.product, "amazon_condition", None) or "n/d"
        packaging_only = bool(getattr(decision.product, "amazon_packaging_only", False))
        condition_tag = " 📦 solo packaging" if packaging_only else ""
        spread_gross = _format_signed_eur(getattr(decision, "spread_gross_eur", None))
        risk = float(getattr(decision, "risk_buffer_eur", 0.0) or 0.0)
        cost = float(getattr(decision, "operating_cost_eur", 0.0) or 0.0)
        risk_cost = ""
        if risk > 0 or cost > 0:
            risk_cost = f" | rischio {_format_eur(risk)} | costi {_format_eur(cost)}"
        ai_label = _ai_usage_label(decision).replace("provider=", "").replace(" | model=", "/").replace(" | mode=", " | ")
        lines.extend(
            [
                "",
                f"{index}. {display_name}",
                f"💶 Buy {_format_eur(decision.product.price_eur)} → {platform_icon} {platform_name} {_format_eur(best_offer.offer_eur if best_offer else None)} | netto {spread_net}",
                f"📉 Lordo {spread_gross}{risk_cost}",
                f"🧪 Condizione: {amazon_condition}{condition_tag} | 🧠 AI: {ai_label}",
                f"🛒 Amazon link: {product_url}",
                f"🔗 Link migliore offerta: {best_offer_url or 'n/d'}",
            ]
        )
    return "\n".join(lines)


def _format_smoke_summary(decisions: list) -> str:
    strategy = get_strategy_profile_snapshot()
    openrouter_count, heuristic_count = _ai_usage_stats(decisions)
    ai_models = _ai_model_overview(decisions)
    ui_drift_count, ui_drift_total = _ui_drift_stats(decisions)

    lines = [
        "🧪 Tech_Sniper_IT | Smoke Report",
        "━━━━━━━━━━━━━━━━━━━━",
        "🔎 Test completato (modalita smoke)",
        f"🧭 Profilo strategia: {strategy.get('profile', 'balanced')}",
        f"📦 Prodotti: {len(decisions)}",
        f"🧠 AI: openrouter={openrouter_count} fallback={heuristic_count}",
        f"🧠 Modelli AI: {ai_models}",
        f"🧩 UI drift: {ui_drift_count}/{ui_drift_total}",
    ]

    preview = decisions[: min(len(decisions), 6)]
    if preview:
        lines.append("")
        for index, decision in enumerate(preview, start=1):
            display_name = decision.normalized_name or getattr(decision.product, "title", "n/d")
            category = getattr(getattr(decision.product, "category", None), "value", "general_tech")
            amazon_price = _format_eur(getattr(decision.product, "price_eur", None))
            lines.append(f"{index}. {display_name}")
            lines.append(f"🏷️ Categoria: {category} | 💶 Amazon: {amazon_price}")
            for offer in getattr(decision, "offers", []) or []:
                platform = str(getattr(offer, "platform", "") or "").strip().lower() or "n/d"
                icon = _platform_icon(platform)
                price = getattr(offer, "offer_eur", None)
                error = _safe_text(getattr(offer, "error", None), max_len=110)
                if price is not None and not error:
                    lines.append(f"{icon} {platform}: {_format_eur(float(price))}")
                else:
                    lines.append(f"{icon} {platform}: n/d ({error or 'no-quote'})")
            lines.append("")
    return "\n".join(lines).rstrip()


async def _run_scan_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    strategy = get_strategy_profile_snapshot()
    print("[scan] Starting worker scan command.")
    print(
        "[scan] Strategy profile -> "
        f"profile={strategy.get('profile')} operating_cost={strategy.get('operating_cost_eur')} "
        f"packaging_factor={strategy.get('packaging_only_factor')}"
    )
    strategy_getter = getattr(getattr(manager, "ai_balancer", None), "get_strategy_snapshot", None)
    if callable(strategy_getter):
        try:
            snapshot = strategy_getter()
            print(f"[scan] AI strategy -> {json.dumps(snapshot, ensure_ascii=False)}")
        except Exception as exc:
            print(f"[scan] AI strategy unavailable: {_safe_error_details(exc)}")
    scoring_context = await _build_prioritization_context(manager)
    products = load_products(_load_github_event_data())
    command_chat = _telegram_target_chat(payload)
    scan_mode = str(payload.get("mode") or _env_or_default("SCAN_MODE", "full")).strip().lower()
    if scan_mode == "smoke":
        print("[scan] Scan mode -> smoke (fast pipeline test)")
        # Avoid polluting persistence during smoke runs.
        manager.storage = None
    headless = _env_or_default("HEADLESS", "true").lower() != "false"
    nav_timeout_ms = int(_env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000"))
    if scan_mode == "smoke":
        nav_timeout_ms = min(nav_timeout_ms, int(_env_or_default("SCAN_SMOKE_NAV_TIMEOUT_MS", "20000")))
    scan_target_products = max(1, int(_env_or_default("SCAN_TARGET_PRODUCTS", _env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "12"))))
    if scan_mode == "smoke":
        scan_target_products = min(scan_target_products, max(1, int(_env_or_default("SCAN_SMOKE_TARGET_PRODUCTS", "3"))))
    candidate_multiplier = max(1, int(_env_or_default("SCAN_CANDIDATE_MULTIPLIER", "4")))
    candidate_budget = scan_target_products * candidate_multiplier
    if not products:
        print("[scan] No explicit products provided. Trying Amazon Warehouse automatic source (IT+EU).")
        try:
            query_target = max(4, int(_env_or_default("SCAN_DYNAMIC_QUERY_LIMIT", "12")))
            dynamic_queries, query_meta = _build_dynamic_warehouse_queries(
                scoring_context=scoring_context,
                target_count=min(query_target, candidate_budget),
            )
            print(
                "[scan] Dynamic query planner | "
                f"mode={query_meta.get('mode')} selected={query_meta.get('selected')}/{query_meta.get('target')} "
                f"trend_slots={query_meta.get('trend_slots')} exploration_slots={query_meta.get('exploration_slots')} "
                f"trend_candidates={query_meta.get('trend_candidates', 0)} "
                f"source_breakdown={json.dumps(query_meta.get('source_breakdown', {}), ensure_ascii=False)}"
            )
            query_preview = dynamic_queries[: min(len(dynamic_queries), 12)]
            if query_preview:
                print("[scan] Warehouse query preview:")
                for index, query in enumerate(query_preview, start=1):
                    print(f"[scan]   q{index}: {query}")
            fetch_kwargs = {
                "headless": headless,
                "nav_timeout_ms": nav_timeout_ms,
                "max_products": candidate_budget,
                "search_queries": dynamic_queries,
            }
            fetch_attempts = [
                dict(fetch_kwargs),
                {key: value for key, value in fetch_kwargs.items() if key != "search_queries"},
                {key: value for key, value in fetch_kwargs.items() if key != "max_products"},
                {
                    key: value
                    for key, value in fetch_kwargs.items()
                    if key not in {"search_queries", "max_products"}
                },
            ]
            warehouse_items: list[dict[str, Any]] | None = None
            type_error: TypeError | None = None
            for kwargs in fetch_attempts:
                try:
                    warehouse_items = await fetch_amazon_warehouse_products(**kwargs)
                    break
                except TypeError as exc:
                    type_error = exc
                    continue
            if warehouse_items is None:
                if type_error is not None:
                    raise type_error
                warehouse_items = []
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
    if scoring_context.get("enabled"):
        health_snapshot = scoring_context.get("platform_health", {})
        print(
            "[scan] Scoring context | "
            f"rows={scoring_context.get('rows_count', 0)} "
            f"exact_models={len(scoring_context.get('exact_offer_median', {}))} "
            f"category_models={len(scoring_context.get('category_offer_median', {}))} "
            f"trend_models={len(scoring_context.get('trend_models', []))} "
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

    evaluation_products = products
    overflow_products: list[AmazonProduct] = []
    if len(products) > scan_target_products:
        selected = _select_balanced_candidates(products, scan_target_products)
        selected_markers = {id(item) for item in selected}
        overflow_products = [item for item in products if id(item) not in selected_markers]
        selected_region_counts = _region_counts(selected)
        total_region_counts = _region_counts(products)
        print(
            "[scan] Candidate selection | "
            f"target={scan_target_products} budget={candidate_budget} selected={len(selected)} "
            f"total_after_filter={len(products)} total_regions={total_region_counts} selected_regions={selected_region_counts}"
        )
        evaluation_products = selected

    if not evaluation_products:
        message = "Nessun candidato disponibile dopo i filtri di esclusione storica."
        print(message)
        if payload.get("source") in {"telegram", "vercel_scan_api", "manual_debug"}:
            await _send_telegram_message(message, command_chat)
        return 0

    max_parallel_products = int(_env_or_default("MAX_PARALLEL_PRODUCTS", "3"))
    print(f"[scan] Loaded products: {len(evaluation_products)} | max_parallel_products={max_parallel_products}")
    enforce_complete_quotes = _is_truthy_env("SCAN_REQUIRE_COMPLETE_RESELLER_QUOTES", "true") and str(
        payload.get("source", "")
    ).lower() != "manual_debug"
    if scan_mode == "smoke":
        enforce_complete_quotes = False
        max_parallel_products = 1
    refill_batch_multiplier = max(1, int(_env_or_default("SCAN_RESELLER_REFILL_BATCH_MULTIPLIER", "2")))
    send_individual_alerts = _is_truthy_env("SCAN_TELEGRAM_INDIVIDUAL_ALERTS", "false")
    original_notifier = getattr(manager, "notifier", None)
    notifier_disabled = False
    if original_notifier is not None and not send_individual_alerts:
        print("[scan] Individual Telegram alerts disabled for scan; using consolidated report only.")
        manager.notifier = None
        notifier_disabled = True

    async def _evaluate_batch(batch_products: list[AmazonProduct], *, stage_label: str) -> list[Any]:
        if not batch_products:
            return []
        if scan_mode != "smoke":
            cart_pricing_stats = await apply_cart_net_pricing(
                batch_products,
                headless=headless,
                nav_timeout_ms=nav_timeout_ms,
            )
            print(
                "[scan] Cart net pricing stage | "
                f"stage={stage_label} checked={cart_pricing_stats.get('checked', 0)} "
                f"updated={cart_pricing_stats.get('updated', 0)} "
                f"skipped={cart_pricing_stats.get('skipped', 0)}"
            )
        else:
            print(f"[scan] Cart net pricing stage skipped (smoke) | stage={stage_label}")
        return await manager.evaluate_many(batch_products, max_parallel_products=max_parallel_products)

    try:
        primary_decisions = await _evaluate_batch(evaluation_products, stage_label="primary")
        decisions = primary_decisions

        optional_platforms: set[str] = set()
        complete_decisions = primary_decisions
        if enforce_complete_quotes:
            if _is_truthy_env("SCAN_ADAPTIVE_REQUIRED_PLATFORMS", "true"):
                optional_platforms = _detect_outage_optional_platforms(primary_decisions)
                if optional_platforms:
                    print(
                        "[scan] Adaptive required platforms | "
                        f"optional_due_to_outage={sorted(optional_platforms)}"
                    )

            complete_decisions, rejected = _split_complete_quote_decisions(
                primary_decisions,
                optional_platforms=optional_platforms,
            )
            if rejected:
                print(
                    "[scan] Real quote coverage filter | "
                    f"accepted={len(complete_decisions)} rejected={len(rejected)} target={len(evaluation_products)}"
                )
                for decision, missing in rejected[:8]:
                    print(
                        "[scan] Incomplete reseller quote -> "
                        f"title='{_safe_text(getattr(decision.product, 'title', ''), max_len=90)}' "
                        f"missing={missing}"
                    )
                if len(rejected) > 8:
                    print(f"[scan] Incomplete reseller quote -> ... and {len(rejected) - 8} more.")

            try:
                max_refill_rounds = int(_env_or_default("SCAN_RESELLER_REFILL_MAX_ROUNDS", "0"))
            except ValueError:
                max_refill_rounds = 0
            max_refill_rounds = max(0, min(max_refill_rounds, 6))

            target_complete = len(evaluation_products)
            refill_round = 0
            while refill_round < max_refill_rounds and len(complete_decisions) < target_complete and overflow_products:
                refill_round += 1
                missing_slots = target_complete - len(complete_decisions)
                batch_size = min(
                    len(overflow_products),
                    max(missing_slots, missing_slots * refill_batch_multiplier),
                )
                refill_batch = overflow_products[:batch_size]
                del overflow_products[:batch_size]
                print(
                    "[scan] Refill batch | "
                    f"round={refill_round} size={len(refill_batch)} "
                    f"missing_slots={missing_slots} remaining_overflow={len(overflow_products)}"
                )
                refill_decisions = await _evaluate_batch(refill_batch, stage_label=f"refill-{refill_round}")
                decisions.extend(refill_decisions)
                accepted_refill, rejected_refill = _split_complete_quote_decisions(
                    refill_decisions,
                    optional_platforms=optional_platforms,
                )
                complete_decisions.extend(accepted_refill)
                if rejected_refill:
                    print(
                        "[scan] Refill coverage | "
                        f"accepted={len(accepted_refill)} rejected={len(rejected_refill)}"
                    )
            if len(complete_decisions) < target_complete and max_refill_rounds > 0:
                print(
                    "[scan] Real quote coverage incomplete after refill | "
                    f"collected={len(complete_decisions)} target={target_complete}"
                )

        # Only write to exclusion cache when quotes are complete without masking outages.
        if enforce_complete_quotes:
            required_with_optional: set[str] = set(optional_platforms or set())
            decisions_for_cache = [
                item
                for item in complete_decisions
                if not (set(_required_platforms_for_category(getattr(item.product, "category", ProductCategory.GENERAL_TECH))) & required_with_optional)
            ]
        else:
            decisions_for_cache = decisions
    finally:
        if notifier_disabled:
            manager.notifier = original_notifier
    await _save_non_profitable_decisions(manager, decisions_for_cache)
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
                    "amazon_condition": getattr(decision.product, "amazon_condition", None),
                    "amazon_packaging_only": getattr(decision.product, "amazon_packaging_only", False),
                    "best_offer": best,
                    "best_platform": decision.best_offer.platform if decision.best_offer else None,
                    "spread_gross_eur": getattr(decision, "spread_gross_eur", None),
                    "risk_buffer_eur": getattr(decision, "risk_buffer_eur", 0.0),
                    "operating_cost_eur": getattr(decision, "operating_cost_eur", 0.0),
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
        summary = (
            _format_smoke_summary(decisions)
            if scan_mode == "smoke"
            else _format_scan_summary(decisions, manager.min_spread_eur)
        )
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
    strategy = get_strategy_profile_snapshot()
    chat_id = _telegram_target_chat(payload)
    openrouter_present = bool(os.getenv("OPENROUTER_API_KEYS", "").strip())

    lines = [
        "🤖 Tech_Sniper_IT status:",
        "⚙️ worker: online",
        f"🎯 threshold spread netto: {manager.min_spread_eur:.2f} EUR",
        f"🧭 strategy profile: {strategy.get('profile', 'balanced')}",
        f"🧠 ai: openrouter={'on' if openrouter_present else 'off'}",
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
