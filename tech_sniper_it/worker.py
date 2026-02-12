from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

from dotenv import load_dotenv
from telegram import Bot

from tech_sniper_it.manager import build_default_manager
from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.sources import fetch_amazon_warehouse_products


MAX_LAST_LIMIT = 10
TELEGRAM_TEXT_LIMIT = 4000


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


def _offer_log_payload(offer) -> dict[str, Any]:  # noqa: ANN001
    valid_value = getattr(offer, "is_valid", None)
    if callable(valid_value):
        try:
            valid_value = bool(valid_value())
        except Exception:
            valid_value = None
    return {
        "platform": getattr(offer, "platform", "unknown"),
        "offer_eur": getattr(offer, "offer_eur", None),
        "condition": getattr(offer, "condition", None),
        "currency": getattr(offer, "currency", "EUR"),
        "valid": valid_value,
        "error": _safe_text(getattr(offer, "error", None)),
        "source_url": getattr(offer, "source_url", None),
    }


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
        return f"{provider} ({model}, {mode})"
    return f"{provider} ({mode})"


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
    return AmazonProduct(
        title=title,
        price_eur=price,
        category=category,
        ean=raw.get("ean"),
        url=raw.get("url"),
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


def _prioritize_products(products: list[AmazonProduct]) -> list[AmazonProduct]:
    category_weight = {
        ProductCategory.APPLE_PHONE: 0,
        ProductCategory.PHOTOGRAPHY: 1,
        ProductCategory.GENERAL_TECH: 2,
    }
    return sorted(
        products,
        key=lambda item: (
            float(item.price_eur),
            category_weight.get(item.category, 9),
            len(item.title),
        ),
    )


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
    removed = 0
    for product in products:
        normalized_url = _normalize_http_url(product.url)
        if normalized_url and normalized_url in excluded_urls:
            removed += 1
            continue
        filtered.append(product)
    print(
        "[scan] Exclusion cache applied | "
        f"removed={removed} kept={len(filtered)} lookback_days={lookback_days} rows={len(excluded_urls)}"
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
    products = load_products(_load_github_event_data())
    command_chat = _telegram_target_chat(payload)
    scan_target_products = max(1, int(_env_or_default("SCAN_TARGET_PRODUCTS", _env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "8"))))
    candidate_multiplier = max(1, int(_env_or_default("SCAN_CANDIDATE_MULTIPLIER", "3")))
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
    products = await _exclude_non_profitable_candidates(manager, products)
    products = _prioritize_products(products)
    if len(products) > scan_target_products:
        print(
            "[scan] Candidate selection | "
            f"target={scan_target_products} budget={candidate_budget} selected={scan_target_products} total_after_filter={len(products)}"
        )
        products = products[:scan_target_products]

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
