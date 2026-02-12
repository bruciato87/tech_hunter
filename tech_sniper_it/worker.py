from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import Bot

from tech_sniper_it.manager import build_default_manager
from tech_sniper_it.models import AmazonProduct, ProductCategory


MAX_LAST_LIMIT = 10
TELEGRAM_TEXT_LIMIT = 4000


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


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


def _format_scan_summary(decisions: list, threshold: float) -> str:
    profitable = [item for item in decisions if item.should_notify and item.spread_eur is not None]
    lines = [
        "Scan completata.",
        f"Prodotti analizzati: {len(decisions)}",
        f"Opportunita > {threshold:.2f} EUR: {len(profitable)}",
    ]
    if profitable:
        best = max(profitable, key=lambda item: item.spread_eur or -10**9)
        lines.append(
            f"Top: {best.normalized_name} | spread {best.spread_eur:.2f} EUR | {best.best_offer.platform if best.best_offer else 'n/a'}"
        )
    return "\n".join(lines)


async def _run_scan_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    products = load_products(_load_github_event_data())
    command_chat = _telegram_target_chat(payload)
    if not products:
        message = "Nessun prodotto valido nel payload. Usa /scan con JSON prodotto o array."
        print(message)
        if payload.get("source") == "telegram":
            await _send_telegram_message(message, command_chat)
        return 0

    max_parallel_products = int(_env_or_default("MAX_PARALLEL_PRODUCTS", "3"))
    decisions = await manager.evaluate_many(products, max_parallel_products=max_parallel_products)
    profitable = [item for item in decisions if item.should_notify]
    print(f"Scanned: {len(decisions)} | Profitable: {len(profitable)}")
    for decision in decisions:
        best = decision.best_offer.offer_eur if decision.best_offer else None
        print(
            json.dumps(
                {
                    "title": decision.product.title,
                    "normalized": decision.normalized_name,
                    "amazon_price": decision.product.price_eur,
                    "best_offer": best,
                    "best_platform": decision.best_offer.platform if decision.best_offer else None,
                    "spread_eur": decision.spread_eur,
                    "should_notify": decision.should_notify,
                },
                ensure_ascii=False,
            )
        )

    if payload.get("source") == "telegram":
        summary = _format_scan_summary(decisions, manager.min_spread_eur)
        await _send_telegram_message(summary, command_chat)
    return 0


async def _run_status_command(payload: dict[str, Any]) -> int:
    manager = build_default_manager()
    chat_id = _telegram_target_chat(payload)
    gemini_present = bool(os.getenv("GEMINI_API_KEYS", "").strip())
    openrouter_present = bool(os.getenv("OPENROUTER_API_KEYS", "").strip())

    lines = [
        "Tech_Sniper_IT status:",
        "worker: online",
        f"threshold: {manager.min_spread_eur:.2f} EUR",
        f"ai: gemini={'on' if gemini_present else 'off'}, openrouter={'on' if openrouter_present else 'off'}",
        f"supabase: {'on' if manager.storage else 'off'}",
        f"telegram alerts default chat: {'on' if manager.notifier else 'off'}",
    ]

    if manager.storage:
        try:
            recent = await manager.storage.get_recent_opportunities(limit=1)
            if recent:
                row = recent[0]
                lines.append(
                    "last opportunity: "
                    f"{row.get('normalized_name', 'n/a')} | spread {row.get('spread_eur', 'n/a')} EUR | {row.get('best_platform', 'n/a')}"
                )
            else:
                lines.append("last opportunity: none")
        except Exception as exc:
            lines.append(f"last opportunity: read error ({_safe_error_details(exc)})")

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
