from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from tech_sniper_it.manager import build_default_manager
from tech_sniper_it.models import AmazonProduct, ProductCategory


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


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


def _load_from_github_event() -> list[dict[str, Any]]:
    event_path = os.getenv("GITHUB_EVENT_PATH")
    if not event_path or not Path(event_path).exists():
        return []
    data = json.loads(Path(event_path).read_text(encoding="utf-8"))
    payload = data.get("client_payload", {})
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


def load_products() -> list[AmazonProduct]:
    raw_items = _load_from_github_event() or _load_from_file() or _load_from_env_json()
    products: list[AmazonProduct] = []
    for item in raw_items:
        try:
            products.append(_coerce_product(item))
        except Exception as exc:
            print(f"Skipping invalid product payload: {exc}")
    return products


async def run_worker() -> int:
    load_dotenv()
    manager = build_default_manager()
    products = load_products()
    if not products:
        print("No products provided. Supply client_payload.products, AMAZON_PRODUCTS_FILE or AMAZON_PRODUCTS_JSON.")
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
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run_worker()))


if __name__ == "__main__":
    main()
