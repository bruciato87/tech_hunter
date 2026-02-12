from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from tech_sniper_it.models import ProductCategory
from tech_sniper_it.utils import parse_eur_price


MARKETPLACE_HOSTS: dict[str, str] = {
    "it": "www.amazon.it",
    "de": "www.amazon.de",
    "fr": "www.amazon.fr",
    "es": "www.amazon.es",
}
MARKETPLACE_GROUPS: dict[str, tuple[str, ...]] = {
    "eu": ("de", "fr", "es"),
}
DEFAULT_MARKETPLACES: tuple[str, ...] = ("it", "de", "fr", "es")
DEFAULT_QUERIES: tuple[str, ...] = (
    "iphone 14 pro 128gb amazon warehouse",
    "macbook air m1 amazon warehouse",
    "sony alpha amazon warehouse",
    "canon eos amazon warehouse",
)


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _is_enabled() -> bool:
    return _env_or_default("AMAZON_WAREHOUSE_ENABLED", "true").lower() not in {"0", "false", "no", "off"}


def _max_price_eur() -> float | None:
    raw = (os.getenv("AMAZON_WAREHOUSE_MAX_PRICE_EUR") or "").strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _build_search_url(host: str, query: str) -> str:
    return f"https://{host}/s?k={quote_plus(query)}"


def _expand_marketplaces(values: list[str]) -> list[str]:
    expanded: list[str] = []
    for item in values:
        key = item.lower()
        if key in MARKETPLACE_GROUPS:
            expanded.extend(MARKETPLACE_GROUPS[key])
        else:
            expanded.append(key)
    deduped: list[str] = []
    for item in expanded:
        if item not in deduped:
            deduped.append(item)
    return deduped


def _canonical_amazon_url(host: str, href: str | None) -> str | None:
    raw = (href or "").strip()
    if not raw:
        return None
    absolute = urljoin(f"https://{host}", raw)
    dp_match = re.search(r"/dp/([A-Z0-9]{10})", absolute)
    if dp_match:
        return f"https://{host}/dp/{dp_match.group(1)}"
    gp_match = re.search(r"/gp/product/([A-Z0-9]{10})", absolute)
    if gp_match:
        return f"https://{host}/dp/{gp_match.group(1)}"
    return absolute


def _extract_products_from_html(html: str, host: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("div[data-component-type='s-search-result']")
    results: list[dict[str, Any]] = []

    for row in rows:
        title_node = row.select_one("h2 a span")
        link_node = row.select_one("h2 a[href]")
        if not title_node or not link_node:
            continue

        title = title_node.get_text(" ", strip=True)
        if not title:
            continue
        if "sponsorizzato" in title.lower():
            continue

        price = parse_eur_price(row.get_text(" ", strip=True))
        if price is None:
            continue

        url = _canonical_amazon_url(host, link_node.get("href"))
        if not url:
            continue

        category = ProductCategory.from_raw(title).value
        results.append(
            {
                "title": title,
                "price_eur": price,
                "category": category,
                "url": url,
            }
        )

    return results


async def _accept_cookie_if_present(page) -> None:  # noqa: ANN001
    selectors = [
        "#sp-cc-accept",
        "input#sp-cc-accept",
        "button:has-text('Accept')",
        "button:has-text('Accetta')",
        "button:has-text('Alle akzeptieren')",
        "button:has-text('Tout accepter')",
        "button:has-text('Aceptar')",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() and await locator.is_visible(timeout=800):
                await locator.click(timeout=1200)
                return
        except PlaywrightError:
            continue


async def fetch_amazon_warehouse_products(
    *,
    headless: bool = True,
    nav_timeout_ms: int = 45000,
) -> list[dict[str, Any]]:
    if not _is_enabled():
        return []

    configured_marketplaces = _split_csv(os.getenv("AMAZON_WAREHOUSE_MARKETPLACES")) or list(DEFAULT_MARKETPLACES)
    marketplaces = _expand_marketplaces(configured_marketplaces)
    queries = _split_csv(os.getenv("AMAZON_WAREHOUSE_QUERIES")) or list(DEFAULT_QUERIES)
    max_products = max(1, int(_env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "8")))
    max_price = _max_price_eur()

    results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context(locale="it-IT")
        page = await context.new_page()
        page.set_default_timeout(nav_timeout_ms)
        try:
            for marketplace in marketplaces:
                host = MARKETPLACE_HOSTS.get(marketplace.lower())
                if not host:
                    print(f"[warehouse] Skipping unsupported marketplace '{marketplace}'.")
                    continue

                for query in queries:
                    if len(results) >= max_products:
                        break
                    search_url = _build_search_url(host, query)
                    print(f"[warehouse] Loading {search_url}")
                    try:
                        await page.goto(search_url, wait_until="domcontentloaded")
                        await _accept_cookie_if_present(page)
                        await page.wait_for_timeout(1200)
                        parsed = _extract_products_from_html(await page.content(), host)
                    except Exception as exc:
                        print(f"[warehouse] Search error on {host}: {type(exc).__name__}: {exc}")
                        continue

                    for item in parsed:
                        item_url = str(item.get("url") or "")
                        if item_url in seen_urls:
                            continue
                        price = item.get("price_eur")
                        if max_price is not None and isinstance(price, (float, int)) and float(price) > max_price:
                            continue

                        seen_urls.add(item_url)
                        item["source_marketplace"] = marketplace.lower()
                        results.append(item)
                        if len(results) >= max_products:
                            break
        finally:
            await context.close()
            await browser.close()

    print(f"[warehouse] Collected products: {len(results)}")
    return results


__all__ = [
    "fetch_amazon_warehouse_products",
    "_canonical_amazon_url",
    "_extract_products_from_html",
]
