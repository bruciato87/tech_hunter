from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import tempfile
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from tech_sniper_it.models import ProductCategory
from tech_sniper_it.utils import infer_amazon_warehouse_condition, parse_eur_price


MARKETPLACE_HOSTS: dict[str, str] = {
    "it": "www.amazon.it",
    "de": "www.amazon.de",
    "fr": "www.amazon.fr",
    "es": "www.amazon.es",
}
MARKETPLACE_FROM_HOST: dict[str, str] = {host: marketplace for marketplace, host in MARKETPLACE_HOSTS.items()}
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
DEFAULT_USER_AGENTS: tuple[str, ...] = (
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/18.0 Safari/605.1.15"
    ),
)
DEFAULT_VIEWPORTS: tuple[tuple[int, int], ...] = (
    (1366, 768),
    (1440, 900),
    (1536, 864),
    (1600, 900),
)
ACCEPT_LANGUAGE_BY_HOST: dict[str, str] = {
    "www.amazon.it": "it-IT,it;q=0.9,en;q=0.8",
    "www.amazon.de": "de-DE,de;q=0.9,en;q=0.8",
    "www.amazon.fr": "fr-FR,fr;q=0.9,en;q=0.8",
    "www.amazon.es": "es-ES,es;q=0.9,en;q=0.8",
}
SUPPORTED_PROXY_SCHEMES: set[str] = {"http", "https", "socks5", "socks5h"}
STORAGE_STATE_ENV_DEFAULT = "AMAZON_WAREHOUSE_STORAGE_STATE_B64"
STORAGE_STATE_ENV_BY_MARKETPLACE: dict[str, str] = {
    "it": "AMAZON_WAREHOUSE_STORAGE_STATE_B64_IT",
    "de": "AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE",
    "fr": "AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR",
    "es": "AMAZON_WAREHOUSE_STORAGE_STATE_B64_ES",
}
SEARCH_ROW_SELECTORS: tuple[str, ...] = (
    "div[data-component-type='s-search-result']",
    "div.s-result-item[data-asin]",
)
SPONSORED_HINTS: tuple[str, ...] = (
    "sponsorizzato",
    "sponsored",
    "gesponsert",
    "sponsorisé",
)
CURRENT_PRICE_SELECTORS: tuple[str, ...] = (
    ".a-price:not(.a-text-price) .a-offscreen",
    "span[data-a-price-type='price'] .a-offscreen",
    "span.a-price .a-offscreen",
)
LIST_PRICE_SELECTORS: tuple[str, ...] = (
    ".a-price.a-text-price .a-offscreen",
    ".a-text-price .a-offscreen",
    "span[data-a-strike='true'] .a-offscreen",
)
DISCOUNT_HINTS: tuple[str, ...] = (
    "coupon",
    "buono",
    "sconto",
    "risparmi",
    "risparmia",
    "save",
    "saving",
    "rabatt",
    "gutschein",
    "economisez",
    "ahorra",
    "checkout",
    "cassa",
    "al pagamento",
    "au paiement",
)
INSTALLMENT_HINTS: tuple[str, ...] = (
    "al mese",
    "a mese",
    "/mese",
    "mensil",
    "month",
    "monat",
    "mois",
    "al mes",
)
EUR_AMOUNT_PATTERN = re.compile(
    r"(?:€\s*\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?|\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?\s*€)"
)
PERCENT_AMOUNT_PATTERN = re.compile(r"(\d{1,2}(?:[.,]\d{1,2})?)\s*%")
BARRIER_HINTS: dict[str, tuple[str, ...]] = {
    "captcha": (
        "captchacharacters",
        "/errors/validatecaptcha",
        "type the characters you see in this image",
    ),
    "robot-check": (
        "robot check",
        "not a robot",
        "sei un robot",
    ),
    "sorry-page": (
        "ci dispiace",
        "tut uns leid",
        "toutes nos excuses",
        "lo sentimos",
        "sorry",
    ),
}
CONSENT_MARKERS: tuple[str, ...] = (
    "sp-cc-accept",
    "sp-cc-rejectall-link",
    "sp-cc-banner",
    "sp-cc",
)
SIGNIN_MARKERS: tuple[str, ...] = (
    "id=\"ap_login_form\"",
    "id='ap_login_form'",
    "id=\"authportal-main-section\"",
    "id='authportal-main-section'",
    "name=\"ap_email\"",
    "name='ap_email'",
    "id=\"ap_email\"",
    "id='ap_email'",
    "id=\"ap_password\"",
    "id='ap_password'",
)
ADD_TO_CART_SELECTORS: tuple[str, ...] = (
    "#add-to-cart-button",
    "input#add-to-cart-button",
    "input[name='submit.add-to-cart']",
    "button[name='submit.add-to-cart']",
    "input[name='submit.add-to-cart.v2']",
    "button#add-to-cart-button",
)
CART_ROW_SELECTORS: tuple[str, ...] = (
    "div.sc-list-item[data-asin]",
    "div[data-asin][data-name='Active Items']",
    "div[data-asin][data-itemid]",
)
CART_DELETE_SELECTORS: tuple[str, ...] = (
    "input[value='Delete']",
    "input[value='Elimina']",
    "input[value='Rimuovi']",
    "input[value='Löschen']",
    "input[value='Supprimer']",
    "input[value='Eliminar']",
    "button[data-action='delete']",
    "[data-action='delete'] input",
    "[data-action='delete'] a",
    "input[name='submit.delete']",
    "input[name^='submit.delete']",
    "a[data-feature-id='item-delete-button']",
)
CART_SUBTOTAL_SELECTORS: tuple[str, ...] = (
    "#sc-subtotal-amount-activecart .a-price .a-offscreen",
    "#sc-subtotal-amount-activecart .a-size-medium.a-color-base",
    "#sc-subtotal-amount-buybox .a-price .a-offscreen",
    "#sc-subtotal-amount-buybox .a-size-medium.a-color-base",
    "#sc-subtotal-amount-buybox",
)
CART_PROMO_DISCOUNT_SELECTORS: tuple[str, ...] = (
    "#sc-subtotal-discount .a-offscreen",
    "#sc-subtotal-discount .a-size-medium.a-color-base",
    "#subtotals-marketplace-table [id*='discount' i] .a-offscreen",
    "#subtotals-marketplace-table [class*='discount' i] .a-offscreen",
    "[data-testid*='discount' i] .a-offscreen",
)
CART_TOTAL_SELECTORS: tuple[str, ...] = (
    "#subtotals-marketplace-table .grand-total-price .a-offscreen",
    "#subtotals-marketplace-table .grand-total-price",
    "#sc-subtotal-amount-buybox .a-price .a-offscreen",
    "#sc-subtotal-amount-buybox .a-size-medium.a-color-base",
    "[data-testid*='grand-total' i] .a-offscreen",
    "[data-testid*='order-total' i] .a-offscreen",
)
CART_SUBTOTAL_HINTS: tuple[str, ...] = (
    "subtotale",
    "subtotal",
    "zwischensumme",
    "sous-total",
)
CART_PROMO_HINTS: tuple[str, ...] = (
    "promozion",
    "sconto",
    "coupon",
    "rispar",
    "discount",
    "promotion",
    "rabatt",
    "gutschein",
    "économ",
    "ahorro",
)
CART_TOTAL_HINTS: tuple[str, ...] = (
    "totale ordine",
    "totale",
    "order total",
    "total",
    "gesamt",
    "montant total",
    "importe total",
)
CART_EMPTY_HINTS: tuple[str, ...] = (
    "il tuo carrello amazon è vuoto",
    "your amazon cart is empty",
    "your basket is empty",
    "ihr einkaufswagen ist leer",
    "votre panier amazon est vide",
    "tu cesta de amazon está vacía",
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


def _collect_search_rows(soup: BeautifulSoup) -> list[Any]:
    rows: list[Any] = []
    seen: set[int] = set()
    for selector in SEARCH_ROW_SELECTORS:
        for row in soup.select(selector):
            marker = id(row)
            if marker in seen:
                continue
            seen.add(marker)
            rows.append(row)
    return rows


def _extract_title_from_row(row: Any) -> str | None:
    selectors = [
        "h2 span",
        "h2 a span",
        "span.a-size-medium.a-color-base.a-text-normal",
        "span.a-size-base-plus.a-color-base.a-text-normal",
        "a.a-link-normal.s-no-outline span",
    ]
    for selector in selectors:
        node = row.select_one(selector)
        if not node:
            continue
        title = node.get_text(" ", strip=True)
        if title:
            return title
    h2_node = row.select_one("h2[aria-label]")
    if h2_node:
        aria_label = (h2_node.get("aria-label") or "").strip()
        if aria_label:
            return aria_label
    image_node = row.select_one("img.s-image[alt]")
    if image_node:
        alt = (image_node.get("alt") or "").strip()
        if alt:
            return alt
    return None


def _extract_link_from_row(host: str, row: Any) -> str | None:
    anchors = row.select("a[href]")
    preferred = None
    fallback = None
    for anchor in anchors:
        href = anchor.get("href")
        if not href:
            continue
        if fallback is None:
            fallback = href
        if re.search(r"/(dp|gp/product)/[A-Z0-9]{10}", href, flags=re.IGNORECASE):
            preferred = href
            break
    return _canonical_amazon_url(host, preferred or fallback)


def _extract_price_from_row(row: Any) -> float | None:
    details = _extract_price_details_from_row(row)
    price = details.get("net_price_eur")
    if isinstance(price, (int, float)):
        return float(price)
    return None


def _extract_prices_by_selectors(row: Any, selectors: tuple[str, ...]) -> list[float]:
    values: list[float] = []
    for selector in selectors:
        for node in row.select(selector):
            context_chunks = [node.get_text(" ", strip=True)]
            parent = getattr(node, "parent", None)
            if parent is not None:
                context_chunks.append(parent.get_text(" ", strip=True))
                grandparent = getattr(parent, "parent", None)
                if grandparent is not None:
                    context_chunks.append(grandparent.get_text(" ", strip=True))
            context_text = " ".join(chunk for chunk in context_chunks if chunk).lower()
            if any(hint in context_text for hint in INSTALLMENT_HINTS):
                continue
            price = parse_eur_price(node.get_text(" ", strip=True))
            if price is None:
                continue
            if price <= 0:
                continue
            values.append(float(price))
    return values


def _extract_discount_amounts_from_text(text: str) -> tuple[list[float], list[float]]:
    if not text:
        return [], []
    lowered = text.lower()
    eur_values: list[float] = []
    pct_values: list[float] = []

    for match in EUR_AMOUNT_PATTERN.finditer(text):
        snippet = lowered[max(0, match.start() - 72) : min(len(lowered), match.end() + 72)]
        if not any(hint in snippet for hint in DISCOUNT_HINTS):
            continue
        value = parse_eur_price(match.group(0))
        if value is None or value <= 0:
            continue
        eur_values.append(float(value))

    for match in PERCENT_AMOUNT_PATTERN.finditer(text):
        snippet = lowered[max(0, match.start() - 72) : min(len(lowered), match.end() + 72)]
        if not any(hint in snippet for hint in DISCOUNT_HINTS):
            continue
        raw = (match.group(1) or "").replace(",", ".").strip()
        try:
            value = float(raw)
        except ValueError:
            continue
        if value <= 0 or value >= 100:
            continue
        pct_values.append(float(value))

    return eur_values, pct_values


def _extract_price_details_from_row(row: Any) -> dict[str, float]:
    current_prices = _extract_prices_by_selectors(row, CURRENT_PRICE_SELECTORS)
    list_prices = _extract_prices_by_selectors(row, LIST_PRICE_SELECTORS)
    all_prices = _extract_prices_by_selectors(row, (".a-price .a-offscreen",))
    row_text = row.get_text(" ", strip=True).lower()
    has_installment_hints = any(hint in row_text for hint in INSTALLMENT_HINTS)
    if has_installment_hints:
        if len(current_prices) >= 2:
            top = max(current_prices)
            current_prices = [value for value in current_prices if value >= (top * 0.55)]
        if len(all_prices) >= 2:
            top = max(all_prices)
            all_prices = [value for value in all_prices if value >= (top * 0.55)]

    displayed_price = current_prices[0] if current_prices else None
    if displayed_price is None and all_prices:
        displayed_price = min(all_prices)
    if displayed_price is None:
        displayed_price = None if has_installment_hints else parse_eur_price(row.get_text(" ", strip=True))
    if displayed_price is None:
        return {}

    list_price: float | None = None
    if list_prices:
        candidate = max(list_prices)
        if candidate > displayed_price:
            list_price = candidate
    elif len(all_prices) >= 2:
        candidate = max(all_prices)
        if candidate > displayed_price:
            list_price = candidate

    implied_discount_eur: float | None = None
    implied_discount_pct: float | None = None
    if list_price is not None and list_price > displayed_price:
        implied_discount_eur = round(list_price - displayed_price, 2)
        implied_discount_pct = round((implied_discount_eur / list_price) * 100, 2)

    text = row.get_text(" ", strip=True)
    eur_discounts, pct_discounts = _extract_discount_amounts_from_text(text)

    extra_discount_eur = 0.0
    for value in eur_discounts:
        if value >= displayed_price:
            continue
        if implied_discount_eur is not None and abs(value - implied_discount_eur) <= 0.60:
            continue
        extra_discount_eur = max(extra_discount_eur, value)

    extra_discount_pct = 0.0
    for value in pct_discounts:
        if implied_discount_pct is not None and abs(value - implied_discount_pct) <= 1.20:
            continue
        extra_discount_pct = max(extra_discount_pct, value)

    net_price = float(displayed_price)
    if extra_discount_pct > 0:
        net_price *= 1 - (extra_discount_pct / 100)
    if extra_discount_eur > 0:
        net_price -= extra_discount_eur
    if net_price <= 0:
        net_price = float(displayed_price)
        extra_discount_eur = 0.0
        extra_discount_pct = 0.0
    net_price = round(net_price, 2)

    details: dict[str, float] = {
        "displayed_price_eur": round(float(displayed_price), 2),
        "net_price_eur": net_price,
    }
    if list_price is not None:
        details["list_price_eur"] = round(float(list_price), 2)
    if implied_discount_eur is not None:
        details["implied_discount_eur"] = implied_discount_eur
    if implied_discount_pct is not None:
        details["implied_discount_pct"] = implied_discount_pct
    if extra_discount_eur > 0:
        details["extra_discount_eur"] = round(extra_discount_eur, 2)
    if extra_discount_pct > 0:
        details["extra_discount_pct"] = round(extra_discount_pct, 2)
    return details


def _is_sponsored(text: str) -> bool:
    lowered = text.lower()
    return any(hint in lowered for hint in SPONSORED_HINTS)


def _detect_page_barriers(html: str, title: str | None = None) -> list[str]:
    lowered = f"{title or ''}\n{html}".lower()
    barriers: list[str] = []
    for label, hints in BARRIER_HINTS.items():
        if any(hint in lowered for hint in hints):
            barriers.append(label)
    if any(marker in lowered for marker in CONSENT_MARKERS):
        barriers.append("consent")
    if any(marker in lowered for marker in SIGNIN_MARKERS):
        barriers.append("signin")
    return barriers


def _is_truthy_env(name: str, default: str) -> bool:
    return _env_or_default(name, default).lower() not in {"0", "false", "no", "off"}


def _debug_dump_dir() -> str:
    return _env_or_default("AMAZON_WAREHOUSE_DEBUG_DIR", "/tmp/tech_sniper_it_debug")


def _should_dump_debug_files() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_DEBUG_ON_EMPTY", "true")


def _dedupe_keep_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped


def _parse_proxy_entry(raw: str) -> dict[str, str] | None:
    text = (raw or "").strip()
    if not text:
        return None
    if "://" not in text:
        text = f"http://{text}"
    try:
        parsed = urlparse(text)
    except ValueError:
        return None
    scheme = (parsed.scheme or "http").lower()
    if scheme not in SUPPORTED_PROXY_SCHEMES:
        return None
    host = parsed.hostname
    if not host:
        return None

    server = f"{scheme}://{host}"
    if parsed.port:
        server = f"{server}:{parsed.port}"

    proxy: dict[str, str] = {"server": server}
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def _load_proxy_pool() -> list[dict[str, str]]:
    raw_values = _split_csv(os.getenv("AMAZON_WAREHOUSE_PROXY_URLS"))
    proxies: list[dict[str, str]] = []
    for raw in raw_values:
        proxy = _parse_proxy_entry(raw)
        if proxy is None:
            print("[warehouse] Ignoring invalid proxy entry from AMAZON_WAREHOUSE_PROXY_URLS.")
            continue
        proxies.append(proxy)
    return proxies


def _proxy_key(proxy: dict[str, str] | None) -> str:
    if not proxy:
        return "direct"
    username = proxy.get("username", "")
    server = proxy.get("server", "")
    return f"{server}|{username}"


def _proxy_label(proxy: dict[str, str] | None) -> str:
    if not proxy:
        return "direct"
    server = proxy.get("server", "")
    return server or "direct"


def _parse_user_agent_list(raw: str | None) -> list[str]:
    value = (raw or "").strip()
    if not value:
        return []
    if value.startswith("["):
        try:
            loaded = json.loads(value)
            if isinstance(loaded, list):
                return _dedupe_keep_order([str(item).strip() for item in loaded if str(item).strip()])
        except json.JSONDecodeError:
            return []
    if "||" in value:
        return _dedupe_keep_order([item.strip() for item in value.split("||") if item.strip()])
    if "\n" in value:
        return _dedupe_keep_order([item.strip() for item in value.splitlines() if item.strip()])
    return [value]


def _load_user_agents() -> list[str]:
    configured = _parse_user_agent_list(os.getenv("AMAZON_WAREHOUSE_USER_AGENTS"))
    if configured:
        return configured
    return list(DEFAULT_USER_AGENTS)


def _max_attempts_per_query() -> int:
    return max(1, int(_env_or_default("AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY", "3")))


def _retry_delay_ms() -> int:
    return max(0, int(_env_or_default("AMAZON_WAREHOUSE_RETRY_DELAY_MS", "700")))


def _per_query_limit(total_budget: int, query_count: int) -> int:
    configured = (os.getenv("AMAZON_WAREHOUSE_PER_QUERY_LIMIT") or "").strip()
    if configured:
        try:
            value = int(configured)
        except ValueError:
            value = 0
        if value > 0:
            return value
    if query_count <= 0:
        return total_budget
    return max(1, (total_budget + query_count - 1) // query_count)


def _per_marketplace_limit(total_budget: int, marketplace_count: int) -> int:
    configured = (os.getenv("AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT") or "").strip()
    if configured:
        try:
            value = int(configured)
        except ValueError:
            value = 0
        if value > 0:
            return value
    if marketplace_count <= 0:
        return total_budget
    return max(1, (total_budget + marketplace_count - 1) // marketplace_count)


def _fail_fast_on_sorry() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_FAIL_FAST_ON_SORRY", "true")


def _stealth_enabled() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_STEALTH", "true")


def _use_storage_state() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_USE_STORAGE_STATE", "true")


def _cart_pricing_enabled() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_CART_PRICING_ENABLED", "false")


def _cart_pricing_max_items(default_target: int) -> int:
    raw = _env_or_default("AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS", str(default_target))
    try:
        value = int(raw)
    except ValueError:
        value = default_target
    return max(1, min(value, max(1, default_target)))


def _cart_pricing_require_empty_cart() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_CART_PRICING_REQUIRE_EMPTY_CART", "true")


def _cart_pricing_allow_delta_non_empty() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_CART_PRICING_ALLOW_DELTA", "true")


def _cart_pricing_force_empty_after_host() -> bool:
    return _is_truthy_env("AMAZON_WAREHOUSE_CART_PRICING_FORCE_EMPTY_AFTER_HOST", "true")


def _retry_delay_for_attempt(base_ms: int, attempt: int) -> int:
    multiplier = max(1, 2 ** max(0, attempt - 1))
    jitter = random.randint(0, 250)
    return min(6000, base_ms * multiplier + jitter)


def _decode_storage_state_b64() -> str | None:
    return _decode_storage_state_env(STORAGE_STATE_ENV_DEFAULT)


def _decode_storage_state_env(env_name: str) -> str | None:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        parsed = json.loads(decoded)
    except Exception as exc:
        print(f"[warehouse] Invalid {env_name}: {type(exc).__name__}: {exc}")
        return None
    if not isinstance(parsed, dict):
        print(f"[warehouse] {env_name} is not a JSON object.")
        return None
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    try:
        json.dump(parsed, handle, ensure_ascii=False)
        handle.flush()
        return handle.name
    finally:
        handle.close()


def _load_storage_state_paths() -> dict[str, str]:
    paths: dict[str, str] = {}
    global_path = _decode_storage_state_env(STORAGE_STATE_ENV_DEFAULT)
    if global_path:
        paths["default"] = global_path
    for marketplace, env_name in STORAGE_STATE_ENV_BY_MARKETPLACE.items():
        path = _decode_storage_state_env(env_name)
        if path:
            paths[marketplace] = path
    return paths


def _storage_state_for_host(storage_state_paths: dict[str, str], host: str) -> str | None:
    marketplace = MARKETPLACE_FROM_HOST.get(host.lower())
    if marketplace and marketplace in storage_state_paths:
        return storage_state_paths[marketplace]
    return storage_state_paths.get("default")


def _remove_file_if_exists(path: str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except Exception as exc:
        print(f"[warehouse] Could not remove temp file '{path}': {type(exc).__name__}: {exc}")


async def _apply_stealth_context(context, *, host: str) -> None:  # noqa: ANN001
    if not _stealth_enabled():
        return
    languages_map: dict[str, list[str]] = {
        "www.amazon.it": ["it-IT", "it", "en-US", "en"],
        "www.amazon.de": ["de-DE", "de", "en-US", "en"],
        "www.amazon.fr": ["fr-FR", "fr", "en-US", "en"],
        "www.amazon.es": ["es-ES", "es", "en-US", "en"],
    }
    languages = languages_map.get(host, ["it-IT", "it", "en-US", "en"])
    script = f"""
    Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
    Object.defineProperty(navigator, 'languages', {{ get: () => {json.dumps(languages)} }});
    Object.defineProperty(navigator, 'plugins', {{ get: () => [1, 2, 3, 4, 5] }});
    window.chrome = window.chrome || {{ runtime: {{}} }};
    """
    try:
        await context.add_init_script(script)
    except Exception as exc:
        print(f"[warehouse] Stealth init script failed: {type(exc).__name__}: {exc}")


def _choose_from_pool(pool: list[Any], index: int, rotate: bool) -> tuple[Any, int]:
    if not pool:
        return None, index
    if not rotate:
        return pool[0], index
    chosen = pool[index % len(pool)]
    return chosen, index + 1


def _viewport_for_session(session_id: int) -> dict[str, int]:
    width, height = DEFAULT_VIEWPORTS[session_id % len(DEFAULT_VIEWPORTS)]
    return {"width": width, "height": height}


def _shorten(text: str | None, limit: int = 64) -> str:
    value = (text or "").strip()
    if not value:
        return "n/d"
    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."


def _format_money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "n/d"
    return f"{float(value):.2f}"


def _should_fail_fast(barriers: list[str], *, proxy_pool_size: int, fail_fast: bool) -> bool:
    if not fail_fast:
        return False
    if proxy_pool_size > 0:
        return False
    blocker_barriers = {"sorry-page", "captcha", "robot-check"}
    return any(barrier in blocker_barriers for barrier in barriers)


def _slug(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")
    return value[:64] or "query"


def _html_excerpt(html: str, *, limit: int = 220) -> str:
    collapsed = re.sub(r"\s+", " ", html).strip()
    return collapsed[:limit]


async def _log_empty_parse_diagnostics(  # noqa: ANN001
    page,
    *,
    host: str,
    query: str,
    html: str,
    row_count: int,
    session_id: int,
    user_agent: str | None,
    proxy_label: str,
) -> list[str]:
    try:
        title = await page.title()
    except Exception:
        title = ""
    barriers = _detect_page_barriers(html, title)
    barrier_text = ",".join(barriers) if barriers else "none"
    print(
        f"[warehouse] Empty parse on {host} | query='{query}' | candidate_rows={row_count} "
        f"| session={session_id} | ua='{_shorten(user_agent)}' | proxy={proxy_label} "
        f"| title='{title}' | barriers={barrier_text}"
    )
    print(f"[warehouse] HTML excerpt: {_html_excerpt(html)}")

    if not _should_dump_debug_files():
        return barriers
    dump_dir = _debug_dump_dir()
    os.makedirs(dump_dir, exist_ok=True)
    base = f"{host.replace('.', '_')}_{_slug(query)}_s{session_id}"
    html_path = os.path.join(dump_dir, f"{base}.html")
    screenshot_path = os.path.join(dump_dir, f"{base}.png")
    try:
        with open(html_path, "w", encoding="utf-8") as handle:
            handle.write(html)
        print(f"[warehouse] Saved debug html: {html_path}")
    except Exception as exc:
        print(f"[warehouse] Failed to save debug html: {type(exc).__name__}: {exc}")
    try:
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"[warehouse] Saved debug screenshot: {screenshot_path}")
    except Exception as exc:
        print(f"[warehouse] Failed to save debug screenshot: {type(exc).__name__}: {exc}")
    return barriers


def _extract_products_from_html(html: str, host: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows = _collect_search_rows(soup)
    results: list[dict[str, Any]] = []

    for row in rows:
        title = _extract_title_from_row(row)
        if not title:
            continue
        if _is_sponsored(title):
            continue

        price_details = _extract_price_details_from_row(row)
        price = price_details.get("net_price_eur")
        if price is None:
            continue

        url = _extract_link_from_row(host, row)
        if not url:
            continue

        category = ProductCategory.from_raw(title).value
        condition_label, condition_confidence, packaging_only = infer_amazon_warehouse_condition(
            f"{title} {row.get_text(' ', strip=True)}"
        )
        item: dict[str, Any] = {
            "title": title,
            "price_eur": float(price),
            "category": category,
            "url": url,
        }
        if condition_label:
            item["amazon_condition"] = condition_label
            item["amazon_condition_confidence"] = float(condition_confidence)
        if packaging_only:
            item["amazon_packaging_only"] = True
        displayed_price = price_details.get("displayed_price_eur")
        if isinstance(displayed_price, (int, float)):
            item["displayed_price_eur"] = float(displayed_price)
        list_price = price_details.get("list_price_eur")
        if isinstance(list_price, (int, float)):
            item["list_price_eur"] = float(list_price)
        extra_discount_eur = price_details.get("extra_discount_eur")
        if isinstance(extra_discount_eur, (int, float)):
            item["extra_discount_eur"] = float(extra_discount_eur)
        extra_discount_pct = price_details.get("extra_discount_pct")
        if isinstance(extra_discount_pct, (int, float)):
            item["extra_discount_pct"] = float(extra_discount_pct)
        results.append(item)

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


def _extract_asin_from_url(url: str | None) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw if raw.startswith(("http://", "https://")) else f"https://{raw}")
    path = parsed.path or ""

    patterns = (
        r"/dp/([A-Z0-9]{10})",
        r"/gp/product/([A-Z0-9]{10})",
        r"/gp/aw/d/([A-Z0-9]{10})",
        r"/gp/-/product/([A-Z0-9]{10})",
        r"/exec/obidos/ASIN/([A-Z0-9]{10})",
    )
    for pattern in patterns:
        match = re.search(pattern, path, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper()

    query = parse_qs(parsed.query or "")
    for key in ("asin", "ASIN", "pd_rd_i"):
        values = query.get(key) or []
        for value in values:
            candidate = (value or "").strip().upper()
            if re.fullmatch(r"[A-Z0-9]{10}", candidate):
                return candidate
    return None


def _cart_url_for_host(host: str) -> str:
    return f"https://{host}/gp/cart/view.html?ref_=nav_cart"


def _collect_cart_rows(soup: BeautifulSoup) -> list[Any]:
    rows: list[Any] = []
    seen: set[int] = set()
    for selector in CART_ROW_SELECTORS:
        for row in soup.select(selector):
            marker = id(row)
            if marker in seen:
                continue
            asin = (row.get("data-asin") or "").strip().upper()
            if not asin:
                continue
            seen.add(marker)
            rows.append(row)
    return rows


def _extract_row_price(row: Any) -> float | None:
    selectors = (
        ".sc-product-price",
        ".a-price .a-offscreen",
        ".sc-price",
    )
    for selector in selectors:
        node = row.select_one(selector)
        if not node:
            continue
        price = parse_eur_price(node.get_text(" ", strip=True))
        if price is not None:
            return price
    return parse_eur_price(row.get_text(" ", strip=True))


def _extract_price_by_selectors(soup: BeautifulSoup, selectors: tuple[str, ...]) -> float | None:
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        value = parse_eur_price(node.get_text(" ", strip=True))
        if isinstance(value, (int, float)) and value > 0:
            return round(float(value), 2)
    return None


def _extract_labeled_price_from_text(text: str, hints: tuple[str, ...]) -> float | None:
    if not text:
        return None
    lowered = text.lower()
    values: list[float] = []
    for match in EUR_AMOUNT_PATTERN.finditer(text):
        snippet = lowered[max(0, match.start() - 80) : min(len(lowered), match.end() + 80)]
        if hints and not any(hint in snippet for hint in hints):
            continue
        value = parse_eur_price(match.group(0))
        if value is None or value <= 0:
            continue
        values.append(round(float(value), 2))
    if not values:
        return None
    return max(values)


def _parse_cart_summary(html: str, asin: str | None) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    rows = _collect_cart_rows(soup)
    cart_asins = [
        (row.get("data-asin") or "").strip().upper()
        for row in rows
        if (row.get("data-asin") or "").strip()
    ]
    full_text = soup.get_text(" ", strip=True)
    lowered = full_text.lower()
    is_empty = (len(rows) == 0) or any(hint in lowered for hint in CART_EMPTY_HINTS)

    subtotal = _extract_price_by_selectors(soup, CART_SUBTOTAL_SELECTORS)
    if subtotal is None:
        subtotal = _extract_labeled_price_from_text(full_text, CART_SUBTOTAL_HINTS)

    promo_discount = _extract_price_by_selectors(soup, CART_PROMO_DISCOUNT_SELECTORS)
    if promo_discount is None:
        promo_discount = _extract_labeled_price_from_text(full_text, CART_PROMO_HINTS)

    total_price = _extract_price_by_selectors(soup, CART_TOTAL_SELECTORS)
    if total_price is None:
        total_price = _extract_labeled_price_from_text(full_text, CART_TOTAL_HINTS)
    if (
        total_price is None
        and isinstance(subtotal, (int, float))
        and isinstance(promo_discount, (int, float))
        and subtotal > promo_discount
    ):
        total_price = round(float(subtotal) - float(promo_discount), 2)

    target_in_cart = False
    target_row_price: float | None = None
    target_row_count = 0
    asin_normalized = (asin or "").strip().upper()
    for row in rows:
        row_asin = (row.get("data-asin") or "").strip().upper()
        if not asin_normalized or row_asin != asin_normalized:
            continue
        target_in_cart = True
        target_row_count += 1
        row_price = _extract_row_price(row)
        if row_price is not None:
            target_row_price = row_price
            break

    return {
        "row_count": len(rows),
        "cart_asins": cart_asins,
        "is_empty": is_empty,
        "target_in_cart": target_in_cart,
        "target_row_price": target_row_price,
        "target_row_count": target_row_count,
        "subtotal_price": subtotal,
        "promo_discount_eur": promo_discount,
        "total_price": total_price,
    }


def _build_cart_cleanup_asins(
    *,
    before_cart_asins: list[str] | None,
    after_cart_asins: list[str] | None,
    target_asin: str | None,
) -> list[str]:
    before = {(asin or "").strip().upper() for asin in (before_cart_asins or []) if (asin or "").strip()}
    cleanup: list[str] = []
    for raw in (after_cart_asins or []):
        asin = (raw or "").strip().upper()
        if not asin:
            continue
        if asin in before:
            continue
        if asin not in cleanup:
            cleanup.append(asin)
    target = (target_asin or "").strip().upper()
    if target and target not in cleanup:
        cleanup.append(target)
    return cleanup


async def _read_cart_summary(page, *, host: str, asin: str | None) -> dict[str, Any]:  # noqa: ANN001
    cart_url = _cart_url_for_host(host)
    await page.goto(cart_url, wait_until="domcontentloaded")
    await _accept_cookie_if_present(page)
    await page.wait_for_timeout(900)
    html = await page.content()
    title = await page.title()
    summary = _parse_cart_summary(html, asin)
    summary["barriers"] = _detect_page_barriers(html, title)
    summary["url"] = page.url
    return summary


def _positive_delta(after_value: Any, before_value: Any) -> float | None:
    if not isinstance(after_value, (int, float)):
        return None
    if before_value is None:
        return round(float(after_value), 2) if float(after_value) > 0 else None
    if not isinstance(before_value, (int, float)):
        return None
    delta = float(after_value) - float(before_value)
    if delta <= 0:
        return None
    return round(delta, 2)


async def _click_add_to_cart(page) -> bool:  # noqa: ANN001
    for selector in ADD_TO_CART_SELECTORS:
        try:
            locator = page.locator(selector).first
            if not await locator.count():
                continue
            await locator.wait_for(state="visible", timeout=1400)
            await locator.click(timeout=1800)
            return True
        except PlaywrightError:
            continue
    return False


async def _remove_asin_from_cart(page, *, host: str, asin: str) -> bool:  # noqa: ANN001
    try:
        await page.goto(_cart_url_for_host(host), wait_until="domcontentloaded")
        await page.wait_for_timeout(800)
    except Exception:
        return False

    row_locator = page.locator(
        ",".join(
            [
                f"div.sc-list-item[data-asin='{asin}']",
                f"div[data-asin='{asin}'][data-name='Active Items']",
                f"div[data-asin='{asin}'][data-itemid]",
                f"[data-asin='{asin}']",
            ]
        )
    ).first
    try:
        if not await row_locator.count():
            return True
    except PlaywrightError:
        return False

    clicked = False
    for selector in CART_DELETE_SELECTORS:
        try:
            target = row_locator.locator(selector).first
            if not await target.count():
                continue
            try:
                await target.scroll_into_view_if_needed(timeout=800)
            except PlaywrightError:
                pass
            await target.click(timeout=1800, force=True)
            clicked = True
            break
        except PlaywrightError:
            continue

    if not clicked:
        remove_pattern = re.compile(r"(delete|remove|elimina|löschen|supprimer|eliminar)", re.IGNORECASE)
        try:
            target = row_locator.get_by_role("button", name=remove_pattern).first
            if await target.count():
                try:
                    await target.scroll_into_view_if_needed(timeout=800)
                except PlaywrightError:
                    pass
                await target.click(timeout=1800, force=True)
                clicked = True
        except PlaywrightError:
            pass

    if not clicked:
        try:
            target = page.locator("a[data-feature-id='item-delete-button'], [data-action='delete'] a").first
            if await target.count() and await target.is_visible(timeout=800):
                await target.click(timeout=1800)
                clicked = True
        except PlaywrightError:
            pass

    if not clicked:
        # Fallback: some cart UIs only support deletion via the quantity dropdown (0 / Delete).
        quantity_selectors = (
            "select[name='quantity']",
            "select[name^='quantity']",
            "select[name*='quantity' i]",
            "select[id*='quantity' i]",
        )

        def _looks_like_delete_option(label: str) -> bool:
            lowered = (label or "").strip().lower()
            if not lowered:
                return False
            if re.search(r"\\b0\\b", lowered):
                return True
            return any(
                token in lowered
                for token in (
                    "delete",
                    "remove",
                    "elimina",
                    "rimuovi",
                    "löschen",
                    "supprimer",
                    "eliminar",
                )
            )

        for selector in quantity_selectors:
            try:
                qty = row_locator.locator(selector).first
                if not await qty.count():
                    continue
                options = qty.locator("option")
                try:
                    option_count = min(await options.count(), 30)
                except PlaywrightError:
                    option_count = 0
                picked_value = None
                picked_label = None
                for index in range(option_count):
                    opt = options.nth(index)
                    try:
                        label = (await opt.inner_text(timeout=800) or "").strip()
                        value = (await opt.get_attribute("value") or "").strip()
                    except PlaywrightError:
                        continue
                    if value == "0":
                        picked_value = "0"
                        picked_label = label
                        break
                    if _looks_like_delete_option(label):
                        picked_value = value or None
                        picked_label = label
                        break
                if picked_value is not None:
                    await qty.select_option(picked_value)
                    clicked = True
                    break
                if picked_label:
                    await qty.select_option(label=picked_label)
                    clicked = True
                    break
            except PlaywrightError:
                continue

    if not clicked:
        return False

    await page.wait_for_timeout(600)
    try:
        await row_locator.wait_for(state="detached", timeout=5000)
    except PlaywrightError:
        pass
    try:
        summary = await _read_cart_summary(page, host=host, asin=asin)
        return not bool(summary.get("target_in_cart"))
    except PlaywrightError:
        return False


async def _cleanup_cart_asins(page, *, host: str, asins: list[str]) -> dict[str, Any]:  # noqa: ANN001
    removed_asins: list[str] = []
    failed_asins: list[str] = []
    deduped: list[str] = []
    for asin in asins:
        normalized = (asin or "").strip().upper()
        if not normalized:
            continue
        if normalized not in deduped:
            deduped.append(normalized)

    for asin in deduped:
        ok = False
        for attempt in range(1, 4):
            try:
                ok = await _remove_asin_from_cart(page, host=host, asin=asin)
            except Exception:
                ok = False
            if ok:
                break
            await page.wait_for_timeout(250 * attempt)
        if ok:
            removed_asins.append(asin)
        else:
            failed_asins.append(asin)

    return {
        "removed": len(failed_asins) == 0,
        "removed_asins": removed_asins,
        "failed_asins": failed_asins,
    }


async def _bulk_delete_visible_cart_items(page, *, max_clicks: int = 5) -> int:  # noqa: ANN001
    clicked = 0
    selectors = (
        "input[name^='submit.delete']",
        "input[name='submit.delete']",
        "[data-action='delete'] input",
        "[data-action='delete'] a",
        "a[data-feature-id='item-delete-button']",
        "button[aria-label*='Delete' i]",
        "button[aria-label*='Remove' i]",
        "button[aria-label*='Elimina' i]",
        "button:has-text('Elimina')",
        "button:has-text('Rimuovi')",
    )
    for selector in selectors:
        if clicked >= max_clicks:
            break
        try:
            locator = page.locator(selector)
            count = min(await locator.count(), max_clicks - clicked)
        except PlaywrightError:
            continue
        for _ in range(count):
            if clicked >= max_clicks:
                break
            try:
                node = locator.first
                if not await node.is_visible(timeout=700):
                    break
                try:
                    await node.scroll_into_view_if_needed(timeout=700)
                except PlaywrightError:
                    pass
                await node.click(timeout=1600, force=True)
                clicked += 1
                await page.wait_for_timeout(450)
            except PlaywrightError:
                break
    return clicked


async def _force_empty_cart(page, *, host: str, max_rounds: int = 5) -> dict[str, Any]:  # noqa: ANN001
    removed_total: list[str] = []
    failed_total: list[str] = []
    final_summary: dict[str, Any] | None = None

    for _ in range(max_rounds):
        try:
            summary = await _read_cart_summary(page, host=host, asin=None)
        except Exception:
            break
        final_summary = summary
        current_asins = [str(item).strip().upper() for item in (summary.get("cart_asins") or []) if str(item).strip()]
        if bool(summary.get("is_empty")) and not current_asins:
            break
        if not current_asins:
            break
        cleanup = await _cleanup_cart_asins(page, host=host, asins=current_asins)
        removed_round = [str(item).strip().upper() for item in (cleanup.get("removed_asins") or []) if str(item).strip()]
        failed_round = [str(item).strip().upper() for item in (cleanup.get("failed_asins") or []) if str(item).strip()]
        for asin in removed_round:
            if asin not in removed_total:
                removed_total.append(asin)
        for asin in failed_round:
            if asin not in failed_total:
                failed_total.append(asin)
        if not removed_round:
            # Fallback: if row-targeted deletion fails (UI drift), try deleting the first visible cart item
            # to make progress and re-evaluate. This is only used inside force-empty cart logic.
            progress = False
            try:
                generic = page.locator(
                    ",".join(
                        [
                            "input[name^='submit.delete']",
                            "[data-action='delete'] input",
                            "[data-action='delete'] a",
                            "a[data-feature-id='item-delete-button']",
                        ]
                    )
                ).first
                if await generic.count():
                    try:
                        await generic.scroll_into_view_if_needed(timeout=800)
                    except PlaywrightError:
                        pass
                    await generic.click(timeout=1800, force=True)
                    await page.wait_for_timeout(900)
                    progress = True
            except Exception:
                progress = False
            if not progress:
                try:
                    bulk_clicks = await _bulk_delete_visible_cart_items(page, max_clicks=4)
                    if bulk_clicks > 0:
                        await page.wait_for_timeout(1000)
                        progress = True
                except Exception:
                    progress = False
            if not progress:
                break

    if final_summary is None or not bool(final_summary.get("is_empty")):
        try:
            final_summary = await _read_cart_summary(page, host=host, asin=None)
        except Exception:
            final_summary = final_summary or {}

    remaining_asins = [
        str(item).strip().upper()
        for item in ((final_summary or {}).get("cart_asins") or [])
        if str(item).strip()
    ]
    empty = bool((final_summary or {}).get("is_empty")) and not remaining_asins
    return {
        "empty": empty,
        "removed_asins": removed_total,
        "failed_asins": failed_total,
        "remaining_asins": remaining_asins,
    }


async def _resolve_cart_net_price(
    page,
    *,
    host: str,
    product_url: str,
    require_empty_cart: bool,
) -> dict[str, Any]:  # noqa: ANN001
    asin = _extract_asin_from_url(product_url)
    result: dict[str, Any] = {
        "asin": asin,
        "net_price_eur": None,
        "removed": False,
        "added": False,
        "subtotal_price": None,
        "promo_discount_eur": None,
        "total_price": None,
        "net_price_source": None,
        "reason": None,
    }
    if not asin:
        result["reason"] = "missing-asin"
        return result

    before = await _read_cart_summary(page, host=host, asin=asin)
    barriers = set(before.get("barriers", []))
    if {"signin", "captcha", "sorry-page"} & barriers:
        result["reason"] = "cart-unavailable"
        return result
    allow_delta = _cart_pricing_allow_delta_non_empty()
    if require_empty_cart and not before.get("is_empty") and not allow_delta:
        result["reason"] = "cart-not-empty"
        return result
    if before.get("target_in_cart"):
        result["reason"] = "target-already-in-cart"
        return result

    added = False
    after_summary: dict[str, Any] | None = None
    before_cart_asins = list(before.get("cart_asins") or [])
    before_cart_empty = bool(before.get("is_empty"))
    try:
        await page.goto(product_url, wait_until="domcontentloaded")
        await _accept_cookie_if_present(page)
        await page.wait_for_timeout(1100)
        html = await page.content()
        title = await page.title()
        product_barriers = set(_detect_page_barriers(html, title))
        if {"signin", "captcha", "sorry-page"} & product_barriers:
            result["reason"] = "product-page-blocked"
            return result
        add_ok = await _click_add_to_cart(page)
        if not add_ok:
            result["reason"] = "add-to-cart-unavailable"
            return result
        added = True
        result["added"] = True
        await page.wait_for_timeout(1700)

        after = await _read_cart_summary(page, host=host, asin=asin)
        after_summary = after
        if not after.get("target_in_cart"):
            result["reason"] = "not-found-in-cart-after-add"
            return result
        subtotal = after.get("subtotal_price")
        promo_discount = after.get("promo_discount_eur")
        total_price = after.get("total_price")
        row_price = after.get("target_row_price")
        result["subtotal_price"] = subtotal
        result["promo_discount_eur"] = promo_discount
        result["total_price"] = total_price
        before_total = before.get("total_price")
        before_subtotal = before.get("subtotal_price")
        before_promo = before.get("promo_discount_eur")
        delta_total = _positive_delta(total_price, before_total)
        delta_subtotal = _positive_delta(subtotal, before_subtotal)
        delta_promo = _positive_delta(promo_discount, before_promo)
        net_price: float | None = None
        if allow_delta and not before.get("is_empty"):
            if isinstance(delta_total, (int, float)) and delta_total > 0:
                net_price = float(delta_total)
                result["net_price_source"] = "delta_total"
            elif isinstance(delta_subtotal, (int, float)) and delta_subtotal > 0:
                if isinstance(delta_promo, (int, float)) and delta_subtotal > delta_promo:
                    net_price = float(delta_subtotal) - float(delta_promo)
                    result["net_price_source"] = "delta_subtotal_minus_promo_delta"
                else:
                    net_price = float(delta_subtotal)
                    result["net_price_source"] = "delta_subtotal"
        if net_price is None and isinstance(total_price, (int, float)) and total_price > 0 and before.get("is_empty"):
            net_price = float(total_price)
            result["net_price_source"] = "cart_total"
        elif (
            net_price is None
            and isinstance(subtotal, (int, float))
            and isinstance(promo_discount, (int, float))
            and subtotal > promo_discount
        ):
            net_price = float(subtotal) - float(promo_discount)
            result["net_price_source"] = "subtotal_minus_promo"
        elif (
            net_price is None
            and require_empty_cart
            and before.get("is_empty")
            and isinstance(subtotal, (int, float))
            and subtotal > 0
        ):
            net_price = float(subtotal)
            result["net_price_source"] = "subtotal_empty_cart"
        elif net_price is None and isinstance(row_price, (int, float)) and row_price > 0:
            net_price = float(row_price)
            result["net_price_source"] = "row_price"
        elif net_price is None and isinstance(subtotal, (int, float)) and subtotal > 0 and before.get("is_empty"):
            net_price = float(subtotal)
            result["net_price_source"] = "subtotal_fallback"
        if net_price is None:
            result["reason"] = "net-price-not-found"
            return result
        result["net_price_eur"] = round(net_price, 2)
        result["reason"] = "ok"
        return result
    finally:
        if added:
            cleanup_asins = _build_cart_cleanup_asins(
                before_cart_asins=before_cart_asins,
                after_cart_asins=list((after_summary or {}).get("cart_asins") or []),
                target_asin=asin,
            )
            cleanup = await _cleanup_cart_asins(page, host=host, asins=cleanup_asins)
            removed = bool(cleanup.get("removed"))
            result["cleanup_asins"] = cleanup_asins
            result["removed_asins"] = list(cleanup.get("removed_asins") or [])
            result["failed_remove_asins"] = list(cleanup.get("failed_asins") or [])
            result["removed"] = removed
            if before_cart_empty:
                try:
                    final_summary = await _read_cart_summary(page, host=host, asin=None)
                    cart_empty_after = bool(final_summary.get("is_empty"))
                    result["cart_empty_after_cleanup"] = cart_empty_after
                    if not cart_empty_after:
                        forced = await _force_empty_cart(page, host=host, max_rounds=4)
                        result["forced_cleanup"] = forced
                        cart_empty_after = bool(forced.get("empty"))
                        result["cart_empty_after_cleanup"] = cart_empty_after
                        if cart_empty_after:
                            forced_removed = list(forced.get("removed_asins") or [])
                            merged_removed = list(dict.fromkeys([*result["removed_asins"], *forced_removed]))
                            result["removed_asins"] = merged_removed
                            result["failed_remove_asins"] = [
                                item
                                for item in list(dict.fromkeys(result["failed_remove_asins"]))
                                if item not in merged_removed
                            ]
                            removed = True
                            result["removed"] = True
                        else:
                            removed = False
                            result["removed"] = False
                except Exception:
                    result["cart_empty_after_cleanup"] = None
            if not removed and result.get("reason") == "ok":
                result["net_price_eur"] = None
                result["reason"] = "remove-failed"


async def apply_cart_net_pricing(
    products: list[Any],
    *,
    headless: bool = True,
    nav_timeout_ms: int = 45000,
) -> dict[str, int]:
    if not _cart_pricing_enabled():
        return {"checked": 0, "updated": 0, "skipped": 0}
    if not products:
        return {"checked": 0, "updated": 0, "skipped": 0}

    use_storage_state = _use_storage_state()
    storage_state_paths = _load_storage_state_paths() if use_storage_state else {}
    if not storage_state_paths:
        print("[warehouse/cart] Pricing validator skipped: storage_state unavailable.")
        return {"checked": 0, "updated": 0, "skipped": len(products)}

    require_empty_cart = _cart_pricing_require_empty_cart()
    allow_delta_non_empty = _cart_pricing_allow_delta_non_empty()
    candidates: list[tuple[Any, str, str]] = []
    seen_urls: set[str] = set()
    for product in products:
        raw_url = str(getattr(product, "url", "") or "").strip()
        if not raw_url or raw_url in seen_urls:
            continue
        parsed = urlparse(raw_url if raw_url.startswith(("http://", "https://")) else f"https://{raw_url}")
        host = (parsed.netloc or "").lower()
        if host not in MARKETPLACE_FROM_HOST:
            continue
        seen_urls.add(raw_url)
        candidates.append((product, host, raw_url))

    if not candidates:
        return {"checked": 0, "updated": 0, "skipped": len(products)}

    max_items = _cart_pricing_max_items(len(candidates))
    candidates = candidates[:max_items]
    checked = 0
    updated = 0
    skipped = 0

    grouped: dict[str, list[tuple[Any, str]]] = {}
    for product, host, raw_url in candidates:
        grouped.setdefault(host, []).append((product, raw_url))

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        try:
            for host, rows in grouped.items():
                storage_state_path = _storage_state_for_host(storage_state_paths, host)
                if not storage_state_path:
                    skipped += len(rows)
                    print(f"[warehouse/cart] Skip host={host}: missing storage state.")
                    continue
                locale = {
                    "www.amazon.it": "it-IT",
                    "www.amazon.de": "de-DE",
                    "www.amazon.fr": "fr-FR",
                    "www.amazon.es": "es-ES",
                }.get(host, "it-IT")
                accept_language = ACCEPT_LANGUAGE_BY_HOST.get(host, "it-IT,it;q=0.9,en;q=0.8")
                context = None
                try:
                    context = await browser.new_context(
                        locale=locale,
                        storage_state=storage_state_path,
                        extra_http_headers={"Accept-Language": accept_language},
                    )
                    await _apply_stealth_context(context, host=host)
                    page = await context.new_page()
                    page.set_default_timeout(nav_timeout_ms)

                    for product, raw_url in rows:
                        checked += 1
                        old_price = float(getattr(product, "price_eur", 0.0))
                        try:
                            pricing = await _resolve_cart_net_price(
                                page,
                                host=host,
                                product_url=raw_url,
                                require_empty_cart=require_empty_cart,
                            )
                        except Exception as exc:
                            skipped += 1
                            print(
                                f"[warehouse/cart] Validation error host={host} url={raw_url}: "
                                f"{type(exc).__name__}: {exc}"
                            )
                            continue

                        new_price = pricing.get("net_price_eur")
                        if isinstance(new_price, (int, float)) and float(new_price) > 0:
                            setattr(product, "price_eur", round(float(new_price), 2))
                            updated += 1
                            print(
                                "[warehouse/cart] Net price resolved | "
                                f"host={host} asin={pricing.get('asin')} old={old_price:.2f} new={float(new_price):.2f} "
                                f"subtotal={_format_money(pricing.get('subtotal_price'))} "
                                f"promo_discount={_format_money(pricing.get('promo_discount_eur'))} "
                                f"total={_format_money(pricing.get('total_price'))} "
                                f"source={pricing.get('net_price_source') or 'n/d'} "
                                f"removed={pricing.get('removed')} "
                                f"cleanup_asins={pricing.get('cleanup_asins') or []} "
                                f"failed_remove_asins={pricing.get('failed_remove_asins') or []} "
                                f"cart_empty_after_cleanup={pricing.get('cart_empty_after_cleanup')}"
                            )
                        else:
                            skipped += 1
                            print(
                                "[warehouse/cart] Net price unavailable | "
                                f"host={host} asin={pricing.get('asin')} reason={pricing.get('reason')} "
                                f"subtotal={_format_money(pricing.get('subtotal_price'))} "
                                f"promo_discount={_format_money(pricing.get('promo_discount_eur'))} "
                                f"total={_format_money(pricing.get('total_price'))} "
                                f"source={pricing.get('net_price_source') or 'n/d'} "
                                f"removed={pricing.get('removed')} "
                                f"cleanup_asins={pricing.get('cleanup_asins') or []} "
                                f"failed_remove_asins={pricing.get('failed_remove_asins') or []} "
                                f"cart_empty_after_cleanup={pricing.get('cart_empty_after_cleanup')}"
                            )

                    if _cart_pricing_force_empty_after_host():
                        try:
                            final_cleanup = await _force_empty_cart(page, host=host, max_rounds=5)
                            print(
                                "[warehouse/cart] Host cleanup summary | "
                                f"host={host} empty={final_cleanup.get('empty')} "
                                f"removed_asins={final_cleanup.get('removed_asins') or []} "
                                f"failed_asins={final_cleanup.get('failed_asins') or []} "
                                f"remaining_asins={final_cleanup.get('remaining_asins') or []}"
                            )
                        except Exception as exc:
                            print(
                                "[warehouse/cart] Host cleanup error | "
                                f"host={host} error={type(exc).__name__}: {exc}"
                            )
                finally:
                    if context is not None:
                        await context.close()
        finally:
            await browser.close()

    print(
        "[warehouse/cart] Pricing validator summary | "
        f"checked={checked} updated={updated} skipped={skipped} "
        f"require_empty_cart={require_empty_cart} allow_delta_non_empty={allow_delta_non_empty}"
    )
    return {"checked": checked, "updated": updated, "skipped": skipped}


async def fetch_amazon_warehouse_products(
    *,
    headless: bool = True,
    nav_timeout_ms: int = 45000,
    max_products: int | None = None,
    search_queries: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not _is_enabled():
        return []

    configured_marketplaces = _split_csv(os.getenv("AMAZON_WAREHOUSE_MARKETPLACES")) or list(DEFAULT_MARKETPLACES)
    marketplaces = _expand_marketplaces(configured_marketplaces)
    supported_marketplaces = [item for item in marketplaces if MARKETPLACE_HOSTS.get(item.lower())]
    if not supported_marketplaces:
        print("[warehouse] No supported marketplaces configured.")
        return []
    configured_queries = search_queries or _split_csv(os.getenv("AMAZON_WAREHOUSE_QUERIES")) or list(DEFAULT_QUERIES)
    queries = [query.strip() for query in configured_queries if query and query.strip()]
    queries = _dedupe_keep_order(queries)
    if not queries:
        queries = list(DEFAULT_QUERIES)
    max_products = max(1, int(max_products or _env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "8")))
    max_price = _max_price_eur()
    proxy_pool = _load_proxy_pool()
    user_agents = _load_user_agents()
    max_attempts = _max_attempts_per_query()
    retry_delay_ms = _retry_delay_ms()
    fail_fast_on_sorry = _fail_fast_on_sorry()
    stealth = _stealth_enabled()
    use_storage_state = _use_storage_state()
    storage_state_paths = _load_storage_state_paths() if use_storage_state else {}
    if use_storage_state and not storage_state_paths:
        print("[warehouse] Storage state enabled but unavailable (missing/invalid). Proceeding without it.")
    elif storage_state_paths:
        active_slots = sorted(storage_state_paths.keys())
        print(
            "[warehouse] Storage state loaded for Amazon authenticated session. "
            f"slots={','.join(active_slots)}"
        )
    rotate_proxy = _is_truthy_env("AMAZON_WAREHOUSE_ROTATE_PROXY", "true")
    rotate_user_agent = _is_truthy_env("AMAZON_WAREHOUSE_ROTATE_USER_AGENT", "true")
    per_query_limit = _per_query_limit(max_products, len(queries))
    per_marketplace_limit = _per_marketplace_limit(max_products, len(supported_marketplaces))
    query_totals: dict[str, int] = {query: 0 for query in queries}
    marketplace_totals: dict[str, int] = {marketplace.lower(): 0 for marketplace in supported_marketplaces}

    results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    proxy_index = 0
    user_agent_index = 0
    session_id = 0

    print(
        "[warehouse] Rotation config: "
        f"proxy_pool={len(proxy_pool)} rotate_proxy={rotate_proxy} "
        f"user_agents={len(user_agents)} rotate_user_agent={rotate_user_agent} "
        f"attempts_per_query={max_attempts} fail_fast_on_sorry={fail_fast_on_sorry} "
        f"stealth={stealth} storage_state={'on' if storage_state_paths else 'off'} "
        f"max_products={max_products} per_query_limit={per_query_limit} "
        f"marketplace_limit={per_marketplace_limit} marketplaces={','.join(supported_marketplaces)}"
    )

    async with async_playwright() as playwright:
        browsers: dict[str, Any] = {}
        try:
            for phase in ("balanced", "topup"):
                if len(results) >= max_products:
                    break
                enforce_marketplace_cap = phase == "balanced" and len(supported_marketplaces) > 1
                if phase == "topup" and len(results) < max_products:
                    print(
                        "[warehouse] Top-up phase enabled | "
                        f"current={len(results)} target={max_products}"
                    )
                for marketplace in supported_marketplaces:
                    host = MARKETPLACE_HOSTS.get(marketplace.lower())
                    if not host:
                        continue
                    if enforce_marketplace_cap and marketplace_totals.get(marketplace.lower(), 0) >= per_marketplace_limit:
                        continue

                    for query in queries:
                        if len(results) >= max_products:
                            break
                        if enforce_marketplace_cap and marketplace_totals.get(marketplace.lower(), 0) >= per_marketplace_limit:
                            break
                        search_url = _build_search_url(host, query)
                        parsed: list[dict[str, Any]] = []
                        for attempt in range(1, max_attempts + 1):
                            proxy_config, proxy_index = _choose_from_pool(proxy_pool, proxy_index, rotate_proxy)
                            user_agent, user_agent_index = _choose_from_pool(
                                user_agents,
                                user_agent_index,
                                rotate_user_agent,
                            )
                            proxy_label = _proxy_label(proxy_config)
                            session_id += 1

                            browser_key = _proxy_key(proxy_config)
                            browser = browsers.get(browser_key)
                            if browser is None:
                                launch_kwargs: dict[str, Any] = {"headless": headless}
                                if proxy_config:
                                    launch_kwargs["proxy"] = proxy_config
                                browser = await playwright.chromium.launch(**launch_kwargs)
                                browsers[browser_key] = browser
                                print(f"[warehouse] Opened browser session for proxy={proxy_label}")

                            context_kwargs: dict[str, Any] = {
                                "locale": "it-IT",
                                "viewport": _viewport_for_session(session_id),
                                "extra_http_headers": {
                                    "Accept-Language": ACCEPT_LANGUAGE_BY_HOST.get(host, "it-IT,it;q=0.9,en;q=0.8")
                                },
                            }
                            host_storage_state_path = _storage_state_for_host(storage_state_paths, host)
                            if host_storage_state_path:
                                context_kwargs["storage_state"] = host_storage_state_path
                            if user_agent:
                                context_kwargs["user_agent"] = user_agent

                            context = await browser.new_context(**context_kwargs)
                            await _apply_stealth_context(context, host=host)
                            page = await context.new_page()
                            page.set_default_timeout(nav_timeout_ms)
                            storage_state_mode = "none"
                            if host_storage_state_path:
                                marketplace_for_host = MARKETPLACE_FROM_HOST.get(host.lower())
                                if marketplace_for_host and marketplace_for_host in storage_state_paths:
                                    storage_state_mode = marketplace_for_host
                                else:
                                    storage_state_mode = "default"
                            print(
                                f"[warehouse] Loading {search_url} | attempt={attempt}/{max_attempts} "
                                f"| session={session_id} | ua='{_shorten(user_agent)}' | proxy={proxy_label} "
                                f"| storage={storage_state_mode}"
                            )
                            barriers: list[str] = []
                            try:
                                await page.goto(search_url, wait_until="domcontentloaded")
                                await _accept_cookie_if_present(page)
                                await page.wait_for_timeout(1200)
                                html = await page.content()
                                parsed = _extract_products_from_html(html, host)
                                print(
                                    f"[warehouse] Parsed offers for {host} query='{query}' "
                                    f"attempt={attempt}: {len(parsed)}"
                                )
                                if not parsed:
                                    row_count = len(_collect_search_rows(BeautifulSoup(html, "html.parser")))
                                    barriers = await _log_empty_parse_diagnostics(
                                        page,
                                        host=host,
                                        query=query,
                                        html=html,
                                        row_count=row_count,
                                        session_id=session_id,
                                        user_agent=user_agent,
                                        proxy_label=proxy_label,
                                    )
                            except Exception as exc:
                                parsed = []
                                print(
                                    f"[warehouse] Search error on {host} query='{query}' "
                                    f"attempt={attempt} session={session_id} proxy={proxy_label}: "
                                    f"{type(exc).__name__}: {exc}"
                                )
                            finally:
                                await context.close()

                            if parsed:
                                break

                            if _should_fail_fast(
                                barriers,
                                proxy_pool_size=len(proxy_pool),
                                fail_fast=fail_fast_on_sorry,
                            ):
                                reason = ",".join(barriers) if barriers else "blocker"
                                print(
                                    f"[warehouse] Fail-fast on {host} query='{query}' after {reason} "
                                    "because proxy_pool=0."
                                )
                                break

                            if attempt < max_attempts:
                                reason = ",".join(barriers) if barriers else "empty-parse"
                                delay_ms = _retry_delay_for_attempt(retry_delay_ms, attempt)
                                print(
                                    f"[warehouse] Retrying {host} query='{query}' in {delay_ms}ms "
                                    f"after {reason}."
                                )
                                if delay_ms:
                                    await asyncio.sleep(delay_ms / 1000)

                        for item in parsed:
                            item_url = str(item.get("url") or "")
                            if item_url in seen_urls:
                                continue
                            if query_totals.get(query, 0) >= per_query_limit:
                                continue
                            if enforce_marketplace_cap and marketplace_totals.get(marketplace.lower(), 0) >= per_marketplace_limit:
                                break
                            price = item.get("price_eur")
                            if max_price is not None and isinstance(price, (float, int)) and float(price) > max_price:
                                continue

                            seen_urls.add(item_url)
                            item["source_marketplace"] = marketplace.lower()
                            results.append(item)
                            query_totals[query] = query_totals.get(query, 0) + 1
                            marketplace_totals[marketplace.lower()] = marketplace_totals.get(marketplace.lower(), 0) + 1
                            if len(results) >= max_products:
                                break
        finally:
            for browser in list(browsers.values()):
                try:
                    await browser.close()
                except Exception:
                    continue
            for path in set(storage_state_paths.values()):
                _remove_file_if_exists(path)

    query_stats = ", ".join(f"'{query}':{query_totals.get(query, 0)}" for query in queries)
    marketplace_stats = ", ".join(
        f"'{marketplace}':{marketplace_totals.get(marketplace.lower(), 0)}" for marketplace in supported_marketplaces
    )
    print(
        f"[warehouse] Collected products: {len(results)} | "
        f"per-query stats: {query_stats} | per-marketplace stats: {marketplace_stats}"
    )
    return results


__all__ = [
    "fetch_amazon_warehouse_products",
    "apply_cart_net_pricing",
    "_canonical_amazon_url",
    "_extract_products_from_html",
    "_per_marketplace_limit",
]
