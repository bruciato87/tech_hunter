from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import tempfile
from typing import Any
from urllib.parse import quote_plus, unquote, urljoin, urlparse

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
    for node in row.select(".a-price .a-offscreen"):
        price = parse_eur_price(node.get_text(" ", strip=True))
        if price is not None:
            return price
    return parse_eur_price(row.get_text(" ", strip=True))


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

        price = _extract_price_from_row(row)
        if price is None:
            continue

        url = _extract_link_from_row(host, row)
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
    "_canonical_amazon_url",
    "_extract_products_from_html",
    "_per_marketplace_limit",
]
