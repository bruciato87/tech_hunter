from __future__ import annotations

import base64
import json
import os
import re
import tempfile
from typing import Any
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator


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
)
SEARCH_SELECTORS: tuple[str, ...] = (
    "input[data-testid='typeahead__input']",
    "input[type='search']",
    "input[name*='search' i]",
    "[role='searchbox']",
)
SUGGESTION_SELECTORS: tuple[str, ...] = (
    "[data-testid*='typeahead' i] [role='option']",
    "[role='option']",
    "[data-testid*='result' i] a",
    "a[href*='/sell/']",
)
DIRECT_SELL_SELECTORS: tuple[str, ...] = (
    "a[href*='/it-it/sell/']",
    "a[href*='/sell/']",
    "a:has-text('Vendi')",
    "button:has-text('Vendi')",
    "a:has-text('Sell')",
    "button:has-text('Sell')",
)
CONDITION_SELECTORS: tuple[str, ...] = (
    "button:has-text('Ottimo')",
    "label:has-text('Ottimo')",
    "[role='button']:has-text('Ottimo')",
    "[role='button']:has-text('Excellent')",
)
PRICE_HINTS: tuple[str, ...] = (
    "offerta",
    "valutazione",
    "ricevi",
    "ti paghiamo",
    "stima",
    "sell",
    "trade-in",
    "we pay",
)
BLOCKER_HINTS: tuple[str, ...] = (
    "ci siamo quasi",
    "turnstile",
    "cloudflare",
    "just a moment",
    "challenge-platform",
    "enable javascript and cookies to continue",
    "verify you are human",
)


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _load_storage_state_b64() -> str | None:
    use_storage_state = _env_or_default("MPB_USE_STORAGE_STATE", "true").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if not use_storage_state:
        return None
    raw = (os.getenv("MPB_STORAGE_STATE_B64") or "").strip()
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        parsed = json.loads(decoded)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    try:
        json.dump(parsed, handle, ensure_ascii=False)
        handle.flush()
        return handle.name
    finally:
        handle.close()


def _remove_file_if_exists(path: str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except Exception:
        return


def _detect_blockers(*chunks: str) -> list[str]:
    lowered = "\n".join(chunks).lower()
    markers: list[str] = []
    for hint in BLOCKER_HINTS:
        if hint in lowered:
            markers.append(hint)
    return markers


def _extract_contextual_price(text: str) -> tuple[float | None, str]:
    if not text:
        return None, ""
    candidates: list[tuple[int, float, str]] = []
    for match in re.finditer(r"\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?\s*€", text):
        snippet = text[max(0, match.start() - 80) : min(len(text), match.end() + 80)]
        normalized = re.sub(r"\s+", " ", snippet).strip().lower()
        value = parse_eur_price(match.group(0))
        if value is None or value <= 0 or value > 15000:
            continue
        score = 0
        for hint in PRICE_HINTS:
            if hint in normalized:
                score += 7
        if "spedizione" in normalized or "prezzo di vendita" in normalized:
            score -= 6
        candidates.append((score, value, snippet.strip()))
    if not candidates:
        return None, ""
    score, value, snippet = max(candidates, key=lambda item: (item[0], item[1]))
    if score <= 0:
        return None, ""
    return value, snippet


def _contains_price_hint(text: str) -> bool:
    lowered = re.sub(r"\s+", " ", text).strip().lower()
    if not lowered:
        return False
    return any(hint in lowered for hint in PRICE_HINTS)


async def _apply_stealth_context(context) -> None:  # noqa: ANN001
    script = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'languages', { get: () => ['it-IT', 'it', 'en-US', 'en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    window.chrome = window.chrome || { runtime: {} };
    """
    try:
        await context.add_init_script(script)
    except Exception:
        return


class MPBValuator(BaseValuator):
    platform_name = "mpb"
    condition_label = "ottimo"
    base_url = "https://www.mpb.com/it-it/sell"

    async def _fetch_offer(
        self,
        product: AmazonProduct,
        normalized_name: str,
    ) -> tuple[float | None, str | None, dict[str, Any]]:
        max_attempts = max(1, int(_env_or_default("MPB_MAX_ATTEMPTS", "3")))
        storage_state_path = _load_storage_state_b64()
        payload: dict[str, Any] = {
            "query": normalized_name,
            "condition_target": "Ottimo",
            "attempts": [],
            "adaptive_fallbacks": {},
            "storage_state": bool(storage_state_path),
        }
        blocker_hits: list[str] = []
        search_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="search_input",
            defaults=list(SEARCH_SELECTORS),
            payload=payload,
        )
        suggestion_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="result_open",
            defaults=list(SUGGESTION_SELECTORS),
            payload=payload,
        )
        direct_sell_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="direct_sell_open",
            defaults=list(DIRECT_SELL_SELECTORS),
            payload=payload,
        )
        condition_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="condition",
            defaults=list(CONDITION_SELECTORS),
            payload=payload,
        )

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            try:
                for attempt in range(1, max_attempts + 1):
                    user_agent = DEFAULT_USER_AGENTS[(attempt - 1) % len(DEFAULT_USER_AGENTS)]
                    context_kwargs: dict[str, Any] = {
                        "locale": "it-IT",
                        "user_agent": user_agent,
                        "extra_http_headers": {
                            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
                        },
                    }
                    if storage_state_path:
                        context_kwargs["storage_state"] = storage_state_path
                    context = await browser.new_context(**context_kwargs)
                    await _apply_stealth_context(context)
                    page = await context.new_page()
                    page.set_default_timeout(self.nav_timeout_ms)
                    try:
                        await page.goto(self.base_url, wait_until="domcontentloaded")
                        await self._accept_cookie_if_present(page)
                        blockers = await self._detect_page_blockers(page)
                        if blockers:
                            blocker_hits.extend(blockers)
                            payload["attempts"].append(
                                {
                                    "attempt": attempt,
                                    "stage": "base_load",
                                    "status": "blocked",
                                    "url": page.url,
                                    "blockers": blockers,
                                }
                            )
                            fallback = await self._direct_search_fallback(
                                page=page,
                                attempt=attempt,
                                normalized_name=normalized_name,
                                condition_selectors=condition_selectors,
                                direct_sell_selectors=direct_sell_selectors,
                                payload=payload,
                            )
                            if fallback["offer"] is not None:
                                return fallback["offer"], fallback["url"], payload
                            blocker_hits.extend(fallback["blockers"])
                            continue

                        search_selector = await self._wait_for_search_input(page, selectors=search_selectors, timeout_ms=10000)
                        semantic_search = False
                        if search_selector:
                            await page.locator(search_selector).first.fill(normalized_name)
                        else:
                            semantic_search = await self._fill_first_semantic(
                                page,
                                value=normalized_name,
                                keywords=["search", "cerca", "modello", "brand", "prodotto"],
                                timeout_ms=3500,
                            )
                            payload["adaptive_fallbacks"]["search_semantic"] = semantic_search

                        if not search_selector and not semantic_search:
                            probe = await self._attach_ui_probe(
                                payload=payload,
                                page=page,
                                site=self.platform_name,
                                stage="search_input_missing",
                                expected_keywords=["mpb", "sell", "search", "camera"],
                            )
                            blockers = await self._detect_page_blockers(page)
                            blockers.extend(_detect_blockers(json.dumps(probe, ensure_ascii=False)))
                            blocker_hits.extend(blockers)
                            payload["attempts"].append(
                                {
                                    "attempt": attempt,
                                    "stage": "search_input",
                                    "status": "missing",
                                    "title": await page.title(),
                                    "blockers": blockers,
                                    "ui_drift": probe.get("drift_suspected"),
                                }
                            )
                            fallback = await self._direct_search_fallback(
                                page=page,
                                attempt=attempt,
                                normalized_name=normalized_name,
                                condition_selectors=condition_selectors,
                                direct_sell_selectors=direct_sell_selectors,
                                payload=payload,
                            )
                            if fallback["offer"] is not None:
                                return fallback["offer"], fallback["url"], payload
                            blocker_hits.extend(fallback["blockers"])
                            continue

                        await page.wait_for_timeout(1000)

                        clicked_result = await self._click_first(
                            page,
                            suggestion_selectors,
                            timeout_ms=2500,
                        )
                        if not clicked_result:
                            name_tokens = [token for token in re.split(r"\W+", normalized_name) if len(token) >= 3][:4]
                            clicked_result = await self._click_first_semantic(
                                page,
                                keywords=[*name_tokens, "sell", "camera", "lens"],
                                timeout_ms=2500,
                                selectors=["a", "button", "[role='option']", "li", "div[role='option']"],
                            )
                            payload["adaptive_fallbacks"]["result_semantic"] = clicked_result
                        else:
                            payload["adaptive_fallbacks"]["result_semantic"] = False

                        if not clicked_result:
                            await page.keyboard.press("Enter")
                        await page.wait_for_timeout(2400)
                        await page.wait_for_load_state("domcontentloaded")

                        condition_selected = await self._click_first(
                            page,
                            condition_selectors,
                            timeout_ms=5000,
                        )
                        if not condition_selected:
                            condition_selected = await self._click_first_semantic(
                                page,
                                keywords=["ottimo", "excellent", "grade a", "come nuovo"],
                                timeout_ms=2400,
                            )
                            payload["adaptive_fallbacks"]["condition_semantic"] = condition_selected
                        else:
                            payload["adaptive_fallbacks"]["condition_semantic"] = False
                        await page.wait_for_timeout(1400)

                        price, price_text = await self._extract_price(page, payload=payload)
                        ui_probe = None
                        if price is None:
                            ui_probe = await self._attach_ui_probe(
                                payload=payload,
                                page=page,
                                site=self.platform_name,
                                stage="price_missing",
                                expected_keywords=["mpb", "offer", "estimate", "€"],
                            )
                            fallback = await self._direct_search_fallback(
                                page=page,
                                attempt=attempt,
                                normalized_name=normalized_name,
                                condition_selectors=condition_selectors,
                                direct_sell_selectors=direct_sell_selectors,
                                payload=payload,
                            )
                            if fallback["offer"] is not None:
                                return fallback["offer"], fallback["url"], payload
                            blocker_hits.extend(fallback["blockers"])
                        payload["attempts"].append(
                            {
                                "attempt": attempt,
                                "stage": "valuation",
                                "status": "ok" if price is not None else "price-missing",
                                "condition_selected": condition_selected,
                                "semantic_search": semantic_search,
                                "result_opened": clicked_result,
                                "url": page.url,
                                "price_text": price_text,
                                "ui_drift": ui_probe.get("drift_suspected") if ui_probe else False,
                            }
                        )
                        if price is not None:
                            payload["price_text"] = price_text
                            payload["condition_selected"] = condition_selected
                            payload["price_source"] = "sell_flow"
                            return price, page.url, payload
                    finally:
                        await context.close()
            finally:
                await browser.close()
                _remove_file_if_exists(storage_state_path)

        if blocker_hits:
            raise RuntimeError("MPB blocked by anti-bot challenge (turnstile/cloudflare).")
        raise RuntimeError("MPB price not found after retries.")

    async def _detect_page_blockers(self, page: Page) -> list[str]:
        try:
            title = await page.title()
        except Exception:
            title = ""
        try:
            html = await page.content()
        except Exception:
            html = ""
        return _detect_blockers(title, html)

    async def _direct_search_fallback(
        self,
        *,
        page: Page,
        attempt: int,
        normalized_name: str,
        condition_selectors: list[str],
        direct_sell_selectors: list[str],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        search_url = f"https://www.mpb.com/it-it/cerca?q={quote_plus(normalized_name)}"
        payload["adaptive_fallbacks"]["direct_search"] = True
        try:
            await page.goto(search_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(900)
            await self._accept_cookie_if_present(page)
        except PlaywrightError:
            return {"offer": None, "url": None, "blockers": []}

        blockers = await self._detect_page_blockers(page)
        if blockers:
            payload["attempts"].append(
                {
                    "attempt": attempt,
                    "stage": "direct_search",
                    "status": "blocked",
                    "url": page.url,
                    "blockers": blockers,
                }
            )
            return {"offer": None, "url": None, "blockers": blockers}

        opened_sell = await self._click_first(page, direct_sell_selectors, timeout_ms=2500)
        if not opened_sell:
            name_tokens = [token for token in re.split(r"\W+", normalized_name) if len(token) >= 3][:4]
            opened_sell = await self._click_first_semantic(
                page,
                keywords=[*name_tokens, "vendi", "sell", "trade"],
                timeout_ms=2200,
                selectors=["a", "button", "[role='link']", "[role='button']"],
            )
            payload["adaptive_fallbacks"]["direct_sell_semantic"] = opened_sell
        else:
            payload["adaptive_fallbacks"]["direct_sell_semantic"] = False

        if opened_sell:
            await page.wait_for_timeout(1600)
            await page.wait_for_load_state("domcontentloaded")

        condition_selected = await self._click_first(
            page,
            condition_selectors,
            timeout_ms=3200,
        )
        if not condition_selected:
            condition_selected = await self._click_first_semantic(
                page,
                keywords=["ottimo", "excellent", "grade a", "come nuovo"],
                timeout_ms=1800,
            )
            payload["adaptive_fallbacks"]["condition_semantic_direct"] = condition_selected
        else:
            payload["adaptive_fallbacks"]["condition_semantic_direct"] = False

        await page.wait_for_timeout(1100)
        price, price_text = await self._extract_price(page, payload=payload)
        payload["attempts"].append(
            {
                "attempt": attempt,
                "stage": "direct_search",
                "status": "ok" if price is not None else "price-missing",
                "url": page.url,
                "opened_sell": opened_sell,
                "condition_selected": condition_selected,
                "price_text": price_text,
            }
        )
        if price is not None:
            payload["price_text"] = price_text
            payload["condition_selected"] = condition_selected
            payload["price_source"] = "direct_search"
            return {"offer": price, "url": page.url, "blockers": []}
        return {"offer": None, "url": None, "blockers": []}

    async def _wait_for_search_input(self, page: Page, selectors: list[str], timeout_ms: int = 10000) -> str | None:
        elapsed = 0
        interval_ms = 350
        while elapsed <= timeout_ms:
            for selector in selectors:
                locator = page.locator(selector).first
                try:
                    if not await locator.count():
                        continue
                    await locator.wait_for(state="visible", timeout=900)
                    return selector
                except PlaywrightError:
                    continue
            await page.wait_for_timeout(interval_ms)
            elapsed += interval_ms
        return None

    async def _extract_price(self, page: Page, *, payload: dict[str, Any] | None = None) -> tuple[float | None, str]:
        allow_unscoped_selector_price = "/sell" in (page.url or "").lower()
        selector_candidates = self._selector_candidates(
            site=self.platform_name,
            slot="price",
            defaults=[
                "[data-testid*='price' i]",
                "[class*='price' i]",
                "[class*='offer' i]",
                "[class*='valuation' i]",
                "text=/\\d+[\\.,]?\\d*\\s?€/",
                "text=/€\\s?\\d+[\\.,]?\\d*/",
            ],
            payload=payload,
        )
        for selector in selector_candidates:
            try:
                locator = page.locator(selector)
                count = min(await locator.count(), 10)
                for index in range(count):
                    text = await locator.nth(index).inner_text(timeout=1200)
                    price, snippet = _extract_contextual_price(text)
                    if price is not None:
                        return price, snippet
                    value = parse_eur_price(text)
                    if value is not None and (allow_unscoped_selector_price or _contains_price_hint(text)):
                        return value, text.strip()
            except PlaywrightError:
                continue

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select("main, [class*='price' i], [class*='offer' i], [class*='valuation' i]"):
            text = node.get_text(" ", strip=True)
            price, snippet = _extract_contextual_price(text)
            if price is not None:
                return price, snippet
        text = soup.get_text(" ", strip=True)
        price, snippet = _extract_contextual_price(text)
        if price is not None:
            return price, snippet
        return None, text[:220]
