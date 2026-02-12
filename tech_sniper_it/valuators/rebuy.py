from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator

PRICE_HINTS: tuple[str, ...] = ("ti paghiamo", "valutazione", "offerta", "ricevi", "vendi")
PRICE_BLOCKERS: tuple[str, ...] = ("ordine min", "spedizione", "cookie", "prezzo di vendita")


def _extract_contextual_price(text: str) -> tuple[float | None, str]:
    if not text:
        return None, ""
    candidates: list[tuple[int, float, str]] = []
    for match in re.finditer(r"\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?\s*€", text):
        snippet = text[max(0, match.start() - 90) : min(len(text), match.end() + 90)]
        normalized = re.sub(r"\s+", " ", snippet).strip().lower()
        value = parse_eur_price(match.group(0))
        if value is None or value <= 0 or value > 10000:
            continue
        score = 0
        for hint in PRICE_HINTS:
            if hint in normalized:
                score += 7
        for blocker in PRICE_BLOCKERS:
            if blocker in normalized:
                score -= 8
        if value >= 100:
            score += 2
        candidates.append((score, value, snippet.strip()))
    if not candidates:
        return None, ""
    score, value, snippet = max(candidates, key=lambda item: (item[0], item[1]))
    if score <= 0:
        return None, ""
    return value, snippet


class RebuyValuator(BaseValuator):
    platform_name = "rebuy"
    condition_label = "come_nuovo"
    base_url = "https://www.rebuy.it/vendi"

    async def _fetch_offer(
        self,
        product: AmazonProduct,
        normalized_name: str,
    ) -> tuple[float | None, str | None, dict[str, Any]]:
        query = product.ean or normalized_name
        payload: dict[str, Any] = {
            "query": query,
            "condition_target": "Come nuovo",
            "adaptive_fallbacks": {},
        }

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(locale="it-IT")
            page = await context.new_page()
            page.set_default_timeout(self.nav_timeout_ms)
            try:
                await page.goto(self.base_url, wait_until="domcontentloaded")
                await self._accept_cookie_if_present(page)

                search_selectors = self._selector_candidates(
                    site=self.platform_name,
                    slot="search_input",
                    defaults=[
                        "input[type='search']",
                        "input[name*='search' i]",
                        "input[placeholder*='Cerca' i]",
                        "[data-testid*='search'] input",
                    ],
                    payload=payload,
                )
                filled = await self._fill_first(
                    page,
                    search_selectors,
                    value=query,
                    timeout_ms=10000,
                )
                if not filled:
                    filled = await self._fill_first_semantic(
                        page,
                        value=query,
                        keywords=["cerca", "search", "ean", "modello", "prodotto"],
                        timeout_ms=4500,
                    )
                    payload["adaptive_fallbacks"]["search_semantic"] = filled
                else:
                    payload["adaptive_fallbacks"]["search_semantic"] = False

                if not filled:
                    probe = await self._attach_ui_probe(
                        payload=payload,
                        page=page,
                        site=self.platform_name,
                        stage="search_input_missing",
                        expected_keywords=["rebuy", "vendi", "cerca", "usato"],
                    )
                    raise RuntimeError(f"Rebuy search input not found (ui_drift={probe.get('drift_suspected')})")

                await page.keyboard.press("Enter")
                await page.wait_for_timeout(2200)

                result_selectors = self._selector_candidates(
                    site=self.platform_name,
                    slot="result_open",
                    defaults=[
                        "[data-testid*='product-card'] a",
                        "a[href*='/vendi/']",
                        "a[href*='/offer']",
                        ".product-card a",
                        "li a:has-text('GB')",
                    ],
                    payload=payload,
                )
                opened = await self._click_first(page, result_selectors, timeout_ms=10000)
                if not opened:
                    name_tokens = [token for token in re.split(r"\W+", normalized_name) if len(token) >= 3][:4]
                    opened = await self._click_first_semantic(
                        page,
                        keywords=[*name_tokens, "gb", "vendi"],
                        timeout_ms=3500,
                    )
                    payload["adaptive_fallbacks"]["result_semantic"] = opened
                else:
                    payload["adaptive_fallbacks"]["result_semantic"] = False
                payload["result_opened"] = opened
                await page.wait_for_load_state("domcontentloaded")

                condition_selectors = self._selector_candidates(
                    site=self.platform_name,
                    slot="condition",
                    defaults=[
                        "button:has-text('Come nuovo')",
                        "label:has-text('Come nuovo')",
                        "[role='button']:has-text('Come nuovo')",
                        "[data-testid*='condition'] button:has-text('A')",
                    ],
                    payload=payload,
                )
                condition_selected = await self._click_first(page, condition_selectors, timeout_ms=5000)
                if not condition_selected:
                    condition_selected = await self._click_first_semantic(
                        page,
                        keywords=["come nuovo", "grade a", "ottimo", "excellent"],
                        timeout_ms=2600,
                    )
                    payload["adaptive_fallbacks"]["condition_semantic"] = condition_selected
                else:
                    payload["adaptive_fallbacks"]["condition_semantic"] = False
                payload["condition_selected"] = condition_selected
                await page.wait_for_timeout(1600)

                price, price_text = await self._extract_price(page, payload=payload)
                payload["price_text"] = price_text
                if price is None:
                    await self._attach_ui_probe(
                        payload=payload,
                        page=page,
                        site=self.platform_name,
                        stage="price_missing",
                        expected_keywords=["rebuy", "vendi", "offerta", "€"],
                    )
                    raise RuntimeError("Rebuy price not found after adaptive fallbacks.")
                return price, page.url, payload
            finally:
                await context.close()
                await browser.close()

    async def _extract_price(self, page: Page, *, payload: dict[str, Any] | None = None) -> tuple[float | None, str]:
        selector_candidates = self._selector_candidates(
            site=self.platform_name,
            slot="price",
            defaults=[
                "[data-testid*='price' i]",
                "[class*='price' i]",
                "[class*='offer' i]",
                "text=/Ti paghiamo\\s*\\d+[\\.,]?\\d*\\s?€/i",
                "text=/\\d+[\\.,]?\\d*\\s?€/",
                "text=/€\\s?\\d+[\\.,]?\\d*/",
            ],
            payload=payload,
        )
        for selector in selector_candidates:
            try:
                locator = page.locator(selector)
                count = min(await locator.count(), 8)
                for index in range(count):
                    text = await locator.nth(index).inner_text(timeout=1500)
                    value, snippet = _extract_contextual_price(text)
                    if value is not None:
                        return value, snippet
                    value = parse_eur_price(text)
                    if value is not None:
                        return value, text.strip()
            except PlaywrightError:
                continue

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select("main, [class*='price' i], [class*='offer' i], [class*='valut' i]"):
            text = node.get_text(" ", strip=True)
            value, snippet = _extract_contextual_price(text)
            if value is not None:
                return value, snippet
        text = soup.get_text(" ", strip=True)
        value, snippet = _extract_contextual_price(text)
        if value is not None:
            return value, snippet
        return parse_eur_price(text), text[:220]


__all__ = ["RebuyValuator", "_extract_contextual_price"]

