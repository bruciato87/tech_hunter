from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator


class MPBValuator(BaseValuator):
    platform_name = "mpb"
    condition_label = "ottimo"
    base_url = "https://www.mpb.com/it-it/sell"

    async def _fetch_offer(
        self,
        product: AmazonProduct,
        normalized_name: str,
    ) -> tuple[float | None, str | None, dict[str, Any]]:
        payload: dict[str, Any] = {
            "query": normalized_name,
            "condition_target": "Ottimo",
        }

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(locale="it-IT")
            page = await context.new_page()
            page.set_default_timeout(self.nav_timeout_ms)
            try:
                await page.goto(self.base_url, wait_until="domcontentloaded")
                await self._accept_cookie_if_present(page)

                filled = await self._fill_first(
                    page,
                    [
                        "input[type='search']",
                        "input[name*='search' i]",
                        "input[placeholder*='Search' i]",
                        "input[placeholder*='Cerca' i]",
                    ],
                    value=normalized_name,
                    timeout_ms=12000,
                )
                if not filled:
                    raise RuntimeError("MPB search input not found")

                await page.keyboard.press("Enter")
                await page.wait_for_timeout(2200)

                await self._click_first(
                    page,
                    [
                        "[data-testid*='search-result'] a",
                        "a[href*='/sell/']",
                        ".search-results a",
                        "li a",
                    ],
                    timeout_ms=10000,
                )
                await page.wait_for_load_state("domcontentloaded")

                condition_selected = await self._click_first(
                    page,
                    [
                        "button:has-text('Ottimo')",
                        "label:has-text('Ottimo')",
                        "[role='button']:has-text('Excellent')",
                    ],
                    timeout_ms=5000,
                )
                payload["condition_selected"] = condition_selected
                await page.wait_for_timeout(1400)

                price, price_text = await self._extract_price(page)
                payload["price_text"] = price_text
                return price, page.url, payload
            finally:
                await context.close()
                await browser.close()

    async def _extract_price(self, page) -> tuple[float | None, str]:
        selector_candidates = [
            "[data-testid*='price' i]",
            "[class*='price' i]",
            "text=/\\d+[\\.,]?\\d*\\s?€/",
            "text=/€\\s?\\d+[\\.,]?\\d*/",
        ]
        for selector in selector_candidates:
            try:
                locator = page.locator(selector).first
                if await locator.count():
                    text = await locator.inner_text(timeout=1500)
                    value = parse_eur_price(text)
                    if value is not None:
                        return value, text.strip()
            except PlaywrightError:
                continue

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        return parse_eur_price(text), text[:220]

