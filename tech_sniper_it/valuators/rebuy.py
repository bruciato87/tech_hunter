from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator


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
                        "input[placeholder*='Cerca' i]",
                        "[data-testid*='search'] input",
                    ],
                    value=query,
                    timeout_ms=10000,
                )
                if not filled:
                    raise RuntimeError("Rebuy search input not found")

                await page.keyboard.press("Enter")
                await page.wait_for_timeout(2200)

                opened = await self._click_first(
                    page,
                    [
                        "[data-testid*='product-card'] a",
                        "a[href*='/vendi/']",
                        "a[href*='/offer']",
                        ".product-card a",
                        "li a:has-text('GB')",
                    ],
                    timeout_ms=10000,
                )
                payload["result_opened"] = opened
                await page.wait_for_load_state("domcontentloaded")

                condition_selected = await self._click_first(
                    page,
                    [
                        "button:has-text('Come nuovo')",
                        "label:has-text('Come nuovo')",
                        "[role='button']:has-text('Come nuovo')",
                        "[data-testid*='condition'] button:has-text('A')",
                    ],
                    timeout_ms=5000,
                )
                payload["condition_selected"] = condition_selected
                await page.wait_for_timeout(1600)

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
            "text=/Ti paghiamo\\s*\\d+[\\.,]?\\d*\\s?€/i",
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

