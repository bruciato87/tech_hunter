from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import detect_color_variants, extract_capacity_gb, parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator


class TrendDeviceValuator(BaseValuator):
    platform_name = "trenddevice"
    condition_label = "grado_a"
    base_url = "https://www.trendevice.com"

    async def _fetch_offer(
        self,
        product: AmazonProduct,
        normalized_name: str,
    ) -> tuple[float | None, str | None, dict[str, Any]]:
        query = product.ean or normalized_name
        capacity = extract_capacity_gb(normalized_name)
        color_variants = detect_color_variants(f"{product.title} {normalized_name}")
        payload: dict[str, Any] = {
            "query": query,
            "capacity_selected": capacity,
            "color_variants": list(color_variants),
            "condition_target": "Grado A",
        }

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(locale="it-IT")
            page = await context.new_page()
            page.set_default_timeout(self.nav_timeout_ms)
            try:
                await page.goto(self.base_url, wait_until="domcontentloaded")
                hostname = (urlparse(page.url).hostname or "").lower()
                if "trendevice.com" not in hostname:
                    raise RuntimeError(f"Unexpected TrendDevice hostname: {hostname or 'n/a'}")
                await self._accept_cookie_if_present(page)

                await self._click_first(
                    page,
                    [
                        "a:has-text('Vendi')",
                        "button:has-text('Vendi')",
                        "a[href*='vendi']",
                        "a[href*='sell']",
                    ],
                    timeout_ms=9000,
                )
                await page.wait_for_load_state("domcontentloaded")

                filled = await self._fill_first(
                    page,
                    [
                        "input[type='search']",
                        "input[placeholder*='Cerca' i]",
                        "input[name*='search' i]",
                        "input[id*='search' i]",
                    ],
                    value=query,
                    timeout_ms=10000,
                )
                if not filled:
                    raise RuntimeError("TrendDevice search field not found")

                await page.keyboard.press("Enter")
                await page.wait_for_timeout(1800)

                opened = await self._click_first(
                    page,
                    [
                        "a[href*='prodotto']",
                        "a[href*='product']",
                        "[data-testid*='product'] a",
                        ".product a",
                        "li:has-text('GB') a",
                    ],
                    timeout_ms=9000,
                )
                payload["result_opened"] = opened
                await page.wait_for_load_state("domcontentloaded")

                if capacity:
                    await self._click_first(
                        page,
                        [
                            f"button:has-text('{capacity}')",
                            f"[role='button']:has-text('{capacity}')",
                            f"label:has-text('{capacity}')",
                            f"li:has-text('{capacity}')",
                        ],
                        timeout_ms=3500,
                    )

                if color_variants:
                    for color in color_variants:
                        clicked = await self._click_first(
                            page,
                            [
                                f"button:has-text('{color}')",
                                f"[role='button']:has-text('{color}')",
                                f"label:has-text('{color}')",
                                f"li:has-text('{color}')",
                            ],
                            timeout_ms=2500,
                        )
                        if clicked:
                            payload["color_selected"] = color
                            break

                condition_selected = await self._click_first(
                    page,
                    [
                        "button:has-text('Grado A')",
                        "label:has-text('Grado A')",
                        "button:has-text('Ottimo')",
                        "label:has-text('Ottimo')",
                        "[role='button']:has-text('Grade A')",
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
