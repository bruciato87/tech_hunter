from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from tech_sniper_it.models import AmazonProduct, ValuationResult


class BaseValuator(ABC):
    platform_name: str = "base"
    condition_label: str = "grade_a"

    def __init__(self, headless: bool = True, nav_timeout_ms: int = 45000) -> None:
        self.headless = headless
        self.nav_timeout_ms = nav_timeout_ms

    async def valuate(self, product: AmazonProduct, normalized_name: str) -> ValuationResult:
        try:
            offer, source_url, payload = await self._fetch_offer(product=product, normalized_name=normalized_name)
            return ValuationResult(
                platform=self.platform_name,
                normalized_name=normalized_name,
                offer_eur=offer,
                condition=self.condition_label,
                source_url=source_url,
                raw_payload=payload,
            )
        except Exception as exc:
            return ValuationResult(
                platform=self.platform_name,
                normalized_name=normalized_name,
                offer_eur=None,
                condition=self.condition_label,
                error=str(exc),
                raw_payload={"error_type": type(exc).__name__},
            )

    @abstractmethod
    async def _fetch_offer(self, product: AmazonProduct, normalized_name: str) -> tuple[float | None, str | None, dict[str, Any]]:
        raise NotImplementedError

    async def _accept_cookie_if_present(self, page: Page) -> None:
        selectors = [
            "button:has-text('Accetta')",
            "button:has-text('Accetto')",
            "button:has-text('Accept all')",
            "button:has-text('Accetta tutto')",
            "[id*='onetrust-accept']",
            "[aria-label*='accept' i]",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if await locator.count() and await locator.is_visible(timeout=1000):
                    await locator.click(timeout=1500)
                    return
            except PlaywrightError:
                continue

    async def _click_first(self, page: Page, selectors: list[str], timeout_ms: int = 5000) -> bool:
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                await locator.wait_for(state="visible", timeout=timeout_ms)
                await locator.click(timeout=timeout_ms)
                return True
            except PlaywrightError:
                continue
        return False

    async def _fill_first(self, page: Page, selectors: list[str], value: str, timeout_ms: int = 5000) -> bool:
        for selector in selectors:
            try:
                locator = page.locator(selector).first
                await locator.wait_for(state="visible", timeout=timeout_ms)
                await locator.fill(value, timeout=timeout_ms)
                return True
            except PlaywrightError:
                continue
        return False

