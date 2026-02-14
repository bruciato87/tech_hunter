from __future__ import annotations

import os
import re
import time
from abc import ABC, abstractmethod
from typing import Any

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from tech_sniper_it.models import AmazonProduct, ValuationResult
from tech_sniper_it.valuators.ui_resilience import keyword_presence, selector_candidates, ui_signature


class ValuatorRuntimeError(RuntimeError):
    def __init__(self, message: str, *, payload: dict[str, Any] | None = None, source_url: str | None = None) -> None:
        super().__init__(message)
        self.payload = payload if isinstance(payload, dict) else {}
        self.source_url = source_url


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
            payload = {"error_type": type(exc).__name__}
            source_url = None
            if isinstance(exc, ValuatorRuntimeError):
                payload.update(exc.payload)
                source_url = exc.source_url
            return ValuationResult(
                platform=self.platform_name,
                normalized_name=normalized_name,
                offer_eur=None,
                condition=self.condition_label,
                error=str(exc),
                source_url=source_url,
                raw_payload=payload,
            )

    @abstractmethod
    async def _fetch_offer(self, product: AmazonProduct, normalized_name: str) -> tuple[float | None, str | None, dict[str, Any]]:
        raise NotImplementedError

    async def _accept_cookie_if_present(self, page: Page) -> None:
        if hasattr(page, "is_closed") and page.is_closed():
            return
        selectors = [
            "#cookiescript_accept",
            "#cookiescript_save",
            "button:has-text('Accetta')",
            "button:has-text('Accetto')",
            "button:has-text('Accept all')",
            "button:has-text('Accetta tutto')",
            "[id*='onetrust-accept']",
            "[aria-label*='accept' i]",
        ]
        for selector in selectors:
            try:
                if hasattr(page, "is_closed") and page.is_closed():
                    return
                locator = page.locator(selector).first
                if await locator.count() and await locator.is_visible(timeout=1000):
                    # Force click avoids long retry loops when blackout overlays intercept pointer events.
                    await locator.click(timeout=900, force=True)
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

    def _selector_candidates(
        self,
        *,
        site: str,
        slot: str,
        defaults: list[str],
        payload: dict[str, Any] | None = None,
    ) -> list[str]:
        merged = selector_candidates(
            site=site,
            slot=slot,
            defaults=defaults,
            overrides_raw=os.getenv("VALUATOR_SELECTOR_OVERRIDES_JSON"),
        )
        extra = max(0, len(merged) - len(defaults))
        if payload is not None and extra > 0:
            payload.setdefault("selector_overrides", {})[slot] = extra
        if extra > 0:
            print(f"[ui] selector override applied | site={site} slot={slot} extra={extra}")
        return merged

    async def _fill_first_semantic(
        self,
        page: Page,
        *,
        value: str,
        keywords: list[str],
        timeout_ms: int = 5000,
        max_candidates: int = 60,
    ) -> bool:
        terms = [item.strip().lower() for item in keywords if item.strip()]
        if not terms:
            return False

        deadline = time.monotonic() + (timeout_ms / 1000)
        while time.monotonic() <= deadline:
            inputs = page.locator("input, textarea, [role='searchbox']")
            try:
                count = min(await inputs.count(), max_candidates)
            except PlaywrightError:
                count = 0
            for index in range(count):
                item = inputs.nth(index)
                try:
                    if not await item.is_visible():
                        continue
                except PlaywrightError:
                    continue

                try:
                    meta = await item.evaluate(
                        """
                        (el) => {
                          const labels = el.labels ? Array.from(el.labels).map((x) => x.textContent || "") : [];
                          const parent = (el.closest("form")?.textContent || el.parentElement?.textContent || "").slice(0, 180);
                          return {
                            type: (el.getAttribute("type") || "").toLowerCase(),
                            name: (el.getAttribute("name") || "").toLowerCase(),
                            id: (el.getAttribute("id") || "").toLowerCase(),
                            placeholder: (el.getAttribute("placeholder") || "").toLowerCase(),
                            aria: (el.getAttribute("aria-label") || "").toLowerCase(),
                            role: (el.getAttribute("role") || "").toLowerCase(),
                            labels: labels.join(" ").toLowerCase(),
                            parent: parent.toLowerCase(),
                          };
                        }
                        """
                    )
                except PlaywrightError:
                    continue
                if not isinstance(meta, dict):
                    continue

                haystack = " ".join(
                    str(meta.get(key, ""))
                    for key in (
                        "type",
                        "name",
                        "id",
                        "placeholder",
                        "aria",
                        "role",
                        "labels",
                        "parent",
                    )
                )
                if not any(term in haystack for term in terms):
                    continue
                try:
                    await item.click(timeout=900)
                except PlaywrightError:
                    pass
                try:
                    await item.fill(value, timeout=1400)
                    return True
                except PlaywrightError:
                    continue

            await page.wait_for_timeout(250)
        return False

    async def _click_first_semantic(
        self,
        page: Page,
        *,
        keywords: list[str],
        timeout_ms: int = 5000,
        max_candidates: int = 14,
        selectors: list[str] | None = None,
    ) -> bool:
        terms = [item.strip() for item in keywords if item and item.strip()]
        if not terms:
            return False
        pattern = re.compile("|".join(re.escape(item) for item in terms), re.IGNORECASE)
        candidates = selectors or ["button", "[role='button']", "a", "label", "li", "div[role='option']"]

        deadline = time.monotonic() + (timeout_ms / 1000)
        while time.monotonic() <= deadline:
            for selector in candidates:
                try:
                    locator = page.locator(selector).filter(has_text=pattern)
                    count = min(await locator.count(), max_candidates)
                except PlaywrightError:
                    continue
                for index in range(count):
                    target = locator.nth(index)
                    try:
                        if not await target.is_visible():
                            continue
                    except PlaywrightError:
                        continue
                    try:
                        await target.click(timeout=1400, force=True)
                        return True
                    except PlaywrightError:
                        continue
            await page.wait_for_timeout(250)
        return False

    async def _collect_ui_probe(
        self,
        *,
        page: Page,
        site: str,
        stage: str,
        expected_keywords: list[str],
        min_hits: int = 2,
    ) -> dict[str, Any]:
        try:
            title = await page.title()
        except PlaywrightError:
            title = ""
        try:
            body = await page.inner_text("body", timeout=1600)
        except PlaywrightError:
            body = ""
        present, missing = keyword_presence(f"{title}\n{body}", expected_keywords)
        drift = len(present) < max(1, min_hits)
        probe = {
            "site": site,
            "stage": stage,
            "url": page.url,
            "title": title[:180],
            "signature": ui_signature(page.url, title, body),
            "expected_keywords": expected_keywords,
            "keyword_hits": present,
            "keyword_missing": missing,
            "drift_suspected": drift,
        }
        print(
            "[ui] probe | "
            f"site={site} stage={stage} drift={drift} hits={len(present)}/{len(expected_keywords)} "
            f"signature={probe['signature']}"
        )
        return probe

    async def _attach_ui_probe(
        self,
        *,
        payload: dict[str, Any],
        page: Page,
        site: str,
        stage: str,
        expected_keywords: list[str],
        min_hits: int = 2,
    ) -> dict[str, Any]:
        probe = await self._collect_ui_probe(
            page=page,
            site=site,
            stage=stage,
            expected_keywords=expected_keywords,
            min_hits=min_hits,
        )
        payload.setdefault("ui_probes", []).append(probe)
        return probe
