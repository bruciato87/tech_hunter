from __future__ import annotations

import os
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.utils import detect_color_variants, extract_capacity_gb, parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator


STEP_DEVICE_FAMILY = "device_family"
STEP_MODEL = "model"
STEP_CAPACITY = "capacity"
STEP_CONDITION = "condition"
STEP_BATTERY = "battery"
STEP_SIM = "sim"
STEP_MARKET = "market"
STEP_COLOR = "color"
STEP_YES_NO = "yes_no"

COLOR_HINTS: tuple[str, ...] = (
    "nero",
    "bianco",
    "argento",
    "oro",
    "blu",
    "azzurro",
    "rosso",
    "verde",
    "grigio",
    "grafite",
    "viola",
    "pink",
    "starlight",
    "siderale",
    "mezzanotte",
)
CONDITION_PREFERENCE: tuple[str, ...] = (
    "normale usura",
    "perfetto",
    "ottimo",
    "come nuovo",
    "grado a",
)
BATTERY_PREFERENCE: tuple[str, ...] = (
    ">= 85%",
    "85% e originale",
    "originale",
)
SIM_PREFERENCE: tuple[str, ...] = (
    "sim card",
    "slot sim",
)
MARKET_PREFERENCE: tuple[str, ...] = (
    "italia",
    "estero",
    "trendevice",
)
PRICE_CONTEXT_HINTS: tuple[str, ...] = (
    "ti offriamo",
    "offerta",
    "valutazione",
    "vale",
    "ricevi",
    "ritiro",
    "quotazione",
    "paghiamo",
    "totale",
)
PRICE_CONTEXT_BLOCKERS: tuple[str, ...] = (
    "ordine min",
    "spedizione",
    "cap.soc",
    "cap soc",
    "diritti riservati",
    "cookie",
    "p.iva",
    "rea",
)


@dataclass(slots=True)
class WizardOption:
    index: int
    text: str
    normalized: str
    selector: str = "label:has(input[name='item'])"


def _normalize_wizard_text(value: str | None) -> str:
    lowered = (value or "").strip().lower()
    lowered = lowered.replace("’", "'")
    lowered = re.sub(r"\s+", " ", lowered)
    lowered = re.sub(r"[^a-z0-9%+<>= ]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _options_signature(options: list[WizardOption]) -> str:
    return "|".join(item.normalized for item in options)


def _is_capacity_step(values: list[str]) -> bool:
    return any(re.search(r"\b\d{2,4}\s*gb\b|\b\d+\s*tb\b", value) for value in values)


def _is_condition_step(values: list[str]) -> bool:
    hints = ("normale usura", "perfetto", "accettabile", "danneggiato", "non funzionante", "ottimo", "come nuovo")
    return any(any(hint in value for hint in hints) for value in values)


def _is_battery_step(values: list[str]) -> bool:
    return any("85%" in value for value in values) or any("non originale" in value for value in values)


def _is_sim_step(values: list[str]) -> bool:
    return any("sim card" in value or "e sim" in value or "esim" in value for value in values)


def _is_market_step(values: list[str]) -> bool:
    return any("italia" in value for value in values) and any("estero" in value for value in values)


def _is_color_step(values: list[str]) -> bool:
    matches = sum(1 for value in values if any(color in value for color in COLOR_HINTS))
    return matches >= max(1, len(values) // 2)


def _detect_wizard_step(options: list[WizardOption]) -> str:
    values = [item.normalized for item in options]
    if not values:
        return STEP_MODEL

    if all(value in {"si", "no"} for value in values):
        return STEP_YES_NO

    if any(value == "iphone" for value in values) and any(value in {"mac", "samsung", "google"} for value in values):
        return STEP_DEVICE_FAMILY
    if _is_capacity_step(values):
        return STEP_CAPACITY
    if _is_condition_step(values):
        return STEP_CONDITION
    if _is_battery_step(values):
        return STEP_BATTERY
    if _is_sim_step(values):
        return STEP_SIM
    if _is_market_step(values):
        return STEP_MARKET
    if _is_color_step(values):
        return STEP_COLOR
    return STEP_MODEL


def _extract_iphone_model_hint(normalized_name: str) -> str:
    value = _normalize_wizard_text(normalized_name)
    match = re.search(
        r"\biphone\s+(?P<base>\d{1,2}|se(?:\s+\d{4})?)\s*(?P<variant>pro max|pro|plus|mini|air|e)?",
        value,
    )
    if not match:
        return value
    base = (match.group("base") or "").strip()
    variant = (match.group("variant") or "").strip()
    return f"{base} {variant}".strip()


def _model_score(option: WizardOption, *, model_hint: str, normalized_name: str) -> int:
    hint = _normalize_wizard_text(model_hint)
    full_name = _normalize_wizard_text(normalized_name)
    text = option.normalized

    score = int(SequenceMatcher(None, text, hint or full_name).ratio() * 100)
    if hint and text == hint:
        score += 200
    if hint and text in hint:
        score += 120
    if hint and hint in text:
        score += 80

    for token in hint.split():
        if token in text:
            score += 25

    number_match = re.search(r"\b\d{1,2}\b", text)
    if number_match:
        number = number_match.group(0)
        if number in hint:
            score += 40
        else:
            score -= 20

    if "pro max" in text and "pro max" not in hint:
        score -= 40
    if "plus" in text and "plus" not in hint:
        score -= 20
    if "mini" in text and "mini" not in hint:
        score -= 20
    return score


def _pick_by_preference(options: list[WizardOption], preferences: tuple[str, ...]) -> WizardOption | None:
    for preferred in preferences:
        preferred_norm = _normalize_wizard_text(preferred)
        for option in options:
            if preferred_norm and preferred_norm in option.normalized:
                return option
    return options[0] if options else None


def _pick_wizard_option(
    *,
    step: str,
    options: list[WizardOption],
    product: AmazonProduct,
    normalized_name: str,
    excluded_models: set[str] | None = None,
) -> WizardOption | None:
    if not options:
        return None

    if step == STEP_DEVICE_FAMILY:
        device_by_category = {
            ProductCategory.APPLE_PHONE: "iphone",
            ProductCategory.PHOTOGRAPHY: "iphone",
            ProductCategory.GENERAL_TECH: "iphone",
        }
        target = device_by_category.get(product.category, "iphone")
        for option in options:
            if target in option.normalized:
                return option
        return options[0]

    if step == STEP_MODEL:
        model_hint = _extract_iphone_model_hint(normalized_name)
        ranked = sorted(
            options,
            key=lambda option: _model_score(option, model_hint=model_hint, normalized_name=normalized_name),
            reverse=True,
        )
        excluded = excluded_models or set()
        for option in ranked:
            if option.normalized not in excluded:
                return option
        return ranked[0] if ranked else None

    if step == STEP_CAPACITY:
        capacity = _normalize_wizard_text(extract_capacity_gb(normalized_name))
        if capacity:
            for option in options:
                if capacity in option.normalized:
                    return option
        return options[0]

    if step == STEP_CONDITION:
        return _pick_by_preference(options, CONDITION_PREFERENCE)

    if step == STEP_BATTERY:
        return _pick_by_preference(options, BATTERY_PREFERENCE)

    if step == STEP_SIM:
        combined = _normalize_wizard_text(f"{product.title} {normalized_name}")
        if "esim" in combined or "e sim" in combined:
            for option in options:
                if "e sim" in option.normalized or "esim" in option.normalized:
                    return option
        return _pick_by_preference(options, SIM_PREFERENCE)

    if step == STEP_MARKET:
        return _pick_by_preference(options, MARKET_PREFERENCE)

    if step == STEP_COLOR:
        variants = detect_color_variants(f"{product.title} {normalized_name}")
        for variant in variants:
            target = _normalize_wizard_text(variant)
            for option in options:
                if target and target in option.normalized:
                    return option
        return options[0]

    if step == STEP_YES_NO:
        for option in options:
            if option.normalized == "si":
                return option
        return options[0]

    return options[0]


def _extract_contextual_price(text: str) -> tuple[float | None, str]:
    if not text:
        return None, ""

    candidates: list[tuple[int, float, str]] = []
    for match in re.finditer(r"\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?\s*€", text):
        snippet = text[max(0, match.start() - 80) : min(len(text), match.end() + 80)]
        snippet_normalized = _normalize_wizard_text(snippet)
        blocker_hits = sum(1 for blocker in PRICE_CONTEXT_BLOCKERS if blocker in snippet_normalized)
        value = parse_eur_price(match.group(0))
        if value is None or value <= 0 or value > 5000:
            continue

        score = 0
        for hint in PRICE_CONTEXT_HINTS:
            if hint in snippet_normalized:
                score += 8
        score -= blocker_hits * 8
        if value >= 120:
            score += 3
        candidates.append((score, value, snippet.strip()))

    if not candidates:
        return None, ""

    score, value, snippet = max(candidates, key=lambda item: (item[0], item[1]))
    if score <= 0:
        return None, ""
    return value, snippet


def _is_email_gate_text(text: str) -> bool:
    normalized = _normalize_wizard_text(text)
    if not normalized:
        return False
    has_mail = "email" in normalized or "e mail" in normalized or "mail" in normalized
    has_submit_cta = "scopri la valutazione" in normalized
    has_wizard_context = "dispositivo usato" in normalized or "guadagnare" in normalized or "valutazione" in normalized
    return has_mail and has_submit_cta and has_wizard_context


class TrendDeviceValuator(BaseValuator):
    platform_name = "trenddevice"
    condition_label = "grado_a"
    base_url = "https://www.trendevice.com/vendi/valutazione/"

    async def _collect_wizard_options(self, page: Page) -> list[WizardOption]:
        options: list[WizardOption] = []
        seen: set[str] = set()
        option_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="wizard_option",
            defaults=[
                "label:has(input[name='item'])",
                "[role='radio']",
                "div[role='option']",
                "button[class*='option' i]",
            ],
        )
        for selector in option_selectors:
            nodes = page.locator(selector)
            try:
                count = min(await nodes.count(), 90)
            except PlaywrightError:
                continue
            for index in range(count):
                node = nodes.nth(index)
                try:
                    if not await node.is_visible():
                        continue
                    text = await node.inner_text(timeout=900)
                except PlaywrightError:
                    continue

                cleaned = re.sub(r"\s+", " ", text).strip()
                normalized = _normalize_wizard_text(cleaned)
                if not cleaned or len(normalized) < 2:
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                options.append(
                    WizardOption(
                        index=index,
                        text=cleaned,
                        normalized=normalized,
                        selector=selector,
                    )
                )
        return options

    async def _wait_for_wizard_options(self, page: Page, timeout_ms: int = 6000) -> list[WizardOption]:
        elapsed = 0
        interval_ms = 350
        while elapsed <= timeout_ms:
            options = await self._collect_wizard_options(page)
            if options:
                return options
            await page.wait_for_timeout(interval_ms)
            elapsed += interval_ms
        return []

    async def _select_option(self, page: Page, option: WizardOption) -> None:
        target = page.locator(option.selector).nth(option.index)
        radio = target.locator("input[type='radio'], input[name='item']").first
        try:
            if await radio.count():
                await radio.check(force=True, timeout=2600)
                return
        except PlaywrightError:
            pass
        await target.click(force=True, timeout=3200)

    async def _click_confirm(self, page: Page) -> bool:
        selectors = self._selector_candidates(
            site=self.platform_name,
            slot="confirm_button",
            defaults=[
                "button:has-text('Conferma')",
                "button:has-text('Valuta')",
                "button:has-text('Continua')",
                "button:has-text('Avanti')",
                "button:has-text('Calcola')",
            ],
        )
        for selector in selectors:
            button = page.locator(selector).first
            try:
                if not await button.count():
                    continue
                await button.wait_for(state="visible", timeout=2200)
                if not await button.is_enabled():
                    await page.wait_for_timeout(300)
                if not await button.is_enabled():
                    continue
                await button.click(timeout=3500)
                return True
            except PlaywrightError:
                continue
        return await self._click_first_semantic(
            page,
            keywords=["conferma", "valuta", "continua", "avanti", "calcola"],
            timeout_ms=1800,
            selectors=["button", "[role='button']"],
        )

    async def _extract_price(self, page: Page, *, payload: dict[str, Any] | None = None) -> tuple[float | None, str]:
        selectors = self._selector_candidates(
            site=self.platform_name,
            slot="price",
            defaults=[
                "[data-testid*='price' i]",
                "[class*='price' i]",
                "[class*='offerta' i]",
                "[class*='valut' i]",
                "text=/ti offriamo/i",
                "text=/valutazione/i",
                "text=/\\d+[\\.,]?\\d*\\s?€/i",
                "text=/€\\s?\\d+[\\.,]?\\d*/i",
            ],
            payload=payload,
        )
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = min(await locator.count(), 8)
                for index in range(count):
                    text = await locator.nth(index).inner_text(timeout=1000)
                    value, snippet = _extract_contextual_price(text)
                    if value is not None:
                        return value, snippet
            except PlaywrightError:
                continue

        sections: list[str] = []
        try:
            main = page.locator("main").first
            if await main.count():
                sections.append(await main.inner_text(timeout=1300))
        except PlaywrightError:
            pass
        try:
            sections.append(await page.inner_text("body", timeout=1300))
        except PlaywrightError:
            pass

        for section in sections:
            value, snippet = _extract_contextual_price(section)
            if value is not None:
                return value, snippet

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select("main, [class*='price' i], [class*='offerta' i], [class*='valut' i]"):
            text = node.get_text(" ", strip=True)
            value, snippet = _extract_contextual_price(text)
            if value is not None:
                return value, snippet
        body_text = soup.get_text(" ", strip=True)
        return None, body_text[:220]

    async def _is_email_gate(self, page: Page) -> bool:
        email_input_selectors = [
            "input[type='email']",
            "input[name*='mail' i]",
            "input[id*='mail' i]",
            "input[placeholder*='mail' i]",
        ]
        cta_selectors = [
            "button:has-text('Scopri la valutazione')",
            "button:has-text('Scopri valutazione')",
            "button:has-text('Scopri')",
        ]
        for email_selector in email_input_selectors:
            try:
                if not await page.locator(email_selector).first.count():
                    continue
            except PlaywrightError:
                continue
            for cta_selector in cta_selectors:
                try:
                    if await page.locator(cta_selector).first.count():
                        return True
                except PlaywrightError:
                    continue
        try:
            body_text = await page.inner_text("body", timeout=900)
        except PlaywrightError:
            return False
        return _is_email_gate_text(body_text)

    async def _submit_email_gate(self, page: Page, payload: dict[str, Any]) -> bool:
        if not await self._is_email_gate(page):
            return False

        email_value = (os.getenv("TRENDDEVICE_LEAD_EMAIL") or "techsniperit@example.com").strip()
        if "@" not in email_value:
            email_value = "techsniperit@example.com"

        email_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="email_input",
            defaults=[
                "input[type='email']",
                "input[name*='mail' i]",
                "input[id*='mail' i]",
                "input[placeholder*='mail' i]",
            ],
            payload=payload,
        )
        filled = await self._fill_first(
            page,
            selectors=email_selectors,
            value=email_value,
            timeout_ms=3500,
        )
        if not filled:
            filled = await self._fill_first_semantic(
                page,
                value=email_value,
                keywords=["email", "mail"],
                timeout_ms=2200,
            )

        consent_checked = 0
        checkboxes = page.locator("input[type='checkbox']")
        try:
            checkbox_count = min(await checkboxes.count(), 4)
        except PlaywrightError:
            checkbox_count = 0
        for index in range(checkbox_count):
            checkbox = checkboxes.nth(index)
            try:
                if not await checkbox.is_visible():
                    continue
                await checkbox.check(force=True, timeout=1500)
                consent_checked += 1
            except PlaywrightError:
                try:
                    await checkbox.click(force=True, timeout=1000)
                    consent_checked += 1
                except PlaywrightError:
                    continue

        if consent_checked == 0:
            clicked_label = await self._click_first(
                page,
                selectors=[
                    "label:has-text('Ho letto')",
                    "label:has-text('Condizioni d\\'uso')",
                    "label:has-text('Privacy Policy')",
                ],
                timeout_ms=1800,
            )
            if not clicked_label:
                await self._click_first_semantic(
                    page,
                    keywords=["ho letto", "condizioni", "privacy"],
                    timeout_ms=1800,
                    selectors=["label", "span", "div", "button"],
                )

        submit_selectors = self._selector_candidates(
            site=self.platform_name,
            slot="email_submit",
            defaults=[
                "button:has-text('Scopri la valutazione')",
                "button:has-text('Scopri valutazione')",
                "button:has-text('Scopri')",
            ],
            payload=payload,
        )
        submitted = await self._click_first(
            page,
            selectors=submit_selectors,
            timeout_ms=4500,
        )
        if not submitted:
            submitted = await self._click_first_semantic(
                page,
                keywords=["scopri la valutazione", "scopri valutazione", "scopri"],
                timeout_ms=2400,
                selectors=["button", "[role='button']", "a"],
            )
        payload["email_gate"] = {
            "detected": True,
            "filled": filled,
            "consent_checked": consent_checked,
            "submitted": submitted,
            "email_domain": email_value.split("@")[-1],
        }

        if submitted:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=6500)
            except PlaywrightError:
                pass
            await page.wait_for_timeout(1400)
        return submitted

    async def _fetch_offer(
        self,
        product: AmazonProduct,
        normalized_name: str,
    ) -> tuple[float | None, str | None, dict[str, Any]]:
        payload: dict[str, Any] = {
            "query": normalized_name,
            "condition_target": "Normale usura (grade A)",
            "wizard": [],
            "adaptive_fallbacks": {},
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
                        "button:has-text('Accetta tutti')",
                        "button:has-text('Accetta')",
                    ],
                    timeout_ms=3500,
                )
                await page.wait_for_timeout(800)

                previous_signature = ""
                stagnant_steps = 0
                max_steps = 18
                excluded_models: set[str] = set()
                reset_after_model = 0
                for step_index in range(1, max_steps + 1):
                    options = await self._wait_for_wizard_options(page)
                    if not options:
                        await self._click_first(
                            page,
                            [
                                "button:has-text('Accetta tutti')",
                                "button:has-text('Accetta')",
                            ],
                            timeout_ms=1800,
                        )
                        await page.wait_for_timeout(900)
                        options = await self._wait_for_wizard_options(page, timeout_ms=3500)
                        if not options:
                            submitted_email = await self._submit_email_gate(page, payload)
                            if submitted_email:
                                payload["wizard_end_reason"] = "email-gate-submitted"
                                options = await self._wait_for_wizard_options(page, timeout_ms=2800)
                                if not options:
                                    await self._attach_ui_probe(
                                        payload=payload,
                                        page=page,
                                        site=self.platform_name,
                                        stage="email_gate_submitted_no_price",
                                        expected_keywords=["mail", "valutazione", "guadagnare", "dispositivo usato"],
                                    )
                                    break
                            else:
                                payload["wizard_end_reason"] = "no-options"
                                await self._attach_ui_probe(
                                    payload=payload,
                                    page=page,
                                    site=self.platform_name,
                                    stage="wizard_no_options",
                                    expected_keywords=["valutazione", "vendi", "iphone", "trendevice"],
                                )
                                break

                    step_name = _detect_wizard_step(options)
                    signature = _options_signature(options)
                    if signature == previous_signature:
                        stagnant_steps += 1
                    else:
                        stagnant_steps = 0
                    previous_signature = signature
                    if stagnant_steps >= 1 and step_name in {STEP_MARKET, STEP_COLOR, STEP_YES_NO}:
                        # Some final steps render "Valuta" instead of "Conferma".
                        if await self._click_confirm(page):
                            payload["wizard"].append(
                                {
                                    "step": step_index,
                                    "step_type": f"{step_name}_finalize",
                                    "selected": "auto-finalize",
                                    "options_count": len(options),
                                    "confirmed": True,
                                }
                            )
                            await page.wait_for_timeout(1200)
                            continue
                    if stagnant_steps >= 2:
                        submitted_email = await self._submit_email_gate(page, payload)
                        if submitted_email:
                            payload["wizard_end_reason"] = "email-gate-submitted-stagnant"
                            await self._attach_ui_probe(
                                payload=payload,
                                page=page,
                                site=self.platform_name,
                                stage="email_gate_submitted_stagnant",
                                expected_keywords=["mail", "valutazione", "guadagnare", "dispositivo usato"],
                            )
                            break
                        payload["wizard_end_reason"] = "stagnant-options"
                        await self._attach_ui_probe(
                            payload=payload,
                            page=page,
                            site=self.platform_name,
                            stage="wizard_stagnant_options",
                            expected_keywords=["valutazione", "vendi", "iphone", "trendevice"],
                        )
                        break

                    if step_name == STEP_DEVICE_FAMILY and payload["wizard"]:
                        previous_step = payload["wizard"][-1]
                        if previous_step.get("step_type") == STEP_MODEL:
                            reset_after_model += 1
                            if reset_after_model >= 3:
                                payload["wizard_end_reason"] = "model-selection-reset"
                                raise RuntimeError("TrendDevice wizard reset after model selection (catalog route unavailable).")

                    chosen = _pick_wizard_option(
                        step=step_name,
                        options=options,
                        product=product,
                        normalized_name=normalized_name,
                        excluded_models=excluded_models,
                    )
                    if not chosen:
                        payload["wizard_end_reason"] = f"no-choice-{step_name}"
                        break

                    await self._select_option(page, chosen)
                    confirmed = await self._click_confirm(page)
                    payload["wizard"].append(
                        {
                            "step": step_index,
                            "step_type": step_name,
                            "selected": chosen.text,
                            "options_count": len(options),
                            "confirmed": confirmed,
                        }
                    )

                    await page.wait_for_timeout(1100)

                price, price_text = await self._extract_price(page, payload=payload)
                payload["price_text"] = price_text
                if price is None:
                    reason = payload.get("wizard_end_reason", "price-missing")
                    await self._attach_ui_probe(
                        payload=payload,
                        page=page,
                        site=self.platform_name,
                        stage="price_missing",
                        expected_keywords=["offerta", "valutazione", "ricevi", "€"],
                    )
                    raise RuntimeError(f"TrendDevice price not found after wizard ({reason})")
                return price, page.url, payload
            finally:
                await context.close()
                await browser.close()


__all__ = [
    "TrendDeviceValuator",
    "_detect_wizard_step",
    "_extract_contextual_price",
    "_extract_iphone_model_hint",
    "_is_email_gate_text",
    "_normalize_wizard_text",
]
