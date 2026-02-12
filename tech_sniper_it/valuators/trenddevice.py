from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import tempfile
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
NETWORK_PRICE_KEYS: tuple[str, ...] = (
    "price",
    "prezzo",
    "offer",
    "offerta",
    "valuation",
    "quote",
    "quotazione",
    "valore",
    "amount",
    "totale",
    "cash",
    "payout",
)
NETWORK_PROMO_BLOCKERS: tuple[str, ...] = (
    "fino al",
    "fino a",
    "sconti",
    "promo",
    "garanzia",
    "reso gratis",
    "rate",
)


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _use_storage_state() -> bool:
    value = _env_or_default("TRENDDEVICE_USE_STORAGE_STATE", "true").lower()
    return value not in {"0", "false", "no", "off"}


def _load_storage_state_b64() -> str | None:
    if not _use_storage_state():
        return None
    raw = (os.getenv("TRENDDEVICE_STORAGE_STATE_B64") or "").strip()
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


def _parse_plain_price(value: str | int | float) -> float | None:
    if isinstance(value, (int, float)):
        parsed = float(value)
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        raw = raw.replace(" ", "")
        if "," in raw and "." in raw:
            # assume European format 1.234,56
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", ".")
        try:
            parsed = float(raw)
        except ValueError:
            return None
    if parsed > 5000 and parsed <= 500000:
        parsed = parsed / 100
    if 20 <= parsed <= 5000:
        return round(parsed, 2)
    return None


def _extract_keyed_prices_from_text(text: str) -> list[tuple[int, float, str]]:
    if not text:
        return []
    normalized = " ".join(text.split())
    candidates: list[tuple[int, float, str]] = []
    for keyword in NETWORK_PRICE_KEYS:
        pattern = re.compile(rf"(?i){re.escape(keyword)}[^0-9€]{{0,40}}(\d{{2,5}}(?:[.,]\d{{1,2}})?)\s*€?")
        for match in pattern.finditer(normalized):
            value = _parse_plain_price(match.group(1))
            if value is None:
                continue
            snippet = normalized[max(0, match.start() - 70) : min(len(normalized), match.end() + 70)]
            score = 46 + (8 if "€" in match.group(0) else 0)
            candidates.append((score, value, snippet.strip()))
    return candidates


def _extract_prices_from_json_blob(blob: Any, path: str = "") -> list[tuple[int, float, str]]:
    candidates: list[tuple[int, float, str]] = []
    if isinstance(blob, dict):
        for key, value in blob.items():
            key_text = str(key)
            next_path = f"{path}.{key_text}" if path else key_text
            candidates.extend(_extract_prices_from_json_blob(value, next_path))
        return candidates
    if isinstance(blob, list):
        for index, value in enumerate(blob):
            next_path = f"{path}[{index}]"
            candidates.extend(_extract_prices_from_json_blob(value, next_path))
        return candidates

    if not isinstance(blob, (str, int, float)):
        return candidates
    path_norm = _normalize_wizard_text(path)
    if not any(keyword in path_norm for keyword in NETWORK_PRICE_KEYS):
        return candidates

    if isinstance(blob, str):
        value = parse_eur_price(blob) or _parse_plain_price(blob)
        if value is None:
            return candidates
    else:
        value = _parse_plain_price(blob)
        if value is None:
            return candidates

    candidates.append((72, value, f"{path}={blob}"))
    return candidates


def _pick_best_network_candidate(candidates: list[dict[str, Any]]) -> tuple[float | None, str]:
    credible = [item for item in candidates if _is_credible_network_candidate(item)]
    if not credible:
        return None, ""
    best = max(credible, key=lambda item: (int(item.get("score", 0)), float(item.get("value", 0.0))))
    value = _parse_plain_price(best.get("value"))
    if value is None:
        return None, ""
    snippet = str(best.get("snippet", "")).strip() or str(best.get("url", "")).strip()
    return value, snippet[:260]


def _is_credible_network_candidate(candidate: dict[str, Any]) -> bool:
    url = str(candidate.get("url", "")).lower()
    if "/_next/static/" in url or url.endswith(".js") or url.endswith(".css"):
        return False

    score = int(candidate.get("score", 0))
    source = str(candidate.get("source", ""))
    snippet_norm = _normalize_wizard_text(str(candidate.get("snippet", "")))
    if any(blocker in snippet_norm for blocker in NETWORK_PROMO_BLOCKERS):
        return False

    if source == "json":
        return score >= 68
    if score < 62:
        return False
    valuation_terms = ("ti offriamo", "valutazione", "ricevi", "paghiamo", "quotazione", "offerta")
    return any(term in snippet_norm for term in valuation_terms)


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
            "network_price_candidates": [],
        }
        storage_state_path = _load_storage_state_b64()
        payload["storage_state"] = bool(storage_state_path)
        email_gate_wait_ms = max(1500, int(_env_or_default("TRENDDEVICE_EMAIL_GATE_WAIT_MS", "6500")))

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context_kwargs: dict[str, Any] = {"locale": "it-IT"}
            if storage_state_path:
                context_kwargs["storage_state"] = storage_state_path
            context = await browser.new_context(**context_kwargs)
            page = await context.new_page()
            page.set_default_timeout(self.nav_timeout_ms)
            network_price_candidates: list[dict[str, Any]] = []
            response_tasks: set[asyncio.Task[Any]] = set()

            async def _capture_response_body(response) -> None:  # noqa: ANN001
                url = str(getattr(response, "url", "") or "")
                url_lower = url.lower()
                headers = getattr(response, "headers", {}) or {}
                content_type = str(headers.get("content-type", "")).lower()
                request = getattr(response, "request", None)
                resource_type = str(getattr(request, "resource_type", "")).lower()
                if "trendevice.com" not in url_lower:
                    is_api_like = resource_type in {"xhr", "fetch"} or any(
                        token in content_type for token in ("json", "text", "javascript")
                    )
                    if not is_api_like:
                        return
                interesting_url = any(
                    token in url_lower
                    for token in ("valut", "offer", "offert", "quote", "quotazione", "/api/", "graphql", "vendi")
                )
                if not interesting_url and not any(token in content_type for token in ("json", "text", "javascript")):
                    return
                try:
                    body = await response.text()
                except Exception:
                    return
                if not body:
                    return
                trimmed = body[:120000]
                local_candidates: list[dict[str, Any]] = []

                contextual_value, contextual_snippet = _extract_contextual_price(trimmed)
                if contextual_value is not None:
                    local_candidates.append(
                        {
                            "score": 68,
                            "value": contextual_value,
                            "snippet": contextual_snippet,
                            "source": "context",
                        }
                    )
                for score, value, snippet in _extract_keyed_prices_from_text(trimmed):
                    local_candidates.append(
                        {
                            "score": score,
                            "value": value,
                            "snippet": snippet,
                            "source": "keyword",
                        }
                    )

                parsed_json = None
                body_stripped = trimmed.strip()
                if (
                    "json" in content_type
                    or body_stripped.startswith("{")
                    or body_stripped.startswith("[")
                ):
                    try:
                        parsed_json = json.loads(body_stripped)
                    except Exception:
                        parsed_json = None
                if parsed_json is not None:
                    for score, value, snippet in _extract_prices_from_json_blob(parsed_json):
                        local_candidates.append(
                            {
                                "score": score,
                                "value": value,
                                "snippet": snippet,
                                "source": "json",
                            }
                        )

                if not local_candidates:
                    return

                ranked = sorted(local_candidates, key=lambda item: (int(item["score"]), float(item["value"])), reverse=True)
                for candidate in ranked[:3]:
                    row = {
                        "url": url,
                        "status": getattr(response, "status", None),
                        "content_type": content_type[:60],
                        "score": int(candidate["score"]),
                        "value": float(candidate["value"]),
                        "snippet": str(candidate["snippet"])[:260],
                        "source": candidate["source"],
                    }
                    if not _is_credible_network_candidate(row):
                        continue
                    network_price_candidates.append(row)
                if len(network_price_candidates) > 40:
                    del network_price_candidates[:-40]

            def _on_response(response) -> None:  # noqa: ANN001
                task = asyncio.create_task(_capture_response_body(response))
                response_tasks.add(task)
                task.add_done_callback(lambda done: response_tasks.discard(done))

            async def _drain_response_tasks() -> None:
                if not response_tasks:
                    return
                await asyncio.gather(*tuple(response_tasks), return_exceptions=True)

            page.on("response", _on_response)
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
                                await page.wait_for_timeout(email_gate_wait_ms)
                                options = await self._wait_for_wizard_options(page, timeout_ms=max(2200, email_gate_wait_ms // 2))
                                if not options:
                                    await _drain_response_tasks()
                                    price, price_text = await self._extract_price(page, payload=payload)
                                    if price is not None:
                                        payload["price_source"] = "dom-post-email"
                                        payload["price_text"] = price_text
                                        return price, page.url, payload
                                    network_price, network_snippet = _pick_best_network_candidate(network_price_candidates)
                                    if network_price is not None:
                                        payload["price_source"] = "network-post-email"
                                        payload["price_text"] = network_snippet
                                        return network_price, page.url, payload
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
                            await page.wait_for_timeout(email_gate_wait_ms)
                            await _drain_response_tasks()
                            price, price_text = await self._extract_price(page, payload=payload)
                            if price is not None:
                                payload["price_source"] = "dom-post-email-stagnant"
                                payload["price_text"] = price_text
                                return price, page.url, payload
                            network_price, network_snippet = _pick_best_network_candidate(network_price_candidates)
                            if network_price is not None:
                                payload["price_source"] = "network-post-email-stagnant"
                                payload["price_text"] = network_snippet
                                return network_price, page.url, payload
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

                if response_tasks:
                    await _drain_response_tasks()

                if network_price_candidates:
                    payload["network_price_candidates"] = network_price_candidates[-12:]

                price, price_text = await self._extract_price(page, payload=payload)
                if price is None:
                    await _drain_response_tasks()
                    network_price, network_snippet = _pick_best_network_candidate(network_price_candidates)
                    if network_price is not None:
                        payload["price_source"] = "network"
                        payload["price_text"] = network_snippet
                        return network_price, page.url, payload
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
                if response_tasks:
                    await _drain_response_tasks()
                await context.close()
                await browser.close()
                _remove_file_if_exists(storage_state_path)


__all__ = [
    "TrendDeviceValuator",
    "_detect_wizard_step",
    "_extract_contextual_price",
    "_extract_iphone_model_hint",
    "_is_email_gate_text",
    "_normalize_wizard_text",
]
