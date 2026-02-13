from __future__ import annotations

import json
import os
import re
import tempfile
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import decode_json_dict_maybe_base64, parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator

PRICE_HINTS: tuple[str, ...] = ("ti paghiamo", "valutazione", "offerta", "ricevi", "vendi")
PRICE_BLOCKERS: tuple[str, ...] = ("ordine min", "spedizione", "cookie", "prezzo di vendita")
MATCH_STOPWORDS: set[str] = {
    "apple",
    "amazon",
    "warehouse",
    "ricondizionato",
    "ricondizionata",
    "renewed",
    "reconditionne",
    "reconditioned",
    "used",
    "usato",
    "con",
    "senza",
    "wifi",
    "wi",
    "fi",
    "pack",
    "combo",
    "with",
    "and",
    "the",
    "plus",
}
ANCHOR_TOKENS: tuple[str, ...] = (
    "iphone",
    "ipad",
    "macbook",
    "steam",
    "deck",
    "rog",
    "ally",
    "legion",
    "dji",
    "mavic",
    "avata",
    "garmin",
    "forerunner",
    "fenix",
    "epix",
    "watch",
    "playstation",
    "xbox",
)
GENERIC_REBUY_CATEGORIES: set[str] = {
    "apple",
    "samsung",
    "notebook-apple",
    "notebook",
    "smartphone",
    "tablet",
    "fotocamere",
    "fotocamera",
    "console",
}
CAPACITY_TOKEN_PATTERN = re.compile(r"\b\d{2,4}\s*(?:gb|tb)\b", re.IGNORECASE)
_REBUY_STORAGE_STATE_ERROR = ""


def _use_storage_state() -> bool:
    raw = (os.getenv("REBUY_USE_STORAGE_STATE") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _load_storage_state_b64() -> str | None:
    global _REBUY_STORAGE_STATE_ERROR
    _REBUY_STORAGE_STATE_ERROR = ""
    if not _use_storage_state():
        return None
    raw = (os.getenv("REBUY_STORAGE_STATE_B64") or "").strip()
    if not raw:
        _REBUY_STORAGE_STATE_ERROR = "empty"
        return None
    parsed, error = decode_json_dict_maybe_base64(raw)
    if not parsed:
        _REBUY_STORAGE_STATE_ERROR = str(error or "invalid-base64-json")
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


def _normalize_match_text(value: str | None) -> str:
    raw = (value or "").lower()
    raw = re.sub(r"[%/]", " ", raw)
    raw = re.sub(r"[^a-z0-9+\- ]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _query_tokens(value: str) -> list[str]:
    normalized = _normalize_match_text(value)
    tokens = [item for item in normalized.split(" ") if item]
    ranked: list[str] = []
    for token in tokens:
        if token in MATCH_STOPWORDS:
            continue
        if len(token) < 2:
            continue
        if token not in ranked:
            ranked.append(token)
    return ranked


def _capacity_tokens(value: str) -> list[str]:
    normalized = _normalize_match_text(value).replace(" ", "")
    return sorted(set(match.group(0).replace(" ", "").lower() for match in CAPACITY_TOKEN_PATTERN.finditer(normalized)))


def _is_generic_rebuy_url(url: str | None) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    if path.startswith("comprare/search") or path == "vendi":
        return True
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return True
    if len(segments) == 1:
        return True
    if len(segments) == 2 and segments[0] == "comprare" and segments[1] in GENERIC_REBUY_CATEGORIES:
        return True
    return False


def _is_search_rebuy_url(url: str | None) -> bool:
    path = (urlparse(url or "").path or "").lower()
    return "/comprare/search" in path or path.endswith("/search")


def _assess_rebuy_match(
    *,
    normalized_name: str,
    candidate_text: str,
    source_url: str | None,
) -> dict[str, Any]:
    query_norm = _normalize_match_text(normalized_name)
    parsed_url = urlparse(source_url or "")
    url_parts = " ".join(
        part
        for part in (
            unquote(parsed_url.path or ""),
            unquote(parsed_url.query or ""),
        )
        if part
    )
    candidate_norm = _normalize_match_text(f"{candidate_text} {url_parts}")

    ratio = SequenceMatcher(None, query_norm, candidate_norm).ratio() if query_norm and candidate_norm else 0.0
    tokens = _query_tokens(normalized_name)
    query_anchors = [token for token in tokens if token in ANCHOR_TOKENS]
    capacities = _capacity_tokens(normalized_name)

    required_tokens: list[str] = []
    for item in capacities:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in query_anchors[:2]:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in tokens:
        if item.isdigit() or re.search(r"\d", item):
            if item not in required_tokens:
                required_tokens.append(item)
    for item in tokens:
        if item not in required_tokens:
            required_tokens.append(item)
        if len(required_tokens) >= 6:
            break

    hit_tokens = [token for token in required_tokens if token and token in candidate_norm]
    anchor_hits = [token for token in query_anchors if token in candidate_norm]
    capacity_hits = [token for token in capacities if token in candidate_norm.replace(" ", "")]
    token_ratio = (len(hit_tokens) / len(required_tokens)) if required_tokens else 0.0
    generic_url = _is_generic_rebuy_url(source_url)
    score = int((ratio * 100) + (len(hit_tokens) * 14) + (len(anchor_hits) * 8) - (40 if generic_url else 0))

    if _is_search_rebuy_url(source_url):
        return {
            "ok": False,
            "reason": "generic-search-url",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if generic_url:
        return {
            "ok": False,
            "reason": "generic-category-url",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }

    if capacities and len(capacity_hits) < len(capacities):
        return {
            "ok": False,
            "reason": "capacity-mismatch",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if query_anchors and not anchor_hits:
        return {
            "ok": False,
            "reason": "anchor-mismatch",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if token_ratio < 0.55 and ratio < 0.60:
        return {
            "ok": False,
            "reason": "low-token-similarity",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if score < 72:
        return {
            "ok": False,
            "reason": "score-too-low",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    return {
        "ok": True,
        "reason": "ok",
        "score": score,
        "ratio": round(ratio, 3),
        "token_ratio": round(token_ratio, 3),
        "generic_url": generic_url,
        "hit_tokens": hit_tokens,
        "required_tokens": required_tokens,
    }


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
        storage_state_path = _load_storage_state_b64()
        payload: dict[str, Any] = {
            "query": query,
            "condition_target": "Come nuovo",
            "adaptive_fallbacks": {},
            "storage_state": bool(storage_state_path),
        }
        if _use_storage_state() and storage_state_path is None:
            payload["storage_state_error"] = _REBUY_STORAGE_STATE_ERROR or "missing"
            print(
                "[rebuy] storage_state missing/invalid | "
                f"reason={payload['storage_state_error']}"
            )

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context_kwargs: dict[str, Any] = {"locale": "it-IT"}
            if storage_state_path:
                context_kwargs["storage_state"] = storage_state_path
            context = await browser.new_context(**context_kwargs)
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
                opened = await self._open_best_result(page, result_selectors, normalized_name, payload=payload)
                if not opened:
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
                await page.wait_for_timeout(1200)

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

                match_text = await self._collect_match_text(page)
                match = _assess_rebuy_match(
                    normalized_name=normalized_name,
                    candidate_text=match_text,
                    source_url=page.url,
                )
                payload["match_quality"] = match
                if not match.get("ok"):
                    reason = match.get("reason", "low-confidence")
                    raise RuntimeError(
                        f"Rebuy low-confidence match ({reason}); discarded to prevent false-positive."
                    )

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
                _remove_file_if_exists(storage_state_path)

    async def _open_best_result(
        self,
        page: Page,
        selectors: list[str],
        normalized_name: str,
        *,
        payload: dict[str, Any],
    ) -> bool:
        candidates: list[dict[str, Any]] = []
        for selector in selectors:
            locator = page.locator(selector)
            try:
                count = min(await locator.count(), 12)
            except PlaywrightError:
                continue
            for index in range(count):
                node = locator.nth(index)
                try:
                    if not await node.is_visible():
                        continue
                    text = await node.inner_text(timeout=1200)
                    href = await node.get_attribute("href")
                except PlaywrightError:
                    continue
                assessment = _assess_rebuy_match(
                    normalized_name=normalized_name,
                    candidate_text=text or "",
                    source_url=href,
                )
                ranking = int(assessment.get("score", 0)) + (35 if assessment.get("ok") else 0)
                candidates.append(
                    {
                        "selector": selector,
                        "index": index,
                        "text": (text or "").strip()[:220],
                        "href": href,
                        "assessment": assessment,
                        "ranking": ranking,
                    }
                )

        if not candidates:
            return False

        ranked = sorted(
            candidates,
            key=lambda item: (item.get("ranking", 0), item.get("assessment", {}).get("token_ratio", 0.0)),
            reverse=True,
        )
        payload["result_candidates"] = [
            {
                "score": row["assessment"].get("score"),
                "ok": row["assessment"].get("ok"),
                "token_ratio": row["assessment"].get("token_ratio"),
                "reason": row["assessment"].get("reason"),
                "href": row.get("href"),
                "text": row.get("text"),
            }
            for row in ranked[:4]
        ]

        for row in ranked[:4]:
            node = page.locator(row["selector"]).nth(int(row["index"]))
            try:
                await node.click(timeout=2800)
                payload["result_pick"] = {
                    "selector": row["selector"],
                    "index": row["index"],
                    "assessment": row["assessment"],
                    "href": row.get("href"),
                    "text": row.get("text"),
                }
                return True
            except PlaywrightError:
                continue
        return False

    async def _collect_match_text(self, page: Page) -> str:
        chunks: list[str] = []
        try:
            chunks.append(await page.title())
        except PlaywrightError:
            pass
        for selector in ("h1", "h2", "[data-testid*='title' i]", "main"):
            try:
                locator = page.locator(selector)
                count = min(await locator.count(), 3)
            except PlaywrightError:
                continue
            for index in range(count):
                try:
                    text = await locator.nth(index).inner_text(timeout=900)
                except PlaywrightError:
                    continue
                cleaned = re.sub(r"\s+", " ", text).strip()
                if cleaned:
                    chunks.append(cleaned[:300])
        chunks.append(unquote(urlparse(page.url).path))
        return " ".join(chunks)

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


__all__ = [
    "RebuyValuator",
    "_assess_rebuy_match",
    "_extract_contextual_price",
    "_load_storage_state_b64",
    "_remove_file_if_exists",
]
