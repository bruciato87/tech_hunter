from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
import time
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import quote_plus, unquote, urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct
from tech_sniper_it.utils import decode_json_dict_maybe_base64, parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator, ValuatorRuntimeError


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
MATCH_STOPWORDS: set[str] = {
    "amazon",
    "warehouse",
    "ricondizionato",
    "ricondizionata",
    "renewed",
    "reconditioned",
    "used",
    "usato",
    "con",
    "senza",
    "with",
    "and",
    "the",
    "kit",
    "bundle",
    "pack",
}
ANCHOR_TOKENS: tuple[str, ...] = (
    "canon",
    "nikon",
    "sony",
    "fujifilm",
    "lumix",
    "panasonic",
    "dji",
    "mavic",
    "avata",
    "mini",
    "gopro",
    "insta360",
    "steam",
    "deck",
)
CAPACITY_TOKEN_PATTERN = re.compile(r"\b\d{2,4}\s*(?:gb|tb)\b", re.IGNORECASE)
MPB_NETWORK_PRICE_KEYS: tuple[str, ...] = (
    "price",
    "prezzo",
    "offer",
    "offerta",
    "valuation",
    "quote",
    "estimate",
    "stima",
    "cash",
    "payout",
    "amount",
)
MPB_NETWORK_BLOCKERS: tuple[str, ...] = (
    "fino a",
    "a partire da",
    "price drop",
    "newsletter",
    "promo",
    "codice",
    "sconto",
    "public api v2 user me",
)
_MPB_BLOCKED_UNTIL_TS = 0.0
_MPB_BLOCK_REASON = ""
_MPB_STORAGE_STATE_ERROR = ""


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _mpb_block_cooldown_seconds() -> int:
    raw = (os.getenv("MPB_BLOCK_COOLDOWN_SECONDS") or "").strip()
    try:
        value = int(raw) if raw else 1800
    except ValueError:
        value = 1800
    return max(60, min(value, 86_400))


def _mpb_block_remaining_seconds() -> int:
    remaining = int(max(0.0, _MPB_BLOCKED_UNTIL_TS - time.time()))
    return remaining


def _mark_mpb_temporarily_blocked(reason: str) -> None:
    global _MPB_BLOCKED_UNTIL_TS, _MPB_BLOCK_REASON
    cooldown = _mpb_block_cooldown_seconds()
    _MPB_BLOCKED_UNTIL_TS = time.time() + float(cooldown)
    _MPB_BLOCK_REASON = reason.strip() or "anti-bot challenge"
    print(
        "[mpb] Temporary block enabled | "
        f"cooldown_s={cooldown} reason='{_MPB_BLOCK_REASON}'"
    )


def _clear_mpb_temporary_block() -> None:
    global _MPB_BLOCKED_UNTIL_TS, _MPB_BLOCK_REASON
    _MPB_BLOCKED_UNTIL_TS = 0.0
    _MPB_BLOCK_REASON = ""


def _load_storage_state_b64() -> str | None:
    global _MPB_STORAGE_STATE_ERROR
    _MPB_STORAGE_STATE_ERROR = ""
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
        _MPB_STORAGE_STATE_ERROR = "empty"
        return None
    parsed, error = decode_json_dict_maybe_base64(raw)
    if not parsed:
        _MPB_STORAGE_STATE_ERROR = str(error or "invalid-base64-json")
        return None

    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
    try:
        json.dump(parsed, handle, ensure_ascii=False)
        handle.flush()
        return handle.name
    finally:
        handle.close()


def _mpb_require_storage_state() -> bool:
    return _env_or_default("MPB_REQUIRE_STORAGE_STATE", "true").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


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


def _extract_keyed_prices_from_text(text: str) -> list[tuple[int, float, str]]:
    if not text:
        return []
    compact = " ".join(text.split())
    rows: list[tuple[int, float, str]] = []
    for keyword in MPB_NETWORK_PRICE_KEYS:
        pattern = re.compile(rf"(?i){re.escape(keyword)}[^0-9€]{{0,42}}(\d{{2,5}}(?:[.,]\d{{1,2}})?)\s*€?")
        for match in pattern.finditer(compact):
            value = parse_eur_price(match.group(1))
            if value is None:
                raw = match.group(1).replace(".", "").replace(",", ".")
                try:
                    value = float(raw)
                except ValueError:
                    value = None
            if value is None or value <= 0 or value > 15000:
                continue
            snippet = compact[max(0, match.start() - 80) : min(len(compact), match.end() + 80)].strip()
            rows.append((56, float(value), snippet))
    return rows


def _extract_prices_from_json_blob(blob: Any, path: str = "") -> list[tuple[int, float, str]]:
    rows: list[tuple[int, float, str]] = []
    if isinstance(blob, dict):
        for key, value in blob.items():
            key_text = str(key)
            next_path = f"{path}.{key_text}" if path else key_text
            rows.extend(_extract_prices_from_json_blob(value, next_path))
        return rows
    if isinstance(blob, list):
        for index, value in enumerate(blob):
            rows.extend(_extract_prices_from_json_blob(value, f"{path}[{index}]"))
        return rows

    if isinstance(blob, bool):
        return rows
    if not isinstance(blob, (str, int, float)):
        return rows

    path_norm = _normalize_match_text(path)
    if not any(keyword in path_norm for keyword in MPB_NETWORK_PRICE_KEYS):
        return rows

    if isinstance(blob, str):
        value = parse_eur_price(blob)
        if value is None:
            raw = blob.replace(" ", "").replace(".", "").replace(",", ".")
            try:
                value = float(raw)
            except ValueError:
                value = None
    else:
        try:
            value = float(blob)
        except (TypeError, ValueError):
            value = None

    if value is not None and value > 15000 and value <= 2_000_000:
        value = value / 100.0
    if value is None or value <= 0 or value > 15000:
        return rows
    rows.append((64, float(value), f"{path}={blob}"))
    return rows


def _pick_best_mpb_network_candidate(
    candidates: list[dict[str, Any]],
    *,
    normalized_name: str,
) -> tuple[float | None, str]:
    if not candidates:
        return None, ""
    tokens = _query_tokens(normalized_name)[:6]
    ranked: list[tuple[int, float, str]] = []
    for candidate in candidates:
        try:
            value = float(candidate.get("value"))
        except (TypeError, ValueError):
            continue
        if value < 20 or value > 15000:
            continue
        snippet = str(candidate.get("snippet") or "").strip()
        url = str(candidate.get("url") or "").strip()
        if "/public-api/v2/user/me" in url.lower():
            continue
        if "count.values" in snippet.lower() or ".count." in snippet.lower():
            continue
        joined = _normalize_match_text(f"{snippet} {url}")
        token_hits = sum(1 for token in tokens if token and token in joined)
        if tokens and token_hits <= 0:
            continue
        blocker_hit = any(marker in joined for marker in MPB_NETWORK_BLOCKERS)
        if blocker_hit:
            continue
        score = int(candidate.get("score", 0)) + (token_hits * 10)
        ranked.append((score, value, snippet or url))

    if not ranked:
        return None, ""
    _score, value, snippet = max(ranked, key=lambda row: (row[0], row[1]))
    return round(value, 2), snippet[:260]


def _contains_price_hint(text: str) -> bool:
    lowered = re.sub(r"\s+", " ", text).strip().lower()
    if not lowered:
        return False
    return any(hint in lowered for hint in PRICE_HINTS)


def _normalize_match_text(value: str | None) -> str:
    raw = (value or "").lower()
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


def _trim_query_variant(value: str) -> str:
    cleaned = re.sub(r"[\[\]\(\)\|,;/]+", " ", value or "")
    cleaned = re.sub(
        r"\b(warehouse|ricondizionat[oa]?|renewed|reconditioned|usato|used|senza scatola|con scatola)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    tokens = cleaned.split(" ")
    if len(tokens) > 7:
        cleaned = " ".join(tokens[:7])
    return cleaned


def _build_query_variants(product: AmazonProduct, normalized_name: str) -> list[str]:
    variants: list[str] = []
    seen: set[str] = set()

    def _push(raw: str | None) -> None:
        value = re.sub(r"\s+", " ", (raw or "").strip())
        if len(value) < 3:
            return
        marker = value.casefold()
        if marker in seen:
            return
        seen.add(marker)
        variants.append(value)

    ean = (product.ean or "").strip()
    if ean and re.fullmatch(r"[0-9\-\s]{8,20}", ean):
        _push(ean)
    _push(normalized_name)
    _push(_trim_query_variant(normalized_name))
    _push(_trim_query_variant(product.title))
    _push(product.title)
    return variants[:5]


def _is_generic_mpb_url(url: str | None) -> bool:
    path = (urlparse(url or "").path or "").strip("/").lower()
    if not path:
        return True
    if path in {"it-it/sell", "sell", "it-it"}:
        return True
    return path.startswith("it-it/cerca")


def _assess_mpb_match(
    *,
    normalized_name: str,
    candidate_text: str,
    source_url: str | None,
) -> dict[str, Any]:
    query_norm = _normalize_match_text(normalized_name)
    parsed_url = urlparse(source_url or "")
    url_text = " ".join(
        part
        for part in (
            unquote(parsed_url.path or ""),
            unquote(parsed_url.query or ""),
        )
        if part
    )
    candidate_norm = _normalize_match_text(f"{candidate_text} {url_text}")

    ratio = SequenceMatcher(None, query_norm, candidate_norm).ratio() if query_norm and candidate_norm else 0.0
    tokens = _query_tokens(normalized_name)
    capacities = _capacity_tokens(normalized_name)
    anchors = [token for token in tokens if token in ANCHOR_TOKENS]

    required_tokens: list[str] = []
    for item in capacities:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in anchors[:2]:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in tokens:
        if item not in required_tokens:
            required_tokens.append(item)
        if len(required_tokens) >= 6:
            break

    hit_tokens = [token for token in required_tokens if token and token in candidate_norm]
    anchor_hits = [token for token in anchors if token in candidate_norm]
    capacity_hits = [token for token in capacities if token in candidate_norm.replace(" ", "")]
    token_ratio = (len(hit_tokens) / len(required_tokens)) if required_tokens else 0.0
    generic_url = _is_generic_mpb_url(source_url)

    score = int((ratio * 100) + (len(hit_tokens) * 14) + (len(anchor_hits) * 8) - (36 if generic_url else 0))
    if generic_url:
        return {
            "ok": False,
            "reason": "generic-url",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
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
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if anchors and not anchor_hits:
        return {
            "ok": False,
            "reason": "anchor-mismatch",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if token_ratio < 0.50 and ratio < 0.58:
        return {
            "ok": False,
            "reason": "low-token-similarity",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if score < 65:
        return {
            "ok": False,
            "reason": "score-too-low",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    return {
        "ok": True,
        "reason": "ok",
        "score": score,
        "ratio": round(ratio, 3),
        "token_ratio": round(token_ratio, 3),
        "hit_tokens": hit_tokens,
        "required_tokens": required_tokens,
    }


def _extract_mpb_sell_link_candidates(
    *,
    html: str,
    base_url: str,
    normalized_name: str,
    limit: int = 8,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    found: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for anchor in soup.select("a[href*='/sell/']"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(base_url, href)
        marker = full_url.lower()
        if marker in seen_urls:
            continue
        seen_urls.add(marker)
        text = anchor.get_text(" ", strip=True)
        context = " ".join(part for part in (text, href) if part)
        assessment = _assess_mpb_match(
            normalized_name=normalized_name,
            candidate_text=context,
            source_url=full_url,
        )
        ranking = int(assessment.get("score", 0)) + (35 if assessment.get("ok") else 0)
        found.append(
            {
                "url": full_url,
                "text": text[:220],
                "href": href,
                "assessment": assessment,
                "ranking": ranking,
            }
        )

    ranked = sorted(
        found,
        key=lambda item: (
            item.get("ranking", 0),
            item.get("assessment", {}).get("token_ratio", 0.0),
            item.get("assessment", {}).get("ratio", 0.0),
        ),
        reverse=True,
    )
    return ranked[: max(1, limit)]


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
        blocked_remaining = _mpb_block_remaining_seconds()
        if blocked_remaining > 0:
            reason = _MPB_BLOCK_REASON or "anti-bot challenge"
            raise ValuatorRuntimeError(
                f"MPB temporarily paused after anti-bot challenge ({reason}); retry in ~{blocked_remaining}s.",
                payload={"block_reason": reason},
            )
        max_attempts = max(1, int(_env_or_default("MPB_MAX_ATTEMPTS", "3")))
        query_candidates = _build_query_variants(product, normalized_name)
        storage_state_path = _load_storage_state_b64()
        if _mpb_require_storage_state() and storage_state_path is None:
            reason = _MPB_STORAGE_STATE_ERROR or "missing"
            raise ValuatorRuntimeError(
                f"MPB storage_state missing/invalid ({reason}); set MPB_STORAGE_STATE_B64 or disable MPB_REQUIRE_STORAGE_STATE.",
                payload={"storage_state": False, "storage_state_error": reason},
            )
        payload: dict[str, Any] = {
            "query": query_candidates[0] if query_candidates else normalized_name,
            "query_candidates": query_candidates,
            "condition_target": "Ottimo",
            "attempts": [],
            "adaptive_fallbacks": {},
            "storage_state": bool(storage_state_path),
        }
        rotate_user_agent = _env_or_default("MPB_ROTATE_USER_AGENT", "true").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        sticky_user_agent = _env_or_default("MPB_USER_AGENT", DEFAULT_USER_AGENTS[1]).strip() or DEFAULT_USER_AGENTS[1]
        payload["user_agent_strategy"] = {
            "rotate": rotate_user_agent and not bool(storage_state_path),
            "sticky_with_storage_state": bool(storage_state_path),
        }
        blocker_hits: list[str] = []
        had_unblocked_attempt = False
        network_price_candidates: list[dict[str, Any]] = []
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
                global_attempt = 0
                for query_index, query in enumerate(query_candidates, start=1):
                    for attempt in range(1, max_attempts + 1):
                        global_attempt += 1
                        if storage_state_path:
                            # Cloudflare clearance cookies are often tied to UA/session.
                            # Keep UA stable when replaying an authenticated storage state.
                            user_agent = sticky_user_agent
                        elif rotate_user_agent:
                            user_agent = DEFAULT_USER_AGENTS[(global_attempt - 1) % len(DEFAULT_USER_AGENTS)]
                        else:
                            user_agent = sticky_user_agent
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
                        response_tasks: set[asyncio.Task[Any]] = set()

                        async def _capture_response_body(response) -> None:  # noqa: ANN001
                            try:
                                url = str(getattr(response, "url", "") or "")
                                if not url:
                                    return
                                url_norm = url.lower()
                                if "mpb.com" not in url_norm:
                                    return
                                if "/public-api/v2/user/me" in url_norm:
                                    return
                                request = getattr(response, "request", None)
                                resource_type = ""
                                if request is not None:
                                    resource_type = str(getattr(request, "resource_type", "") or "").lower()
                                if resource_type not in {"xhr", "fetch"} and not any(
                                    token in url_norm for token in ("offer", "valuation", "quote", "price", "sell", "api", "graphql")
                                ):
                                    return
                                body = await response.text()
                                if not body:
                                    return

                                local_rows: list[tuple[int, float, str, str]] = []
                                contextual_value, contextual_snippet = _extract_contextual_price(body)
                                if contextual_value is not None:
                                    local_rows.append((62, contextual_value, contextual_snippet, "context"))
                                for score, value, snippet in _extract_keyed_prices_from_text(body):
                                    local_rows.append((score, value, snippet, "keyword"))

                                parsed_json: Any | None = None
                                body_stripped = body.strip()
                                if body_stripped.startswith("{") or body_stripped.startswith("["):
                                    try:
                                        parsed_json = json.loads(body_stripped)
                                    except Exception:
                                        parsed_json = None
                                if parsed_json is not None:
                                    for score, value, snippet in _extract_prices_from_json_blob(parsed_json):
                                        local_rows.append((score, value, snippet, "json"))

                                if not local_rows:
                                    return
                                for score, value, snippet, source in local_rows:
                                    snippet_norm = _normalize_match_text(snippet)
                                    if any(marker in snippet_norm for marker in MPB_NETWORK_BLOCKERS):
                                        continue
                                    network_price_candidates.append(
                                        {
                                            "url": url,
                                            "status": getattr(response, "status", None),
                                            "score": int(score),
                                            "value": float(value),
                                            "snippet": snippet[:260],
                                            "source": source,
                                            "query": query,
                                            "query_index": query_index,
                                            "attempt": attempt,
                                        }
                                    )
                                if len(network_price_candidates) > 50:
                                    del network_price_candidates[:-50]
                            except Exception:
                                return

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
                            payload["query"] = query
                            await page.goto(self.base_url, wait_until="domcontentloaded")
                            await self._accept_cookie_if_present(page)
                            blockers = await self._detect_page_blockers(page)
                            if blockers:
                                blocker_hits.extend(blockers)
                                payload["attempts"].append(
                                    {
                                        "attempt": attempt,
                                        "query_index": query_index,
                                        "query": query,
                                        "stage": "base_load",
                                        "status": "blocked",
                                        "url": page.url,
                                        "blockers": blockers,
                                    }
                                )
                                fallback = await self._direct_search_fallback(
                                    page=page,
                                    attempt=attempt,
                                    query=query,
                                    query_index=query_index,
                                    normalized_name=normalized_name,
                                    condition_selectors=condition_selectors,
                                    direct_sell_selectors=direct_sell_selectors,
                                    payload=payload,
                                )
                                if fallback["offer"] is not None:
                                    _clear_mpb_temporary_block()
                                    return fallback["offer"], fallback["url"], payload
                                network_price, network_snippet = _pick_best_mpb_network_candidate(
                                    network_price_candidates,
                                    normalized_name=normalized_name,
                                )
                                network_match = _assess_mpb_match(
                                    normalized_name=normalized_name,
                                    candidate_text=network_snippet,
                                    source_url=str(fallback.get("url") or page.url),
                                )
                                if network_price is not None and network_match.get("ok"):
                                    _clear_mpb_temporary_block()
                                    payload["query"] = query
                                    payload["query_index"] = query_index
                                    payload["price_text"] = network_snippet
                                    payload["price_source"] = "network-fallback"
                                    payload["match_quality"] = network_match
                                    payload["network_price_candidates"] = network_price_candidates[-12:]
                                    return network_price, str(fallback.get("url") or page.url), payload
                                if bool(fallback.get("unblocked")):
                                    had_unblocked_attempt = True
                                blocker_hits.extend(fallback["blockers"])
                                continue
                            had_unblocked_attempt = True

                            search_selector = await self._wait_for_search_input(
                                page,
                                selectors=search_selectors,
                                timeout_ms=10000,
                            )
                            semantic_search = False
                            if search_selector:
                                await page.locator(search_selector).first.fill(query)
                            else:
                                semantic_search = await self._fill_first_semantic(
                                    page,
                                    value=query,
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
                                        "query_index": query_index,
                                        "query": query,
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
                                    query=query,
                                    query_index=query_index,
                                    normalized_name=normalized_name,
                                    condition_selectors=condition_selectors,
                                    direct_sell_selectors=direct_sell_selectors,
                                    payload=payload,
                                )
                                if fallback["offer"] is not None:
                                    _clear_mpb_temporary_block()
                                    return fallback["offer"], fallback["url"], payload
                                network_price, network_snippet = _pick_best_mpb_network_candidate(
                                    network_price_candidates,
                                    normalized_name=normalized_name,
                                )
                                network_match = _assess_mpb_match(
                                    normalized_name=normalized_name,
                                    candidate_text=network_snippet,
                                    source_url=str(fallback.get("url") or page.url),
                                )
                                if network_price is not None and network_match.get("ok"):
                                    _clear_mpb_temporary_block()
                                    payload["query"] = query
                                    payload["query_index"] = query_index
                                    payload["price_text"] = network_snippet
                                    payload["price_source"] = "network-fallback"
                                    payload["match_quality"] = network_match
                                    payload["network_price_candidates"] = network_price_candidates[-12:]
                                    return network_price, str(fallback.get("url") or page.url), payload
                                if bool(fallback.get("unblocked")):
                                    had_unblocked_attempt = True
                                blocker_hits.extend(fallback["blockers"])
                                continue

                            await page.wait_for_timeout(1000)

                            clicked_result = await self._click_first(
                                page,
                                suggestion_selectors,
                                timeout_ms=2500,
                            )
                            if not clicked_result:
                                name_tokens = [token for token in re.split(r"\W+", query) if len(token) >= 3][:4]
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

                            match_text = await self._collect_match_text(page)
                            match = _assess_mpb_match(
                                normalized_name=normalized_name,
                                candidate_text=match_text,
                                source_url=page.url,
                            )
                            payload["match_quality"] = match

                            price, price_text = await self._extract_price(page, payload=payload)
                            ui_probe = None
                            if price is None or not match.get("ok"):
                                if price is None:
                                    network_price, network_snippet = _pick_best_mpb_network_candidate(
                                        network_price_candidates,
                                        normalized_name=normalized_name,
                                    )
                                    if network_price is not None and match.get("ok"):
                                        _clear_mpb_temporary_block()
                                        payload["query"] = query
                                        payload["query_index"] = query_index
                                        payload["price_text"] = network_snippet
                                        payload["condition_selected"] = condition_selected
                                        payload["price_source"] = "network"
                                        payload["network_price_candidates"] = network_price_candidates[-12:]
                                        return network_price, page.url, payload
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
                                    query=query,
                                    query_index=query_index,
                                    normalized_name=normalized_name,
                                    condition_selectors=condition_selectors,
                                    direct_sell_selectors=direct_sell_selectors,
                                    payload=payload,
                                )
                                if fallback["offer"] is not None:
                                    _clear_mpb_temporary_block()
                                    return fallback["offer"], fallback["url"], payload
                                network_price, network_snippet = _pick_best_mpb_network_candidate(
                                    network_price_candidates,
                                    normalized_name=normalized_name,
                                )
                                network_match = _assess_mpb_match(
                                    normalized_name=normalized_name,
                                    candidate_text=network_snippet,
                                    source_url=str(fallback.get("url") or page.url),
                                )
                                if network_price is not None and network_match.get("ok"):
                                    _clear_mpb_temporary_block()
                                    payload["query"] = query
                                    payload["query_index"] = query_index
                                    payload["price_text"] = network_snippet
                                    payload["price_source"] = "network-fallback"
                                    payload["match_quality"] = network_match
                                    payload["network_price_candidates"] = network_price_candidates[-12:]
                                    return network_price, str(fallback.get("url") or page.url), payload
                                if bool(fallback.get("unblocked")):
                                    had_unblocked_attempt = True
                                blocker_hits.extend(fallback["blockers"])

                            payload["attempts"].append(
                                {
                                    "attempt": attempt,
                                    "query_index": query_index,
                                    "query": query,
                                    "stage": "valuation",
                                    "status": (
                                        "ok"
                                        if price is not None and match.get("ok")
                                        else ("low-confidence-match" if price is not None else "price-missing")
                                    ),
                                    "condition_selected": condition_selected,
                                    "semantic_search": semantic_search,
                                    "result_opened": clicked_result,
                                    "url": page.url,
                                    "price_text": price_text,
                                    "match_quality": match,
                                    "ui_drift": ui_probe.get("drift_suspected") if ui_probe else False,
                                }
                            )
                            if price is not None and match.get("ok"):
                                _clear_mpb_temporary_block()
                                payload["query"] = query
                                payload["query_index"] = query_index
                                payload["price_text"] = price_text
                                payload["condition_selected"] = condition_selected
                                payload["price_source"] = "sell_flow"
                                return price, page.url, payload
                        finally:
                            await _drain_response_tasks()
                            if network_price_candidates:
                                payload["network_price_candidates"] = network_price_candidates[-12:]
                            try:
                                page.off("response", _on_response)
                            except Exception:
                                pass
                            await context.close()
            finally:
                await browser.close()
                _remove_file_if_exists(storage_state_path)

        if network_price_candidates:
            payload["network_price_candidates"] = network_price_candidates[-12:]
        if blocker_hits and not had_unblocked_attempt:
            _mark_mpb_temporarily_blocked("turnstile/cloudflare")
            payload["blocker_hits"] = blocker_hits[:40]
            raise ValuatorRuntimeError(
                "MPB blocked by anti-bot challenge (turnstile/cloudflare).",
                payload=payload,
                source_url=self.base_url,
            )
        if blocker_hits:
            payload["blocker_hits"] = blocker_hits[:40]
        raise ValuatorRuntimeError(
            "MPB price not found after retries.",
            payload=payload,
            source_url=self.base_url,
        )

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
        query: str,
        query_index: int,
        normalized_name: str,
        condition_selectors: list[str],
        direct_sell_selectors: list[str],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        search_url = f"https://www.mpb.com/it-it/cerca?q={quote_plus(query)}"
        payload["adaptive_fallbacks"]["direct_search"] = True
        try:
            await page.goto(search_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(900)
            await self._accept_cookie_if_present(page)
        except PlaywrightError:
            return {"offer": None, "url": None, "blockers": [], "unblocked": False}

        blockers = await self._detect_page_blockers(page)
        if blockers:
            payload["attempts"].append(
                {
                    "attempt": attempt,
                    "query_index": query_index,
                    "query": query,
                    "stage": "direct_search",
                    "status": "blocked",
                    "url": page.url,
                    "blockers": blockers,
                }
            )
            return {"offer": None, "url": None, "blockers": blockers, "unblocked": False}

        opened_sell = await self._click_first(page, direct_sell_selectors, timeout_ms=2500)
        if not opened_sell:
            name_tokens = [token for token in re.split(r"\W+", query) if len(token) >= 3][:4]
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
        match_text = await self._collect_match_text(page)
        match = _assess_mpb_match(
            normalized_name=normalized_name,
            candidate_text=match_text,
            source_url=page.url,
        )
        payload["match_quality"] = match
        price, price_text = await self._extract_price(page, payload=payload)
        payload["attempts"].append(
            {
                "attempt": attempt,
                "query_index": query_index,
                "query": query,
                "stage": "direct_search",
                "status": (
                    "ok"
                    if price is not None and match.get("ok")
                    else ("low-confidence-match" if price is not None else "price-missing")
                ),
                "url": page.url,
                "opened_sell": opened_sell,
                "condition_selected": condition_selected,
                "price_text": price_text,
                "match_quality": match,
            }
        )
        if price is not None and match.get("ok"):
            payload["price_text"] = price_text
            payload["condition_selected"] = condition_selected
            payload["price_source"] = "direct_search"
            payload["query"] = query
            payload["query_index"] = query_index
            return {"offer": price, "url": page.url, "blockers": [], "unblocked": True}
        return await self._deep_link_fallback(
            page=page,
            attempt=attempt,
            query=query,
            query_index=query_index,
            normalized_name=normalized_name,
            condition_selectors=condition_selectors,
            payload=payload,
        )

    async def _deep_link_fallback(
        self,
        *,
        page: Page,
        attempt: int,
        query: str,
        query_index: int,
        normalized_name: str,
        condition_selectors: list[str],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        payload["adaptive_fallbacks"]["direct_sell_links"] = True
        try:
            html = await page.content()
        except PlaywrightError:
            return {"offer": None, "url": None, "blockers": [], "unblocked": True}

        candidates = _extract_mpb_sell_link_candidates(
            html=html,
            base_url=page.url,
            normalized_name=normalized_name,
            limit=10,
        )
        payload["sell_link_candidates"] = [
            {
                "url": item["url"],
                "text": item["text"],
                "score": item["assessment"].get("score"),
                "ok": item["assessment"].get("ok"),
                "reason": item["assessment"].get("reason"),
            }
            for item in candidates[:6]
        ]
        if not candidates:
            return {"offer": None, "url": None, "blockers": [], "unblocked": True}

        link_limit = max(1, int(_env_or_default("MPB_DEEP_LINK_LIMIT", "4")))
        blockers_acc: list[str] = []
        for rank, candidate in enumerate(candidates[:link_limit], start=1):
            candidate_url = str(candidate.get("url") or "").strip()
            if not candidate_url:
                continue
            try:
                await page.goto(candidate_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(900)
                await self._accept_cookie_if_present(page)
            except PlaywrightError:
                continue

            blockers = await self._detect_page_blockers(page)
            if blockers:
                blockers_acc.extend(blockers)
                payload["attempts"].append(
                    {
                        "attempt": attempt,
                        "query_index": query_index,
                        "query": query,
                        "stage": "direct_sell_link",
                        "status": "blocked",
                        "rank": rank,
                        "url": page.url,
                        "blockers": blockers,
                    }
                )
                continue

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
            await page.wait_for_timeout(1000)

            match_text = await self._collect_match_text(page)
            match = _assess_mpb_match(
                normalized_name=normalized_name,
                candidate_text=match_text,
                source_url=page.url,
            )
            price, price_text = await self._extract_price(page, payload=payload)
            payload["attempts"].append(
                {
                    "attempt": attempt,
                    "query_index": query_index,
                    "query": query,
                    "stage": "direct_sell_link",
                    "status": (
                        "ok"
                        if price is not None and match.get("ok")
                        else ("low-confidence-match" if price is not None else "price-missing")
                    ),
                    "rank": rank,
                    "url": page.url,
                    "condition_selected": condition_selected,
                    "price_text": price_text,
                    "match_quality": match,
                }
            )
            if price is None or not match.get("ok"):
                continue
            payload["query"] = query
            payload["query_index"] = query_index
            payload["match_quality"] = match
            payload["price_text"] = price_text
            payload["condition_selected"] = condition_selected
            payload["price_source"] = "direct_sell_link"
            return {"offer": price, "url": page.url, "blockers": blockers_acc, "unblocked": True}

        return {"offer": None, "url": None, "blockers": blockers_acc, "unblocked": True}

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
                    chunks.append(cleaned[:280])
        chunks.append(unquote(urlparse(page.url).path))
        return " ".join(chunks)

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
        script_rows: list[tuple[int, float, str]] = []
        for script in soup.select("script"):
            script_text = ""
            if script.string:
                script_text = script.string
            elif script.get_text(strip=True):
                script_text = script.get_text(" ", strip=True)
            if not script_text or len(script_text) < 30:
                continue
            trimmed = script_text[:100000]
            for score, value, snippet in _extract_keyed_prices_from_text(trimmed):
                snippet_norm = _normalize_match_text(snippet)
                if not any(hint in snippet_norm for hint in ("offerta", "valuation", "estimate", "we pay", "ti paghiamo")):
                    continue
                script_rows.append((score, value, snippet))
            script_type = (script.get("type") or "").lower()
            raw = trimmed.strip()
            if "json" in script_type or raw.startswith("{") or raw.startswith("["):
                try:
                    parsed = json.loads(raw)
                except Exception:
                    parsed = None
                if parsed is not None:
                    for score, value, snippet in _extract_prices_from_json_blob(parsed):
                        snippet_norm = _normalize_match_text(snippet)
                        if not any(hint in snippet_norm for hint in ("offerta", "valuation", "estimate", "we pay", "ti paghiamo")):
                            continue
                        script_rows.append((score, value, snippet))
        if script_rows:
            _score, value, snippet = max(script_rows, key=lambda row: (row[0], row[1]))
            return value, snippet[:260]
        text = soup.get_text(" ", strip=True)
        price, snippet = _extract_contextual_price(text)
        if price is not None:
            return price, snippet
        return None, text[:220]
