from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
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

PRICE_HINTS: tuple[str, ...] = (
    "ti paghiamo",
    "valutazione",
    "offerta",
    "ricevi",
    "vendi",
    # Cash-out hints (prefer these over promo/store credit).
    "pagamento diretto",
    "bonifico",
    "paypal",
    "conto corrente",
)
PRICE_BLOCKERS: tuple[str, ...] = (
    "ordine min",
    "spedizione",
    "cookie",
    "prezzo di vendita",
    # Avoid overestimations: promo/store credit are not immediate cash-out.
    "codice promozionale",
    "buono rebuy",
    "buono",
    "gift card",
    "promo",
)
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
REBUY_PRODUCT_ID_PATTERN = re.compile(r"^\d{4,}$")
REBUY_SELL_ID_SUFFIX_PATTERN = re.compile(r"_\d{4,}$")
REBUY_SELL_PRODUCT_ID_PATTERN = re.compile(r"^\d{4,}$")
REBUY_EMBEDDED_URL_PATTERN = re.compile(
    r"(?:https?://www\.rebuy\.it)?/(?:vendere/p/[a-z0-9\-_%]+/\d{4,}|vendere/[a-z0-9\-_%]+/[a-z0-9\-_%]*_\d{4,})",
    flags=re.IGNORECASE,
)
_REBUY_STORAGE_STATE_ERROR = ""


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _use_storage_state() -> bool:
    raw = (os.getenv("REBUY_USE_STORAGE_STATE") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _rebuy_deep_link_limit() -> int:
    raw = (_env_or_default("REBUY_DEEP_LINK_LIMIT", "4") or "").strip()
    try:
        value = int(raw) if raw else 4
    except ValueError:
        value = 4
    return max(1, min(value, 10))


def _rebuy_wizard_max_steps() -> int:
    raw = (_env_or_default("REBUY_WIZARD_MAX_STEPS", "4") or "").strip()
    try:
        value = int(raw) if raw else 4
    except ValueError:
        value = 4
    return max(2, min(value, 8))


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


def _extract_rebuy_cash_payout(text: str) -> tuple[float | None, str]:
    """Extract the immediate cash-out offer (Pagamento Diretto / bank transfer).

    Rebuy often shows multiple amounts (cash, promo, store credit). We want the *cash* figure.
    """

    if not text:
        return None, ""
    money = r"\\d{1,3}(?:[.\\s]\\d{3})*(?:,\\d{2})?\\s*€"
    patterns = [
        rf"Pagamento\\s*Diretto[^\\d]{{0,120}}({money})",
        rf"Pagamento\\s*immediato[^\\d]{{0,120}}({money})",
        rf"Bonifico[^\\d]{{0,120}}({money})",
        rf"PayPal[^\\d]{{0,120}}({money})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        snippet = match.group(0).strip()
        value = parse_eur_price(match.group(1))
        if value is None or value <= 0 or value > 10000:
            continue
        # Ignore promo/store-credit snippets even if they match.
        lowered = snippet.lower()
        if any(blocker in lowered for blocker in ("codice promozionale", "buono", "gift", "promo")):
            continue
        return value, snippet[:240]
    return None, ""


def _pick_best_rebuy_network_candidate(
    candidates: list[dict[str, Any]],
    *,
    normalized_name: str | None = None,
) -> tuple[float | None, str]:
    if not candidates:
        return None, ""
    tokens = _query_tokens(normalized_name or "")[:6] if normalized_name else []
    ranked: list[tuple[int, float, str]] = []
    for item in candidates:
        try:
            value = float(item.get("price"))
        except (TypeError, ValueError):
            continue
        if value <= 0 or value > 10000:
            continue
        snippet = str(item.get("snippet") or "").strip()
        snippet_norm = snippet.lower()
        url_norm = str(item.get("url") or "").lower()
        joined_norm = _normalize_match_text(f"{snippet} {unquote(urlparse(url_norm).path or '')} {unquote(urlparse(url_norm).query or '')}")
        if tokens:
            token_hits = sum(1 for token in tokens if token and token in joined_norm)
            if token_hits <= 0:
                continue
        else:
            token_hits = 0
        if any(marker in snippet_norm for marker in ("newsletter", "sconto", "codice", "coupon", "iscriviti")):
            continue
        source = str(item.get("source") or "").strip().lower()
        score = 0
        if "pagamento diretto" in snippet_norm:
            score += 40
        if "bonifico" in snippet_norm or "paypal" in snippet_norm:
            score += 20
        if "codice promozionale" in snippet_norm or "buono" in snippet_norm:
            score -= 25
        if "/vendere/" in url_norm:
            score += 10
        if int(item.get("status", 0) or 0) == 200:
            score += 3
        if source.startswith("network-context") and token_hits < 2:
            continue
        score += token_hits * 10
        ranked.append((score, value, snippet))
    if not ranked:
        return None, ""
    _, value, snippet = max(ranked, key=lambda row: (row[0], row[1]))
    return value, snippet


def _rebuy_wizard_state(text: str) -> str:
    normalized = re.sub(r"\s+", " ", (text or "")).strip().lower()
    if not normalized:
        return "empty"
    if "pagamento diretto" in normalized or "offerta preliminare" in normalized:
        return "offer"
    if "3 di 3" in normalized:
        return "step3"
    if "2 di 3" in normalized:
        return "step2"
    if "1 di 3" in normalized:
        return "step1"
    if "che aspetto ha il dispositivo" in normalized:
        return "condition"
    return "unknown"


def _extract_embedded_rebuy_urls(html: str, *, base_url: str) -> list[str]:
    if not html:
        return []
    raw = html.replace("\\/", "/")
    urls: list[str] = []
    seen: set[str] = set()
    for match in REBUY_EMBEDDED_URL_PATTERN.finditer(raw):
        candidate = str(match.group(0) or "").strip().strip("\"'")
        if not candidate:
            continue
        absolute = urljoin(base_url, candidate)
        marker = absolute.casefold()
        if marker in seen:
            continue
        seen.add(marker)
        urls.append(absolute)
    return urls


def _extract_rebuy_product_link_candidates(
    *,
    html: str,
    base_url: str,
    normalized_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    root = soup.select_one("main") or soup
    found: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for anchor in root.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        if href.startswith(("javascript:", "mailto:")):
            continue
        href_lower = href.lower()
        if "/comprare/" not in href_lower and "/vendere/" not in href_lower and "/vendi/" not in href_lower:
            continue
        if "/comprare/search" in href_lower:
            continue
        if "/vendere/cerca" in href_lower:
            continue
        full_url = urljoin(base_url, href)
        # Only keep product-like links. Header/category anchors are too noisy and cause false positives.
        parsed = urlparse(full_url)
        segments = [segment for segment in (parsed.path or "").strip("/").lower().split("/") if segment]
        if not segments:
            continue
        if segments[0] == "comprare":
            if len(segments) < 3 or not REBUY_PRODUCT_ID_PATTERN.fullmatch(segments[-1] or ""):
                continue
        if segments[0] == "vendere":
            # Rebuy's sell flow has at least two URL shapes:
            # - Legacy: /vendere/<category>/<slug>_<id>
            # - Current (Angular): /vendere/p/<slug>/<id>[?from=...]
            is_legacy = bool(REBUY_SELL_ID_SUFFIX_PATTERN.search(segments[-1] or ""))
            is_modern = (
                len(segments) >= 4
                and segments[1] == "p"
                and bool(REBUY_SELL_PRODUCT_ID_PATTERN.fullmatch(segments[-1] or ""))
            )
            if not (is_legacy or is_modern):
                continue
        if segments[0] == "vendi" and len(segments) <= 1:
            continue
        marker = full_url.lower()
        if marker in seen_urls:
            continue
        seen_urls.add(marker)
        text = anchor.get_text(" ", strip=True)
        context = " ".join(part for part in (text, href) if part)
        assessment = _assess_rebuy_match(
            normalized_name=normalized_name,
            candidate_text=context,
            source_url=full_url,
        )
        # We use the assessment only for ranking: final match is validated after navigation.
        ranking = int(assessment.get("score", 0)) + (35 if assessment.get("ok") else 0)
        if assessment.get("reason") in {"generic-search-url", "generic-category-url"}:
            ranking -= 80
        found.append(
            {
                "url": full_url,
                "text": text[:220],
                "href": href,
                "assessment": assessment,
                "ranking": ranking,
            }
        )

    # SPA pages sometimes render results without clickable anchors in static HTML;
    # fallback to product URLs embedded in script payloads.
    if len(found) < max(2, limit // 2):
        for url in _extract_embedded_rebuy_urls(html, base_url=base_url):
            marker = url.casefold()
            if marker in seen_urls:
                continue
            seen_urls.add(marker)
            assessment = _assess_rebuy_match(
                normalized_name=normalized_name,
                candidate_text=unquote(urlparse(url).path or ""),
                source_url=url,
            )
            ranking = int(assessment.get("score", 0)) + (20 if assessment.get("ok") else -20)
            found.append(
                {
                    "url": url,
                    "text": unquote(urlparse(url).path or "")[:220],
                    "href": url,
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
    if path.startswith("comprare/search") or path.startswith("vendere/cerca") or path in {"vendi", "vendere"}:
        return True
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return True
    if len(segments) == 1:
        return True
    if segments[0] == "vendere":
        # /vendere/<category> and other non-product pages are generic.
        # Accept both:
        # - /vendere/<category>/<slug>_<id>
        # - /vendere/p/<slug>/<id>
        if REBUY_SELL_ID_SUFFIX_PATTERN.search(segments[-1] or ""):
            return False
        if len(segments) >= 4 and segments[1] == "p" and REBUY_SELL_PRODUCT_ID_PATTERN.fullmatch(segments[-1] or ""):
            return False
        return True
    if segments[0] == "comprare":
        # /comprare/<category> (and similar) are never product pages.
        if len(segments) == 2:
            return True
        # Treat /comprare/* as product only if the last segment is a numeric id.
        if len(segments) >= 3 and not REBUY_PRODUCT_ID_PATTERN.fullmatch(segments[-1] or ""):
            return True
    return False


def _is_search_rebuy_url(url: str | None) -> bool:
    path = (urlparse(url or "").path or "").lower()
    return "/comprare/search" in path or path.startswith("/vendere/cerca") or path.endswith("/search")


def _absolutize_rebuy_url(url: str | None) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return urljoin("https://www.rebuy.it", raw)


def _resolve_rebuy_source_url(current_url: str | None, payload: dict[str, Any]) -> str | None:
    """Rebuy is partially SPA-driven; page.url can remain /vendere even after selecting a product.

    Prefer the clicked href (result_pick) or the deep-link URL (deep_link_pick) to keep verification strict.
    """
    current = (current_url or "").strip()
    if current and not _is_generic_rebuy_url(current) and not _is_search_rebuy_url(current):
        return current

    deep_pick = payload.get("deep_link_pick")
    if isinstance(deep_pick, dict):
        picked = str(deep_pick.get("url") or "").strip()
        if picked:
            picked = _absolutize_rebuy_url(picked) or picked
            if picked and not _is_generic_rebuy_url(picked) and not _is_search_rebuy_url(picked):
                return picked

    result_pick = payload.get("result_pick")
    if isinstance(result_pick, dict):
        picked = _absolutize_rebuy_url(result_pick.get("href"))
        if picked and not _is_generic_rebuy_url(picked) and not _is_search_rebuy_url(picked):
            return picked

    return current or None


def _assess_rebuy_match(
    *,
    normalized_name: str,
    candidate_text: str,
    source_url: str | None,
) -> dict[str, Any]:
    query_norm = _normalize_match_text(normalized_name)
    parsed_url = urlparse(source_url or "")
    path = (parsed_url.path or "").strip("/").lower()
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
    strong_generic_match = (
        generic_url
        and token_ratio >= 0.72
        and (ratio >= 0.60 or len(hit_tokens) >= 3)
        and (not capacities or len(capacity_hits) >= len(capacities))
        and (not query_anchors or bool(anchor_hits))
    )
    score = int((ratio * 100) + (len(hit_tokens) * 14) + (len(anchor_hits) * 8) - (40 if generic_url else 0))
    if strong_generic_match:
        score += 18

    if _is_search_rebuy_url(source_url):
        return {
            "ok": False,
            "reason": "generic-search-url",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "generic_override": False,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if generic_url and not strong_generic_match:
        return {
            "ok": False,
            "reason": "generic-category-url",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "generic_override": strong_generic_match,
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
            "generic_override": strong_generic_match,
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
            "generic_override": strong_generic_match,
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
            "generic_override": strong_generic_match,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    score_floor = 72
    if not generic_url and (query_anchors or capacities):
        score_floor = 62
    if not generic_url and token_ratio >= 0.6:
        score_floor = min(score_floor, 54)
    if path.startswith("vendere/p/"):
        score_floor = min(score_floor, 58)
    if score < score_floor:
        return {
            "ok": False,
            "reason": "score-too-low",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "generic_override": strong_generic_match,
            "score_floor": score_floor,
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
        "generic_override": strong_generic_match,
        "hit_tokens": hit_tokens,
        "required_tokens": required_tokens,
    }


class RebuyValuator(BaseValuator):
    platform_name = "rebuy"
    condition_label = "come_nuovo"
    base_url = "https://www.rebuy.it/vendere"

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
            network_price_candidates: list[dict[str, Any]] = []
            response_tasks: set[asyncio.Task[Any]] = set()

            async def _capture_response_body(response) -> None:  # noqa: ANN001
                try:
                    url = str(getattr(response, "url", "") or "")
                    if not url:
                        return
                    url_norm = url.lower()
                    request = getattr(response, "request", None)
                    resource_type = ""
                    if request is not None:
                        resource_type = str(getattr(request, "resource_type", "") or "").lower()
                    if resource_type not in {"xhr", "fetch"} and not any(
                        token in url_norm for token in ("offer", "offert", "quote", "valuat", "price", "/api/", "graphql", "vendere")
                    ):
                        return
                    body = await response.text()
                    if not body:
                        return
                    price, snippet = _extract_rebuy_cash_payout(body)
                    source = "network-cash"
                    if price is None:
                        price, snippet = _extract_contextual_price(body)
                        source = "network-context"
                    if price is None:
                        return
                    snippet_norm = re.sub(r"\s+", " ", (snippet or "")).strip().lower()
                    if source == "network-context":
                        # Contextual prices are noisy; accept only if they look like explicit buyback cash-out.
                        if not any(
                            term in snippet_norm
                            for term in ("pagamento diretto", "offerta preliminare", "ti paghiamo", "ricevi")
                        ):
                            return
                    if any(blocker in snippet_norm for blocker in ("fino a", "a partire da", "promo", "buono")):
                        return
                    network_price_candidates.append(
                        {
                            "price": float(price),
                            "snippet": snippet[:240],
                            "url": url,
                            "status": getattr(response, "status", None),
                            "source": source,
                        }
                    )
                    if len(network_price_candidates) > 40:
                        del network_price_candidates[:-40]
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
                search_url = f"{self.base_url}/cerca?query={quote_plus(str(query))}"
                await page.goto(search_url, wait_until="domcontentloaded")
                await self._accept_cookie_if_present(page)
                await page.wait_for_timeout(1200)

                deep_opened = await self._open_best_result_deep_link(page, normalized_name, payload=payload)
                if not deep_opened:
                    await page.wait_for_timeout(1500)
                    deep_opened = await self._open_best_result_deep_link(page, normalized_name, payload=payload)
                payload["adaptive_fallbacks"]["deep_link_opened"] = deep_opened

                result_selectors = self._selector_candidates(
                    site=self.platform_name,
                    slot="result_open",
                    defaults=[
                        "[data-testid*='product-card'] a",
                        "a[href*='/vendere/p/']",
                        "a[href*='/vendere/']",
                        "a[href*='/offer']",
                        ".product-card a",
                        "li a:has-text('GB')",
                    ],
                    payload=payload,
                )
                opened = deep_opened or await self._open_best_result(page, result_selectors, normalized_name, payload=payload)
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
                await self._accept_cookie_if_present(page)

                # Rebuy sell flow is a multi-step wizard. Some products start with a yes/no
                # functionality check before the condition step appears.
                try:
                    has_condition = await page.locator("text=/Come nuovo/i").first.is_visible(timeout=1200)
                except PlaywrightError:
                    has_condition = False
                if not has_condition:
                    progressed = await self._click_first_semantic(
                        page,
                        keywords=["sì", "si", "yes"],
                        selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                        timeout_ms=2800,
                    )
                    if progressed:
                        payload.setdefault("wizard", []).append({"step": "yes_no", "selected": "si"})
                        await page.wait_for_timeout(1600)

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

                resolved_source_url = _resolve_rebuy_source_url(page.url, payload)
                payload["resolved_source_url"] = resolved_source_url

                match_text = await self._collect_match_text(page)
                try:
                    body_text = await page.inner_text("body", timeout=1800)
                except PlaywrightError:
                    body_text = ""
                payload["wizard_state_before_match"] = _rebuy_wizard_state(body_text)
                match = _assess_rebuy_match(
                    normalized_name=normalized_name,
                    candidate_text=match_text,
                    source_url=resolved_source_url or page.url,
                )
                payload["match_quality"] = match
                if not match.get("ok"):
                    reason = match.get("reason", "low-confidence")
                    score = match.get("score")
                    token_ratio = match.get("token_ratio")
                    ratio = match.get("ratio")
                    detail = f"{reason};score={score};token_ratio={token_ratio};ratio={ratio}"
                    if reason in {"generic-search-url", "generic-category-url"}:
                        rescue = await self._deep_link_rescue(
                            page=page,
                            normalized_name=normalized_name,
                            condition_selectors=condition_selectors,
                            payload=payload,
                            network_price_candidates=network_price_candidates,
                        )
                        if rescue["offer"] is not None:
                            payload["price_source"] = "deep_link_rescue"
                            return rescue["offer"], rescue["url"], payload
                    raise ValuatorRuntimeError(
                        f"Rebuy low-confidence match ({detail}); discarded to prevent false-positive.",
                        payload=payload,
                        source_url=resolved_source_url or page.url,
                    )

                # Complete remaining wizard steps (e.g. accessories) until we see an offer.
                price = None
                price_text = ""
                wizard_max_steps = _rebuy_wizard_max_steps()
                for step_attempt in range(1, wizard_max_steps + 1):
                    network_price, network_snippet = _pick_best_rebuy_network_candidate(
                        network_price_candidates,
                        normalized_name=normalized_name,
                    )
                    if network_price is not None:
                        payload["price_source"] = "network"
                        payload["price_text"] = network_snippet
                        payload["network_price_candidates"] = network_price_candidates[-12:]
                        return network_price, resolved_source_url or page.url, payload

                    try:
                        step_body = await page.inner_text("body", timeout=1600)
                    except PlaywrightError:
                        step_body = ""
                    step_compact = re.sub(r"\s+", " ", step_body).strip()
                    payload.setdefault("wizard_states", []).append(
                        {"attempt": step_attempt, "state": _rebuy_wizard_state(step_body), "excerpt": step_compact[:140]}
                    )
                    price, price_text = await self._extract_price(page, payload=payload)
                    if price is not None:
                        break

                    await self._accept_cookie_if_present(page)

                    # If we are on the condition step, pick "Come nuovo" before trying to advance with yes/no.
                    condition_progress = await self._click_first_semantic(
                        page,
                        keywords=["come nuovo"],
                        timeout_ms=2000,
                    )
                    if condition_progress:
                        payload["condition_selected"] = True
                        payload.setdefault("wizard", []).append({"step": "condition", "attempt": step_attempt, "selected": "come_nuovo"})
                        await page.wait_for_timeout(1600)
                        continue

                    progressed = await self._click_first_semantic(
                        page,
                        keywords=["sì", "si", "yes"],
                        selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                        timeout_ms=2600,
                    )
                    if not progressed:
                        progressed = await self._click_first_semantic(
                            page,
                            keywords=["no", "non", "nein"],
                            selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                            timeout_ms=2000,
                        )
                    if not progressed:
                        progressed = await self._click_first(
                            page,
                            selectors=[
                                "button:has-text('Continua')",
                                "button:has-text('Avanti')",
                                "button:has-text('Weiter')",
                                "[role='button']:has-text('Continua')",
                                "[role='button']:has-text('Avanti')",
                            ],
                            timeout_ms=2600,
                        )
                    if not progressed:
                        progressed = await self._click_first(
                            page,
                            selectors=[
                                "label:has(input[type='radio'])",
                                "label:has(input[type='checkbox'])",
                                "div[role='option']",
                                "li[role='option']",
                                "[data-testid*='option' i]",
                            ],
                            timeout_ms=2200,
                        )
                    if not progressed:
                        progressed = await self._click_first_semantic(
                            page,
                            keywords=["vendi", "valuta", "continua", "inizia", "start"],
                            selectors=["button", "[role='button']", "a", "label", "div"],
                            timeout_ms=1800,
                        )
                    if progressed:
                        payload.setdefault("wizard", []).append({"step": "auto", "attempt": step_attempt, "action": "progress"})
                        await page.wait_for_timeout(1600)
                    else:
                        # Give the UI one last chance to render the offer (SPA updates can lag behind clicks).
                        await page.wait_for_timeout(1800)
                        price, price_text = await self._extract_price(page, payload=payload)
                        if price is not None:
                            break
                        break

                payload["price_text"] = price_text
                if price is None:
                    network_price, network_snippet = _pick_best_rebuy_network_candidate(
                        network_price_candidates,
                        normalized_name=normalized_name,
                    )
                    if network_price is not None:
                        payload["price_source"] = "network"
                        payload["price_text"] = network_snippet
                        payload["network_price_candidates"] = network_price_candidates[-12:]
                        return network_price, resolved_source_url or page.url, payload
                    await self._attach_ui_probe(
                        payload=payload,
                        page=page,
                        site=self.platform_name,
                        stage="price_missing",
                        expected_keywords=["rebuy", "vendere", "offerta", "pagamento", "€"],
                    )
                    last_state = payload.get("wizard_states", [])[-1]["state"] if payload.get("wizard_states") else "unknown"
                    raise ValuatorRuntimeError(
                        f"Rebuy price not found after adaptive fallbacks (wizard_state={last_state}).",
                        payload=payload,
                        source_url=resolved_source_url or page.url,
                    )
                payload["price_source"] = str(payload.get("price_source") or "dom")
                return price, resolved_source_url or page.url, payload
            finally:
                await _drain_response_tasks()
                if network_price_candidates:
                    payload["network_price_candidates"] = network_price_candidates[-12:]
                try:
                    page.off("response", _on_response)
                except Exception:
                    pass
                await context.close()
                await browser.close()
                _remove_file_if_exists(storage_state_path)

    async def _open_best_result_deep_link(self, page: Page, normalized_name: str, *, payload: dict[str, Any]) -> bool:
        candidates: list[dict[str, Any]] = []
        for wait_ms in (0, 900, 1600):
            if wait_ms:
                await page.wait_for_timeout(wait_ms)
            try:
                html = await page.content()
            except PlaywrightError:
                continue
            candidates = _extract_rebuy_product_link_candidates(
                html=html,
                base_url=page.url,
                normalized_name=normalized_name,
                limit=12,
            )
            if candidates:
                break
        payload["deep_link_candidates"] = [
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
            return False

        limit = _rebuy_deep_link_limit()
        for rank, candidate in enumerate(candidates[:limit], start=1):
            url = str(candidate.get("url") or "").strip()
            if not url:
                continue
            if _is_generic_rebuy_url(url) or _is_search_rebuy_url(url):
                continue
            try:
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_timeout(900)
                await self._accept_cookie_if_present(page)
            except PlaywrightError:
                continue
            payload["deep_link_pick"] = {
                "rank": rank,
                "url": url,
                "assessment": candidate.get("assessment"),
                "text": candidate.get("text"),
            }
            print(
                "[rebuy] Deep-link navigation | "
                f"rank={rank}/{limit} url='{url}' score={candidate.get('assessment', {}).get('score')}"
            )
            return True
        return False

    async def _deep_link_rescue(
        self,
        *,
        page: Page,
        normalized_name: str,
        condition_selectors: list[str],
        payload: dict[str, Any],
        network_price_candidates: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload["adaptive_fallbacks"]["deep_link_rescue"] = True
        opened = await self._open_best_result_deep_link(page, normalized_name, payload=payload)
        if not opened:
            return {"offer": None, "url": None}

        await self._accept_cookie_if_present(page)

        try:
            has_condition = await page.locator("text=/Come nuovo/i").first.is_visible(timeout=1200)
        except PlaywrightError:
            has_condition = False
        if not has_condition:
            progressed = await self._click_first_semantic(
                page,
                keywords=["sì", "si", "yes"],
                selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                timeout_ms=2600,
            )
            if progressed:
                payload.setdefault("wizard", []).append({"step": "yes_no", "selected": "si"})
                await page.wait_for_timeout(1600)

        condition_selected = await self._click_first(page, condition_selectors, timeout_ms=5000)
        if not condition_selected:
            condition_selected = await self._click_first_semantic(
                page,
                keywords=["come nuovo", "grade a", "ottimo", "excellent"],
                timeout_ms=2600,
            )
        payload["condition_selected"] = condition_selected
        await page.wait_for_timeout(1400)

        match_text = await self._collect_match_text(page)
        resolved_source_url = _resolve_rebuy_source_url(page.url, payload)
        payload["resolved_source_url"] = resolved_source_url
        match = _assess_rebuy_match(
            normalized_name=normalized_name,
            candidate_text=match_text,
            source_url=resolved_source_url or page.url,
        )
        payload["match_quality"] = match
        if not match.get("ok"):
            return {"offer": None, "url": None}

        price = None
        price_text = ""
        wizard_max_steps = _rebuy_wizard_max_steps()
        for step_attempt in range(1, wizard_max_steps + 1):
            network_price, network_snippet = _pick_best_rebuy_network_candidate(
                network_price_candidates or [],
                normalized_name=normalized_name,
            )
            if network_price is not None:
                payload["price_source"] = "deep_link_rescue_network"
                payload["price_text"] = network_snippet
                if network_price_candidates:
                    payload["network_price_candidates"] = network_price_candidates[-12:]
                return {"offer": network_price, "url": resolved_source_url or page.url}
            try:
                step_body = await page.inner_text("body", timeout=1600)
            except PlaywrightError:
                step_body = ""
            step_compact = re.sub(r"\s+", " ", step_body).strip()
            payload.setdefault("wizard_states", []).append(
                {"attempt": step_attempt, "state": _rebuy_wizard_state(step_body), "excerpt": step_compact[:140]}
            )
            price, price_text = await self._extract_price(page, payload=payload)
            if price is not None:
                break

            await self._accept_cookie_if_present(page)

            condition_progress = await self._click_first_semantic(
                page,
                keywords=["come nuovo"],
                timeout_ms=2000,
            )
            if condition_progress:
                payload["condition_selected"] = True
                payload.setdefault("wizard", []).append({"step": "condition", "attempt": step_attempt, "selected": "come_nuovo"})
                await page.wait_for_timeout(1600)
                continue
            progressed = await self._click_first_semantic(
                page,
                keywords=["sì", "si", "yes"],
                selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                timeout_ms=2600,
            )
            if not progressed:
                progressed = await self._click_first_semantic(
                    page,
                    keywords=["no", "non", "nein"],
                    selectors=["button", "[role='button']", "a", "label", "li", "div[role='option']"],
                    timeout_ms=2000,
                )
            if not progressed:
                progressed = await self._click_first(
                    page,
                    selectors=[
                        "button:has-text('Continua')",
                        "button:has-text('Avanti')",
                        "button:has-text('Weiter')",
                        "[role='button']:has-text('Continua')",
                        "[role='button']:has-text('Avanti')",
                    ],
                    timeout_ms=2600,
                )
            if not progressed:
                progressed = await self._click_first(
                    page,
                    selectors=[
                        "label:has(input[type='radio'])",
                        "label:has(input[type='checkbox'])",
                        "div[role='option']",
                        "li[role='option']",
                        "[data-testid*='option' i]",
                    ],
                    timeout_ms=2200,
                )
            if not progressed:
                progressed = await self._click_first_semantic(
                    page,
                    keywords=["vendi", "valuta", "continua", "inizia", "start"],
                    selectors=["button", "[role='button']", "a", "label", "div"],
                    timeout_ms=1800,
                )
            if not progressed:
                await page.wait_for_timeout(1800)
                price, price_text = await self._extract_price(page, payload=payload)
                if price is not None:
                    break
                break
            payload.setdefault("wizard", []).append({"step": "auto", "attempt": step_attempt, "action": "progress"})
            await page.wait_for_timeout(1600)
        payload["price_text"] = price_text
        if price is None:
            return {"offer": None, "url": None}
        payload["price_source"] = "deep_link_rescue_dom"
        return {"offer": price, "url": resolved_source_url or page.url}

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

        # Prefer non-generic candidates for first click attempt.
        primary = [row for row in ranked if row.get("assessment", {}).get("reason") not in {"generic-search-url", "generic-category-url"}]
        if not primary:
            primary = ranked

        for row in primary[:4]:
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
        # Prefer immediate cash-out offers over promo/store credit.
        try:
            body_text = await page.inner_text("body", timeout=2200)
        except PlaywrightError:
            body_text = ""
        cash_value, cash_snippet = _extract_rebuy_cash_payout(body_text)
        if cash_value is not None:
            if payload is not None:
                payload["price_source"] = "dom-cash"
            return cash_value, cash_snippet

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
            value, snippet = _extract_rebuy_cash_payout(trimmed)
            if value is not None:
                script_rows.append((80, value, snippet))
            value, snippet = _extract_contextual_price(trimmed)
            if value is not None:
                snippet_norm = _normalize_match_text(snippet)
                if any(term in snippet_norm for term in ("pagamento diretto", "offerta preliminare", "ti paghiamo", "ricevi")):
                    script_rows.append((66, value, snippet))
        if script_rows:
            _score, value, snippet = max(script_rows, key=lambda row: (row[0], row[1]))
            if payload is not None and _score >= 80:
                payload["price_source"] = "dom-script-cash"
            return value, snippet[:260]
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
