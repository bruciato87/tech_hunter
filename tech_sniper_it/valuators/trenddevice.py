from __future__ import annotations

import asyncio
import json
import os
import random
import re
import tempfile
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page
from playwright.async_api import async_playwright

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.utils import decode_json_dict_maybe_base64, detect_color_variants, extract_capacity_gb, parse_eur_price
from tech_sniper_it.valuators.base import BaseValuator, ValuatorRuntimeError


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
    # Lead pages often contain "fino a XXX€" which is not a real buyback quote.
    "fino a",
    "fino al",
    "fino ad",
    "massimo",
    "max",
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
    "sconto",
    "sconti",
    "promo",
    "newsletter",
    "coupon",
    "garanzia",
    "reso gratis",
    "rate",
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
    "with",
    "and",
    "the",
    "gps",
    "cellular",
    "lte",
    "mm",
    "pack",
    "plus",
}
OPTIONAL_DESCRIPTOR_TOKENS: set[str] = {
    "titanio",
    "titanium",
    "acciaio",
    "steel",
    "alluminio",
    "aluminum",
    "argent",
    "argento",
    "silver",
    "black",
    "nero",
    "blu",
    "blue",
    "verde",
    "green",
    "bianco",
    "white",
    "oro",
    "gold",
    "grigio",
    "gray",
    "grey",
    "natural",
    "naturale",
    "starlight",
    "siderale",
    "midnight",
    "mezzanotte",
    "small",
    "medium",
    "large",
    "taglia",
    "loop",
    "ocean",
    "alpine",
    "trail",
    "cassa",
    "cinturino",
    "correa",
}
ANCHOR_TOKENS: tuple[str, ...] = (
    "iphone",
    "ipad",
    "apple watch",
    "watch",
    "garmin",
    "fenix",
    "epix",
    "forerunner",
    "dji",
    "steam",
    "deck",
    "rog",
    "ally",
    "legion",
    "macbook",
)
CAPACITY_TOKEN_PATTERN = re.compile(r"\b\d{2,4}\s*(?:gb|tb)\b", re.IGNORECASE)
_TRENDDEVICE_STORAGE_STATE_ERROR = ""
_TRENDDEVICE_DEFAULT_API_BASE_URL = "https://0lpt5fe6f2.execute-api.eu-south-1.amazonaws.com/prod"


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _use_storage_state() -> bool:
    value = _env_or_default("TRENDDEVICE_USE_STORAGE_STATE", "true").lower()
    return value not in {"0", "false", "no", "off"}


def _load_storage_state_b64() -> str | None:
    global _TRENDDEVICE_STORAGE_STATE_ERROR
    _TRENDDEVICE_STORAGE_STATE_ERROR = ""
    if not _use_storage_state():
        return None
    raw = (os.getenv("TRENDDEVICE_STORAGE_STATE_B64") or "").strip()
    if not raw:
        _TRENDDEVICE_STORAGE_STATE_ERROR = "empty"
        return None
    parsed, error = decode_json_dict_maybe_base64(raw)
    if not parsed:
        _TRENDDEVICE_STORAGE_STATE_ERROR = str(error or "invalid-base64-json")
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


def _trenddevice_api_enabled() -> bool:
    raw = _env_or_default("TRENDDEVICE_API_ENABLED", "true").lower()
    return raw not in {"0", "false", "no", "off"}


def _trenddevice_api_base_url() -> str:
    return _env_or_default("TRENDDEVICE_API_BASE_URL", _TRENDDEVICE_DEFAULT_API_BASE_URL).rstrip("/")


def _trenddevice_api_timeout_seconds() -> float:
    raw = (_env_or_default("TRENDDEVICE_API_TIMEOUT_SECONDS", "18") or "").strip()
    try:
        value = float(raw) if raw else 18.0
    except ValueError:
        value = 18.0
    return max(5.0, min(value, 60.0))


def _trenddevice_api_email_candidates() -> list[str]:
    raw = (os.getenv("TRENDDEVICE_LEAD_EMAIL") or "").strip()
    candidates: list[str] = []
    if raw and "@" in raw:
        local, _, domain = raw.partition("@")
        local_base = local.split("+")[0] or "techsniperit"
        suffix = f"{int(time.time())}{random.randint(100, 999)}"
        candidates.append(f"{local_base}+scan{suffix}@{domain}")
        candidates.append(raw)
    ts = f"{int(time.time())}{random.randint(1000, 9999)}"
    candidates.append(f"techsniperit{ts}@gmail.com")
    candidates.append(f"techsniperit{ts}@outlook.com")
    unique: list[str] = []
    for value in candidates:
        marker = value.casefold()
        if marker not in {item.casefold() for item in unique}:
            unique.append(value)
    return unique[:4]


def _trenddevice_api_request_json(
    *,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: float,
) -> tuple[Any | None, dict[str, Any]]:
    url = f"{_trenddevice_api_base_url()}/{path.lstrip('/')}"
    body: bytes | None = None
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib_request.Request(
        url,
        data=body,
        method=method.upper(),
        headers=headers,
    )
    try:
        with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", 200) or 200)
            raw = response.read().decode("utf-8", errors="ignore")
        parsed = json.loads(raw) if raw else None
        return parsed, {"ok": True, "status": status, "url": url}
    except urllib_error.HTTPError as exc:
        try:
            raw = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            raw = ""
        details: dict[str, Any] = {"ok": False, "status": int(exc.code), "url": url, "raw": raw[:400]}
        try:
            details["json"] = json.loads(raw) if raw else None
        except Exception:
            details["json"] = None
        return None, details
    except Exception as exc:
        return None, {"ok": False, "status": None, "url": url, "error": str(exc)}


def _trenddevice_api_label_text(characteristic: dict[str, Any]) -> str:
    values = characteristic.get("usato_caratteristiche_valori")
    if isinstance(values, list):
        for row in values:
            if not isinstance(row, dict):
                continue
            name = str(row.get("nome") or "").strip()
            description = str(row.get("descrizione") or "").strip()
            text = " ".join(part for part in (name, description) if part).strip()
            if text:
                return text
    return ""


def _trenddevice_api_option_name(option: dict[str, Any]) -> str:
    values = option.get("usato_opzioni_valori")
    if isinstance(values, list):
        for row in values:
            if not isinstance(row, dict):
                continue
            name = str(row.get("nome") or "").strip()
            if name:
                return name
    fallback = str(option.get("nome") or "").strip()
    return fallback


def _trenddevice_api_step_type(characteristic: dict[str, Any], options: list[WizardOption]) -> str:
    label_norm = _normalize_wizard_text(_trenddevice_api_label_text(characteristic))
    if "capacita" in label_norm:
        return STEP_CAPACITY
    if "condizion" in label_norm or "stato" in label_norm:
        return STEP_CONDITION
    if "batteria" in label_norm:
        return STEP_BATTERY
    if "network" in label_norm or "sim" in label_norm:
        return STEP_SIM
    if "provenienza" in label_norm or "comprato" in label_norm:
        return STEP_MARKET
    if "color" in label_norm:
        return STEP_COLOR
    return _detect_wizard_step(options)


def _trenddevice_api_pick_device(
    *,
    devices: list[dict[str, Any]],
    product: AmazonProduct,
    normalized_name: str,
) -> dict[str, Any] | None:
    targets = _infer_family_targets(product, normalized_name)
    query = _normalize_wizard_text(f"{product.title} {normalized_name}")
    best_row: dict[str, Any] | None = None
    best_score = -10_000
    for row in devices:
        if not isinstance(row, dict):
            continue
        models = row.get("models")
        if not isinstance(models, list) or not models:
            continue
        name = _normalize_wizard_text(str(row.get("nome") or ""))
        if not name:
            continue
        score = int(SequenceMatcher(None, query, name).ratio() * 30)
        for target in targets:
            if target and target in name:
                score += 110
        if product.category == ProductCategory.APPLE_PHONE:
            score += 220 if "iphone" in name else -120
        if product.category == ProductCategory.SMARTWATCH:
            if "watch" in name or "garmin" in name:
                score += 220
            if "iphone" in name:
                score -= 140
        if "iphone" in query and "iphone" in name:
            score += 80
        if "watch" in query and "watch" in name:
            score += 80
        if score > best_score:
            best_row = row
            best_score = score
    return best_row


def _trenddevice_api_pick_model(
    *,
    models: list[dict[str, Any]],
    normalized_name: str,
) -> dict[str, Any] | None:
    if not models:
        return None
    model_hint = _extract_iphone_model_hint(normalized_name)
    ranked: list[tuple[int, dict[str, Any]]] = []
    for index, row in enumerate(models):
        if not isinstance(row, dict):
            continue
        name = str(row.get("nome") or "").strip()
        if not name:
            continue
        option = WizardOption(index=index, text=name, normalized=_normalize_wizard_text(name))
        score = _model_score(option, model_hint=model_hint, normalized_name=normalized_name)
        ranked.append((score, row))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[0][1]


def _trenddevice_api_extract_stima(data: Any) -> float | None:
    if isinstance(data, dict):
        richiesta = data.get("richiesta")
        if isinstance(richiesta, dict):
            value = _parse_plain_price(richiesta.get("stima"))
            if value is not None:
                return value
            # Last-resort fallback: if stima is missing, use TD-money quote.
            value = _parse_plain_price(richiesta.get("stima_money_td"))
            if value is not None:
                return value
        value = _parse_plain_price(data.get("stima"))
        if value is not None:
            return value
    return None


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

    family_markers = {
        "iphone",
        "ipad",
        "apple watch",
        "watch",
        "garmin",
        "mac",
        "macbook",
        "samsung",
        "google",
    }
    family_hits = [
        value
        for value in values
        if value in family_markers or value.startswith("mac")
    ]
    # "Family" steps are typically short, brand-like options (iphone/ipad/watch/mac/samsung...),
    # not model lists with numbers. We keep detection conservative to avoid misclassifying model steps.
    if "iphone" in values and any(
        value in {"ipad", "apple watch", "watch", "samsung", "google", "garmin"} or value.startswith("mac")
        for value in values
    ):
        return STEP_DEVICE_FAMILY
    if len(set(family_hits)) >= 2 and len(values) <= 15:
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


def _query_tokens(value: str) -> list[str]:
    normalized = _normalize_wizard_text(value)
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
    normalized = _normalize_wizard_text(value).replace(" ", "")
    return sorted(set(match.group(0).replace(" ", "").lower() for match in CAPACITY_TOKEN_PATTERN.finditer(normalized)))


def _infer_family_targets(product: AmazonProduct, normalized_name: str) -> list[str]:
    combined = _normalize_wizard_text(f"{product.title} {normalized_name}")
    targets: list[str] = []

    def _push(value: str) -> None:
        normalized_value = _normalize_wizard_text(value)
        if normalized_value and normalized_value not in targets:
            targets.append(normalized_value)

    if product.category == ProductCategory.APPLE_PHONE:
        _push("iphone")
    if product.category == ProductCategory.SMARTWATCH:
        _push("apple watch")
        _push("smartwatch")
        _push("watch")
        _push("garmin")

    if "iphone" in combined:
        _push("iphone")
    if "ipad" in combined:
        _push("ipad")
    if "apple watch" in combined or "watch ultra" in combined:
        _push("apple watch")
        _push("watch")
    if "garmin" in combined:
        _push("garmin")
        _push("watch")
    if "fenix" in combined:
        _push("fenix")
        _push("watch")
    if "epix" in combined:
        _push("epix")
        _push("watch")
    return targets


def _pick_device_family_option(
    options: list[WizardOption],
    *,
    product: AmazonProduct,
    normalized_name: str,
) -> WizardOption | None:
    if not options:
        return None
    targets = _infer_family_targets(product, normalized_name)
    if not targets:
        return options[0]

    watch_intent = any(item in {"watch", "apple watch", "garmin", "fenix", "epix"} for item in targets)
    iphone_intent = "iphone" in targets
    best: WizardOption | None = None
    best_score = -10_000
    for option in options:
        text = option.normalized
        score = 0
        for target in targets:
            if text == target:
                score += 220
            if target in text:
                score += 110
            for token in target.split():
                if token in text:
                    score += 28
        score += int(SequenceMatcher(None, text, " ".join(targets[:2])).ratio() * 35)

        if watch_intent and "iphone" in text and "watch" not in text and "garmin" not in text:
            score -= 160
        if iphone_intent and "watch" in text and "iphone" not in text:
            score -= 60

        if score > best_score:
            best = option
            best_score = score

    if best is not None and best_score > 0:
        return best
    return options[0]


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
        return _pick_device_family_option(options, product=product, normalized_name=normalized_name)

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


def _pick_best_network_candidate(
    candidates: list[dict[str, Any]],
    *,
    normalized_name: str | None = None,
    wizard_steps: list[dict[str, Any]] | None = None,
) -> tuple[float | None, str]:
    credible = [item for item in candidates if _is_credible_network_candidate(item)]
    if not credible:
        return None, ""
    model_tokens = _query_tokens(normalized_name or "")[:7] if normalized_name else []
    selected_tokens: list[str] = []
    for step in (wizard_steps or []):
        if not isinstance(step, dict):
            continue
        selected = _normalize_wizard_text(str(step.get("selected", "")))
        if not selected:
            continue
        for token in selected.split():
            if len(token) < 2:
                continue
            if token not in selected_tokens:
                selected_tokens.append(token)
            if len(selected_tokens) >= 8:
                break
    ranked_rows: list[dict[str, Any]] = []
    for item in credible:
        snippet = str(item.get("snippet", ""))
        url = str(item.get("url", ""))
        joined = _normalize_wizard_text(f"{snippet} {urlparse(url).path} {urlparse(url).query}")
        token_hits = sum(1 for token in model_tokens if token and token in joined)
        selected_hits = sum(1 for token in selected_tokens if token and token in joined)
        source = str(item.get("source", "")).strip().lower()
        if model_tokens and token_hits <= 0:
            continue
        if source == "json" and model_tokens and token_hits < 2:
            continue
        row = dict(item)
        row["token_hits"] = token_hits
        row["selected_hits"] = selected_hits
        ranked_rows.append(row)

    if not ranked_rows:
        return None, ""
    best = max(
        ranked_rows,
        key=lambda item: (
            int(item.get("wizard_progress", 0) or 0),
            int(item.get("token_hits", 0) or 0),
            int(item.get("selected_hits", 0) or 0),
            int(item.get("score", 0)),
            float(item.get("value", 0.0)),
        ),
    )
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
        # JSON payloads can include entire catalog price lists. Require stronger quote context
        # than just "price" to avoid overestimations.
        strong_terms = (
            "valuation",
            "valutazione",
            "quote",
            "quotazione",
            "offerta",
            "offer",
            "cash",
            "payout",
            "amount",
            "totale",
            "ti offriamo",
            "ricevi",
            "paghiamo",
        )
        if not any(term in snippet_norm for term in strong_terms):
            return False
        return score >= 68
    if score < 62:
        return False
    valuation_terms = ("ti offriamo", "valutazione", "ricevi", "paghiamo", "quotazione", "offerta")
    return any(term in snippet_norm for term in valuation_terms)


def _is_generic_trenddevice_url(url: str | None) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    return path in {"vendi", "vendi/valutazione"}


def _assess_trenddevice_match(
    *,
    product: AmazonProduct,
    normalized_name: str,
    wizard_steps: list[dict[str, Any]],
    source_url: str | None,
    price_text: str | None,
) -> dict[str, Any]:
    def _extract_watch_generation_signature(text: str) -> dict[str, str]:
        normalized = _normalize_wizard_text(text)
        signature: dict[str, str] = {}

        series_hits: list[tuple[int, str]] = []
        for pattern in (r"\b(?:serie|series)\s*(\d{1,2})\b", r"\b(\d{1,2})\s*(?:serie|series)\b"):
            for match in re.finditer(pattern, normalized):
                raw_value = match.group(1)
                try:
                    parsed_value = int(raw_value)
                except ValueError:
                    continue
                if 1 <= parsed_value <= 20:
                    series_hits.append((match.start(), str(parsed_value)))
        if series_hits:
            series_hits.sort(key=lambda item: item[0])
            signature["series"] = series_hits[0][1]

        ultra_generation: str | None = None
        for match in re.finditer(r"\bultra\s*(\d{1,2})\b", normalized):
            raw_value = match.group(1)
            try:
                parsed_value = int(raw_value)
            except ValueError:
                continue
            tail = normalized[match.end() : match.end() + 5]
            if "mm" in tail:
                continue
            if 1 <= parsed_value <= 5:
                ultra_generation = str(parsed_value)
                break
        if ultra_generation is not None:
            signature["ultra"] = ultra_generation
        elif "ultra" in normalized:
            signature["ultra"] = "base"
        return signature

    query_norm = _normalize_wizard_text(normalized_name)
    selected_parts = [
        _normalize_wizard_text(str(step.get("selected", "")))
        for step in wizard_steps
        if isinstance(step, dict)
    ]
    selected_combined = " ".join(part for part in selected_parts if part)
    url_parts = " ".join(part for part in ((urlparse(source_url or "").path or ""), (urlparse(source_url or "").query or "")) if part)
    candidate_norm = _normalize_wizard_text(" ".join((selected_combined, str(price_text or ""), url_parts)))

    ratio = SequenceMatcher(None, query_norm, candidate_norm).ratio() if query_norm and candidate_norm else 0.0
    tokens = _query_tokens(normalized_name)
    capacities = _capacity_tokens(normalized_name)
    anchor_word_pool = {
        token
        for anchor in ANCHOR_TOKENS
        for token in _normalize_wizard_text(anchor).split()
        if token
    }
    anchor_words = sorted({token for token in tokens if token in anchor_word_pool})
    if "apple watch" in query_norm and "watch" not in anchor_words:
        anchor_words.append("watch")

    required_tokens: list[str] = []
    for item in capacities:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in anchor_words:
        if item not in required_tokens:
            required_tokens.append(item)
    for item in tokens:
        if item.isdigit() or re.search(r"\d", item):
            if item not in required_tokens:
                required_tokens.append(item)
    for item in tokens:
        if item in OPTIONAL_DESCRIPTOR_TOKENS:
            continue
        if item not in required_tokens:
            required_tokens.append(item)
        if len(required_tokens) >= 7:
            break

    candidate_compact = candidate_norm.replace(" ", "")

    def _token_present(token: str) -> bool:
        normalized_token = _normalize_wizard_text(token)
        if not normalized_token:
            return False
        if normalized_token in candidate_norm:
            return True
        compact_token = normalized_token.replace(" ", "")
        if compact_token and compact_token in candidate_compact:
            return True
        if normalized_token == "series" and "serie" in candidate_norm:
            return True
        if normalized_token == "serie" and "series" in candidate_norm:
            return True
        return False

    hit_tokens = [token for token in required_tokens if token and _token_present(token)]
    capacity_hits = [token for token in capacities if _token_present(token)]
    anchor_hits = [token for token in anchor_words if _token_present(token)]
    token_ratio = (len(hit_tokens) / len(required_tokens)) if required_tokens else 0.0
    generic_url = _is_generic_trenddevice_url(source_url)
    has_model_step = any(str(step.get("step_type")) == STEP_MODEL for step in wizard_steps if isinstance(step, dict))
    score = int((ratio * 100) + (len(hit_tokens) * 13) + (len(anchor_hits) * 10) + (14 if has_model_step else -14) - (34 if generic_url else 0))

    watch_intent = any(token in query_norm for token in ("watch", "garmin", "fenix", "epix", "forerunner"))
    query_generation = _extract_watch_generation_signature(query_norm)
    candidate_generation = _extract_watch_generation_signature(candidate_norm)
    if watch_intent and query_generation:
        if query_generation.get("series") and candidate_generation.get("series"):
            if query_generation["series"] != candidate_generation["series"]:
                return {
                    "ok": False,
                    "reason": "model-generation-mismatch",
                    "score": score,
                    "ratio": round(ratio, 3),
                    "token_ratio": round(token_ratio, 3),
                    "generic_url": generic_url,
                    "has_model_step": has_model_step,
                    "hit_tokens": hit_tokens,
                    "required_tokens": required_tokens,
                }
        if query_generation.get("ultra") and candidate_generation.get("ultra"):
            if query_generation["ultra"] != candidate_generation["ultra"]:
                return {
                    "ok": False,
                    "reason": "model-generation-mismatch",
                    "score": score,
                    "ratio": round(ratio, 3),
                    "token_ratio": round(token_ratio, 3),
                    "generic_url": generic_url,
                    "has_model_step": has_model_step,
                    "hit_tokens": hit_tokens,
                    "required_tokens": required_tokens,
                }
        if query_generation.get("ultra") == "base" and candidate_generation.get("ultra") not in {None, "base"}:
            return {
                "ok": False,
                "reason": "model-generation-mismatch",
                "score": score,
                "ratio": round(ratio, 3),
                "token_ratio": round(token_ratio, 3),
                "generic_url": generic_url,
                "has_model_step": has_model_step,
                "hit_tokens": hit_tokens,
                "required_tokens": required_tokens,
            }
    if watch_intent and "iphone" in selected_combined and "watch" not in selected_combined and "garmin" not in selected_combined:
        return {
            "ok": False,
            "reason": "device-family-mismatch",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }

    if generic_url and not has_model_step:
        return {
            "ok": False,
            "reason": "generic-url-no-model-step",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
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
            "has_model_step": has_model_step,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if anchor_words and not anchor_hits:
        return {
            "ok": False,
            "reason": "anchor-mismatch",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if generic_url and token_ratio < 0.75:
        return {
            "ok": False,
            "reason": "generic-url-low-coverage",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    low_token_ratio_floor = 0.55
    low_ratio_floor = 0.60
    if watch_intent and has_model_step and not generic_url:
        low_token_ratio_floor = 0.48
        low_ratio_floor = 0.56
    if token_ratio < low_token_ratio_floor and ratio < low_ratio_floor:
        return {
            "ok": False,
            "reason": "low-token-similarity",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
            "hit_tokens": hit_tokens,
            "required_tokens": required_tokens,
        }
    if score < 74:
        return {
            "ok": False,
            "reason": "score-too-low",
            "score": score,
            "ratio": round(ratio, 3),
            "token_ratio": round(token_ratio, 3),
            "generic_url": generic_url,
            "has_model_step": has_model_step,
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
        "has_model_step": has_model_step,
        "hit_tokens": hit_tokens,
        "required_tokens": required_tokens,
    }


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
        script_rows: list[tuple[int, float, str]] = []
        for script in soup.select("script"):
            script_text = ""
            if script.string:
                script_text = script.string
            elif script.get_text(strip=True):
                script_text = script.get_text(" ", strip=True)
            if not script_text or len(script_text) < 30:
                continue
            trimmed = script_text[:120000]
            for score, value, snippet in _extract_keyed_prices_from_text(trimmed):
                snippet_norm = _normalize_wizard_text(snippet)
                if not any(term in snippet_norm for term in ("valutazione", "quote", "offerta", "ti offriamo", "ricevi")):
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
                        snippet_norm = _normalize_wizard_text(snippet)
                        if not any(term in snippet_norm for term in ("valutazione", "quote", "offerta", "ti offriamo", "ricevi")):
                            continue
                        script_rows.append((score, value, snippet))
        if script_rows:
            _score, value, snippet = max(script_rows, key=lambda row: (row[0], row[1]))
            return value, snippet[:260]
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

    async def _try_api_offer(
        self,
        *,
        product: AmazonProduct,
        normalized_name: str,
        payload: dict[str, Any],
    ) -> tuple[float | None, str | None]:
        if not _trenddevice_api_enabled():
            payload["api"] = {"enabled": False, "reason": "disabled-by-env"}
            return None, None

        timeout_seconds = _trenddevice_api_timeout_seconds()

        def _run() -> dict[str, Any]:
            trace: dict[str, Any] = {
                "enabled": True,
                "base_url": _trenddevice_api_base_url(),
                "timeout_seconds": timeout_seconds,
                "post_attempts": [],
            }

            catalog_data, catalog_meta = _trenddevice_api_request_json(
                method="GET",
                path="/vendi/usato",
                timeout_seconds=timeout_seconds,
            )
            trace["catalog"] = catalog_meta
            if not isinstance(catalog_data, dict):
                return {"ok": False, "reason": "catalog-unavailable", "trace": trace}
            devices = catalog_data.get("usatoDevice")
            if not isinstance(devices, list) or not devices:
                return {"ok": False, "reason": "catalog-empty", "trace": trace}

            device = _trenddevice_api_pick_device(
                devices=[item for item in devices if isinstance(item, dict)],
                product=product,
                normalized_name=normalized_name,
            )
            if not isinstance(device, dict):
                return {"ok": False, "reason": "device-not-found", "trace": trace}

            models = device.get("models")
            if not isinstance(models, list) or not models:
                return {"ok": False, "reason": "model-list-empty", "trace": trace}
            model = _trenddevice_api_pick_model(
                models=[item for item in models if isinstance(item, dict)],
                normalized_name=normalized_name,
            )
            if not isinstance(model, dict):
                return {"ok": False, "reason": "model-not-found", "trace": trace}

            model_id = int(model.get("id") or 0)
            if model_id <= 0:
                return {"ok": False, "reason": "invalid-model-id", "trace": trace}
            trace["selected"] = {
                "device_id": int(device.get("id") or 0),
                "device_name": str(device.get("nome") or ""),
                "model_id": model_id,
                "model_name": str(model.get("nome") or ""),
            }

            detail_data, detail_meta = _trenddevice_api_request_json(
                method="GET",
                path=f"/vendi/usato/{model_id}",
                timeout_seconds=timeout_seconds,
            )
            trace["model_detail"] = detail_meta
            if not isinstance(detail_data, dict):
                return {"ok": False, "reason": "model-detail-unavailable", "trace": trace}

            characteristics = detail_data.get("usatoDevice")
            if not isinstance(characteristics, list) or not characteristics:
                return {"ok": False, "reason": "characteristics-empty", "trace": trace}

            selected_characteristics: list[dict[str, Any]] = []
            wizard_steps: list[dict[str, Any]] = [
                {
                    "step": 1,
                    "step_type": STEP_DEVICE_FAMILY,
                    "selected": str(device.get("nome") or ""),
                    "options_count": len([item for item in devices if isinstance(item, dict)]),
                    "confirmed": True,
                    "source": "api",
                },
                {
                    "step": 2,
                    "step_type": STEP_MODEL,
                    "selected": str(model.get("nome") or ""),
                    "options_count": len([item for item in models if isinstance(item, dict)]),
                    "confirmed": True,
                    "source": "api",
                },
            ]
            for step_index, characteristic in enumerate(characteristics, start=3):
                if not isinstance(characteristic, dict):
                    continue
                options_raw = characteristic.get("usato_opzioni")
                if not isinstance(options_raw, list) or not options_raw:
                    continue
                wizard_options: list[WizardOption] = []
                for option_index, option in enumerate(options_raw):
                    if not isinstance(option, dict):
                        continue
                    name = _trenddevice_api_option_name(option)
                    if not name:
                        continue
                    wizard_options.append(
                        WizardOption(
                            index=option_index,
                            text=name,
                            normalized=_normalize_wizard_text(name),
                            selector="api",
                        )
                    )
                if not wizard_options:
                    continue
                step_type = _trenddevice_api_step_type(characteristic, wizard_options)
                chosen = _pick_wizard_option(
                    step=step_type,
                    options=wizard_options,
                    product=product,
                    normalized_name=normalized_name,
                )
                if chosen is None:
                    chosen = wizard_options[0]
                option_payload = options_raw[chosen.index]
                if not isinstance(option_payload, dict):
                    continue
                characteristic_payload = dict(characteristic)
                characteristic_payload["usato_opzioni"] = [dict(option_payload)]
                selected_characteristics.append(characteristic_payload)
                wizard_steps.append(
                    {
                        "step": step_index,
                        "step_type": step_type,
                        "selected": chosen.text,
                        "options_count": len(wizard_options),
                        "confirmed": True,
                        "source": "api",
                    }
                )

            if not selected_characteristics:
                return {"ok": False, "reason": "no-characteristics-selected", "trace": trace}

            model_payload = dict(model)
            model_payload["options"] = selected_characteristics
            device_payload = dict(device)
            device_payload["models"] = [model_payload]
            device_payload["options"] = None

            for email in _trenddevice_api_email_candidates():
                request_payload = {
                    "usatoDevice": device_payload,
                    "email": email,
                }
                post_data, post_meta = _trenddevice_api_request_json(
                    method="POST",
                    path="/vendi/usato",
                    payload=request_payload,
                    timeout_seconds=timeout_seconds,
                )
                attempt_row = {
                    "email_domain": email.split("@")[-1],
                    "status": post_meta.get("status"),
                    "ok": bool(post_meta.get("ok")),
                }
                trace["post_attempts"].append(attempt_row)
                if not isinstance(post_data, dict):
                    continue

                richiesta = post_data.get("richiesta") if isinstance(post_data.get("richiesta"), dict) else {}
                request_id = int(richiesta.get("id") or 0) if richiesta else 0
                stima = _trenddevice_api_extract_stima(post_data)
                detail_meta_payload: dict[str, Any] = {}
                detail_data = None
                if stima is None and request_id > 0:
                    detail_data, detail_meta_payload = _trenddevice_api_request_json(
                        method="GET",
                        path=f"/richiesta/{request_id}",
                        timeout_seconds=timeout_seconds,
                    )
                    if isinstance(detail_data, dict):
                        stima = _trenddevice_api_extract_stima(detail_data)
                if stima is None:
                    continue

                td_money = None
                if isinstance(richiesta, dict):
                    td_money = _parse_plain_price(richiesta.get("stima_money_td"))
                if td_money is None and isinstance(detail_data, dict):
                    detail_richiesta = detail_data.get("richiesta")
                    if isinstance(detail_richiesta, dict):
                        td_money = _parse_plain_price(detail_richiesta.get("stima_money_td"))
                price_text = f"stima={stima:.2f}€"
                if td_money is not None:
                    price_text += f" | stima_money_td={td_money:.2f}€"
                match_parts = [str(step.get("selected") or "") for step in wizard_steps if isinstance(step, dict)]
                match_text = " | ".join(part for part in match_parts if part)
                if match_text:
                    match_text = f"{price_text} | {match_text}"
                else:
                    match_text = price_text
                source_url = f"{self.base_url}?model={model_id}&request={request_id}" if request_id > 0 else f"{self.base_url}?model={model_id}"
                device_label = _normalize_wizard_text(str(device.get("nome") or "")).replace(" ", "+")
                model_label = _normalize_wizard_text(str(model.get("nome") or "")).replace(" ", "+")
                validation_url = f"{_trenddevice_api_base_url()}/vendi/usato/{model_id}?device={device_label}&model={model_label}"
                trace["selected"]["request_id"] = request_id
                if detail_meta_payload:
                    trace["richiesta_detail"] = detail_meta_payload
                return {
                    "ok": True,
                    "offer": stima,
                    "price_text": price_text,
                    "match_text": match_text,
                    "source_url": source_url,
                    "validation_url": validation_url,
                    "wizard": wizard_steps,
                    "trace": trace,
                }
            return {"ok": False, "reason": "post-no-stima", "trace": trace}

        result = await asyncio.to_thread(_run)
        trace = result.get("trace") if isinstance(result, dict) else {}
        payload["api"] = trace if isinstance(trace, dict) else {}
        if not isinstance(result, dict) or not result.get("ok"):
            payload["api"]["status"] = "fallback-dom"
            if isinstance(result, dict):
                payload["api"]["reason"] = result.get("reason", "unknown")
            return None, None

        payload["api"]["status"] = "ok"
        payload["wizard"] = list(result.get("wizard") or [])
        payload["price_source"] = "api"
        payload["price_text"] = str(result.get("price_text") or "")
        match_text = str(result.get("match_text") or payload["price_text"])
        self._validate_match_or_raise(
            product=product,
            normalized_name=normalized_name,
            source_url=str(result.get("validation_url") or result.get("source_url") or self.base_url),
            price_text=match_text,
            payload=payload,
        )
        return float(result["offer"]), str(result.get("source_url") or self.base_url)

    def _validate_match_or_raise(
        self,
        *,
        product: AmazonProduct,
        normalized_name: str,
        source_url: str | None,
        price_text: str | None,
        payload: dict[str, Any],
    ) -> None:
        match = _assess_trenddevice_match(
            product=product,
            normalized_name=normalized_name,
            wizard_steps=payload.get("wizard", []),
            source_url=source_url,
            price_text=price_text,
        )
        payload["match_quality"] = match
        if not match.get("ok"):
            reason = str(match.get("reason", "low-confidence"))
            raise ValuatorRuntimeError(
                f"TrendDevice low-confidence match ({reason}); discarded to prevent false-positive.",
                payload=payload,
                source_url=source_url,
            )

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
        api_offer, api_source_url = await self._try_api_offer(
            product=product,
            normalized_name=normalized_name,
            payload=payload,
        )
        if api_offer is not None:
            return api_offer, api_source_url or self.base_url, payload

        storage_state_path = _load_storage_state_b64()
        payload["storage_state"] = bool(storage_state_path)
        if _use_storage_state() and storage_state_path is None:
            payload["storage_state_error"] = _TRENDDEVICE_STORAGE_STATE_ERROR or "missing"
            print(
                "[trenddevice] storage_state missing/invalid | "
                f"reason={payload['storage_state_error']}"
            )
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
            wizard_progress = 0

            async def _capture_response_body(response) -> None:  # noqa: ANN001
                url = str(getattr(response, "url", "") or "")
                url_lower = url.lower()
                headers = getattr(response, "headers", {}) or {}
                content_type = str(headers.get("content-type", "")).lower()
                request = getattr(response, "request", None)
                resource_type = str(getattr(request, "resource_type", "")).lower()
                if "trendevice.com" not in url_lower:
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
                        "wizard_progress": wizard_progress,
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
                    raise ValuatorRuntimeError(
                        f"Unexpected TrendDevice hostname: {hostname or 'n/a'}",
                        payload=payload,
                        source_url=page.url,
                    )

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
                                        self._validate_match_or_raise(
                                            product=product,
                                            normalized_name=normalized_name,
                                            source_url=page.url,
                                            price_text=price_text,
                                            payload=payload,
                                        )
                                        return price, page.url, payload
                                    network_price, network_snippet = _pick_best_network_candidate(
                                        network_price_candidates,
                                        normalized_name=normalized_name,
                                        wizard_steps=payload.get("wizard", []),
                                    )
                                    if network_price is not None:
                                        payload["price_source"] = "network-post-email"
                                        payload["price_text"] = network_snippet
                                        self._validate_match_or_raise(
                                            product=product,
                                            normalized_name=normalized_name,
                                            source_url=page.url,
                                            price_text=network_snippet,
                                            payload=payload,
                                        )
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
                                self._validate_match_or_raise(
                                    product=product,
                                    normalized_name=normalized_name,
                                    source_url=page.url,
                                    price_text=price_text,
                                    payload=payload,
                                )
                                return price, page.url, payload
                            network_price, network_snippet = _pick_best_network_candidate(
                                network_price_candidates,
                                normalized_name=normalized_name,
                                wizard_steps=payload.get("wizard", []),
                            )
                            if network_price is not None:
                                payload["price_source"] = "network-post-email-stagnant"
                                payload["price_text"] = network_snippet
                                self._validate_match_or_raise(
                                    product=product,
                                    normalized_name=normalized_name,
                                    source_url=page.url,
                                    price_text=network_snippet,
                                    payload=payload,
                                )
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
                            failing_model = _normalize_wizard_text(str(previous_step.get("selected") or ""))
                            if failing_model:
                                excluded_models.add(failing_model)
                                payload.setdefault("adaptive_fallbacks", {})["excluded_model_on_reset"] = failing_model
                            if reset_after_model >= 3:
                                payload["wizard_end_reason"] = "model-selection-reset"
                                await _drain_response_tasks()
                                price, price_text = await self._extract_price(page, payload=payload)
                                if price is not None:
                                    payload["price_source"] = "dom-pre-reset"
                                    payload["price_text"] = price_text
                                    self._validate_match_or_raise(
                                        product=product,
                                        normalized_name=normalized_name,
                                        source_url=page.url,
                                        price_text=price_text,
                                        payload=payload,
                                    )
                                    return price, page.url, payload
                                network_price, network_snippet = _pick_best_network_candidate(
                                    network_price_candidates,
                                    normalized_name=normalized_name,
                                    wizard_steps=payload.get("wizard", []),
                                )
                                if network_price is not None:
                                    payload["price_source"] = "network-pre-reset"
                                    payload["price_text"] = network_snippet
                                    self._validate_match_or_raise(
                                        product=product,
                                        normalized_name=normalized_name,
                                        source_url=page.url,
                                        price_text=network_snippet,
                                        payload=payload,
                                    )
                                    return network_price, page.url, payload
                                raise ValuatorRuntimeError(
                                    "TrendDevice wizard reset after model selection (catalog route unavailable).",
                                    payload=payload,
                                    source_url=page.url,
                                )

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

                    wizard_progress = step_index
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
                    network_price, network_snippet = _pick_best_network_candidate(
                        network_price_candidates,
                        normalized_name=normalized_name,
                        wizard_steps=payload.get("wizard", []),
                    )
                    if network_price is not None:
                        payload["price_source"] = "network"
                        payload["price_text"] = network_snippet
                        self._validate_match_or_raise(
                            product=product,
                            normalized_name=normalized_name,
                            source_url=page.url,
                            price_text=network_snippet,
                            payload=payload,
                        )
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
                    raise ValuatorRuntimeError(
                        f"TrendDevice price not found after wizard ({reason})",
                        payload=payload,
                        source_url=page.url,
                    )
                payload["price_source"] = str(payload.get("price_source") or "dom")
                self._validate_match_or_raise(
                    product=product,
                    normalized_name=normalized_name,
                    source_url=page.url,
                    price_text=price_text,
                    payload=payload,
                )
                return price, page.url, payload
            finally:
                if response_tasks:
                    await _drain_response_tasks()
                await context.close()
                await browser.close()
                _remove_file_if_exists(storage_state_path)


__all__ = [
    "TrendDeviceValuator",
    "_assess_trenddevice_match",
    "_detect_wizard_step",
    "_extract_contextual_price",
    "_extract_iphone_model_hint",
    "_trenddevice_api_extract_stima",
    "_trenddevice_api_option_name",
    "_trenddevice_api_pick_device",
    "_trenddevice_api_pick_model",
    "_trenddevice_api_step_type",
    "_is_email_gate_text",
    "_normalize_wizard_text",
]
