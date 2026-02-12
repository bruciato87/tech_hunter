from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip().lower()


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in values:
        raw = str(item).strip()
        if not raw:
            continue
        if raw in seen:
            continue
        seen.add(raw)
        deduped.append(raw)
    return deduped


def parse_selector_overrides(raw: str | None) -> dict[str, dict[str, list[str]]]:
    payload = (raw or "").strip()
    if not payload:
        return {}

    try:
        decoded = json.loads(payload)
    except Exception:
        return {}
    if not isinstance(decoded, dict):
        return {}

    parsed: dict[str, dict[str, list[str]]] = {}
    for site_key, slots_raw in decoded.items():
        site = _normalize_space(str(site_key))
        if not site or not isinstance(slots_raw, dict):
            continue

        slot_map: dict[str, list[str]] = {}
        for slot_key, selector_values in slots_raw.items():
            slot = _normalize_space(str(slot_key))
            if not slot:
                continue
            if isinstance(selector_values, str):
                selectors = [selector_values]
            elif isinstance(selector_values, list):
                selectors = [str(item) for item in selector_values]
            else:
                continue
            deduped = _dedupe_keep_order(selectors)
            if deduped:
                slot_map[slot] = deduped
        if slot_map:
            parsed[site] = slot_map
    return parsed


def selector_candidates(
    *,
    site: str,
    slot: str,
    defaults: list[str],
    overrides_raw: str | None,
) -> list[str]:
    merged = _dedupe_keep_order(defaults)
    overrides = parse_selector_overrides(overrides_raw)
    if not overrides:
        return merged

    site_key = _normalize_space(site)
    slot_key = _normalize_space(slot)
    extra = []
    global_slots = overrides.get("*", {})
    extra.extend(global_slots.get(slot_key, []))
    site_slots = overrides.get(site_key, {})
    extra.extend(site_slots.get(slot_key, []))
    if not extra:
        return merged
    return _dedupe_keep_order([*merged, *extra])


def keyword_presence(text: str, keywords: Iterable[str]) -> tuple[list[str], list[str]]:
    normalized_text = _normalize_space(text)
    seen_terms: set[str] = set()
    present: list[str] = []
    missing: list[str] = []
    for raw in keywords:
        term = _normalize_space(str(raw))
        if not term or term in seen_terms:
            continue
        seen_terms.add(term)
        if term in normalized_text:
            present.append(term)
        else:
            missing.append(term)
    return present, missing


def ui_signature(*parts: str, max_chars: int = 6000) -> str:
    normalized = _normalize_space(" | ".join(parts))[:max_chars]
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]

