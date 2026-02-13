from __future__ import annotations

import re


PRICE_PATTERN = re.compile(r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?)\s*€|€\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?)")
CAPACITY_PATTERN = re.compile(r"\b(\d{2,4})\s?gb\b", re.IGNORECASE)

AMAZON_CONDITION_PATTERNS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    (
        "like_new",
        (
            "come nuovo",
            "pari al nuovo",
            "like new",
            "as new",
            "wie neu",
            "comme neuf",
            "como nuevo",
        ),
        0.96,
    ),
    (
        "very_good",
        (
            "ottime condizioni",
            "molto buone condizioni",
            "very good",
            "sehr gut",
            "très bon état",
            "muy buen estado",
        ),
        0.92,
    ),
    (
        "good",
        (
            "buone condizioni",
            "good",
            "gut",
            "bon état",
            "buen estado",
        ),
        0.88,
    ),
    (
        "acceptable",
        (
            "condizioni accettabili",
            "accettabile",
            "acceptable",
            "akzeptabel",
            "état acceptable",
            "aceptable",
        ),
        0.84,
    ),
)

AMAZON_PACKAGING_ONLY_HINTS: tuple[str, ...] = (
    "confezione danneggiata",
    "scatola danneggiata",
    "imballo danneggiato",
    "packaging damaged",
    "damaged packaging",
    "damaged box",
    "box damaged",
    "missing original packaging",
    "confezione originale mancante",
    "packaging may be damaged",
)

COLOR_MAP = {
    "black": ("nero", "black"),
    "white": ("bianco", "white"),
    "silver": ("argento", "silver"),
    "gold": ("oro", "gold"),
    "blue": ("blu", "azzurro", "blue"),
    "green": ("verde", "green"),
    "red": ("rosso", "red"),
    "gray": ("grigio", "gray", "space grey", "grafite"),
    "purple": ("viola", "purple"),
}


def parse_eur_price(text: str) -> float | None:
    if not text:
        return None
    match = PRICE_PATTERN.search(text)
    if not match:
        return None
    raw = (match.group(1) or match.group(2) or "").replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def extract_capacity_gb(text: str) -> str | None:
    match = CAPACITY_PATTERN.search(text or "")
    if not match:
        return None
    return f"{match.group(1)} GB"


def detect_color_variants(text: str) -> tuple[str, ...]:
    value = (text or "").lower()
    for variants in COLOR_MAP.values():
        if any(color in value for color in variants):
            return variants
    return ()


def infer_amazon_warehouse_condition(text: str | None) -> tuple[str | None, float, bool]:
    lowered = (text or "").strip().lower()
    if not lowered:
        return None, 0.0, False

    normalized = re.sub(r"\s+", " ", lowered)
    packaging_only = any(marker in normalized for marker in AMAZON_PACKAGING_ONLY_HINTS)

    for label, tokens, confidence in AMAZON_CONDITION_PATTERNS:
        if any(token in normalized for token in tokens):
            adjusted = confidence
            if label == "acceptable" and packaging_only:
                adjusted = min(0.99, confidence + 0.08)
            return label, round(adjusted, 2), packaging_only
    return None, 0.0, packaging_only
