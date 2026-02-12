from __future__ import annotations

import re


PRICE_PATTERN = re.compile(r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?)\s*€|€\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{2})?)")
CAPACITY_PATTERN = re.compile(r"\b(\d{2,4})\s?gb\b", re.IGNORECASE)

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

