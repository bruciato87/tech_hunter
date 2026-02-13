from __future__ import annotations

import base64
import json
import os

import pytest

from tech_sniper_it.valuators.rebuy import (
    _assess_rebuy_match,
    _extract_contextual_price,
    _extract_rebuy_product_link_candidates,
    _load_storage_state_b64,
    _remove_file_if_exists,
)


def test_rebuy_extract_contextual_price_prefers_offer_copy() -> None:
    value, snippet = _extract_contextual_price("Spedizione 12 €. Ti paghiamo subito 422,50 €.")
    assert value == 422.5
    assert "ti paghiamo" in snippet.lower()


def test_rebuy_extract_contextual_price_ignores_banner() -> None:
    value, _ = _extract_contextual_price("Spedizione gratis con ordine min. 100 €")
    assert value is None


def test_rebuy_assess_match_rejects_generic_search_url() -> None:
    match = _assess_rebuy_match(
        normalized_name="Valve Steam Deck OLED 1TB",
        candidate_text="Risultati ricerca rebuy",
        source_url="https://www.rebuy.it/comprare/search?q=Valve%20Steam%20Deck%20OLED%201TB",
    )
    assert match["ok"] is False
    assert match["reason"] in {"generic-search-url", "generic-url-low-coverage", "score-too-low", "low-token-similarity"}


def test_rebuy_assess_match_accepts_specific_product_url() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple iPhone 14 Pro 128GB",
        candidate_text="Apple iPhone 14 Pro 128GB Nero",
        source_url="https://www.rebuy.it/comprare/apple-iphone-14-pro-128gb-nero/123456",
    )
    assert match["ok"] is True


def test_rebuy_assess_match_rejects_generic_category_url() -> None:
    match = _assess_rebuy_match(
        normalized_name='Apple iPad Air 13" M3 256GB Wi-Fi + 5G',
        candidate_text="Apple iPad Air 13 M3",
        source_url="https://www.rebuy.it/comprare/apple",
    )
    assert match["ok"] is False
    assert match["reason"] == "generic-category-url"


def test_rebuy_assess_match_rejects_generic_category_url_for_any_category() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple Watch Series 9 GPS + Cellular 45mm",
        candidate_text="Wearable",
        source_url="https://www.rebuy.it/comprare/wearable",
    )
    assert match["ok"] is False
    assert match["reason"] == "generic-category-url"


def test_rebuy_load_storage_state_b64_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("REBUY_STORAGE_STATE_B64", encoded)
    path = _load_storage_state_b64()
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_rebuy_load_storage_state_b64_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REBUY_STORAGE_STATE_B64", "not-base64")
    assert _load_storage_state_b64() is None


def test_rebuy_load_storage_state_b64_accepts_raw_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REBUY_STORAGE_STATE_B64", '{"cookies":[],"origins":[]}')
    path = _load_storage_state_b64()
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_rebuy_load_storage_state_b64_disabled_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("REBUY_STORAGE_STATE_B64", encoded)
    monkeypatch.setenv("REBUY_USE_STORAGE_STATE", "false")
    assert _load_storage_state_b64() is None


def test_extract_rebuy_product_link_candidates_prefers_specific_product_urls() -> None:
    html = """
    <html><body>
      <a href="/comprare/search?q=Valve%20Steam%20Deck%20OLED%201TB">Search</a>
      <a href="/comprare/apple">Apple category</a>
      <a href="/comprare/wearable">Wearable category</a>
      <a href="/comprare/valve-steam-deck-oled-1tb/123456">Valve Steam Deck OLED 1TB</a>
      <a href="/comprare/valve-steam-deck/999999">Valve Steam Deck</a>
    </body></html>
    """
    candidates = _extract_rebuy_product_link_candidates(
        html=html,
        base_url="https://www.rebuy.it/comprare/search?q=Valve%20Steam%20Deck%20OLED%201TB",
        normalized_name="Valve Steam Deck OLED 1TB",
    )
    assert candidates
    assert "steam-deck-oled-1tb" in candidates[0]["url"]
