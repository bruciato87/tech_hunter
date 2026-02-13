from __future__ import annotations

from tech_sniper_it.valuators.rebuy import _assess_rebuy_match, _extract_contextual_price


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
