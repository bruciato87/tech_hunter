from __future__ import annotations

from tech_sniper_it.valuators.rebuy import _extract_contextual_price


def test_rebuy_extract_contextual_price_prefers_offer_copy() -> None:
    value, snippet = _extract_contextual_price("Spedizione 12 €. Ti paghiamo subito 422,50 €.")
    assert value == 422.5
    assert "ti paghiamo" in snippet.lower()


def test_rebuy_extract_contextual_price_ignores_banner() -> None:
    value, _ = _extract_contextual_price("Spedizione gratis con ordine min. 100 €")
    assert value is None
