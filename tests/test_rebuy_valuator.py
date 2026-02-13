from __future__ import annotations

import base64
import json
import os

import pytest

from tech_sniper_it.valuators.rebuy import (
    _assess_rebuy_match,
    _extract_embedded_rebuy_urls,
    _extract_contextual_price,
    _extract_rebuy_ry_inject_price,
    _pick_best_rebuy_network_candidate,
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


def test_rebuy_assess_match_accepts_specific_sell_flow_url() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple iPhone 14 128GB",
        candidate_text="Apple iPhone 14 128GB mezzanotte",
        source_url="https://www.rebuy.it/vendere/p/apple-iphone-14/12684558",
    )
    assert match["ok"] is True


def test_rebuy_assess_match_accepts_specific_sell_flow_url_without_capacity_token() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple iPhone 14 128GB",
        candidate_text="Apple iPhone 14",
        source_url="https://www.rebuy.it/vendere/p/apple-iphone-14/12684558",
    )
    assert match["ok"] is True


def test_rebuy_assess_match_rejects_watch_ultra_unversioned_vs_ultra_3() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple Watch Ultra GPS + Cellular 49mm",
        candidate_text="Apple Watch Ultra 3 49 mm cassa in titanio",
        source_url="https://www.rebuy.it/vendere/smartwatch/apple-watch-ultra-3-49-mm-cassa-in-titanio-nero-wifi-plus-cellulare_21513678",
    )
    assert match["ok"] is False
    assert match["reason"] == "model-generation-mismatch"


def test_rebuy_assess_match_rejects_iphone_generation_mismatch() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple iPhone 14 Pro 128GB",
        candidate_text="Apple iPhone 15 Pro 128GB",
        source_url="https://www.rebuy.it/vendere/p/apple-iphone-15-pro/12345678",
    )
    assert match["ok"] is False
    assert match["reason"] == "model-generation-mismatch"


def test_rebuy_assess_match_rejects_explicit_capacity_conflict() -> None:
    match = _assess_rebuy_match(
        normalized_name='Apple iPad Air 13" M3 256GB',
        candidate_text="Apple iPad Air 6 13 1TB Wi-Fi + Cellulare Blu",
        source_url="https://www.rebuy.it/vendere/tablet-e-ebook-reader/apple-ipad-air-6-13-1tb-wifi-plus-cellulare-blu_15426786",
    )
    assert match["ok"] is False
    assert match["reason"] == "capacity-mismatch"


def test_rebuy_assess_match_rejects_generic_category_url() -> None:
    match = _assess_rebuy_match(
        normalized_name='Apple iPad Air 13" M3 256GB Wi-Fi + 5G',
        candidate_text="Apple iPad Air 13 M3",
        source_url="https://www.rebuy.it/comprare/apple",
    )
    assert match["ok"] is False
    assert match["reason"] == "generic-category-url"


def test_rebuy_assess_match_allows_generic_when_match_is_strong() -> None:
    match = _assess_rebuy_match(
        normalized_name="Apple Watch Series 9 GPS + Cellular 45mm",
        candidate_text="Apple Watch Series 9 GPS + Cellular 45mm Midnight",
        source_url="https://www.rebuy.it/vendere/wearables",
    )
    assert match["ok"] is True
    assert match["generic_override"] is True


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


def test_extract_rebuy_product_link_candidates_accepts_sell_flow_links() -> None:
    html = """
    <html><body>
      <main>
        <a href="/vendere/cerca?query=iphone+14">Search</a>
        <a href="/vendere/p/apple-iphone-14/12684558?from=rom">Apple iPhone 14</a>
        <a href="/vendere/apple">Apple category</a>
      </main>
    </body></html>
    """
    candidates = _extract_rebuy_product_link_candidates(
        html=html,
        base_url="https://www.rebuy.it/vendere/cerca?query=iphone+14",
        normalized_name="Apple iPhone 14 128GB",
    )
    assert candidates
    assert "/vendere/p/apple-iphone-14/12684558" in candidates[0]["url"]


def test_extract_embedded_rebuy_urls_supports_escaped_script_payload() -> None:
    html = r"""
    <script>
      window.__NUXT__ = {"items":[
        {"url":"\/vendere\/p\/apple-watch-series-9\/12684558"},
        {"url":"\/vendere\/smartwatch\/apple-watch-series-9_12684558"}
      ]};
    </script>
    """
    urls = _extract_embedded_rebuy_urls(html, base_url="https://www.rebuy.it/vendere/cerca?query=apple+watch")
    assert len(urls) == 2
    assert urls[0].startswith("https://www.rebuy.it/vendere/")


def test_extract_rebuy_ry_inject_price_prefers_target_grade() -> None:
    html = """
    <html><body>
      <script id="ry-inject" type="application/json">
        {"iphoneGroupViewDto":{"product":{
          "price_purchase":22902,
          "purchase_a0_price":24750,
          "purchase_a1_price":22902,
          "variants":[{"purchasePrice":22902}]
        }}}
      </script>
    </body></html>
    """
    value_a1, snippet_a1 = _extract_rebuy_ry_inject_price(html, target_grade="a1")
    value_a0, snippet_a0 = _extract_rebuy_ry_inject_price(html, target_grade="a0")
    assert value_a1 == 229.02
    assert "purchase_a1_price" in snippet_a1
    assert value_a0 == 247.5
    assert "purchase_a0_price" in snippet_a0


def test_extract_rebuy_ry_inject_price_ignores_relations_zero_values() -> None:
    html = """
    <html><body>
      <script id="ry-inject" type="application/json">
        {"iphoneGroupViewDto":{
          "relations":{"root":[{"purchase_a1_price":0}]},
          "product":{"purchase_a1_price":13500}
        }}
      </script>
    </body></html>
    """
    value, snippet = _extract_rebuy_ry_inject_price(html, target_grade="a1")
    assert value == 135.0
    assert "product.purchase_a1_price" in snippet


def test_extract_rebuy_ry_inject_price_ignores_relations_higher_prices() -> None:
    html = """
    <html><body>
      <script id="ry-inject" type="application/json">
        {"iphoneGroupViewDto":{
          "relations":{"rom":[{"purchase_a1_price":36974}]},
          "product":{"price_purchase":22902,"purchase_a1_price":22902}
        }}
      </script>
    </body></html>
    """
    value, snippet = _extract_rebuy_ry_inject_price(html, target_grade="a1")
    assert value == 229.02
    assert "product.price_purchase" in snippet or "product.purchase_a1_price" in snippet


def test_pick_best_rebuy_network_candidate_requires_model_coherence() -> None:
    candidates = [
        {
            "price": 520.0,
            "snippet": "Pagamento Diretto 520,00 € Apple Watch Ultra 2",
            "url": "https://www.rebuy.it/vendere/p/apple-watch-ultra-2/123",
            "status": 200,
            "source": "network-cash",
        },
        {
            "price": 800.0,
            "snippet": "Pagamento Diretto 800,00 € DJI Mini 4 Pro",
            "url": "https://www.rebuy.it/vendere/p/dji-mini-4-pro/999",
            "status": 200,
            "source": "network-cash",
        },
    ]
    value, snippet = _pick_best_rebuy_network_candidate(
        candidates,
        normalized_name="Apple Watch Ultra 2 GPS + Cellular 49mm",
    )
    assert value == 520.0
    assert "apple watch" in snippet.lower()
