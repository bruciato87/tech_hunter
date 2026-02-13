from __future__ import annotations

import base64
import json
import os

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.valuators.mpb import (
    _assess_mpb_match,
    _build_query_variants,
    _clear_mpb_temporary_block,
    _contains_price_hint,
    _detect_blockers,
    _env_or_default,
    _extract_mpb_sell_link_candidates,
    _extract_contextual_price,
    _load_storage_state_b64,
    _mark_mpb_temporarily_blocked,
    _mpb_block_remaining_seconds,
    _mpb_require_storage_state,
    _remove_file_if_exists,
)


def test_env_or_default_prefers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_MAX_ATTEMPTS", "5")
    assert _env_or_default("MPB_MAX_ATTEMPTS", "3") == "5"


def test_env_or_default_falls_back_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_MAX_ATTEMPTS", "  ")
    assert _env_or_default("MPB_MAX_ATTEMPTS", "3") == "3"


def test_load_storage_state_b64_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("MPB_STORAGE_STATE_B64", encoded)
    path = _load_storage_state_b64()
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_load_storage_state_b64_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_STORAGE_STATE_B64", "not-base64")
    assert _load_storage_state_b64() is None


def test_load_storage_state_b64_accepts_raw_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_STORAGE_STATE_B64", '{"cookies":[],"origins":[]}')
    path = _load_storage_state_b64()
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_load_storage_state_b64_disabled_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("MPB_STORAGE_STATE_B64", encoded)
    monkeypatch.setenv("MPB_USE_STORAGE_STATE", "false")
    assert _load_storage_state_b64() is None


def test_remove_file_if_exists_is_safe() -> None:
    _remove_file_if_exists("/tmp/definitely-not-existing-file-mpb.json")


def test_detect_blockers_cloudflare_and_turnstile() -> None:
    markers = _detect_blockers("Ci siamo quasi…", "<html>Cloudflare Turnstile challenge-platform</html>")
    assert "ci siamo quasi" in markers
    assert "cloudflare" in markers
    assert "turnstile" in markers


def test_extract_contextual_price_prefers_offer_context() -> None:
    text = "Spedizione 20 €. Ti paghiamo 412,99 € con valutazione immediata."
    value, snippet = _extract_contextual_price(text)
    assert value == 412.99
    assert "ti paghiamo" in snippet.lower()


def test_extract_contextual_price_returns_none_without_context() -> None:
    value, _ = _extract_contextual_price("Prezzo di vendita: 499,99 €")
    assert value is None


def test_contains_price_hint_handles_sell_context() -> None:
    assert _contains_price_hint("Ti paghiamo 320,00 € subito") is True
    assert _contains_price_hint("Prezzo di vendita: 320,00 €") is False


def test_mpb_temporary_block_mark_and_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_BLOCK_COOLDOWN_SECONDS", "120")
    _clear_mpb_temporary_block()
    _mark_mpb_temporarily_blocked("turnstile/cloudflare")
    remaining = _mpb_block_remaining_seconds()
    assert remaining > 0
    _clear_mpb_temporary_block()
    assert _mpb_block_remaining_seconds() == 0


def test_mpb_require_storage_state_defaults_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MPB_REQUIRE_STORAGE_STATE", raising=False)
    assert _mpb_require_storage_state() is True
    monkeypatch.setenv("MPB_REQUIRE_STORAGE_STATE", "false")
    assert _mpb_require_storage_state() is False


def test_build_query_variants_prioritizes_ean_and_normalized_name() -> None:
    product = AmazonProduct(
        title="Canon EOS R6 Mark II Corpo + Kit",
        price_eur=1200.0,
        category=ProductCategory.PHOTOGRAPHY,
        ean="4549292200510",
    )
    variants = _build_query_variants(product, "Canon EOS R6 Mark II 24-105")
    assert variants[0] == "4549292200510"
    assert "Canon EOS R6 Mark II 24-105" in variants
    assert len(set(variants)) == len(variants)


def test_assess_mpb_match_rejects_generic_search_url() -> None:
    match = _assess_mpb_match(
        normalized_name="DJI Mini 4 Pro Fly More Combo",
        candidate_text="Risultati ricerca MPB",
        source_url="https://www.mpb.com/it-it/cerca?q=DJI+Mini+4+Pro",
    )
    assert match["ok"] is False
    assert match["reason"] == "generic-url"


def test_assess_mpb_match_accepts_specific_sell_url() -> None:
    match = _assess_mpb_match(
        normalized_name="Canon EOS R7 Body",
        candidate_text="Canon EOS R7 Body - Sell your camera",
        source_url="https://www.mpb.com/it-it/sell/product/canon-eos-r7/12345",
    )
    assert match["ok"] is True


def test_extract_mpb_sell_link_candidates_ranks_specific_urls() -> None:
    html = """
    <html><body>
      <a href="/it-it/cerca?q=canon+eos+r7">Risultati ricerca</a>
      <a href="/it-it/sell/product/canon-eos-r7/12345">Canon EOS R7 Body</a>
      <a href="/it-it/sell/product/canon-eos-r6/99999">Canon EOS R6 Body</a>
    </body></html>
    """
    candidates = _extract_mpb_sell_link_candidates(
        html=html,
        base_url="https://www.mpb.com/it-it/cerca?q=canon+eos+r7",
        normalized_name="Canon EOS R7 Body",
    )
    assert candidates
    assert candidates[0]["url"].startswith("https://www.mpb.com/it-it/sell/product/canon-eos-r7")
