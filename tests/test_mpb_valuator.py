from __future__ import annotations

import base64
import json
import os
from types import SimpleNamespace

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.valuators.mpb import (
    MPBValuator,
    _assess_mpb_match,
    _build_query_variants,
    _clear_mpb_temporary_block,
    _contains_price_hint,
    _detect_blockers,
    _env_or_default,
    _extract_mpb_api_models,
    _extract_mpb_sell_link_candidates,
    _extract_prices_from_json_blob,
    _extract_contextual_price,
    _load_storage_state_b64,
    _mark_mpb_temporarily_blocked,
    _mpb_api_market,
    _mpb_block_remaining_seconds,
    _mpb_require_storage_state,
    _mpb_skip_ui_on_api_block,
    _mpb_total_time_budget_seconds,
    _pick_best_mpb_network_candidate,
    _rank_mpb_api_models,
    _remove_file_if_exists,
)
from tech_sniper_it.valuators.base import ValuatorRuntimeError


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


def test_detect_blockers_ignores_passive_turnstile_settings_flags() -> None:
    markers = _detect_blockers('{"turnstile":{"enabled":false},"cloudflareImageResizeEnabled":false}')
    assert "turnstile" not in markers
    assert "cloudflare" not in markers


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


def test_mpb_skip_ui_on_api_block_defaults_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MPB_SKIP_UI_ON_API_BLOCK", raising=False)
    assert _mpb_skip_ui_on_api_block() is True
    monkeypatch.setenv("MPB_SKIP_UI_ON_API_BLOCK", "false")
    assert _mpb_skip_ui_on_api_block() is False


def test_mpb_total_time_budget_seconds_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_TOTAL_TIME_BUDGET_SECONDS", "2")
    assert _mpb_total_time_budget_seconds() == 8.0
    monkeypatch.setenv("MPB_TOTAL_TIME_BUDGET_SECONDS", "25")
    assert _mpb_total_time_budget_seconds() == 25.0
    monkeypatch.setenv("MPB_TOTAL_TIME_BUDGET_SECONDS", "999")
    assert _mpb_total_time_budget_seconds() == 90.0


def test_mpb_api_market_fallbacks_to_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_API_MARKET", "xx")
    assert _mpb_api_market() == "it"
    monkeypatch.setenv("MPB_API_MARKET", "de")
    assert _mpb_api_market() == "de"


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


def test_pick_best_mpb_network_candidate_requires_model_overlap() -> None:
    candidates = [
        {
            "score": 70,
            "value": 620.0,
            "snippet": "quote valuation sony a7 iv body 620,00 €",
            "url": "https://www.mpb.com/it-it/sell/product/sony-a7-iv/999",
        },
        {
            "score": 72,
            "value": 500.0,
            "snippet": "quote valuation canon eos r7 body 500,00 €",
            "url": "https://www.mpb.com/it-it/sell/product/canon-eos-r7/123",
        },
    ]
    value, snippet = _pick_best_mpb_network_candidate(candidates, normalized_name="Canon EOS R7 Body")
    assert value == 500.0
    assert "canon eos r7" in snippet.lower()


def test_extract_prices_from_json_blob_ignores_boolean_values() -> None:
    rows = _extract_prices_from_json_blob({"offer": True, "price": False, "price_purchase": 5020})
    values = [item[1] for item in rows]
    assert values == [5020.0]


def test_extract_prices_from_json_blob_converts_large_minor_units_to_major() -> None:
    rows = _extract_prices_from_json_blob({"payload": {"offer_price": 50200}})
    values = [item[1] for item in rows]
    assert values == [502.0]


def test_pick_best_mpb_network_candidate_skips_count_metrics() -> None:
    candidates = [
        {
            "score": 90,
            "value": 120.0,
            "snippet": "results[0].product_price.count.values[0]=120",
            "url": "https://www.mpb.com/search-service/product/query/?query=canon",
        },
        {
            "score": 72,
            "value": 500.0,
            "snippet": "quote valuation canon eos r7 body 500,00 €",
            "url": "https://www.mpb.com/it-it/sell/product/canon-eos-r7/123",
        },
    ]
    value, snippet = _pick_best_mpb_network_candidate(candidates, normalized_name="Canon EOS R7 Body")
    assert value == 500.0
    assert "canon eos r7" in snippet.lower()


def test_pick_best_mpb_network_candidate_skips_user_profile_endpoint() -> None:
    candidates = [
        {
            "score": 90,
            "value": 620.0,
            "snippet": "quote valuation canon eos r7 body 620,00 €",
            "url": "https://www.mpb.com/public-api/v2/user/me/",
        },
        {
            "score": 72,
            "value": 500.0,
            "snippet": "quote valuation canon eos r7 body 500,00 €",
            "url": "https://www.mpb.com/it-it/sell/product/canon-eos-r7/123",
        },
    ]
    value, snippet = _pick_best_mpb_network_candidate(candidates, normalized_name="Canon EOS R7 Body")
    assert value == 500.0
    assert "canon eos r7" in snippet.lower()


@pytest.mark.asyncio
async def test_mpb_api_block_with_storage_state_continues_ui_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_SKIP_UI_ON_API_BLOCK", "true")
    monkeypatch.setenv("MPB_REQUIRE_STORAGE_STATE", "false")
    monkeypatch.setattr("tech_sniper_it.valuators.mpb._load_storage_state_b64", lambda: "/tmp/mpb-storage.json")

    valuator = MPBValuator(headless=True)

    async def _fake_api(**kwargs) -> tuple[float | None, str | None]:
        payload = kwargs["payload"]
        payload["api_purchase_price"] = {
            "blocked": True,
            "blockers": ["cloudflare"],
        }
        return None, None

    monkeypatch.setattr(valuator, "_fetch_offer_via_purchase_price_api", _fake_api)

    class _FakeBrowser:
        async def new_context(self, **kwargs):  # noqa: ANN003
            raise RuntimeError("ui-called")

        async def close(self) -> None:
            return None

    class _FakeChromium:
        async def launch(self, **kwargs):  # noqa: ANN003
            return _FakeBrowser()

    class _FakePlaywrightContext:
        async def __aenter__(self):
            return SimpleNamespace(chromium=_FakeChromium())

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            return False

    monkeypatch.setattr("tech_sniper_it.valuators.mpb.async_playwright", lambda: _FakePlaywrightContext())

    product = AmazonProduct(
        title="Canon EOS R7 Body",
        price_eur=900.0,
        category=ProductCategory.PHOTOGRAPHY,
    )
    with pytest.raises(RuntimeError, match="ui-called"):
        await valuator._fetch_offer(product, "Canon EOS R7 Body")


@pytest.mark.asyncio
async def test_mpb_api_block_without_storage_state_skips_ui(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPB_SKIP_UI_ON_API_BLOCK", "true")
    monkeypatch.setenv("MPB_REQUIRE_STORAGE_STATE", "false")
    monkeypatch.setattr("tech_sniper_it.valuators.mpb._load_storage_state_b64", lambda: None)

    valuator = MPBValuator(headless=True)

    async def _fake_api(**kwargs) -> tuple[float | None, str | None]:
        payload = kwargs["payload"]
        payload["api_purchase_price"] = {
            "blocked": True,
            "blockers": ["cloudflare"],
        }
        return None, None

    monkeypatch.setattr(valuator, "_fetch_offer_via_purchase_price_api", _fake_api)

    product = AmazonProduct(
        title="Canon EOS R7 Body",
        price_eur=900.0,
        category=ProductCategory.PHOTOGRAPHY,
    )
    with pytest.raises(ValuatorRuntimeError, match="UI fallback skipped"):
        await valuator._fetch_offer(product, "Canon EOS R7 Body")


def test_extract_mpb_api_models_parses_nested_values() -> None:
    payload = {
        "results": [
            {
                "model_id": {"values": ["69474"]},
                "model_name": {"values": ["Canon EOS R6"]},
                "model_url_segment": {"values": ["canon-eos-r6"]},
                "model_description": {"values": ["Mirrorless"]},
            },
            {"foo": "bar"},
        ]
    }
    models = _extract_mpb_api_models(payload)
    assert models == [
        {
            "model_id": "69474",
            "model_name": "Canon EOS R6",
            "model_url_segment": "canon-eos-r6",
            "model_description": "Mirrorless",
        }
    ]


def test_rank_mpb_api_models_prefers_higher_similarity() -> None:
    models = [
        {
            "model_id": "111",
            "model_name": "Canon EOS R6",
            "model_url_segment": "canon-eos-r6",
            "model_description": "Mirrorless camera",
        },
        {
            "model_id": "222",
            "model_name": "Sony A7 IV",
            "model_url_segment": "sony-a7-iv",
            "model_description": "Mirrorless camera",
        },
    ]
    ranked = _rank_mpb_api_models(models, normalized_name="Canon EOS R6")
    assert ranked
    assert ranked[0]["model_id"] == "111"
    assert ranked[0]["assessment"]["ok"] is True
