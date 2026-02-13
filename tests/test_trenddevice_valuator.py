from __future__ import annotations

import base64
import json

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.valuators.trenddevice import (
    STEP_BATTERY,
    STEP_CAPACITY,
    STEP_COLOR,
    STEP_CONDITION,
    STEP_DEVICE_FAMILY,
    STEP_MARKET,
    STEP_MODEL,
    STEP_SIM,
    STEP_YES_NO,
    WizardOption,
    _assess_trenddevice_match,
    _detect_wizard_step,
    _extract_contextual_price,
    _extract_keyed_prices_from_text,
    _extract_iphone_model_hint,
    _extract_prices_from_json_blob,
    _is_credible_network_candidate,
    _is_email_gate_text,
    _load_storage_state_b64,
    _normalize_wizard_text,
    _parse_plain_price,
    _pick_best_network_candidate,
    _pick_wizard_option,
    _remove_file_if_exists,
)


def test_normalize_wizard_text_compacts_and_lowercases() -> None:
    assert _normalize_wizard_text("  iPhone 14 Pro, 128GB  ") == "iphone 14 pro 128gb"


def test_extract_iphone_model_hint_prefers_model_and_variant() -> None:
    assert _extract_iphone_model_hint("Apple iPhone 14 Pro Max 128GB Nero") == "14 pro max"
    assert _extract_iphone_model_hint("Apple iPhone 15 128GB") == "15"


def test_detect_wizard_step_capacity() -> None:
    options = [
        WizardOption(index=0, text="128 GB", normalized="128 gb"),
        WizardOption(index=1, text="256 GB", normalized="256 gb"),
    ]
    assert _detect_wizard_step(options) == STEP_CAPACITY


def test_detect_wizard_step_condition() -> None:
    options = [
        WizardOption(index=0, text="Perfetto", normalized="perfetto"),
        WizardOption(index=1, text="Normale usura", normalized="normale usura"),
        WizardOption(index=2, text="Accettabile", normalized="accettabile"),
    ]
    assert _detect_wizard_step(options) == STEP_CONDITION


def test_detect_wizard_step_battery_market_color_yes_no() -> None:
    assert _detect_wizard_step([WizardOption(index=0, text="85% e originale", normalized="85% e originale")]) == STEP_BATTERY
    assert _detect_wizard_step(
        [
            WizardOption(index=0, text="Italia", normalized="italia"),
            WizardOption(index=1, text="Estero", normalized="estero"),
        ]
    ) == STEP_MARKET
    assert _detect_wizard_step(
        [
            WizardOption(index=0, text="Nero", normalized="nero"),
            WizardOption(index=1, text="Argento", normalized="argento"),
        ]
    ) == STEP_COLOR
    assert _detect_wizard_step(
        [
            WizardOption(index=0, text="Si", normalized="si"),
            WizardOption(index=1, text="No", normalized="no"),
        ]
    ) == STEP_YES_NO


def test_detect_wizard_step_device_family() -> None:
    options = [
        WizardOption(index=0, text="iPhone", normalized="iphone"),
        WizardOption(index=1, text="Samsung", normalized="samsung"),
    ]
    assert _detect_wizard_step(options) == STEP_DEVICE_FAMILY


def test_pick_model_prefers_exact_model_not_pro_max() -> None:
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
    )
    options = [
        WizardOption(index=0, text="14 Pro Max", normalized="14 pro max"),
        WizardOption(index=1, text="14 Pro", normalized="14 pro"),
        WizardOption(index=2, text="13 Pro", normalized="13 pro"),
    ]
    picked = _pick_wizard_option(step=STEP_MODEL, options=options, product=product, normalized_name=product.title)
    assert picked is not None
    assert picked.normalized == "14 pro"


def test_pick_sim_prefers_physical_sim_card() -> None:
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
    )
    options = [
        WizardOption(index=0, text="e-Sim (no sim fisica)", normalized="e sim no sim fisica"),
        WizardOption(index=1, text="Sim Card (con slot sim)", normalized="sim card con slot sim"),
    ]
    picked = _pick_wizard_option(step=STEP_SIM, options=options, product=product, normalized_name=product.title)
    assert picked is not None
    assert "sim card" in picked.normalized


def test_pick_device_family_prefers_watch_for_smartwatch_products() -> None:
    product = AmazonProduct(
        title="Apple Watch Series 9 GPS + Cellular 45mm",
        price_eur=350.0,
        category=ProductCategory.SMARTWATCH,
    )
    options = [
        WizardOption(index=0, text="iPhone", normalized="iphone"),
        WizardOption(index=1, text="Apple Watch", normalized="apple watch"),
    ]
    picked = _pick_wizard_option(step=STEP_DEVICE_FAMILY, options=options, product=product, normalized_name=product.title)
    assert picked is not None
    assert "watch" in picked.normalized


def test_pick_wizard_option_other_steps() -> None:
    product = AmazonProduct(
        title="Apple iPhone 14 Pro 128GB Nero",
        price_eur=500.0,
        category=ProductCategory.APPLE_PHONE,
    )
    assert (
        _pick_wizard_option(
            step=STEP_DEVICE_FAMILY,
            options=[
                WizardOption(index=0, text="Samsung", normalized="samsung"),
                WizardOption(index=1, text="iPhone", normalized="iphone"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "iphone"
    )
    assert (
        _pick_wizard_option(
            step=STEP_CAPACITY,
            options=[
                WizardOption(index=0, text="64 GB", normalized="64 gb"),
                WizardOption(index=1, text="128 GB", normalized="128 gb"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "128 gb"
    )
    assert (
        _pick_wizard_option(
            step=STEP_CONDITION,
            options=[
                WizardOption(index=0, text="Accettabile", normalized="accettabile"),
                WizardOption(index=1, text="Normale usura", normalized="normale usura"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "normale usura"
    )
    assert (
        _pick_wizard_option(
            step=STEP_BATTERY,
            options=[
                WizardOption(index=0, text="Non originale", normalized="non originale"),
                WizardOption(index=1, text=">= 85%", normalized=">= 85%"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == ">= 85%"
    )
    assert (
        _pick_wizard_option(
            step=STEP_MARKET,
            options=[
                WizardOption(index=0, text="Estero", normalized="estero"),
                WizardOption(index=1, text="Italia", normalized="italia"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "italia"
    )
    assert (
        _pick_wizard_option(
            step=STEP_COLOR,
            options=[
                WizardOption(index=0, text="Nero", normalized="nero"),
                WizardOption(index=1, text="Argento", normalized="argento"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "nero"
    )
    assert (
        _pick_wizard_option(
            step=STEP_YES_NO,
            options=[
                WizardOption(index=0, text="No", normalized="no"),
                WizardOption(index=1, text="Si", normalized="si"),
            ],
            product=product,
            normalized_name=product.title,
        ).normalized
        == "si"
    )


def test_extract_contextual_price_ignores_company_capital() -> None:
    text = "Cap.Soc. 248.059,43 € i.v. Ti offriamo 412,99 € con pagamento veloce."
    value, snippet = _extract_contextual_price(text)
    assert value == 412.99
    assert "offriamo" in snippet.lower()


def test_extract_contextual_price_skips_non_offer_banner() -> None:
    value, _ = _extract_contextual_price("Spedizione gratis (ordine min. 100 €).")
    assert value is None


def test_is_email_gate_text_detects_lead_form_copy() -> None:
    text = (
        "Inserisci la tua mail e scopri quanto puoi guadagnare dal tuo dispositivo usato! "
        "Scopri la valutazione"
    )
    assert _is_email_gate_text(text) is True


def test_is_email_gate_text_rejects_generic_page_text() -> None:
    assert _is_email_gate_text("Vendi il tuo iPhone in pochi minuti.") is False


def test_parse_plain_price_supports_eu_and_cents() -> None:
    assert _parse_plain_price("412,99") == 412.99
    assert _parse_plain_price("41299") == 412.99
    assert _parse_plain_price("10") is None


def test_extract_keyed_prices_from_text_detects_offer_tokens() -> None:
    rows = _extract_keyed_prices_from_text("price: 399,99 EUR | offerta 412.50€")
    values = [item[1] for item in rows]
    assert any(abs(value - 399.99) < 0.001 for value in values)
    assert any(abs(value - 412.5) < 0.001 for value in values)


def test_extract_prices_from_json_blob_detects_nested_price_fields() -> None:
    data = {"result": {"offerPrice": 37900, "meta": {"prezzo_finale": "412,99 €"}}}
    rows = _extract_prices_from_json_blob(data)
    values = sorted(item[1] for item in rows)
    assert 379.0 in values
    assert 412.99 in values


def test_pick_best_network_candidate_prefers_high_score_then_value() -> None:
    value, snippet = _pick_best_network_candidate(
        [
            {"score": 50, "value": 300.0, "snippet": "candidate a", "source": "context", "url": "https://www.trendevice.com/vendi/valutazione/"},
            {
                "score": 72,
                "value": 250.0,
                "snippet": "ti offriamo 250€ dopo valutazione",
                "source": "context",
                "url": "https://www.trendevice.com/vendi/valutazione/",
            },
            {
                "score": 72,
                "value": 280.0,
                "snippet": "ti offriamo 280€ dopo valutazione",
                "source": "context",
                "url": "https://www.trendevice.com/vendi/valutazione/",
            },
        ]
    )
    assert value == 280.0
    assert "280" in snippet


def test_is_credible_network_candidate_rejects_static_promotional_chunk() -> None:
    candidate = {
        "url": "https://www.trendevice.com/_next/static/chunks/abc.js",
        "score": 80,
        "value": 800.0,
        "snippet": "fino a -800€",
        "source": "context",
    }
    assert _is_credible_network_candidate(candidate) is False


def test_assess_trenddevice_match_rejects_watch_mismatch_on_generic_url() -> None:
    product = AmazonProduct(
        title="Apple Watch Series 9 GPS + Cellular 45mm",
        price_eur=279.0,
        category=ProductCategory.SMARTWATCH,
    )
    match = _assess_trenddevice_match(
        product=product,
        normalized_name=product.title,
        wizard_steps=[
            {"step_type": STEP_DEVICE_FAMILY, "selected": "iPhone"},
            {"step_type": STEP_MODEL, "selected": "14 Pro"},
            {"step_type": STEP_CAPACITY, "selected": "128 GB"},
        ],
        source_url="https://www.trendevice.com/vendi/valutazione/",
        price_text="Ti offriamo 525,00 €",
    )
    assert match["ok"] is False
    assert match["reason"] in {
        "device-family-mismatch",
        "generic-url-low-coverage",
        "anchor-mismatch",
    }


def test_assess_trenddevice_match_accepts_coherent_watch_quote() -> None:
    product = AmazonProduct(
        title="Apple Watch Series 9 GPS + Cellular 45mm",
        price_eur=279.0,
        category=ProductCategory.SMARTWATCH,
    )
    match = _assess_trenddevice_match(
        product=product,
        normalized_name=product.title,
        wizard_steps=[
            {"step_type": STEP_DEVICE_FAMILY, "selected": "Apple Watch"},
            {"step_type": STEP_MODEL, "selected": "Series 9"},
            {"step_type": STEP_CONDITION, "selected": "Normale usura"},
        ],
        source_url="https://www.trendevice.com/vendi/valutazione/series-9",
        price_text="Ti offriamo 135,00 €",
    )
    assert match["ok"] is True


def test_load_storage_state_b64_decodes_valid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = json.dumps({"cookies": [], "origins": []}, ensure_ascii=False).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    monkeypatch.setenv("TRENDDEVICE_USE_STORAGE_STATE", "true")
    monkeypatch.setenv("TRENDDEVICE_STORAGE_STATE_B64", encoded)
    path = _load_storage_state_b64()
    assert path is not None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        assert isinstance(loaded, dict)
        assert loaded.get("cookies") == []
    finally:
        _remove_file_if_exists(path)


def test_load_storage_state_b64_returns_none_on_invalid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRENDDEVICE_USE_STORAGE_STATE", "true")
    monkeypatch.setenv("TRENDDEVICE_STORAGE_STATE_B64", "not-base64")
    assert _load_storage_state_b64() is None


def test_load_storage_state_b64_accepts_raw_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRENDDEVICE_USE_STORAGE_STATE", "true")
    monkeypatch.setenv("TRENDDEVICE_STORAGE_STATE_B64", '{"cookies":[],"origins":[]}')
    path = _load_storage_state_b64()
    assert path is not None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        assert isinstance(loaded, dict)
        assert loaded.get("cookies") == []
    finally:
        _remove_file_if_exists(path)
