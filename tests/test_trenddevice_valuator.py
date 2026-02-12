from __future__ import annotations

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
    _detect_wizard_step,
    _extract_contextual_price,
    _extract_iphone_model_hint,
    _is_email_gate_text,
    _normalize_wizard_text,
    _pick_wizard_option,
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
