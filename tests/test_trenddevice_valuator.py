from __future__ import annotations

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.valuators.trenddevice import (
    STEP_CAPACITY,
    STEP_CONDITION,
    STEP_MODEL,
    STEP_SIM,
    WizardOption,
    _detect_wizard_step,
    _extract_contextual_price,
    _extract_iphone_model_hint,
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


def test_extract_contextual_price_ignores_company_capital() -> None:
    text = "Cap.Soc. 248.059,43 € i.v. Ti offriamo 412,99 € con pagamento veloce."
    value, snippet = _extract_contextual_price(text)
    assert value == 412.99
    assert "offriamo" in snippet.lower()


def test_extract_contextual_price_skips_non_offer_banner() -> None:
    value, _ = _extract_contextual_price("Spedizione gratis (ordine min. 100 €).")
    assert value is None

