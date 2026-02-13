from tech_sniper_it.utils import (
    detect_color_variants,
    extract_capacity_gb,
    infer_amazon_warehouse_condition,
    parse_eur_price,
)


def test_parse_eur_price_suffix() -> None:
    assert parse_eur_price("Ti paghiamo 1.234,56 € oggi") == 1234.56


def test_parse_eur_price_prefix() -> None:
    assert parse_eur_price("Prezzo: € 899,99") == 899.99


def test_parse_eur_price_returns_none_on_invalid() -> None:
    assert parse_eur_price("nessun prezzo disponibile") is None


def test_extract_capacity_gb() -> None:
    assert extract_capacity_gb("Apple iPhone 14 Pro 128GB") == "128 GB"
    assert extract_capacity_gb("Nikon D750 body") is None


def test_detect_color_variants() -> None:
    assert "nero" in detect_color_variants("iPhone nero 128gb")
    assert detect_color_variants("macbook edition unknown") == ()


def test_infer_amazon_warehouse_condition_detects_acceptable_with_packaging_hint() -> None:
    condition, confidence, packaging_only = infer_amazon_warehouse_condition(
        "Usato - Condizioni accettabili. Confezione danneggiata."
    )
    assert condition == "acceptable"
    assert confidence >= 0.9
    assert packaging_only is True


def test_infer_amazon_warehouse_condition_detects_like_new() -> None:
    condition, confidence, packaging_only = infer_amazon_warehouse_condition("Used - Like new")
    assert condition == "like_new"
    assert confidence > 0.9
    assert packaging_only is False
