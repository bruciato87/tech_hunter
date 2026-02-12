from tech_sniper_it.models import ProductCategory, ValuationResult


def test_product_category_from_raw() -> None:
    assert ProductCategory.from_raw("fotografia reflex") == ProductCategory.PHOTOGRAPHY
    assert ProductCategory.from_raw("Apple iPhone") == ProductCategory.APPLE_PHONE
    assert ProductCategory.from_raw("gaming laptop") == ProductCategory.GENERAL_TECH


def test_valuation_result_is_valid_flag() -> None:
    valid = ValuationResult(platform="rebuy", normalized_name="iPhone", offer_eur=600.0)
    invalid = ValuationResult(platform="rebuy", normalized_name="iPhone", offer_eur=None, error="timeout")
    assert valid.is_valid is True
    assert invalid.is_valid is False
