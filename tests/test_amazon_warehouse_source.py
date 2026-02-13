from __future__ import annotations

import base64
import json
import os

from tech_sniper_it.sources.amazon_warehouse import (
    _candidate_product_urls_for_cart,
    _build_cart_cleanup_asins,
    _canonical_amazon_url,
    _decode_storage_state_b64,
    _decode_storage_state_env,
    _detect_page_barriers,
    _expand_marketplaces,
    _extract_asin_from_url,
    _extract_products_from_html,
    _infer_cart_addition,
    _parse_cart_summary,
    _load_storage_state_paths,
    _parse_proxy_entry,
    _parse_user_agent_list,
    _per_marketplace_limit,
    _query_variants_for_host,
    _rotate_values,
    _remove_file_if_exists,
    _storage_state_for_host,
    _should_fail_fast,
)


def test_canonical_amazon_url_prefers_dp() -> None:
    url = _canonical_amazon_url("www.amazon.it", "/Apple-Phone/dp/B0ABCDE123/ref=sr_1_1")
    assert url == "https://www.amazon.it/dp/B0ABCDE123"


def test_extract_products_from_html_parses_title_price_and_url() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPhone 14 Pro 128GB Usato - Condizioni accettabili</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">679,00 €</span></span>
          <span>Confezione danneggiata</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["title"] == "Apple iPhone 14 Pro 128GB Usato - Condizioni accettabili"
    assert item["price_eur"] == 679.0
    assert item["displayed_price_eur"] == 679.0
    assert item["category"] == "apple_phone"
    assert item["amazon_condition"] == "acceptable"
    assert item["amazon_packaging_only"] is True
    assert item["url"] == "https://www.amazon.it/dp/B0ABCDE123"


def test_extract_products_from_html_supports_h2_span_title_markup() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result" data-asin="B0ABCDE123">
          <a class="a-link-normal s-no-outline" href="/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1">
            <img class="s-image" alt="Apple iPhone 14 Pro 128GB - Space Black (Ricondizionato)" />
          </a>
          <h2 class="a-size-base-plus a-spacing-none a-color-base a-text-normal">
            <span>Apple iPhone 14 Pro 128GB - Space Black (Ricondizionato)</span>
          </h2>
          <span class="a-price"><span class="a-offscreen">629,90 €</span></span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["title"] == "Apple iPhone 14 Pro 128GB - Space Black (Ricondizionato)"
    assert item["price_eur"] == 629.9
    assert item["displayed_price_eur"] == 629.9
    assert item["category"] == "apple_phone"
    assert item["url"] == "https://www.amazon.it/dp/B0ABCDE123"


def test_candidate_product_urls_for_cart_prioritizes_canonical_dp() -> None:
    urls = _candidate_product_urls_for_cart(
        "www.amazon.it",
        product_url="https://www.amazon.it/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1",
        asin="B0ABCDE123",
    )
    assert urls[0] == "https://www.amazon.it/dp/B0ABCDE123"
    assert "https://www.amazon.it/dp/B0ABCDE123?psc=1&th=1" in urls
    assert "https://www.amazon.it/gp/aw/d/B0ABCDE123" in urls


def test_expand_marketplaces_handles_eu_alias() -> None:
    assert _expand_marketplaces(["it", "eu"]) == ["it", "de", "fr", "es"]


def test_rotate_values_offsets_order() -> None:
    assert _rotate_values(["it", "de", "fr", "es"], 1) == ["de", "fr", "es", "it"]
    assert _rotate_values(["it", "de", "fr", "es"], 5) == ["de", "fr", "es", "it"]


def test_query_variants_for_host_builds_localized_fallback(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("AMAZON_WAREHOUSE_QUERY_VARIANTS_PER_HOST", "3")
    variants = _query_variants_for_host("www.amazon.de", "apple watch ultra 2 amazon warehouse")
    assert variants[0] == "apple watch ultra 2 amazon warehouse"
    assert "apple watch ultra 2" in variants
    assert "apple watch ultra 2 warehouse deals" in variants


def test_extract_products_from_html_supports_fallback_card_markup() -> None:
    html = """
    <html>
      <body>
        <div class="s-result-item" data-asin="B0ABCDE123">
          <a class="a-link-normal s-no-outline" href="/gp/product/B0ABCDE123/ref=sr_1_1">
            <span class="a-size-medium a-color-base a-text-normal">Apple iPhone 14 Pro Max 256GB</span>
          </a>
          <span class="a-price">
            <span class="a-offscreen">899,99 €</span>
          </span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["title"] == "Apple iPhone 14 Pro Max 256GB"
    assert item["price_eur"] == 899.99
    assert item["displayed_price_eur"] == 899.99
    assert item["category"] == "apple_phone"
    assert item["url"] == "https://www.amazon.it/dp/B0ABCDE123"


def test_extract_products_from_html_ignores_installment_price_when_full_price_present() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPad-Air-13-M3-256GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPad Air 13" M3 256GB Wi-Fi</span>
            </a>
          </h2>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">121,80 €</span></span>
            <span>al mese</span>
          </div>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">609,00 €</span></span>
          </div>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 609.0
    assert item["price_eur"] == 609.0


def test_extract_products_from_html_ignores_installment_price_spanish_por_mes() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPad-Air-13-M3-256GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPad Air 13" M3 256GB Wi-Fi</span>
            </a>
          </h2>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">121,80 €</span></span>
            <span>por mes</span>
          </div>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">599,00 €</span></span>
          </div>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.es")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 599.0
    assert item["price_eur"] == 599.0


def test_extract_products_from_html_discards_low_outlier_even_without_installment_hint() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPad-Air-13-M3-256GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPad Air 13" M3 256GB Wi-Fi</span>
            </a>
          </h2>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">121,80 €</span></span>
          </div>
          <div class="pricing">
            <span class="a-price"><span class="a-offscreen">589,00 €</span></span>
          </div>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.es")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 589.0
    assert item["price_eur"] == 589.0


def test_extract_products_from_html_rejects_low_current_price_when_list_anchor_indicates_outlier() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPad-Air-13-M3-256GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPad Air 13" M3 256GB Wi-Fi + Cellular</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">185,44 €</span></span>
          <span class="a-price a-text-price"><span class="a-offscreen">927,20 €</span></span>
          <span>Risparmi 185,44 €</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 927.2
    assert item["price_eur"] == 927.2


def test_extract_products_from_html_does_not_fallback_to_ambiguous_row_text_prices() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPad-Air-13-M3-256GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPad Air 13" M3 256GB Wi-Fi</span>
            </a>
          </h2>
          <span>Risparmi 185,44 €</span>
          <span>Prezzo precedente 927,20 €</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert items == []


def test_extract_products_from_html_applies_coupon_eur_discount_to_net_price() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPhone 14 Pro 128GB Ricondizionato</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">679,00 €</span></span>
          <span>Coupon 40 € applicato al momento del pagamento.</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 679.0
    assert item["extra_discount_eur"] == 40.0
    assert item["price_eur"] == 639.0


def test_extract_products_from_html_applies_coupon_percent_discount_to_net_price() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPhone 14 Pro 128GB Ricondizionato</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">679,00 €</span></span>
          <span>Sconto extra 10% al checkout.</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 679.0
    assert item["extra_discount_pct"] == 10.0
    assert item["price_eur"] == 611.1


def test_extract_products_from_html_does_not_double_count_implied_list_discount() -> None:
    html = """
    <html>
      <body>
        <div data-component-type="s-search-result">
          <h2>
            <a href="/Apple-iPhone-14-Pro-128GB/dp/B0ABCDE123/ref=sr_1_1">
              <span>Apple iPhone 14 Pro 128GB Ricondizionato</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">679,00 €</span></span>
          <span class="a-price a-text-price"><span class="a-offscreen">849,00 €</span></span>
          <span>Risparmi 20%</span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["displayed_price_eur"] == 679.0
    assert item["list_price_eur"] == 849.0
    assert "extra_discount_pct" not in item
    assert item["price_eur"] == 679.0


def test_parse_cart_summary_extracts_subtotal_promo_and_total() -> None:
    html = """
    <html>
      <body>
        <div class="sc-list-item" data-asin="B0ABCDE123">
          <span class="sc-product-price">699,00 €</span>
        </div>
        <div id="sc-subtotal-amount-activecart">
          <span class="a-price"><span class="a-offscreen">699,00 €</span></span>
        </div>
        <div id="sc-subtotal-discount">
          <span class="a-offscreen">-70,00 €</span>
        </div>
        <div id="subtotals-marketplace-table">
          <span class="grand-total-price"><span class="a-offscreen">629,00 €</span></span>
        </div>
      </body>
    </html>
    """
    summary = _parse_cart_summary(html, "B0ABCDE123")
    assert summary["target_in_cart"] is True
    assert summary["cart_asins"] == ["B0ABCDE123"]
    assert summary["target_row_price"] == 699.0
    assert summary["subtotal_price"] == 699.0
    assert summary["promo_discount_eur"] == 70.0
    assert summary["total_price"] == 629.0


def test_infer_cart_addition_accepts_asin_mismatch_when_new_row_is_added() -> None:
    before = {
        "row_count": 0,
        "cart_asins": [],
        "subtotal_price": None,
        "total_price": None,
        "target_in_cart": False,
    }
    after = {
        "row_count": 1,
        "cart_asins": ["B0ZZZZ9999"],
        "subtotal_price": 699.0,
        "total_price": 699.0,
        "target_in_cart": False,
    }
    result = _infer_cart_addition(before, after, "B0ABCDE123")
    assert result["added"] is True
    assert result["asin_mismatch"] is True
    assert result["new_asins"] == ["B0ZZZZ9999"]


def test_infer_cart_addition_rejects_when_no_cart_change_detected() -> None:
    before = {
        "row_count": 2,
        "cart_asins": ["B0AAA11111", "B0BBB22222"],
        "subtotal_price": 500.0,
        "total_price": 480.0,
        "target_in_cart": False,
    }
    after = {
        "row_count": 2,
        "cart_asins": ["B0AAA11111", "B0BBB22222"],
        "subtotal_price": 500.0,
        "total_price": 480.0,
        "target_in_cart": False,
    }
    result = _infer_cart_addition(before, after, "B0ABCDE123")
    assert result["added"] is False
    assert result["asin_mismatch"] is False
    assert result["new_asins"] == []


def test_infer_cart_addition_rejects_small_delta_without_row_or_asin_signal(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("AMAZON_WAREHOUSE_CART_PRICING_MIN_ADD_DELTA_EUR", "25")
    before = {
        "row_count": 0,
        "cart_asins": [],
        "subtotal_price": None,
        "total_price": 10.0,
        "target_in_cart": False,
    }
    after = {
        "row_count": 0,
        "cart_asins": [],
        "subtotal_price": None,
        "total_price": 17.96,
        "target_in_cart": False,
    }
    result = _infer_cart_addition(before, after, "B0ABCDE123")
    assert result["added"] is False
    assert result["strong"] is False
    assert result["delta_total"] == 7.96


def test_infer_cart_addition_accepts_large_delta_signal(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("AMAZON_WAREHOUSE_CART_PRICING_MIN_ADD_DELTA_EUR", "25")
    before = {
        "row_count": 0,
        "cart_asins": [],
        "subtotal_price": None,
        "total_price": 0.0,
        "target_in_cart": False,
    }
    after = {
        "row_count": 0,
        "cart_asins": [],
        "subtotal_price": None,
        "total_price": 129.9,
        "target_in_cart": False,
    }
    result = _infer_cart_addition(before, after, "B0ABCDE123")
    assert result["added"] is True
    assert result["strong"] is False
    assert result["delta_total"] == 129.9


def test_parse_cart_summary_falls_back_to_subtotal_minus_promo_when_total_missing() -> None:
    html = """
    <html>
      <body>
        <div class="sc-list-item" data-asin="B0ABCDE123">
          <span class="sc-product-price">499,00 €</span>
        </div>
        <div id="sc-subtotal-amount-activecart">
          <span class="a-price"><span class="a-offscreen">499,00 €</span></span>
        </div>
        <div id="sc-subtotal-discount">
          <span class="a-offscreen">30,00 €</span>
        </div>
      </body>
    </html>
    """
    summary = _parse_cart_summary(html, "B0ABCDE123")
    assert summary["subtotal_price"] == 499.0
    assert summary["promo_discount_eur"] == 30.0
    assert summary["total_price"] == 469.0


def test_build_cart_cleanup_asins_removes_only_new_items_and_target() -> None:
    cleanup = _build_cart_cleanup_asins(
        before_cart_asins=["B0AAA11111", "B0BBB22222"],
        after_cart_asins=["B0AAA11111", "B0BBB22222", "B0CCC33333"],
        target_asin="B0DDD44444",
    )
    assert cleanup == ["B0CCC33333", "B0DDD44444"]


def test_build_cart_cleanup_asins_keeps_target_once() -> None:
    cleanup = _build_cart_cleanup_asins(
        before_cart_asins=[],
        after_cart_asins=["B0ABCDEF12"],
        target_asin="B0ABCDEF12",
    )
    assert cleanup == ["B0ABCDEF12"]


def test_extract_asin_from_url_supports_aw_path_and_query_fallback() -> None:
    assert _extract_asin_from_url("https://www.amazon.it/gp/aw/d/B0ABCDEF12?smid=A11") == "B0ABCDEF12"
    assert _extract_asin_from_url("https://www.amazon.es/s?k=test&pd_rd_i=B09XYZ1234") == "B09XYZ1234"


def test_detect_page_barriers_flags_captcha_and_consent() -> None:
    captcha_html = """
    <html>
      <body>
        <h1>Robot Check</h1>
        <form action="/errors/validateCaptcha">
          <input id="captchacharacters" />
        </form>
      </body>
    </html>
    """
    consent_html = """
    <html>
      <body>
        <button id="sp-cc-accept">Accept all cookies</button>
      </body>
    </html>
    """
    sorry_html = "<html><head><title>Toutes nos excuses</title></head><body></body></html>"
    captcha_barriers = _detect_page_barriers(captcha_html, "Robot Check")
    consent_barriers = _detect_page_barriers(consent_html, "Cookie settings")
    sorry_barriers = _detect_page_barriers(sorry_html)

    assert "captcha" in captcha_barriers
    assert "robot-check" in captcha_barriers
    assert "consent" in consent_barriers
    assert "sorry-page" in sorry_barriers


def test_detect_page_barriers_does_not_flag_header_signin_link() -> None:
    html = """
    <html>
      <body>
        <header>
          <a href="/ap/signin">Ciao, accedi</a>
        </header>
        <div data-component-type="s-search-result">
          <h2><span>Apple iPhone 14 Pro 128GB</span></h2>
          <span class="a-price"><span class="a-offscreen">699,00 €</span></span>
        </div>
      </body>
    </html>
    """
    barriers = _detect_page_barriers(html, "Amazon.it : iphone 14 pro 128gb amazon warehouse")
    assert "signin" not in barriers
    assert "consent" not in barriers


def test_parse_proxy_entry_supports_auth_and_port() -> None:
    proxy = _parse_proxy_entry("http://user:pass@proxy.example.com:8080")
    assert proxy == {
        "server": "http://proxy.example.com:8080",
        "username": "user",
        "password": "pass",
    }


def test_parse_proxy_entry_rejects_invalid_scheme() -> None:
    assert _parse_proxy_entry("ftp://proxy.example.com:21") is None


def test_parse_user_agent_list_supports_json_and_separator() -> None:
    parsed_json = _parse_user_agent_list('["UA-1","UA-2","UA-1"]')
    parsed_pipe = _parse_user_agent_list("UA-3||UA-4")
    assert parsed_json == ["UA-1", "UA-2"]
    assert parsed_pipe == ["UA-3", "UA-4"]


def test_should_fail_fast_only_without_proxy_pool() -> None:
    assert _should_fail_fast(["sorry-page"], proxy_pool_size=0, fail_fast=True) is True
    assert _should_fail_fast(["captcha"], proxy_pool_size=0, fail_fast=True) is True
    assert _should_fail_fast(["consent"], proxy_pool_size=0, fail_fast=True) is False
    assert _should_fail_fast(["sorry-page"], proxy_pool_size=1, fail_fast=True) is False
    assert _should_fail_fast(["sorry-page"], proxy_pool_size=0, fail_fast=False) is False


def test_per_marketplace_limit_balances_total_budget(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.delenv("AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT", raising=False)
    assert _per_marketplace_limit(12, 4) == 3
    assert _per_marketplace_limit(24, 4) == 6


def test_decode_storage_state_b64_valid(monkeypatch) -> None:  # noqa: ANN001
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("AMAZON_WAREHOUSE_STORAGE_STATE_B64", encoded)
    path = _decode_storage_state_b64()
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_decode_storage_state_b64_invalid(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("AMAZON_WAREHOUSE_STORAGE_STATE_B64", "not-base64")
    assert _decode_storage_state_b64() is None


def test_decode_storage_state_env_valid_for_marketplace_specific_secret(monkeypatch) -> None:  # noqa: ANN001
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE", encoded)
    path = _decode_storage_state_env("AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE")
    try:
        assert path is not None
        assert os.path.exists(path)
    finally:
        _remove_file_if_exists(path)


def test_load_storage_state_paths_prefers_marketplace_specific_and_fallback(monkeypatch) -> None:  # noqa: ANN001
    payload = {"cookies": [], "origins": []}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    monkeypatch.setenv("AMAZON_WAREHOUSE_STORAGE_STATE_B64", encoded)
    monkeypatch.setenv("AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR", encoded)
    paths = _load_storage_state_paths()
    try:
        assert "default" in paths
        assert "fr" in paths
        assert _storage_state_for_host(paths, "www.amazon.fr") == paths["fr"]
        assert _storage_state_for_host(paths, "www.amazon.it") == paths["default"]
    finally:
        for path in set(paths.values()):
            _remove_file_if_exists(path)
