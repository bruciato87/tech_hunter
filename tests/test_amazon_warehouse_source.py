from __future__ import annotations

import base64
import json
import os

from tech_sniper_it.sources.amazon_warehouse import (
    _canonical_amazon_url,
    _decode_storage_state_b64,
    _decode_storage_state_env,
    _detect_page_barriers,
    _expand_marketplaces,
    _extract_asin_from_url,
    _extract_products_from_html,
    _parse_cart_summary,
    _load_storage_state_paths,
    _parse_proxy_entry,
    _parse_user_agent_list,
    _per_marketplace_limit,
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
              <span>Apple iPhone 14 Pro 128GB Ricondizionato</span>
            </a>
          </h2>
          <span class="a-price"><span class="a-offscreen">679,00 €</span></span>
        </div>
      </body>
    </html>
    """
    items = _extract_products_from_html(html, "www.amazon.it")
    assert len(items) == 1
    item = items[0]
    assert item["title"] == "Apple iPhone 14 Pro 128GB Ricondizionato"
    assert item["price_eur"] == 679.0
    assert item["displayed_price_eur"] == 679.0
    assert item["category"] == "apple_phone"
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


def test_expand_marketplaces_handles_eu_alias() -> None:
    assert _expand_marketplaces(["it", "eu"]) == ["it", "de", "fr", "es"]


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
