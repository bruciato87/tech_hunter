from __future__ import annotations

from tech_sniper_it.sources.amazon_warehouse import (
    _canonical_amazon_url,
    _detect_page_barriers,
    _expand_marketplaces,
    _extract_products_from_html,
    _parse_proxy_entry,
    _parse_user_agent_list,
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
    assert item["category"] == "apple_phone"
    assert item["url"] == "https://www.amazon.it/dp/B0ABCDE123"


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
