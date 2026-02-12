from __future__ import annotations

from tech_sniper_it.sources.amazon_warehouse import _canonical_amazon_url, _expand_marketplaces, _extract_products_from_html


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
