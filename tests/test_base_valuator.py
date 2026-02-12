from __future__ import annotations

import pytest

from tech_sniper_it.models import AmazonProduct, ProductCategory
from tech_sniper_it.valuators.base import BaseValuator
from tech_sniper_it.valuators.trenddevice import TrendDeviceValuator


class DummySuccessValuator(BaseValuator):
    platform_name = "dummy_success"

    async def _fetch_offer(self, product: AmazonProduct, normalized_name: str):
        return 321.0, "https://example.com", {"normalized": normalized_name}


class DummyFailureValuator(BaseValuator):
    platform_name = "dummy_failure"

    async def _fetch_offer(self, product: AmazonProduct, normalized_name: str):
        raise RuntimeError("scrape_failed")


@pytest.mark.asyncio
async def test_valuate_success() -> None:
    valuator = DummySuccessValuator()
    product = AmazonProduct(title="iPhone", price_eur=100.0, category=ProductCategory.APPLE_PHONE)
    result = await valuator.valuate(product, "iPhone 14")
    assert result.platform == "dummy_success"
    assert result.offer_eur == 321.0
    assert result.error is None
    assert result.raw_payload["normalized"] == "iPhone 14"


@pytest.mark.asyncio
async def test_valuate_failure_returns_error_payload() -> None:
    valuator = DummyFailureValuator()
    product = AmazonProduct(title="iPhone", price_eur=100.0, category=ProductCategory.APPLE_PHONE)
    result = await valuator.valuate(product, "iPhone 14")
    assert result.platform == "dummy_failure"
    assert result.offer_eur is None
    assert "scrape_failed" in (result.error or "")
    assert result.raw_payload["error_type"] == "RuntimeError"


def test_trenddevice_points_to_trendevice_domain() -> None:
    assert TrendDeviceValuator.base_url.startswith("https://www.trendevice.com")
