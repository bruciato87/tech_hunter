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


def test_base_selector_candidates_applies_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "VALUATOR_SELECTOR_OVERRIDES_JSON",
        '{"dummy_success":{"search_input":["input[data-testid=search]"]}}',
    )
    payload: dict[str, int] = {}
    valuator = DummySuccessValuator()
    selectors = valuator._selector_candidates(
        site="dummy_success",
        slot="search_input",
        defaults=["input[type=search]"],
        payload=payload,
    )
    assert selectors == ["input[type=search]", "input[data-testid=search]"]
    assert payload["selector_overrides"]["search_input"] == 1


class _FakeInput:
    def __init__(self, *, visible: bool, meta: dict[str, str]) -> None:
        self.visible = visible
        self.meta = meta
        self.filled_value: str | None = None

    async def is_visible(self) -> bool:
        return self.visible

    async def evaluate(self, _script: str):  # noqa: ANN001
        return self.meta

    async def click(self, timeout: int = 0) -> None:  # noqa: ARG002
        return None

    async def fill(self, value: str, timeout: int = 0) -> None:  # noqa: ARG002
        self.filled_value = value


class _FakeClickTarget:
    def __init__(self, *, visible: bool = True) -> None:
        self.visible = visible
        self.clicked = False

    async def is_visible(self) -> bool:
        return self.visible

    async def click(self, timeout: int = 0, force: bool = False) -> None:  # noqa: ARG002
        self.clicked = True


class _FakeLocator:
    def __init__(self, items: list) -> None:  # noqa: ANN401
        self.items = items

    def filter(self, has_text=None):  # noqa: ANN001
        return self

    async def count(self) -> int:
        return len(self.items)

    def nth(self, index: int):  # noqa: ANN201
        return self.items[index]


class _FakePage:
    def __init__(self, *, inputs: list[_FakeInput], clickables: dict[str, list[_FakeClickTarget]], body: str) -> None:
        self._inputs = inputs
        self._clickables = clickables
        self._body = body
        self.url = "https://example.test/page"

    def locator(self, selector: str):  # noqa: ANN201
        if selector == "input, textarea, [role='searchbox']":
            return _FakeLocator(self._inputs)
        return _FakeLocator(self._clickables.get(selector, []))

    async def wait_for_timeout(self, _timeout_ms: int) -> None:
        return None

    async def title(self) -> str:
        return "Example Rebuy Sell"

    async def inner_text(self, selector: str, timeout: int = 0) -> str:  # noqa: ARG002
        assert selector == "body"
        return self._body


@pytest.mark.asyncio
async def test_fill_first_semantic_fills_matching_input() -> None:
    valuator = DummySuccessValuator()
    page = _FakePage(
        inputs=[
            _FakeInput(visible=True, meta={"placeholder": "cerca prodotto"}),
        ],
        clickables={},
        body="rebuy vendi",
    )
    ok = await valuator._fill_first_semantic(
        page,
        value="iPhone 14 Pro",
        keywords=["cerca", "search"],
        timeout_ms=200,
    )
    assert ok is True
    assert page._inputs[0].filled_value == "iPhone 14 Pro"


@pytest.mark.asyncio
async def test_click_first_semantic_clicks_matching_target() -> None:
    valuator = DummySuccessValuator()
    target = _FakeClickTarget()
    page = _FakePage(
        inputs=[],
        clickables={"button": [target]},
        body="rebuy vendi",
    )
    ok = await valuator._click_first_semantic(
        page,
        keywords=["continua"],
        timeout_ms=200,
        selectors=["button"],
    )
    assert ok is True
    assert target.clicked is True


@pytest.mark.asyncio
async def test_collect_ui_probe_reports_drift_when_keywords_missing() -> None:
    valuator = DummySuccessValuator()
    page = _FakePage(inputs=[], clickables={}, body="pagina generica senza token target")
    probe = await valuator._collect_ui_probe(
        page=page,
        site="rebuy",
        stage="search_input_missing",
        expected_keywords=["rebuy", "vendi", "search"],
        min_hits=2,
    )
    assert probe["drift_suspected"] is True
    assert probe["site"] == "rebuy"


@pytest.mark.asyncio
async def test_collect_ui_probe_reports_no_drift_when_keywords_present() -> None:
    valuator = DummySuccessValuator()
    page = _FakePage(inputs=[], clickables={}, body="rebuy vendi search usato")
    probe = await valuator._collect_ui_probe(
        page=page,
        site="rebuy",
        stage="ok",
        expected_keywords=["rebuy", "vendi", "search"],
        min_hits=2,
    )
    assert probe["drift_suspected"] is False
