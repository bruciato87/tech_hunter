from __future__ import annotations

import pytest

from tech_sniper_it.ai_balancer import SmartAIBalancer, _split_csv


def test_split_csv() -> None:
    assert _split_csv("k1, k2 ,,k3") == ["k1", "k2", "k3"]
    assert _split_csv(None) == []


@pytest.mark.asyncio
async def test_normalize_uses_gemini_first(monkeypatch: pytest.MonkeyPatch) -> None:
    balancer = SmartAIBalancer(gemini_keys=["g1"], openrouter_keys=["o1"])

    async def fake_gemini(api_key: str, prompt: str, title: str) -> str:
        assert api_key == "g1"
        return '"Apple iPhone 14 Pro 128GB colore nero"'

    async def fail_openrouter(api_key: str, prompt: str, title: str) -> str:
        raise AssertionError("OpenRouter should not be called when Gemini succeeds")

    monkeypatch.setattr(balancer, "_call_gemini", fake_gemini)
    monkeypatch.setattr(balancer, "_call_openrouter", fail_openrouter)

    result = await balancer.normalize_product_name("Apple iPhone 14 Pro 128GB Nero")
    assert result == "Apple iPhone 14 Pro 128GB"


@pytest.mark.asyncio
async def test_normalize_fallbacks_to_openrouter(monkeypatch: pytest.MonkeyPatch) -> None:
    balancer = SmartAIBalancer(gemini_keys=["g1"], openrouter_keys=["o1"])

    async def fail_gemini(api_key: str, prompt: str, title: str) -> str:
        raise RuntimeError("quota_exceeded")

    async def fake_openrouter(api_key: str, prompt: str, title: str) -> str:
        assert api_key == "o1"
        return "iPhone 14 Pro 128GB"

    monkeypatch.setattr(balancer, "_call_gemini", fail_gemini)
    monkeypatch.setattr(balancer, "_call_openrouter", fake_openrouter)

    result = await balancer.normalize_product_name("Apple iPhone 14 Pro 128GB Nero")
    assert result == "iPhone 14 Pro 128GB"


@pytest.mark.asyncio
async def test_normalize_uses_heuristic_when_no_keys() -> None:
    balancer = SmartAIBalancer(gemini_keys=[], openrouter_keys=[])
    result = await balancer.normalize_product_name("Apple iPhone 14 Pro 128GB Nero Ottime Condizioni")
    assert "Nero" not in result
    assert "Ottime Condizioni" not in result
    assert "iPhone 14 Pro 128GB" in result


def test_sanitize_result_removes_markdown_wrappers() -> None:
    balancer = SmartAIBalancer(gemini_keys=[], openrouter_keys=[])
    value = balancer._sanitize_result("**Apple iPhone 15 128 GB**")
    assert value == "Apple iPhone 15 128 GB"


def test_sanitize_result_takes_first_clean_line() -> None:
    balancer = SmartAIBalancer(gemini_keys=[], openrouter_keys=[])
    raw = """```text
Nome prodotto: iPhone 14 Pro Max 256GB
```"""
    value = balancer._sanitize_result(raw)
    assert value == "iPhone 14 Pro Max 256GB"


def test_sanitize_result_removes_citation_markers() -> None:
    balancer = SmartAIBalancer(gemini_keys=[], openrouter_keys=[])
    value = balancer._sanitize_result("Apple iPhone 16 Pro 128GB[1][2][4]")
    assert value == "Apple iPhone 16 Pro 128GB"
