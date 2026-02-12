from __future__ import annotations

import httpx
import pytest

from tech_sniper_it.ai_balancer import SmartAIBalancer, _extract_openrouter_resolved_model, _split_csv


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
async def test_openrouter_prefers_most_powerful_free_model(monkeypatch: pytest.MonkeyPatch) -> None:
    balancer = SmartAIBalancer(
        gemini_keys=[],
        openrouter_keys=["o1"],
        openrouter_model="openrouter/auto",
        openrouter_free_models=["model-small:free", "model-ultra:free", "model-mid:free"],
        openrouter_model_power={"model-small:free": 10, "model-ultra:free": 100, "model-mid:free": 60},
        openrouter_max_models_per_request=3,
    )
    attempts: list[str] = []

    async def fake_openrouter(api_key: str, prompt: str, title: str, model: str | None = None) -> tuple[str, str | None]:
        attempts.append(model or "")
        return "iPhone 15 Pro 128GB", model

    monkeypatch.setattr(balancer, "_call_openrouter", fake_openrouter)

    result, usage = await balancer.normalize_with_meta("Apple iPhone 15 Pro 128GB Nero")
    assert result == "iPhone 15 Pro 128GB"
    assert usage["provider"] == "openrouter"
    assert usage["model"] == "model-ultra:free"
    assert attempts[0] == "model-ultra:free"


@pytest.mark.asyncio
async def test_openrouter_cooldown_skips_quota_model(monkeypatch: pytest.MonkeyPatch) -> None:
    balancer = SmartAIBalancer(
        gemini_keys=[],
        openrouter_keys=["o1"],
        openrouter_model="openrouter/auto",
        openrouter_free_models=["model-top:free", "model-safe:free"],
        openrouter_model_power={"model-top:free": 120, "model-safe:free": 80},
        openrouter_max_models_per_request=2,
        openrouter_cooldown_seconds=999,
    )
    attempts: list[str] = []

    async def fake_openrouter(api_key: str, prompt: str, title: str, model: str | None = None) -> tuple[str, str | None]:
        attempts.append(model or "")
        if model == "model-top:free":
            request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
            response = httpx.Response(429, request=request, json={"error": {"message": "quota exceeded"}})
            raise httpx.HTTPStatusError("429 quota", request=request, response=response)
        return "Canon EOS R50", model

    monkeypatch.setattr(balancer, "_call_openrouter", fake_openrouter)

    first, usage_first = await balancer.normalize_with_meta("Canon EOS R50 Kit")
    second, usage_second = await balancer.normalize_with_meta("Canon EOS R50 Corpo")

    assert first == "Canon EOS R50"
    assert second == "Canon EOS R50"
    assert usage_first["model"] == "model-safe:free"
    assert usage_second["model"] == "model-safe:free"
    assert attempts[:2] == ["model-top:free", "model-safe:free"]
    assert attempts[2] == "model-safe:free"


def test_strategy_snapshot_includes_power_selector() -> None:
    balancer = SmartAIBalancer(gemini_keys=[], openrouter_keys=["o1"])
    snapshot = balancer.get_strategy_snapshot()
    assert snapshot["openrouter_selection_mode"] == "power-first-free-with-availability"
    assert isinstance(snapshot["openrouter_model_pool"], list)


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


def test_extract_openrouter_resolved_model_prefers_top_level() -> None:
    data = {"model": "anthropic/claude-3.5-sonnet"}
    assert _extract_openrouter_resolved_model(data) == "anthropic/claude-3.5-sonnet"


def test_extract_openrouter_resolved_model_uses_header_fallback() -> None:
    data = {"choices": [{"message": {"content": "ok"}}]}
    headers = {"x-openrouter-model": "openai/gpt-4.1-mini"}
    assert _extract_openrouter_resolved_model(data, headers) == "openai/gpt-4.1-mini"
