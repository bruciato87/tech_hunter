from __future__ import annotations

import asyncio
import itertools
import os
import re
from typing import Iterable

import httpx


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _mask_secret(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return "n/a"
    if len(raw) <= 6:
        return "***"
    return f"{raw[:2]}***{raw[-2:]}"


def _short_error(exc: Exception, limit: int = 120) -> str:
    text = " ".join(str(exc).split())
    if not text:
        return exc.__class__.__name__
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _short_title(value: str, limit: int = 80) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


class SmartAIBalancer:
    """Rotates Gemini free keys first, then falls back to OpenRouter keys."""

    def __init__(
        self,
        gemini_keys: Iterable[str] | None = None,
        openrouter_keys: Iterable[str] | None = None,
        gemini_model: str | None = None,
        openrouter_model: str | None = None,
        openrouter_base_url: str | None = None,
        timeout_seconds: float = 25.0,
    ) -> None:
        self.gemini_keys = list(gemini_keys or _split_csv(os.getenv("GEMINI_API_KEYS")))
        self.openrouter_keys = list(openrouter_keys or _split_csv(os.getenv("OPENROUTER_API_KEYS")))
        self.gemini_model = gemini_model or _env_or_default("GEMINI_MODEL", "gemini-2.0-flash")
        self.openrouter_model = openrouter_model or _env_or_default("OPENROUTER_MODEL", "openrouter/auto")
        self.openrouter_base_url = openrouter_base_url or _env_or_default(
            "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1/chat/completions"
        )
        self.timeout = timeout_seconds
        self._gemini_cycle = itertools.cycle(self.gemini_keys) if self.gemini_keys else None
        self._openrouter_cycle = itertools.cycle(self.openrouter_keys) if self.openrouter_keys else None
        self._cache: dict[str, tuple[str, dict[str, str | bool | None]]] = {}
        self._last_usage: dict[str, str | bool | None] = {
            "provider": None,
            "model": None,
            "mode": "fallback",
            "ai_used": False,
        }

    async def normalize_product_name(self, title: str) -> str:
        normalized, _ = await self.normalize_with_meta(title)
        return normalized

    async def normalize_with_meta(self, title: str) -> tuple[str, dict[str, str | bool | None]]:
        cache_key = (title or "").strip()
        print(
            "[ai] normalize request | "
            f"title='{_short_title(cache_key)}' | "
            f"gemini_keys={len(self.gemini_keys)} model={self.gemini_model} | "
            f"openrouter_keys={len(self.openrouter_keys)} model={self.openrouter_model}"
        )
        cached = self._cache.get(cache_key)
        if cached:
            normalized, meta = cached
            cached_meta = dict(meta)
            cached_meta["mode"] = "cache"
            self._last_usage = cached_meta
            print(
                "[ai] cache hit | "
                f"provider={cached_meta.get('provider')} | "
                f"model={cached_meta.get('model') or 'rule-based'} | "
                f"normalized='{normalized}'"
            )
            return normalized, cached_meta

        prompt = (
            "Estrai il modello prodotto in formato breve e rivendibile in Italia. "
            "Mantieni marca/modello/taglio memoria essenziale. "
            "Rimuovi colore, aggettivi marketing, stato e testo promozionale. "
            "Rispondi SOLO con il nome pulito."
        )

        if self._gemini_cycle:
            for attempt in range(1, len(self.gemini_keys) + 1):
                api_key = next(self._gemini_cycle)
                print(
                    "[ai] attempt | "
                    f"provider=gemini model={self.gemini_model} "
                    f"attempt={attempt}/{len(self.gemini_keys)} key={_mask_secret(api_key)}"
                )
                try:
                    response = await self._call_gemini(api_key, prompt, title)
                    cleaned = self._sanitize_result(response)
                    if cleaned:
                        usage = {
                            "provider": "gemini",
                            "model": self.gemini_model,
                            "mode": "live",
                            "ai_used": True,
                        }
                        self._cache[cache_key] = (cleaned, usage)
                        self._last_usage = usage
                        print(
                            "[ai] selected | "
                            f"provider=gemini model={self.gemini_model} normalized='{cleaned}'"
                        )
                        return cleaned, usage
                    print(
                        "[ai] empty response | "
                        f"provider=gemini model={self.gemini_model} attempt={attempt}"
                    )
                except Exception as exc:
                    print(
                        "[ai] failed | "
                        f"provider=gemini model={self.gemini_model} attempt={attempt} "
                        f"error={_short_error(exc)}"
                    )
                    continue

        if self._openrouter_cycle:
            for attempt in range(1, len(self.openrouter_keys) + 1):
                api_key = next(self._openrouter_cycle)
                print(
                    "[ai] attempt | "
                    f"provider=openrouter model={self.openrouter_model} "
                    f"attempt={attempt}/{len(self.openrouter_keys)} key={_mask_secret(api_key)}"
                )
                try:
                    response = await self._call_openrouter(api_key, prompt, title)
                    cleaned = self._sanitize_result(response)
                    if cleaned:
                        usage = {
                            "provider": "openrouter",
                            "model": self.openrouter_model,
                            "mode": "live",
                            "ai_used": True,
                        }
                        self._cache[cache_key] = (cleaned, usage)
                        self._last_usage = usage
                        print(
                            "[ai] selected | "
                            f"provider=openrouter model={self.openrouter_model} normalized='{cleaned}'"
                        )
                        return cleaned, usage
                    print(
                        "[ai] empty response | "
                        f"provider=openrouter model={self.openrouter_model} attempt={attempt}"
                    )
                except Exception as exc:
                    print(
                        "[ai] failed | "
                        f"provider=openrouter model={self.openrouter_model} attempt={attempt} "
                        f"error={_short_error(exc)}"
                    )
                    continue

        fallback = self._heuristic_normalize(title)
        usage = {
            "provider": "heuristic",
            "model": None,
            "mode": "fallback",
            "ai_used": False,
        }
        self._cache[cache_key] = (fallback, usage)
        self._last_usage = usage
        print(f"[ai] fallback heuristic | normalized='{fallback}'")
        return fallback, usage

    def get_last_usage(self) -> dict[str, str | bool | None]:
        return dict(self._last_usage)

    def get_strategy_snapshot(self) -> dict[str, str | int]:
        return {
            "gemini_keys": len(self.gemini_keys),
            "gemini_model": self.gemini_model,
            "openrouter_keys": len(self.openrouter_keys),
            "openrouter_model": self.openrouter_model,
            "order": "gemini->openrouter->heuristic",
        }

    async def _call_gemini(self, api_key: str, prompt: str, title: str) -> str:
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.gemini_model}:generateContent?key={api_key}"
        )
        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": f"{prompt}\n\nTitolo: {title}"},
                    ]
                }
            ],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 64},
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return ""
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            return ""
        return parts[0].get("text", "").strip()

    async def _call_openrouter(self, api_key: str, prompt: str, title: str) -> str:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.openrouter_model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": title},
            ],
            "temperature": 0.1,
            "max_tokens": 64,
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(self.openrouter_base_url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        choices = data.get("choices", [])
        if not choices:
            return ""
        return choices[0].get("message", {}).get("content", "").strip()

    def _sanitize_result(self, text: str) -> str:
        value = (text or "").strip()
        value = re.sub(r"```(?:\w+)?", "", value)
        value = value.replace("```", "")
        lines = [line.strip() for line in value.splitlines() if line.strip()]
        value = lines[0] if lines else value
        value = re.sub(r"^[\-\*\d\.\)\s]+", "", value)
        value = value.strip().strip("\"'")
        value = re.sub(r"[*_`~]", "", value)
        value = re.sub(r"\[(?:\d+(?:\s*,\s*\d+)*)\]", "", value)
        value = re.sub(r"^nome(?:\s+prodotto)?\s*:\s*", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value)
        value = re.sub(r"\b(colore|color|ottime condizioni|ricondizionato)\b.*", "", value, flags=re.IGNORECASE)
        value = value.strip(" -:,")
        return value[:120]

    def _heuristic_normalize(self, title: str) -> str:
        text = re.sub(r"\((.*?)\)", "", title)
        text = re.sub(
            r"\b(ottime condizioni|ricondizionato|warehouse|amazon|come nuovo|grado a|excellent)\b",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(
            r"\b(nero|black|bianco|white|argento|silver|grafite|space gray|grigio|blu|azzurro|rosso|verde|viola)\b",
            "",
            text,
            flags=re.IGNORECASE,
        )
        text = re.sub(r"\s+", " ", text).strip(" -:,")
        return text[:120]


async def demo() -> None:
    balancer = SmartAIBalancer()
    print(await balancer.normalize_product_name("Apple iPhone 14 Pro Max 128GB Sideral Gray Ottime Condizioni"))


if __name__ == "__main__":
    asyncio.run(demo())
