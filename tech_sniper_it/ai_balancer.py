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

    async def normalize_product_name(self, title: str) -> str:
        prompt = (
            "Estrai il modello prodotto in formato breve e rivendibile in Italia. "
            "Mantieni marca/modello/taglio memoria essenziale. "
            "Rimuovi colore, aggettivi marketing, stato e testo promozionale. "
            "Rispondi SOLO con il nome pulito."
        )

        if self._gemini_cycle:
            for _ in range(len(self.gemini_keys)):
                api_key = next(self._gemini_cycle)
                try:
                    response = await self._call_gemini(api_key, prompt, title)
                    cleaned = self._sanitize_result(response)
                    if cleaned:
                        return cleaned
                except Exception:
                    continue

        if self._openrouter_cycle:
            for _ in range(len(self.openrouter_keys)):
                api_key = next(self._openrouter_cycle)
                try:
                    response = await self._call_openrouter(api_key, prompt, title)
                    cleaned = self._sanitize_result(response)
                    if cleaned:
                        return cleaned
                except Exception:
                    continue

        return self._heuristic_normalize(title)

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
        value = (text or "").strip().strip("\"'")
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
