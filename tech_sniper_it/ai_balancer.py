from __future__ import annotations

import asyncio
import itertools
import json
import os
import re
import time
from typing import Any
from typing import Iterable

import httpx


DEFAULT_OPENROUTER_FREE_MODELS = [
    "deepseek/deepseek-r1:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "perplexity/sonar",
]


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        item = (value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
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


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _extract_openrouter_resolved_model(data: dict[str, Any], headers: dict[str, str] | None = None) -> str | None:
    top_level_model = data.get("model")
    if isinstance(top_level_model, str) and top_level_model.strip():
        return top_level_model.strip()

    choices = data.get("choices", [])
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        for key in ("model", "provider_model", "resolved_model"):
            value = first.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    if headers:
        for key in ("x-openrouter-model", "x-model", "x-upstream-model"):
            value = headers.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _extract_openrouter_error_message(exc: Exception) -> str:
    details = _short_error(exc, limit=200).lower()
    response = getattr(exc, "response", None)
    if response is not None:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                error_blob = payload.get("error")
                if isinstance(error_blob, dict):
                    message = error_blob.get("message")
                    if isinstance(message, str) and message.strip():
                        details = message.strip().lower()
                elif isinstance(error_blob, str) and error_blob.strip():
                    details = error_blob.strip().lower()
        except Exception:
            pass
    return details


def _classify_openrouter_error(exc: Exception) -> tuple[str, int | None]:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    message = _extract_openrouter_error_message(exc)

    if status_code == 429 or "rate limit" in message or "too many requests" in message:
        return "rate_limited", status_code
    if status_code == 402 or "insufficient credit" in message or "insufficient balance" in message:
        return "credits_exhausted", status_code
    if "quota" in message or ("token" in message and ("exceed" in message or "insufficient" in message)):
        return "token_exhausted", status_code
    if status_code == 404:
        return "model_not_found", status_code
    if status_code == 400 and (
        "model not found" in message
        or "unknown model" in message
        or "invalid model" in message
        or "does not exist" in message
    ):
        return "model_not_found", status_code
    if status_code is not None and status_code >= 500:
        return "upstream_error", status_code
    if isinstance(exc, (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException)):
        return "transport_error", status_code
    return "generic_error", status_code


class SmartAIBalancer:
    """Rotates Gemini free keys first, then falls back to OpenRouter keys."""

    def __init__(
        self,
        gemini_keys: Iterable[str] | None = None,
        openrouter_keys: Iterable[str] | None = None,
        gemini_model: str | None = None,
        openrouter_model: str | None = None,
        openrouter_free_models: Iterable[str] | None = None,
        openrouter_model_power: dict[str, float] | None = None,
        openrouter_base_url: str | None = None,
        openrouter_max_models_per_request: int | None = None,
        openrouter_cooldown_seconds: int | None = None,
        openrouter_not_found_cooldown_seconds: int | None = None,
        openrouter_transient_cooldown_seconds: int | None = None,
        timeout_seconds: float = 25.0,
    ) -> None:
        self.gemini_keys = list(gemini_keys or _split_csv(os.getenv("GEMINI_API_KEYS")))
        self.openrouter_keys = list(openrouter_keys or _split_csv(os.getenv("OPENROUTER_API_KEYS")))
        self.gemini_model = gemini_model or _env_or_default("GEMINI_MODEL", "gemini-2.0-flash")
        self.openrouter_model = openrouter_model or _env_or_default("OPENROUTER_MODEL", "openrouter/auto")
        free_models_raw = (
            list(openrouter_free_models)
            if openrouter_free_models is not None
            else _split_csv(os.getenv("OPENROUTER_FREE_MODELS"))
        )
        if not free_models_raw:
            free_models_raw = list(DEFAULT_OPENROUTER_FREE_MODELS)
        self.openrouter_free_models = _dedupe_keep_order(free_models_raw)
        self.openrouter_model_pool = self._build_openrouter_model_pool()
        self.openrouter_max_models_per_request = max(
            1,
            openrouter_max_models_per_request
            if openrouter_max_models_per_request is not None
            else _env_int("OPENROUTER_MAX_MODELS_PER_REQUEST", 3),
        )
        self.openrouter_cooldown_seconds = max(
            1,
            openrouter_cooldown_seconds
            if openrouter_cooldown_seconds is not None
            else _env_int("OPENROUTER_MODEL_COOLDOWN_SECONDS", 900),
        )
        self.openrouter_not_found_cooldown_seconds = max(
            1,
            openrouter_not_found_cooldown_seconds
            if openrouter_not_found_cooldown_seconds is not None
            else _env_int("OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS", 86400),
        )
        self.openrouter_transient_cooldown_seconds = max(
            1,
            openrouter_transient_cooldown_seconds
            if openrouter_transient_cooldown_seconds is not None
            else _env_int("OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS", 120),
        )
        self.openrouter_model_power = dict(openrouter_model_power or self._load_model_power_overrides())
        self.openrouter_base_url = openrouter_base_url or _env_or_default(
            "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1/chat/completions"
        )
        self.timeout = timeout_seconds
        self._gemini_cycle = itertools.cycle(self.gemini_keys) if self.gemini_keys else None
        self._openrouter_cycle = itertools.cycle(self.openrouter_keys) if self.openrouter_keys else None
        self._openrouter_model_stats: dict[str, dict[str, Any]] = {
            model: {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "avg_latency_ms": None,
                "blocked_until": 0.0,
                "last_error_kind": None,
                "last_error_message": None,
            }
            for model in self.openrouter_model_pool
        }
        self._last_successful_openrouter_model: str | None = None
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
                ranked_models = self._rank_openrouter_models()
                if not ranked_models:
                    ranked_models = [self.openrouter_model]
                candidate_models = self._augment_openrouter_candidates(
                    ranked_models[: self.openrouter_max_models_per_request],
                    ranked_models,
                )
                preview = ", ".join(candidate_models)
                print(
                    "[ai] openrouter ranking | "
                    f"requested={self.openrouter_model} "
                    f"attempt={attempt}/{len(self.openrouter_keys)} "
                    f"key={_mask_secret(api_key)} "
                    f"candidates={preview}"
                )
                for model_index, candidate_model in enumerate(candidate_models, start=1):
                    print(
                        "[ai] attempt | "
                        f"provider=openrouter model={candidate_model} "
                        f"key_attempt={attempt}/{len(self.openrouter_keys)} "
                        f"model_attempt={model_index}/{len(candidate_models)} "
                        f"key={_mask_secret(api_key)}"
                    )
                    started_at = time.perf_counter()
                    try:
                        response = await self._invoke_openrouter_with_model(api_key, prompt, title, candidate_model)
                        latency_ms = max(1.0, (time.perf_counter() - started_at) * 1000.0)
                        if isinstance(response, tuple):
                            response_text, resolved_model = response
                        else:
                            response_text, resolved_model = str(response), None
                        selected_model = resolved_model or candidate_model
                        cleaned = self._sanitize_result(response_text)
                        if cleaned:
                            self._mark_openrouter_success(candidate_model, latency_ms, selected_model)
                            usage = {
                                "provider": "openrouter",
                                "model": selected_model,
                                "mode": "live",
                                "ai_used": True,
                            }
                            self._cache[cache_key] = (cleaned, usage)
                            self._last_usage = usage
                            print(
                                "[ai] selected | "
                                f"provider=openrouter model={selected_model} "
                                f"requested={self.openrouter_model} "
                                f"candidate={candidate_model} "
                                f"latency_ms={latency_ms:.0f} normalized='{cleaned}'"
                            )
                            return cleaned, usage
                        self._mark_openrouter_failure(
                            candidate_model,
                            error_kind="empty_response",
                            error_message="empty response text after sanitize",
                            cooldown_seconds=self.openrouter_transient_cooldown_seconds,
                            latency_ms=latency_ms,
                        )
                        print(
                            "[ai] empty response | "
                            f"provider=openrouter model={candidate_model} "
                            f"latency_ms={latency_ms:.0f}"
                        )
                    except Exception as exc:
                        latency_ms = max(1.0, (time.perf_counter() - started_at) * 1000.0)
                        error_kind, status_code = _classify_openrouter_error(exc)
                        error_message = _short_error(exc, limit=180)
                        cooldown_seconds = self._cooldown_for_error_kind(error_kind)
                        self._mark_openrouter_failure(
                            candidate_model,
                            error_kind=error_kind,
                            error_message=error_message,
                            cooldown_seconds=cooldown_seconds,
                            latency_ms=latency_ms,
                        )
                        cooldown_left = self._cooldown_remaining(candidate_model)
                        status_text = str(status_code) if status_code is not None else "n/a"
                        print(
                            "[ai] failed | "
                            f"provider=openrouter model={candidate_model} "
                            f"status={status_text} error_kind={error_kind} "
                            f"cooldown_s={cooldown_left:.0f} latency_ms={latency_ms:.0f} "
                            f"error={error_message}"
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

    def get_strategy_snapshot(self) -> dict[str, str | int | list[str] | dict[str, dict[str, Any]]]:
        stats_snapshot = {
            model: {
                "attempts": int(state.get("attempts", 0)),
                "successes": int(state.get("successes", 0)),
                "failures": int(state.get("failures", 0)),
                "avg_latency_ms": state.get("avg_latency_ms"),
                "cooldown_s": round(self._cooldown_remaining(model), 2),
                "last_error_kind": state.get("last_error_kind"),
            }
            for model, state in self._openrouter_model_stats.items()
        }
        return {
            "gemini_keys": len(self.gemini_keys),
            "gemini_model": self.gemini_model,
            "openrouter_keys": len(self.openrouter_keys),
            "openrouter_model_requested": self.openrouter_model,
            "openrouter_free_models": list(self.openrouter_free_models),
            "openrouter_model_pool": list(self.openrouter_model_pool),
            "openrouter_max_models_per_request": self.openrouter_max_models_per_request,
            "openrouter_selection_mode": "power-first-free-with-availability",
            "order": "gemini->openrouter(power-first-free)->heuristic",
            "openrouter_stats": stats_snapshot,
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

    async def _call_openrouter(
        self,
        api_key: str,
        prompt: str,
        title: str,
        model: str | None = None,
    ) -> tuple[str, str | None]:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model or self.openrouter_model,
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
            response_headers = {key.lower(): value for key, value in response.headers.items()}
        resolved_model = _extract_openrouter_resolved_model(data, response_headers)
        choices = data.get("choices", [])
        if not choices:
            return "", resolved_model
        content = choices[0].get("message", {}).get("content", "").strip()
        return content, resolved_model

    async def _invoke_openrouter_with_model(
        self,
        api_key: str,
        prompt: str,
        title: str,
        candidate_model: str,
    ) -> tuple[str, str | None] | str:
        try:
            return await self._call_openrouter(api_key, prompt, title, model=candidate_model)
        except TypeError as exc:
            # Backward compatibility for monkeypatched tests that still use old signature.
            if "unexpected keyword argument 'model'" not in str(exc):
                raise
        return await self._call_openrouter(api_key, prompt, title)  # type: ignore[misc]

    def _load_model_power_overrides(self) -> dict[str, float]:
        raw = (os.getenv("OPENROUTER_MODEL_POWER_JSON") or "").strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        overrides: dict[str, float] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or not key.strip():
                continue
            parsed = _parse_float(value)
            if parsed is None:
                continue
            overrides[key.strip()] = parsed
        return overrides

    def _build_openrouter_model_pool(self) -> list[str]:
        requested = (self.openrouter_model or "").strip()
        pool: list[str] = []
        if requested and requested != "openrouter/auto":
            pool.append(requested)
        pool.extend(self.openrouter_free_models)
        if requested:
            pool.append(requested)
        if "openrouter/auto" not in pool:
            pool.append("openrouter/auto")
        return _dedupe_keep_order(pool)

    def _cooldown_for_error_kind(self, error_kind: str) -> int:
        if error_kind in {"rate_limited", "credits_exhausted", "token_exhausted"}:
            return self.openrouter_cooldown_seconds
        if error_kind == "model_not_found":
            return self.openrouter_not_found_cooldown_seconds
        if error_kind in {"upstream_error", "transport_error", "empty_response"}:
            return self.openrouter_transient_cooldown_seconds
        return max(15, self.openrouter_transient_cooldown_seconds // 2)

    def _cooldown_remaining(self, model: str) -> float:
        stats = self._openrouter_model_stats.get(model)
        if not stats:
            return 0.0
        blocked_until = float(stats.get("blocked_until") or 0.0)
        if blocked_until <= 0:
            return 0.0
        return max(0.0, blocked_until - time.monotonic())

    def _model_power_score(self, model: str) -> float:
        override = self.openrouter_model_power.get(model)
        if override is not None:
            return float(override)
        lowered = model.lower()
        score = 0.0
        if lowered.endswith(":free"):
            score += 8.0
        if "sonar" in lowered:
            score += 10.0
        if "reason" in lowered or re.search(r"(?:^|[\-_/])r1(?:$|[\-_/])", lowered):
            score += 25.0
        if "sonnet" in lowered:
            score += 20.0
        if "opus" in lowered:
            score += 28.0
        if "mini" in lowered:
            score -= 12.0
        if "small" in lowered:
            score -= 8.0
        if "nano" in lowered:
            score -= 18.0
        size_match = re.search(r"(\d+(?:\.\d+)?)b", lowered)
        if size_match:
            try:
                score += float(size_match.group(1))
            except ValueError:
                pass
        context_match = re.search(r"(\d{2,3})k", lowered)
        if context_match:
            try:
                score += float(context_match.group(1)) / 10.0
            except ValueError:
                pass
        if score <= 0.0:
            score = 10.0
        return score

    def _dynamic_model_score(self, model: str) -> float:
        stats = self._openrouter_model_stats.setdefault(
            model,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "avg_latency_ms": None,
                "blocked_until": 0.0,
                "last_error_kind": None,
                "last_error_message": None,
            },
        )
        base = self._model_power_score(model)
        attempts = int(stats.get("attempts", 0))
        successes = int(stats.get("successes", 0))
        failures = int(stats.get("failures", 0))
        success_ratio = successes / attempts if attempts > 0 else 1.0
        latency = _parse_float(stats.get("avg_latency_ms")) or 0.0
        latency_penalty = min(8.0, latency / 600.0)
        failure_penalty = min(10.0, failures * 0.8)
        availability_bonus = 3.0 if self._cooldown_remaining(model) <= 0 else -50.0
        return base + (success_ratio * 5.0) - latency_penalty - failure_penalty + availability_bonus

    def _rank_openrouter_models(self) -> list[str]:
        rows: list[tuple[float, str]] = []
        for model in self.openrouter_model_pool:
            rows.append((self._dynamic_model_score(model), model))
        rows.sort(key=lambda item: (-item[0], item[1]))
        available = [model for _, model in rows if self._cooldown_remaining(model) <= 0]
        blocked = [model for _, model in rows if self._cooldown_remaining(model) > 0]
        return [*available, *blocked]

    def _mark_openrouter_success(self, candidate_model: str, latency_ms: float, resolved_model: str | None) -> None:
        selected = resolved_model or candidate_model
        if selected:
            self._last_successful_openrouter_model = selected
        for model in _dedupe_keep_order([candidate_model, resolved_model or ""]):
            stats = self._openrouter_model_stats.setdefault(
                model,
                {
                    "attempts": 0,
                    "successes": 0,
                    "failures": 0,
                    "avg_latency_ms": None,
                    "blocked_until": 0.0,
                    "last_error_kind": None,
                    "last_error_message": None,
                },
            )
            stats["attempts"] = int(stats.get("attempts", 0)) + 1
            stats["successes"] = int(stats.get("successes", 0)) + 1
            previous_latency = _parse_float(stats.get("avg_latency_ms"))
            if previous_latency is None:
                stats["avg_latency_ms"] = round(latency_ms, 2)
            else:
                stats["avg_latency_ms"] = round((previous_latency * 0.7) + (latency_ms * 0.3), 2)
            stats["last_error_kind"] = None
            stats["last_error_message"] = None
            stats["blocked_until"] = 0.0

    def _mark_openrouter_failure(
        self,
        model: str,
        error_kind: str,
        error_message: str,
        cooldown_seconds: int,
        latency_ms: float,
    ) -> None:
        stats = self._openrouter_model_stats.setdefault(
            model,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "avg_latency_ms": None,
                "blocked_until": 0.0,
                "last_error_kind": None,
                "last_error_message": None,
            },
        )
        stats["attempts"] = int(stats.get("attempts", 0)) + 1
        stats["failures"] = int(stats.get("failures", 0)) + 1
        previous_latency = _parse_float(stats.get("avg_latency_ms"))
        if previous_latency is None:
            stats["avg_latency_ms"] = round(latency_ms, 2)
        else:
            stats["avg_latency_ms"] = round((previous_latency * 0.8) + (latency_ms * 0.2), 2)
        stats["last_error_kind"] = error_kind
        stats["last_error_message"] = error_message
        stats["blocked_until"] = time.monotonic() + max(1, cooldown_seconds)

    def _augment_openrouter_candidates(self, initial: list[str], ranked_models: list[str]) -> list[str]:
        candidates = _dedupe_keep_order(initial)
        rescue_chain = [
            self._last_successful_openrouter_model or "",
            "perplexity/sonar",
            "openrouter/auto",
        ]
        for model in rescue_chain:
            if not model:
                continue
            if model not in ranked_models:
                continue
            if model in candidates:
                continue
            if self._cooldown_remaining(model) > 0:
                continue
            candidates.append(model)
        return candidates

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
