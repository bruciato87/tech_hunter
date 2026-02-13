from __future__ import annotations

import base64
import json
import os
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default


def _warehouse_enabled() -> bool:
    return _env_or_default("AMAZON_WAREHOUSE_ENABLED", "true").lower() not in {"0", "false", "no", "off"}


def _parse_marketplaces(value: str | None) -> list[str]:
    return [item.strip().lower() for item in (value or "").split(",") if item.strip()]


def _parse_proxy(value: str) -> tuple[bool, str]:
    raw = value.strip()
    if not raw:
        return False, "empty"
    if "://" not in raw:
        raw = f"http://{raw}"
    try:
        parsed = urlparse(raw)
    except ValueError:
        return False, "invalid-url"
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https", "socks5", "socks5h"}:
        return False, f"unsupported-scheme:{scheme or 'none'}"
    if not parsed.hostname:
        return False, "missing-host"
    return True, ""


def _parse_selector_overrides(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    try:
        decoded = json.loads(raw)
    except Exception:
        return False
    if not isinstance(decoded, dict):
        return False
    for _, site_value in decoded.items():
        if not isinstance(site_value, dict):
            return False
    return True


def _parse_openrouter_model_power_json(value: str | None) -> bool:
    raw = (value or "").strip()
    if not raw:
        return True
    try:
        decoded = json.loads(raw)
    except Exception:
        return False
    if not isinstance(decoded, dict):
        return False
    for key, score in decoded.items():
        if not isinstance(key, str) or not key.strip():
            return False
        if not isinstance(score, (int, float)):
            return False
    return True


def main() -> int:
    load_dotenv()
    errors: list[str] = []
    warnings: list[str] = []

    gemini_keys = _split_csv(os.getenv("GEMINI_API_KEYS"))
    openrouter_keys = _split_csv(os.getenv("OPENROUTER_API_KEYS"))
    if not gemini_keys and not openrouter_keys:
        errors.append("Set at least one AI provider key: GEMINI_API_KEYS or OPENROUTER_API_KEYS.")

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if bool(supabase_url) ^ bool(supabase_key):
        errors.append("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set together.")
    if not supabase_url:
        warnings.append("Supabase disabled. Profitable deals will not be persisted.")

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    if bool(tg_token) ^ bool(tg_chat):
        errors.append("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set together.")
    if not tg_token:
        warnings.append("Telegram disabled. Profitable deals will not trigger notifications.")

    try:
        float(_env_or_default("MIN_SPREAD_EUR", "40"))
    except ValueError:
        errors.append("MIN_SPREAD_EUR must be numeric.")

    try:
        int(_env_or_default("MAX_PARALLEL_PRODUCTS", "3"))
    except ValueError:
        errors.append("MAX_PARALLEL_PRODUCTS must be integer.")

    try:
        scan_target = int(_env_or_default("SCAN_TARGET_PRODUCTS", "12"))
        if scan_target < 1:
            errors.append("SCAN_TARGET_PRODUCTS must be >= 1.")
    except ValueError:
        errors.append("SCAN_TARGET_PRODUCTS must be integer.")

    try:
        candidate_multiplier = int(_env_or_default("SCAN_CANDIDATE_MULTIPLIER", "4"))
        if candidate_multiplier < 1:
            errors.append("SCAN_CANDIDATE_MULTIPLIER must be >= 1.")
    except ValueError:
        errors.append("SCAN_CANDIDATE_MULTIPLIER must be integer.")

    try:
        dynamic_query_limit = int(_env_or_default("SCAN_DYNAMIC_QUERY_LIMIT", "12"))
        if dynamic_query_limit < 1:
            errors.append("SCAN_DYNAMIC_QUERY_LIMIT must be >= 1.")
    except ValueError:
        errors.append("SCAN_DYNAMIC_QUERY_LIMIT must be integer.")

    try:
        dynamic_exploration_ratio = float(_env_or_default("SCAN_DYNAMIC_EXPLORATION_RATIO", "0.35"))
        if dynamic_exploration_ratio <= 0 or dynamic_exploration_ratio >= 1:
            errors.append("SCAN_DYNAMIC_EXPLORATION_RATIO must be between 0 and 1.")
    except ValueError:
        errors.append("SCAN_DYNAMIC_EXPLORATION_RATIO must be numeric.")

    try:
        float(_env_or_default("SCAN_DYNAMIC_TREND_MIN_SCORE", "-35"))
    except ValueError:
        errors.append("SCAN_DYNAMIC_TREND_MIN_SCORE must be numeric.")

    try:
        exclude_min_keep = int(_env_or_default("EXCLUDE_MIN_KEEP", "4"))
        if exclude_min_keep < 0:
            errors.append("EXCLUDE_MIN_KEEP must be >= 0.")
    except ValueError:
        errors.append("EXCLUDE_MIN_KEEP must be integer.")

    try:
        exclude_lookback_days = int(_env_or_default("EXCLUDE_LOOKBACK_DAYS", "1"))
        if exclude_lookback_days < 1:
            errors.append("EXCLUDE_LOOKBACK_DAYS must be >= 1.")
    except ValueError:
        errors.append("EXCLUDE_LOOKBACK_DAYS must be integer.")

    exclude_daily_reset = _env_or_default("EXCLUDE_DAILY_RESET", "true").lower() not in {"0", "false", "no", "off"}
    exclude_reset_tz = _env_or_default("EXCLUDE_RESET_TIMEZONE", "Europe/Rome")
    if exclude_daily_reset:
        try:
            ZoneInfo(exclude_reset_tz)
        except Exception:
            warnings.append(
                "EXCLUDE_RESET_TIMEZONE is invalid; worker will fallback to UTC for daily reset window."
            )

    try:
        scan_it_quota = int(_env_or_default("SCAN_IT_QUOTA", "6"))
        if scan_it_quota < 0:
            errors.append("SCAN_IT_QUOTA must be >= 0.")
    except ValueError:
        errors.append("SCAN_IT_QUOTA must be integer.")

    try:
        scan_eu_quota = int(_env_or_default("SCAN_EU_QUOTA", "6"))
        if scan_eu_quota < 0:
            errors.append("SCAN_EU_QUOTA must be >= 0.")
    except ValueError:
        errors.append("SCAN_EU_QUOTA must be integer.")

    try:
        scoring_lookback = int(_env_or_default("SCORING_LOOKBACK_DAYS", "30"))
        if scoring_lookback < 1:
            errors.append("SCORING_LOOKBACK_DAYS must be >= 1.")
    except ValueError:
        errors.append("SCORING_LOOKBACK_DAYS must be integer.")

    try:
        scoring_limit = int(_env_or_default("SCORING_HISTORY_LIMIT", "2000"))
        if scoring_limit < 100:
            errors.append("SCORING_HISTORY_LIMIT must be >= 100.")
    except ValueError:
        errors.append("SCORING_HISTORY_LIMIT must be integer.")

    try:
        int(_env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000"))
    except ValueError:
        errors.append("PLAYWRIGHT_NAV_TIMEOUT_MS must be integer.")

    try:
        mpb_attempts = int(_env_or_default("MPB_MAX_ATTEMPTS", "3"))
        if mpb_attempts < 1:
            errors.append("MPB_MAX_ATTEMPTS must be >= 1.")
    except ValueError:
        errors.append("MPB_MAX_ATTEMPTS must be integer.")
    mpb_use_storage_state = _env_or_default("MPB_USE_STORAGE_STATE", "true").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    mpb_storage_state = (os.getenv("MPB_STORAGE_STATE_B64") or "").strip()
    if mpb_use_storage_state and mpb_storage_state:
        try:
            decoded = base64.b64decode(mpb_storage_state).decode("utf-8")
            parsed = json.loads(decoded)
            if not isinstance(parsed, dict):
                warnings.append("MPB_STORAGE_STATE_B64 must decode to a JSON object.")
        except Exception:
            warnings.append("MPB_STORAGE_STATE_B64 is not valid base64 JSON.")

    trenddevice_email = (os.getenv("TRENDDEVICE_LEAD_EMAIL") or "").strip()
    if trenddevice_email and "@" not in trenddevice_email:
        warnings.append("TRENDDEVICE_LEAD_EMAIL seems invalid (missing '@').")
    try:
        trend_wait_ms = int(_env_or_default("TRENDDEVICE_EMAIL_GATE_WAIT_MS", "6500"))
        if trend_wait_ms < 1000:
            errors.append("TRENDDEVICE_EMAIL_GATE_WAIT_MS must be >= 1000.")
    except ValueError:
        errors.append("TRENDDEVICE_EMAIL_GATE_WAIT_MS must be integer.")
    trenddevice_use_storage_state = _env_or_default("TRENDDEVICE_USE_STORAGE_STATE", "true").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    trenddevice_storage_state = (os.getenv("TRENDDEVICE_STORAGE_STATE_B64") or "").strip()
    if trenddevice_use_storage_state and trenddevice_storage_state:
        try:
            decoded = base64.b64decode(trenddevice_storage_state).decode("utf-8")
            parsed = json.loads(decoded)
            if not isinstance(parsed, dict):
                warnings.append("TRENDDEVICE_STORAGE_STATE_B64 must decode to a JSON object.")
        except Exception:
            warnings.append("TRENDDEVICE_STORAGE_STATE_B64 is not valid base64 JSON.")

    raw_selector_overrides = (os.getenv("VALUATOR_SELECTOR_OVERRIDES_JSON") or "").strip()
    if raw_selector_overrides and not _parse_selector_overrides(raw_selector_overrides):
        warnings.append("VALUATOR_SELECTOR_OVERRIDES_JSON is set but invalid (ignored).")

    openrouter_free_models = _split_csv(os.getenv("OPENROUTER_FREE_MODELS"))
    if openrouter_keys and not openrouter_free_models:
        warnings.append(
            "OPENROUTER_FREE_MODELS is empty. The balancer will use the built-in default free-tier pool."
        )

    openrouter_model_power_json = (os.getenv("OPENROUTER_MODEL_POWER_JSON") or "").strip()
    if openrouter_model_power_json and not _parse_openrouter_model_power_json(openrouter_model_power_json):
        warnings.append("OPENROUTER_MODEL_POWER_JSON is set but invalid (ignored).")

    try:
        openrouter_max_models = int(_env_or_default("OPENROUTER_MAX_MODELS_PER_REQUEST", "3"))
        if openrouter_max_models < 1:
            errors.append("OPENROUTER_MAX_MODELS_PER_REQUEST must be >= 1.")
    except ValueError:
        errors.append("OPENROUTER_MAX_MODELS_PER_REQUEST must be integer.")

    try:
        cooldown = int(_env_or_default("OPENROUTER_MODEL_COOLDOWN_SECONDS", "900"))
        if cooldown < 1:
            errors.append("OPENROUTER_MODEL_COOLDOWN_SECONDS must be >= 1.")
    except ValueError:
        errors.append("OPENROUTER_MODEL_COOLDOWN_SECONDS must be integer.")

    try:
        not_found_cooldown = int(_env_or_default("OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS", "86400"))
        if not_found_cooldown < 1:
            errors.append("OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS must be >= 1.")
    except ValueError:
        errors.append("OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS must be integer.")

    try:
        transient_cooldown = int(_env_or_default("OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS", "120"))
        if transient_cooldown < 1:
            errors.append("OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS must be >= 1.")
    except ValueError:
        errors.append("OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS must be integer.")

    if _warehouse_enabled():
        try:
            int(_env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "12"))
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_MAX_PRODUCTS must be integer.")

        try:
            per_marketplace_limit = int(_env_or_default("AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT", "4"))
            if per_marketplace_limit < 1:
                errors.append("AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT must be >= 1.")
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT must be integer.")

        max_price = (os.getenv("AMAZON_WAREHOUSE_MAX_PRICE_EUR") or "").strip()
        if max_price:
            try:
                float(max_price)
            except ValueError:
                errors.append("AMAZON_WAREHOUSE_MAX_PRICE_EUR must be numeric when set.")

        marketplaces = _parse_marketplaces(os.getenv("AMAZON_WAREHOUSE_MARKETPLACES")) or ["it", "de", "fr", "es"]
        unsupported = [item for item in marketplaces if item not in {"it", "de", "fr", "es", "eu"}]
        if unsupported:
            warnings.append(
                "AMAZON_WAREHOUSE_MARKETPLACES has unsupported codes: "
                + ",".join(sorted(set(unsupported)))
                + " (supported: it,de,fr,es,eu)."
            )

        try:
            attempts = int(_env_or_default("AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY", "3"))
            if attempts < 1:
                errors.append("AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY must be >= 1.")
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY must be integer.")

        try:
            retry_delay = int(_env_or_default("AMAZON_WAREHOUSE_RETRY_DELAY_MS", "700"))
            if retry_delay < 0:
                errors.append("AMAZON_WAREHOUSE_RETRY_DELAY_MS must be >= 0.")
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_RETRY_DELAY_MS must be integer.")

        proxy_values = _split_csv(os.getenv("AMAZON_WAREHOUSE_PROXY_URLS"))
        for proxy in proxy_values:
            ok, reason = _parse_proxy(proxy)
            if not ok:
                warnings.append(
                    "AMAZON_WAREHOUSE_PROXY_URLS contains invalid proxy entry: "
                    + proxy
                    + f" ({reason})."
                )

        use_storage_state = _env_or_default("AMAZON_WAREHOUSE_USE_STORAGE_STATE", "true").lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        storage_state_envs = [
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64",
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64_IT",
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE",
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR",
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64_ES",
        ]
        for env_name in storage_state_envs:
            raw_storage_state = (os.getenv(env_name) or "").strip()
            if not use_storage_state or not raw_storage_state:
                continue
            try:
                decoded = base64.b64decode(raw_storage_state).decode("utf-8")
                parsed = json.loads(decoded)
                if not isinstance(parsed, dict):
                    warnings.append(f"{env_name} must decode to a JSON object.")
            except Exception:
                warnings.append(f"{env_name} is not valid base64 JSON.")

        cart_pricing_enabled = _env_or_default("AMAZON_WAREHOUSE_CART_PRICING_ENABLED", "false").lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        try:
            cart_pricing_max_items = int(_env_or_default("AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS", "4"))
            if cart_pricing_max_items < 1:
                errors.append("AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS must be >= 1.")
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS must be integer.")

        if cart_pricing_enabled and not use_storage_state:
            warnings.append(
                "AMAZON_WAREHOUSE_CART_PRICING_ENABLED=true but AMAZON_WAREHOUSE_USE_STORAGE_STATE=false."
            )
        if cart_pricing_enabled and use_storage_state:
            has_storage_state = any((os.getenv(env_name) or "").strip() for env_name in storage_state_envs)
            if not has_storage_state:
                warnings.append(
                    "AMAZON_WAREHOUSE_CART_PRICING_ENABLED=true but no AMAZON_WAREHOUSE_STORAGE_STATE_B64* is configured."
                )

    if errors:
        print("Environment validation failed:")
        for error in errors:
            print(f"- {error}")
    else:
        print("Environment validation passed.")

    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
