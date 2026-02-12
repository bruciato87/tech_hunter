from __future__ import annotations

import os

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
        int(_env_or_default("PLAYWRIGHT_NAV_TIMEOUT_MS", "45000"))
    except ValueError:
        errors.append("PLAYWRIGHT_NAV_TIMEOUT_MS must be integer.")

    if _warehouse_enabled():
        try:
            int(_env_or_default("AMAZON_WAREHOUSE_MAX_PRODUCTS", "8"))
        except ValueError:
            errors.append("AMAZON_WAREHOUSE_MAX_PRODUCTS must be integer.")

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
