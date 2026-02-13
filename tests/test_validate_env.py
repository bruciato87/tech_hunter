from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "validate_env.py"


def _run_validate_env(tmp_path: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONIOENCODING": "utf-8"}
    env.update(extra_env)
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_validate_env_fails_without_ai_keys(tmp_path: Path) -> None:
    result = _run_validate_env(tmp_path, {})
    assert result.returncode == 1
    assert "Set at least one AI provider key" in result.stdout


def test_validate_env_passes_with_minimal_valid_config(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "80",
            "MAX_PARALLEL_PRODUCTS": "3",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
        },
    )
    assert result.returncode == 0
    assert "Environment validation passed." in result.stdout


def test_validate_env_warns_on_invalid_selector_override(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "VALUATOR_SELECTOR_OVERRIDES_JSON": "{invalid-json}",
        },
    )
    assert result.returncode == 0
    assert "VALUATOR_SELECTOR_OVERRIDES_JSON is set but invalid" in result.stdout


def test_validate_env_warns_on_invalid_openrouter_power_json(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "OPENROUTER_MODEL_POWER_JSON": '["invalid"]',
        },
    )
    assert result.returncode == 0
    assert "OPENROUTER_MODEL_POWER_JSON is set but invalid" in result.stdout


def test_validate_env_warns_on_invalid_trenddevice_storage_state(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "TRENDDEVICE_USE_STORAGE_STATE": "true",
            "TRENDDEVICE_STORAGE_STATE_B64": "invalid-base64",
        },
    )
    assert result.returncode == 0
    assert "TRENDDEVICE_STORAGE_STATE_B64 is not valid base64 JSON." in result.stdout


def test_validate_env_warns_on_invalid_mpb_storage_state(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "MPB_USE_STORAGE_STATE": "true",
            "MPB_STORAGE_STATE_B64": "invalid-base64",
        },
    )
    assert result.returncode == 0
    assert "MPB_STORAGE_STATE_B64 is not valid base64 JSON." in result.stdout


def test_validate_env_warns_on_invalid_amazon_marketplace_storage_state(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "AMAZON_WAREHOUSE_USE_STORAGE_STATE": "true",
            "AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR": "invalid-base64",
        },
    )
    assert result.returncode == 0
    assert "AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR is not valid base64 JSON." in result.stdout


def test_validate_env_fails_on_invalid_mpb_max_attempts(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "MPB_MAX_ATTEMPTS": "0",
        },
    )
    assert result.returncode == 1
    assert "MPB_MAX_ATTEMPTS must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_openrouter_max_models(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "OPENROUTER_MAX_MODELS_PER_REQUEST": "0",
        },
    )
    assert result.returncode == 1
    assert "OPENROUTER_MAX_MODELS_PER_REQUEST must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_region_quotas(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "SCAN_IT_QUOTA": "-1",
        },
    )
    assert result.returncode == 1
    assert "SCAN_IT_QUOTA must be >= 0." in result.stdout


def test_validate_env_fails_on_invalid_dynamic_query_limit(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "SCAN_DYNAMIC_QUERY_LIMIT": "0",
        },
    )
    assert result.returncode == 1
    assert "SCAN_DYNAMIC_QUERY_LIMIT must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_dynamic_exploration_ratio(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "SCAN_DYNAMIC_EXPLORATION_RATIO": "1.3",
        },
    )
    assert result.returncode == 1
    assert "SCAN_DYNAMIC_EXPLORATION_RATIO must be between 0 and 1." in result.stdout


def test_validate_env_fails_on_invalid_exclude_lookback_days(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "OPENROUTER_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "EXCLUDE_LOOKBACK_DAYS": "0",
        },
    )
    assert result.returncode == 1
    assert "EXCLUDE_LOOKBACK_DAYS must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_cart_pricing_max_items(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS": "0",
        },
    )
    assert result.returncode == 1
    assert "AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS must be >= 1." in result.stdout


def test_validate_env_warns_when_cart_pricing_enabled_without_storage_state(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "AMAZON_WAREHOUSE_CART_PRICING_ENABLED": "true",
            "AMAZON_WAREHOUSE_USE_STORAGE_STATE": "true",
        },
    )
    assert result.returncode == 0
    assert "AMAZON_WAREHOUSE_CART_PRICING_ENABLED=true but no AMAZON_WAREHOUSE_STORAGE_STATE_B64*" in result.stdout


def test_validate_env_fails_on_invalid_non_profitable_save_parallel(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "NON_PROFITABLE_SAVE_MAX_PARALLEL": "0",
        },
    )
    assert result.returncode == 1
    assert "NON_PROFITABLE_SAVE_MAX_PARALLEL must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_supabase_write_attempts(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "SUPABASE_WRITE_MAX_ATTEMPTS": "0",
        },
    )
    assert result.returncode == 1
    assert "SUPABASE_WRITE_MAX_ATTEMPTS must be >= 1." in result.stdout


def test_validate_env_fails_on_invalid_mpb_block_cooldown(tmp_path: Path) -> None:
    result = _run_validate_env(
        tmp_path,
        {
            "GEMINI_API_KEYS": "k1",
            "MIN_SPREAD_EUR": "40",
            "MAX_PARALLEL_PRODUCTS": "2",
            "PLAYWRIGHT_NAV_TIMEOUT_MS": "45000",
            "MPB_BLOCK_COOLDOWN_SECONDS": "10",
        },
    )
    assert result.returncode == 1
    assert "MPB_BLOCK_COOLDOWN_SECONDS must be >= 60." in result.stdout
