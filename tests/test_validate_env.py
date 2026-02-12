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
