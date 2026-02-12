#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${ROOT_DIR}/.venv/bin/python"
PY_COVERAGE_FAIL_UNDER="${PY_COVERAGE_FAIL_UNDER:-55}"

if [[ -x "${VENV_PY}" ]]; then
  PYTHON_BIN="${VENV_PY}"
else
  PYTHON_BIN="python3"
fi

echo "[tests] Running Python test suite..."
echo "[tests] Python coverage gate: ${PY_COVERAGE_FAIL_UNDER}%"
"${PYTHON_BIN}" -m pytest \
  --cov=tech_sniper_it \
  --cov-report=term-missing \
  --cov-fail-under="${PY_COVERAGE_FAIL_UNDER}"

echo "[tests] Running Node API test suite..."
node --test "${ROOT_DIR}/tests_js"/*.test.js

echo "[tests] All tests passed."
