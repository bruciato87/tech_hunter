#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${ROOT_DIR}/.venv/bin/python"

if [[ -x "${VENV_PY}" ]]; then
  PYTHON_BIN="${VENV_PY}"
else
  PYTHON_BIN="python3"
fi

echo "[tests] Running Python test suite..."
"${PYTHON_BIN}" -m pytest

echo "[tests] Running Node API test suite..."
node --test "${ROOT_DIR}/tests_js"/*.test.js

echo "[tests] All tests passed."
