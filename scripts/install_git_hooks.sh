#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK_PATH="${ROOT_DIR}/.git/hooks/pre-push"

if [[ ! -d "${ROOT_DIR}/.git" ]]; then
  echo "No .git directory found in ${ROOT_DIR}"
  exit 1
fi

cat > "${HOOK_PATH}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
"${ROOT_DIR}/scripts/run_tests.sh"
EOF

chmod +x "${HOOK_PATH}"
echo "Installed pre-push hook at ${HOOK_PATH}"
