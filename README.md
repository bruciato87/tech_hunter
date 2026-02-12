# Tech_Sniper_IT

Serverless-friendly arbitrage worker for Amazon Warehouse IT -> recommerce valuation.

## Engine Rules

- `photography` -> query `MPB + Rebuy`, keep max offer.
- `apple_phone` -> query `TrendDevice + Rebuy`, keep max offer.
- `general_tech` -> query `Rebuy`.
- Grade simulation for resale condition:
  - TrendDevice: `Grado A`
  - MPB: `Ottimo`
  - Rebuy: `Come nuovo`
- If `spread_eur > MIN_SPREAD_EUR` (default `40`) -> save to Supabase + Telegram notification.

## Quick Start

```bash
./scripts/bootstrap_local.sh
source .venv/bin/activate
python scripts/validate_env.py
python -m tech_sniper_it.worker
```

`bootstrap_local.sh` also installs a Git `pre-push` hook that runs the full test suite and blocks pushes on failures.

`AMAZON_PRODUCTS_JSON` example:

```json
[
  {
    "title": "Apple iPhone 14 Pro 128GB Nero Siderale",
    "price_eur": 679,
    "category": "apple_phone",
    "ean": "0194253401234",
    "url": "https://www.amazon.it/..."
  }
]
```

## Environment Variables

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_TABLE` (default: `arbitrage_opportunities`)
- `SUPABASE_SCANNER_USER_ID` (optional, for non-service-role inserts)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `GEMINI_API_KEYS` (comma-separated)
- `GEMINI_MODEL` (default: `gemini-2.0-flash`)
- `OPENROUTER_API_KEYS` (comma-separated)
- `OPENROUTER_MODEL` (default: `openrouter/auto`)
- `OPENROUTER_BASE_URL` (default: `https://openrouter.ai/api/v1/chat/completions`)
- `OPENROUTER_FREE_MODELS` (ordered free-tier model pool; default starts from currently most reliable model in production runs)
- `OPENROUTER_MODEL_POWER_JSON` (optional JSON score overrides for ranking)
- `OPENROUTER_MAX_MODELS_PER_REQUEST` (default: `2`)
- `OPENROUTER_MODEL_COOLDOWN_SECONDS` (default: `900`, quota/rate-limit cooldown)
- `OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS` (default: `86400`)
- `OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS` (default: `120`)
- `MIN_SPREAD_EUR` (default: `40`)
- `MAX_PARALLEL_PRODUCTS` (default: `3`)
- `SCAN_TARGET_PRODUCTS` (default: `12`)
- `SCAN_CANDIDATE_MULTIPLIER` (default: `4`)
- `EXCLUDE_MIN_KEEP` (default: `4`, re-includes a small slice of excluded URLs to avoid 0/1-product scans)
- `PLAYWRIGHT_NAV_TIMEOUT_MS` (default: `45000`)
- `HEADLESS` (default: `true`)
- `MPB_MAX_ATTEMPTS` (default: `3`)
- `TRENDDEVICE_LEAD_EMAIL` (optional lead email used by TrendDevice wizard when required)
- `VALUATOR_SELECTOR_OVERRIDES_JSON` (optional JSON selector overrides for automatic UI drift adaptation)
- `AMAZON_PRODUCTS_JSON` (optional JSON array)
- `AMAZON_PRODUCTS_FILE` (optional path to JSON file)
- `AMAZON_WAREHOUSE_ENABLED` (default: `true`)
- `AMAZON_WAREHOUSE_MARKETPLACES` (default: `it,eu`; `eu` expands to `de,fr,es`)
- `AMAZON_WAREHOUSE_MAX_PRODUCTS` (default: `12`)
- `AMAZON_WAREHOUSE_MAX_PRICE_EUR` (optional ceiling)
- `AMAZON_WAREHOUSE_QUERIES` (optional comma-separated search seeds)
- `AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT` (default: `4`, balances IT/EU collection before top-up)
- `AMAZON_WAREHOUSE_PROXY_URLS` (optional comma-separated proxy endpoints, e.g. `http://user:pass@host:port`)
- `AMAZON_WAREHOUSE_ROTATE_PROXY` (default: `true`)
- `AMAZON_WAREHOUSE_USER_AGENTS` (optional JSON array or `||`-separated list)
- `AMAZON_WAREHOUSE_ROTATE_USER_AGENT` (default: `true`)
- `AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY` (default: `3`)
- `AMAZON_WAREHOUSE_RETRY_DELAY_MS` (default: `700`)
- `AMAZON_WAREHOUSE_FAIL_FAST_ON_SORRY` (default: `true`, stops retries on hard block pages when no proxy pool is configured)
- `AMAZON_WAREHOUSE_STEALTH` (default: `true`, enables basic anti-bot fingerprint hardening)
- `AMAZON_WAREHOUSE_USE_STORAGE_STATE` (default: `true`)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64` (optional base64 Playwright storage state for logged-in Amazon session)
- `AMAZON_WAREHOUSE_DEBUG_ON_EMPTY` (default: `true`, saves diagnostic dump on zero parsed results)
- `AMAZON_WAREHOUSE_DEBUG_DIR` (default: `/tmp/tech_sniper_it_debug`)

### AI Free-Tier Selection Logic

When `OPENROUTER_MODEL=openrouter/auto`, the balancer does not rely blindly on auto-routing.
It ranks models from `OPENROUTER_FREE_MODELS` (power-first), then skips temporarily unavailable models based on runtime errors:

- `rate limit / quota / token exhausted` -> cooldown `OPENROUTER_MODEL_COOLDOWN_SECONDS`
- `model not found` -> cooldown `OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS`
- `transient upstream/network` -> cooldown `OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS`

For each normalization request it tries up to `OPENROUTER_MAX_MODELS_PER_REQUEST` candidates in ranked order and logs:

- requested model (`openrouter/auto` or explicit)
- candidate model attempted
- resolved upstream model actually used
- cooldown reason when a model is temporarily skipped

Production note:
- the default worker config now starts with `perplexity/sonar` because in recent runs it is the only consistently available model for your key; then it falls back to stronger free-tier candidates if available.
- worker defaults also include `OPENROUTER_MODEL_POWER_JSON` tuned to prioritize currently working models first, while keeping fallback options.

### Why Some Runs Analyze Few Products

If you see scans with 1-2 products, it is typically due to:

- exclusion cache removing historically non-profitable URLs
- low `SCAN_TARGET_PRODUCTS` / `AMAZON_WAREHOUSE_MAX_PRODUCTS`
- unbalanced marketplace crawl (IT saturating budget before EU)

Current defaults now mitigate this by:

- raising target and candidate budgets
- balancing per-marketplace collection (`AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT`)
- allowing controlled exclusion relaxation (`EXCLUDE_MIN_KEEP`)

### UI Drift Auto-Adaptation

Each reseller valuator now has:

- semantic fallback selectors (search/condition/result buttons)
- runtime UI probes (page signature + expected keyword hits)
- drift flag in logs (`ui_drift=true`) when page structure/content is suspicious

Override selectors without deploy via env:

```json
{
  "*": {
    "price": ["[data-testid*='price' i]"]
  },
  "rebuy": {
    "search_input": ["input[aria-label*='cerca' i]"]
  },
  "mpb": {
    "result_open": ["[role='option'] a"]
  },
  "trenddevice": {
    "confirm_button": ["button:has-text('Valuta')"]
  }
}
```

### Free Bypass With Your Amazon Account Session

To reduce `sorry-page` blocks without paid proxies, capture a logged-in Amazon Playwright session locally and store it as secret.

Generate storage state base64:

```bash
source .venv/bin/activate
python scripts/capture_amazon_storage_state.py --domain www.amazon.it
```

Copy the printed base64 value to GitHub Secret:

- `AMAZON_WAREHOUSE_STORAGE_STATE_B64`

Keep enabled:

- `AMAZON_WAREHOUSE_USE_STORAGE_STATE=true`
- `AMAZON_WAREHOUSE_STEALTH=true`
- `AMAZON_WAREHOUSE_FAIL_FAST_ON_SORRY=true`

## Supabase Setup (RLS Enabled)

Migration file is already prepared at:

- `supabase/migrations/20260212_000001_init_arbitrage.sql`

It creates `public.arbitrage_opportunities`, enables + forces RLS, and adds policies for:

- `authenticated` users: only own rows (`scanner_user_id = auth.uid()`).
- `service_role`: full access (for GitHub worker with service key).

## GitHub Actions

- Worker run: `.github/workflows/worker.yml`
- Secret leak scan: `.github/workflows/security.yml` (Gitleaks)
- Full regression suite: `.github/workflows/tests.yml`

Store all production keys only in GitHub Secrets, never in tracked files.

### Worker Configuration Model

`worker.yml` now uses:

- secure values from `GitHub Secrets` (tokens/keys/passwords)
- non-sensitive knobs from `GitHub Repository Variables`
- code defaults when variables are not set

#### Minimal GitHub Secrets (Worker)

- `SUPABASE_SERVICE_ROLE_KEY`
- `TELEGRAM_BOT_TOKEN`
- `GEMINI_API_KEYS` or `OPENROUTER_API_KEYS` (at least one provider)
- `AMAZON_WAREHOUSE_PROXY_URLS` (required for real anti-bot resilient warehouse autoscan)

Optional secrets:

- `SUPABASE_URL` (can be moved to Variables)
- `TELEGRAM_CHAT_ID` (can be moved to Variables)

#### Recommended GitHub Variables (Worker, non-sensitive)

- `SUPABASE_URL`
- `SUPABASE_TABLE`
- `TELEGRAM_CHAT_ID`
- `MIN_SPREAD_EUR`
- `MAX_PARALLEL_PRODUCTS`
- `PLAYWRIGHT_NAV_TIMEOUT_MS`
- `GEMINI_MODEL`
- `OPENROUTER_MODEL`
- `OPENROUTER_BASE_URL`
- `OPENROUTER_FREE_MODELS`
- `OPENROUTER_MODEL_POWER_JSON`
- `OPENROUTER_MAX_MODELS_PER_REQUEST`
- `OPENROUTER_MODEL_COOLDOWN_SECONDS`
- `OPENROUTER_MODEL_NOT_FOUND_COOLDOWN_SECONDS`
- `OPENROUTER_MODEL_TRANSIENT_COOLDOWN_SECONDS`
- `SCAN_TARGET_PRODUCTS`
- `SCAN_CANDIDATE_MULTIPLIER`
- `EXCLUDE_MIN_KEEP`
- `AMAZON_WAREHOUSE_ENABLED`
- `AMAZON_WAREHOUSE_MARKETPLACES`
- `AMAZON_WAREHOUSE_MAX_PRODUCTS`
- `AMAZON_WAREHOUSE_MAX_PRICE_EUR`
- `AMAZON_WAREHOUSE_QUERIES`
- `AMAZON_WAREHOUSE_PER_MARKETPLACE_LIMIT`
- `AMAZON_WAREHOUSE_ROTATE_PROXY`
- `AMAZON_WAREHOUSE_ROTATE_USER_AGENT`
- `AMAZON_WAREHOUSE_USER_AGENTS`
- `AMAZON_WAREHOUSE_MAX_ATTEMPTS_PER_QUERY`
- `AMAZON_WAREHOUSE_RETRY_DELAY_MS`
- `AMAZON_WAREHOUSE_DEBUG_ON_EMPTY`
- `AMAZON_WAREHOUSE_DEBUG_DIR`

## Testing

Run the complete suite locally:

```bash
./scripts/run_tests.sh
```

Coverage gate for Python tests is enabled (`pytest-cov`) with baseline `55%` by default.
Override for stricter checks when needed:

```bash
PY_COVERAGE_FAIL_UNDER=65 ./scripts/run_tests.sh
```

Install/update git hooks manually:

```bash
./scripts/install_git_hooks.sh
```

### Automatic Vercel deploy from Git push

`tests.yml` now deploys to Vercel production automatically after tests pass on every push to `main`.

Required GitHub Secrets:

- `VERCEL_TOKEN`
- `VERCEL_ORG_ID`
- `VERCEL_PROJECT_ID`

## Telegram + Vercel Orchestrator

Heavy operations are delegated to GitHub Actions. Vercel only validates commands and enqueues jobs.

### Endpoints

- `POST /telegram/webhook` -> Telegram commands router (rewritten to `api/telegram-webhook.js`)
- `POST /scan` -> generic scan trigger API (rewritten to `api/scan.js`)

### Telegram Commands

- `/start`
- `/help`
- `/id`
- `/rules`
- `/scan [json object or json array]` (delegated to GitHub Actions; payload optional)
  Without payload, worker auto-loads products from Amazon Warehouse IT+EU sources.
- `/status` (delegated to GitHub Actions)
- `/last [n]` (delegated to GitHub Actions, max 10 rows)

### Vercel Environment Variables

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_WEBHOOK_SECRET_TOKEN` (recommended)
- `TELEGRAM_ALLOWED_CHAT_IDS` (comma-separated allowlist, recommended)
- `GITHUB_TOKEN` (PAT with `repo` + `workflow`)
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_EVENT_TYPE` (default: `scan`)
- `SCAN_SECRET` (for `/scan` bearer auth)

### Set Telegram Webhook

```bash
curl -X POST "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook" \
  -d "url=https://<YOUR_VERCEL_DOMAIN>/telegram/webhook" \
  -d "secret_token=<TELEGRAM_WEBHOOK_SECRET_TOKEN>"
```

Note:
- `secret_token` must be the value of `TELEGRAM_WEBHOOK_SECRET_TOKEN` (random secret), not the bot token.
- If the bot token was ever exposed publicly, rotate it in BotFather and update secrets.

### Webhook + Commands Smoke Test

1. Verify webhook registration:

```bash
curl "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getWebhookInfo"
```

2. In Telegram chat with the bot, run:
- `/id` (collect chat_id and ensure allowlist is correct)
- `/status` (must enqueue a GitHub Action)
- `/scan {"title":"Apple iPhone 14 Pro 128GB","price_eur":679,"category":"apple_phone"}`

3. Confirm in GitHub Actions that the workflow `Tech Sniper IT Worker` starts from repository dispatch.

### `/scan` API payload example

```json
{
  "products": [
    {
      "title": "Apple iPhone 14 Pro 128GB",
      "price_eur": 679,
      "category": "apple_phone"
    }
  ]
}
```
