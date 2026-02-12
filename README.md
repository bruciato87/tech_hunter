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
- `MIN_SPREAD_EUR` (default: `40`)
- `MAX_PARALLEL_PRODUCTS` (default: `3`)
- `PLAYWRIGHT_NAV_TIMEOUT_MS` (default: `45000`)
- `HEADLESS` (default: `true`)
- `AMAZON_PRODUCTS_JSON` (optional JSON array)
- `AMAZON_PRODUCTS_FILE` (optional path to JSON file)

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
- `/scan <json object or json array>` (delegated to GitHub Actions)
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
