# Tech_Sniper_IT

Serverless-friendly arbitrage worker for Amazon Warehouse IT -> recommerce valuation.

## Engine Rules

- `photography` -> query `MPB + Rebuy`, keep max offer.
- `apple_phone` -> query `TrendDevice + Rebuy`, keep max offer.
- `smartwatch` -> query `TrendDevice + Rebuy`, keep max offer.
- `drone` -> query `MPB + Rebuy`, keep max offer.
- `handheld_console` -> query `Rebuy`.
- `general_tech` -> query `Rebuy`.
- Grade simulation for resale condition:
  - TrendDevice: `Grado A`
  - MPB: `Ottimo`
  - Rebuy: `Come nuovo`
- Rebuy anti-false-positive guard:
  - validates model/capacity/url match quality and discards generic search/category pages.
- Net spread formula:
  - `spread_net = best_offer - amazon_price - operating_cost - risk_buffer(condition)`
- Strategy profile:
  - `STRATEGY_PROFILE=conservative|balanced|aggressive` (risk/cost parameters come from internal profile defaults)
- If `spread_net > MIN_SPREAD_EUR` (default `40`) -> save to Supabase + Telegram notification.

## Quick Start

```bash
./scripts/bootstrap_local.sh
source .venv/bin/activate
python scripts/validate_env.py
python -m tech_sniper_it.worker
```

`bootstrap_local.sh` also installs a Git `pre-push` hook that runs the full test suite and blocks pushes on failures.

## MPB Self-Hosted (Cloud Free)

To improve MPB reliability vs Cloudflare/Turnstile while keeping cloud + free, run worker/smoke on a self-hosted GitHub runner with stable IP.

Workflows now support a single runner switch variable:

- `ACTIONS_RUNS_ON_JSON` (GitHub Repository Variable)
  - default (current behavior): `["ubuntu-latest"]`
  - self-hosted example: `["self-hosted","linux","x64","tech-sniper"]`

After this variable is set, `worker.yml` and `smoke.yml` will run on your self-hosted runner labels.

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

- `ACTIONS_RUNS_ON_JSON` (GitHub Repository Variable used by workflows; default `["ubuntu-latest"]`)
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_TABLE` (default: `arbitrage_opportunities`)
- `SUPABASE_SCANNER_USER_ID` (optional, for non-service-role inserts)
- `SUPABASE_WRITE_MAX_ATTEMPTS` (default: `3`, retry attempts for transient Supabase write failures)
- `SUPABASE_WRITE_RETRY_DELAY_MS` (default: `250`, base backoff for Supabase write retry)
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
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
- `STRATEGY_PROFILE` (default: `balanced`, one of `conservative|balanced|aggressive`; sets operating cost + risk buffers internally)
- `MAX_PARALLEL_PRODUCTS` (default: `3`)
- `SCAN_TELEGRAM_INDIVIDUAL_ALERTS` (default: `false`, send one message per opportunity in addition to consolidated scan report)
- `SCAN_TARGET_PRODUCTS` (default: `16`)
- `SCAN_CANDIDATE_MULTIPLIER` (default: `4`)
- `SCAN_DYNAMIC_QUERIES_ENABLED` (default: `true`, enables trend-driven query planning at scan start)
- `SCAN_DYNAMIC_QUERY_LIMIT` (default: `16`, max auto-generated Amazon Warehouse queries per scan)
- `SCAN_TARGET_PRODUCTS_AUTO_BOOST` (default: `true`, auto-increase evaluated products when candidate pool is rich)
- `SCAN_TARGET_PRODUCTS_MAX` (default: `20`, hard ceiling for auto-boosted evaluation target)
- `SCAN_TARGET_PRODUCTS_BOOST_TRIGGER_MULTIPLIER` (default: `2.5`, minimum pool/base ratio required before boost)
- `SCAN_TARGET_PRODUCTS_BOOST_STEP` (default: `4`, extra products evaluated when boost triggers)
- `SCAN_DYNAMIC_EXPLORATION_RATIO` (default: `0.35`, share of rotating exploration queries vs historical trend queries)
- `SCAN_DYNAMIC_TREND_MIN_SCORE` (default: `-35`, minimum trend score required for history-driven query inclusion)
- `SCAN_REQUIRE_COMPLETE_RESELLER_QUOTES` (default: `true`, keeps only decisions with real quotes from all required reseller strategies for that category)
- `SCAN_RESELLER_REFILL_BATCH_MULTIPLIER` (default: `2`, evaluates extra candidates when quote-complete decisions are missing)
- `EXCLUDE_MIN_KEEP` (default: `4`, re-includes a small slice of excluded URLs to avoid 0/1-product scans)
- `EXCLUDE_LOOKBACK_DAYS` (default: `1`)
- `EXCLUDE_DAILY_RESET` (default: `true`, reset exclusion cache at day boundary)
- `EXCLUDE_RESET_TIMEZONE` (default: `Europe/Rome`)
- `NON_PROFITABLE_SAVE_MAX_PARALLEL` (default: `3`, throttles concurrent non-profitable writes to Supabase)
- `SCAN_IT_QUOTA` (default: `6`, preferred IT candidates in final selection)
- `SCAN_EU_QUOTA` (default: `6`, preferred EU candidates in final selection)
- `SCORING_ENABLE` (default: `true`, enables historical expected-spread ranking)
- `SCORING_LOOKBACK_DAYS` (default: `30`)
- `SCORING_HISTORY_LIMIT` (default: `2000`)
- `SCAN_FILTER_ACCESSORIES` (default: `true`, blocks covers/cases/accessories from deal evaluation)
- `PLAYWRIGHT_NAV_TIMEOUT_MS` (default: `45000`)
- `HEADLESS` (default: `true`)
- `REBUY_USE_STORAGE_STATE` (default: `true`)
- `REBUY_STORAGE_STATE_B64` (optional Playwright storage state for logged-in Rebuy session; accepts base64 JSON or raw JSON)
- `MPB_MAX_ATTEMPTS` (default: `3`)
- `MPB_USE_STORAGE_STATE` (default: `true`)
- `MPB_STORAGE_STATE_B64` (optional Playwright storage state for MPB challenge bypass; accepts base64 JSON or raw JSON)
- `MPB_BLOCK_COOLDOWN_SECONDS` (default: `1800`, temporary MPB pause after anti-bot challenge detection)
- `MPB_REQUIRE_STORAGE_STATE` (default: `true`, skip MPB run when storage state is missing/invalid to avoid challenge loops)
- `MPB_API_PURCHASE_PRICE_ENABLED` (default: `true`, use MPB API-first path for faster/stabler buyback extraction)
- `MPB_API_MARKET` (default: `it`)
- `MPB_API_CONDITION` (default: `excellent`)
- `MPB_API_QUERY_LIMIT` (default: `1`)
- `MPB_API_MODEL_LIMIT` (default: `2`)
- `MPB_API_SEARCH_ROWS` (default: `8`)
- `MPB_API_TIME_BUDGET_SECONDS` (default: `12`)
- `VALUATOR_MAX_PARALLEL_MPB` (default: `1`, serializes MPB requests to reduce anti-bot triggers)
- `VALUATOR_MAX_PARALLEL_TRENDDEVICE` (default: `2`)
- `VALUATOR_QUERY_VARIANTS_MPB_MAX` (default: `1`)
- `VALUATOR_QUERY_VARIANTS_TRENDDEVICE_MAX` (default: `2`)
- `VALUATOR_QUERY_VARIANTS_REBUY_MAX` (default: `2`)
- `VALUATOR_TIMEOUT_MPB_SECONDS` (default: `28`)
- `VALUATOR_TIMEOUT_TRENDDEVICE_SECONDS` (default: `45`)
- `VALUATOR_TIMEOUT_REBUY_SECONDS` (default: `45`)
- `TRENDDEVICE_LEAD_EMAIL` (optional lead email used by TrendDevice wizard when required)
- `TRENDDEVICE_USE_STORAGE_STATE` (default: `true`)
- `TRENDDEVICE_STORAGE_STATE_B64` (optional Playwright storage state for logged-in TrendDevice session; accepts base64 JSON or raw JSON)
- `TRENDDEVICE_EMAIL_GATE_WAIT_MS` (default: `6500`, wait after lead form submit before fallback extraction)
- `VALUATOR_SELECTOR_OVERRIDES_JSON` (optional JSON selector overrides for automatic UI drift adaptation)
- `AMAZON_PRODUCTS_JSON` (optional JSON array)
- `AMAZON_PRODUCTS_FILE` (optional path to JSON file)
- `AMAZON_WAREHOUSE_ENABLED` (default: `true`)
- `AMAZON_WAREHOUSE_MARKETPLACES` (default: `it,eu`; `eu` expands to `de,fr,es`)
- `AMAZON_WAREHOUSE_MAX_PRODUCTS` (default: `8`)
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
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_IT` (optional domain-specific session for `amazon.it`)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE` (optional domain-specific session for `amazon.de`)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR` (optional domain-specific session for `amazon.fr`)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_ES` (optional domain-specific session for `amazon.es`)
- `AMAZON_WAREHOUSE_CART_PRICING_ENABLED` (default: `false`, validates real cart net price in authenticated session)
- `AMAZON_WAREHOUSE_CART_PRICING_MAX_ITEMS` (default: `4`, max candidates per scan validated via cart)
- `AMAZON_WAREHOUSE_CART_PRICING_REQUIRE_EMPTY_CART` (default: `true`, skip cart validation unless cart is empty to avoid side effects)
- `AMAZON_WAREHOUSE_CART_PRICING_ALLOW_DELTA` (default: `true`, when cart is not empty computes candidate net price from before/after cart delta)
- `AMAZON_WAREHOUSE_CART_PRICING_FORCE_EMPTY_AFTER_HOST` (default: `true`, hard-cleanup cart at end of each host session)
- `AMAZON_WAREHOUSE_DEBUG_ON_EMPTY` (default: `true`, saves diagnostic dump on zero parsed results)
- `AMAZON_WAREHOUSE_DEBUG_DIR` (default: `/tmp/tech_sniper_it_debug`)

Cart promo validator behavior:

- when enabled, the worker adds the candidate to cart, reads subtotal/promo/total, and removes that same item immediately.
- when the cart already contains other items, delta mode can still compute net price from cart totals (if enabled).
- if item removal fails, the cart price is discarded to prevent persistent cart pollution.

### AI Free-Tier Selection Logic

Gemini routing is disabled in runtime (OpenRouter-only + heuristic fallback).

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
- daily exclusion reset (`EXCLUDE_DAILY_RESET=true`, `EXCLUDE_RESET_TIMEZONE=Europe/Rome`)

Candidate ordering now also prioritizes likely profitable items first using:

- historical expected offer/spread from Supabase
- market liquidity signals (model families with faster resale)
- valuator health weighting (penalize categories whose required platforms are currently failing)
- IT/EU balanced final pick (`SCAN_IT_QUOTA` / `SCAN_EU_QUOTA`)

At scan start, when no explicit payload products are provided, the worker now builds dynamic Amazon Warehouse queries from:

- recent Supabase trend models (spread + recency + reseller coverage),
- category-level momentum and valuator health,
- rotating exploration catalog (hourly rotation) to avoid repeatedly scanning the same few objects.

Additionally, accessory guardrails drop non-core device listings (e.g., covers/cases with compatibility text) before valuation to avoid false-positive opportunities.

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

### Free Bypass With Your Amazon Account Session (IT + EU)

To reduce `sorry-page` blocks without paid proxies, capture a logged-in Amazon Playwright session locally and store it as secret.

Generate storage state base64 for each marketplace:

```bash
source .venv/bin/activate
python scripts/capture_amazon_storage_state.py --domain www.amazon.it
python scripts/capture_amazon_storage_state.py --domain www.amazon.de
python scripts/capture_amazon_storage_state.py --domain www.amazon.fr
python scripts/capture_amazon_storage_state.py --domain www.amazon.es
```

Copy the printed base64 value to GitHub Secret:

- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_IT`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_ES`

Optional fallback secret (used when domain-specific one is missing):

- `AMAZON_WAREHOUSE_STORAGE_STATE_B64`

Keep enabled:

- `AMAZON_WAREHOUSE_USE_STORAGE_STATE=true`
- `AMAZON_WAREHOUSE_STEALTH=true`
- `AMAZON_WAREHOUSE_FAIL_FAST_ON_SORRY=true`

### Free Bypass With Your TrendDevice Account Session

TrendDevice can stop on the email lead-gate without returning a valuation in the DOM.
To improve quote extraction reliability, capture a logged-in TrendDevice Playwright session and store it as secret.

Generate storage state base64:

```bash
source .venv/bin/activate
python scripts/capture_trenddevice_storage_state.py
```

Copy the printed base64 value to GitHub Secret:

- `TRENDDEVICE_STORAGE_STATE_B64`

Keep enabled:

- `TRENDDEVICE_USE_STORAGE_STATE=true`
- `TRENDDEVICE_EMAIL_GATE_WAIT_MS=6500` (or slightly higher if your account flow is slow)

### Free Bypass With MPB Session State

MPB can return a Cloudflare/Turnstile page before the sell wizard is visible.
To improve stability, capture a local Playwright session after challenge resolution and store it as secret.

Generate storage state base64:

```bash
source .venv/bin/activate
python scripts/capture_mpb_storage_state.py
```

Copy the printed base64 value to GitHub Secret:

- `MPB_STORAGE_STATE_B64`

Keep enabled:

- `MPB_USE_STORAGE_STATE=true`

### Free Bypass With Rebuy Session State

Rebuy can occasionally degrade to anti-bot/cookie gating and hide reliable sell quotes.
To keep quote extraction stable, capture a local Playwright session and store it as secret.

Generate storage state base64:

```bash
source .venv/bin/activate
python scripts/capture_rebuy_storage_state.py
```

Copy the printed base64 value to GitHub Secret:

- `REBUY_STORAGE_STATE_B64`

Keep enabled:

- `REBUY_USE_STORAGE_STATE=true`

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
- `OPENROUTER_API_KEYS`
- `AMAZON_WAREHOUSE_PROXY_URLS` (required for real anti-bot resilient warehouse autoscan)

Optional secrets:

- `SUPABASE_URL` (can be moved to Variables)
- `TELEGRAM_CHAT_ID` (can be moved to Variables)
- `TRENDDEVICE_LEAD_EMAIL`
- `TRENDDEVICE_STORAGE_STATE_B64` (recommended if TrendDevice email-gate blocks quote extraction)
- `REBUY_STORAGE_STATE_B64` (recommended if Rebuy anti-bot/cookie gate blocks stable quote extraction)
- `MPB_STORAGE_STATE_B64` (recommended if MPB Cloudflare challenge blocks the sell wizard)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64` (global fallback)
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_IT`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_DE`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_FR`
- `AMAZON_WAREHOUSE_STORAGE_STATE_B64_ES`

#### Recommended GitHub Variables (Worker, non-sensitive)

- `SUPABASE_URL`
- `SUPABASE_TABLE`
- `TELEGRAM_CHAT_ID`
- `MIN_SPREAD_EUR`
- `MAX_PARALLEL_PRODUCTS`
- `PLAYWRIGHT_NAV_TIMEOUT_MS`
- `REBUY_USE_STORAGE_STATE`
- `TRENDDEVICE_USE_STORAGE_STATE`
- `TRENDDEVICE_EMAIL_GATE_WAIT_MS`
- `MPB_MAX_ATTEMPTS`
- `MPB_USE_STORAGE_STATE`
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
- `SCAN_DYNAMIC_QUERIES_ENABLED`
- `SCAN_DYNAMIC_QUERY_LIMIT`
- `SCAN_TARGET_PRODUCTS_AUTO_BOOST`
- `SCAN_TARGET_PRODUCTS_MAX`
- `SCAN_TARGET_PRODUCTS_BOOST_TRIGGER_MULTIPLIER`
- `SCAN_TARGET_PRODUCTS_BOOST_STEP`
- `SCAN_DYNAMIC_EXPLORATION_RATIO`
- `SCAN_DYNAMIC_TREND_MIN_SCORE`
- `SCAN_REQUIRE_COMPLETE_RESELLER_QUOTES`
- `SCAN_RESELLER_REFILL_BATCH_MULTIPLIER`
- `EXCLUDE_MIN_KEEP`
- `EXCLUDE_LOOKBACK_DAYS`
- `EXCLUDE_DAILY_RESET`
- `EXCLUDE_RESET_TIMEZONE`
- `SCAN_IT_QUOTA`
- `SCAN_EU_QUOTA`
- `SCORING_ENABLE`
- `SCORING_LOOKBACK_DAYS`
- `SCORING_HISTORY_LIMIT`
- `SCAN_FILTER_ACCESSORIES`
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
- `/smoke` (delegated to GitHub Actions; fast smoke run)
- `/status` (delegated to GitHub Actions)
- `/last [n]` (delegated to GitHub Actions, max 10 rows)
- `/profile [show|conservative|balanced|aggressive]` (reads/updates `STRATEGY_PROFILE` GitHub variable)

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
- `/profile` (shows current strategy profile)
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
