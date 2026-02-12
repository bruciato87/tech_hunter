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
- If `spread_eur > MIN_SPREAD_EUR` (default `80`) -> save to Supabase + Telegram notification.

## Quick Start

```bash
./scripts/bootstrap_local.sh
source .venv/bin/activate
python scripts/validate_env.py
python -m tech_sniper_it.worker
```

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
- `MIN_SPREAD_EUR` (default: `80`)
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

Store all production keys only in GitHub Secrets, never in tracked files.
