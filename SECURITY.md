# Security Notes

## Secrets

- Never commit `.env` or any real keys.
- Use only GitHub Actions Secrets for cloud execution.
- Rotate leaked keys immediately (Supabase service role, Telegram bot token, AI provider keys).

## Database Access

- `public.arbitrage_opportunities` is configured with `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY`.
- Authenticated users can access only their own rows (`scanner_user_id = auth.uid()`).
- Worker automation is expected to use Supabase `service_role` key from secure CI secret storage.

## CI Hardening

- Gitleaks workflow runs on pushes and PRs: `.github/workflows/security.yml`.
- Keep branch protections enabled before opening to collaborators.

## Vercel Webhook Security

- Protect Telegram webhook with `TELEGRAM_WEBHOOK_SECRET_TOKEN`.
- Restrict command usage with `TELEGRAM_ALLOWED_CHAT_IDS`.
- Protect `/scan` endpoint with `SCAN_SECRET` bearer token.
