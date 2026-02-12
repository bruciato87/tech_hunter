create extension if not exists pgcrypto;

create table if not exists public.arbitrage_opportunities (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default timezone('utc', now()),
    scanned_at timestamptz not null default timezone('utc', now()),
    product_title text not null,
    normalized_name text not null,
    category text not null check (category in ('photography', 'apple_phone', 'general_tech')),
    amazon_price_eur numeric(12, 2) not null check (amazon_price_eur >= 0),
    best_platform text not null,
    best_offer_eur numeric(12, 2) not null check (best_offer_eur >= 0),
    spread_eur numeric(12, 2) not null,
    condition_target text not null default 'grade_a',
    offers_payload jsonb not null default '[]'::jsonb,
    source_url text,
    ean text,
    scanner_user_id uuid default auth.uid() references auth.users(id) on delete set null
);

create index if not exists idx_arbitrage_created_at
    on public.arbitrage_opportunities (created_at desc);

create index if not exists idx_arbitrage_spread
    on public.arbitrage_opportunities (spread_eur desc);

create index if not exists idx_arbitrage_category
    on public.arbitrage_opportunities (category);

alter table public.arbitrage_opportunities enable row level security;
alter table public.arbitrage_opportunities force row level security;

drop policy if exists "authenticated_select_own_opportunities" on public.arbitrage_opportunities;
create policy "authenticated_select_own_opportunities"
on public.arbitrage_opportunities
for select
to authenticated
using (scanner_user_id = auth.uid());

drop policy if exists "authenticated_insert_own_opportunities" on public.arbitrage_opportunities;
create policy "authenticated_insert_own_opportunities"
on public.arbitrage_opportunities
for insert
to authenticated
with check (scanner_user_id = auth.uid());

drop policy if exists "authenticated_update_own_opportunities" on public.arbitrage_opportunities;
create policy "authenticated_update_own_opportunities"
on public.arbitrage_opportunities
for update
to authenticated
using (scanner_user_id = auth.uid())
with check (scanner_user_id = auth.uid());

drop policy if exists "service_role_full_access_opportunities" on public.arbitrage_opportunities;
create policy "service_role_full_access_opportunities"
on public.arbitrage_opportunities
for all
to service_role
using (true)
with check (true);
