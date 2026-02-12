from __future__ import annotations

from types import SimpleNamespace

import pytest

from tech_sniper_it.models import AmazonProduct, ArbitrageDecision, ProductCategory, ValuationResult
from tech_sniper_it.storage import SupabaseStorage


class FakeTableQuery:
    def __init__(self) -> None:
        self.insert_payload = None
        self.limit_value = None
        self.data = [{"normalized_name": "iPhone", "spread_eur": 120, "best_platform": "rebuy"}]

    def insert(self, payload):
        self.insert_payload = payload
        return self

    def select(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, value: int):
        self.limit_value = value
        return self

    def execute(self):
        return SimpleNamespace(data=self.data)


class FakeSupabaseClient:
    def __init__(self) -> None:
        self.table_query = FakeTableQuery()
        self.table_names = []

    def table(self, name: str):
        self.table_names.append(name)
        return self.table_query


@pytest.mark.asyncio
async def test_save_opportunity_inserts_expected_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = FakeSupabaseClient()
    monkeypatch.setattr("tech_sniper_it.storage.create_client", lambda _url, _key: fake_client)
    monkeypatch.setenv("SUPABASE_SCANNER_USER_ID", "11111111-1111-1111-1111-111111111111")

    storage = SupabaseStorage(url="https://supabase.local", key="service-role-key", table="arbitrage_opportunities")
    decision = ArbitrageDecision(
        product=AmazonProduct(
            title="Apple iPhone 14 Pro 128GB",
            price_eur=500.0,
            category=ProductCategory.APPLE_PHONE,
            ean="123",
            url="https://amazon.it/item",
        ),
        normalized_name="iPhone 14 Pro 128GB",
        offers=[ValuationResult(platform="rebuy", normalized_name="iPhone 14 Pro 128GB", offer_eur=620.0)],
        best_offer=ValuationResult(
            platform="rebuy",
            normalized_name="iPhone 14 Pro 128GB",
            offer_eur=620.0,
            condition="come_nuovo",
        ),
        spread_eur=120.0,
        should_notify=True,
    )

    await storage.save_opportunity(decision)
    payload = fake_client.table_query.insert_payload

    assert payload is not None
    assert payload["normalized_name"] == "iPhone 14 Pro 128GB"
    assert payload["condition_target"] == "come_nuovo"
    assert payload["scanner_user_id"] == "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_get_recent_opportunities_clamps_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_client = FakeSupabaseClient()
    monkeypatch.setattr("tech_sniper_it.storage.create_client", lambda _url, _key: fake_client)

    storage = SupabaseStorage(url="https://supabase.local", key="service-role-key", table="arbitrage_opportunities")
    rows = await storage.get_recent_opportunities(limit=999)

    assert fake_client.table_query.limit_value == 20
    assert rows and rows[0]["normalized_name"] == "iPhone"
