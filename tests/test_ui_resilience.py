from __future__ import annotations

from tech_sniper_it.valuators.ui_resilience import (
    keyword_presence,
    parse_selector_overrides,
    selector_candidates,
    ui_signature,
)


def test_parse_selector_overrides_accepts_site_and_global_slots() -> None:
    raw = (
        '{"*":{"price":["[data-testid*=price]"]},'
        '"rebuy":{"search_input":["input[type=search]","input[type=search]"]}}'
    )
    parsed = parse_selector_overrides(raw)
    assert "*" in parsed
    assert parsed["*"]["price"] == ["[data-testid*=price]"]
    assert parsed["rebuy"]["search_input"] == ["input[type=search]"]


def test_parse_selector_overrides_invalid_returns_empty() -> None:
    assert parse_selector_overrides("not-json") == {}
    assert parse_selector_overrides("[]") == {}


def test_selector_candidates_merges_defaults_global_and_site() -> None:
    raw = (
        '{"*":{"search_input":["input[aria-label*=search i]"]},'
        '"mpb":{"search_input":["input[data-testid=typeahead__input]"]}}'
    )
    merged = selector_candidates(
        site="mpb",
        slot="search_input",
        defaults=["input[type=search]"],
        overrides_raw=raw,
    )
    assert merged == [
        "input[type=search]",
        "input[aria-label*=search i]",
        "input[data-testid=typeahead__input]",
    ]


def test_keyword_presence_tracks_hits_and_missing() -> None:
    present, missing = keyword_presence("rebuy vendi usato", ["rebuy", "search", "vendi"])
    assert present == ["rebuy", "vendi"]
    assert missing == ["search"]


def test_ui_signature_is_stable_for_equivalent_whitespace() -> None:
    first = ui_signature("https://example.com", "Titolo", "A  B   C")
    second = ui_signature("https://example.com", "Titolo", "A B C")
    assert first == second
