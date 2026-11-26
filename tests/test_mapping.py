from binance_listing.config import parse_offset, parse_quotes
from binance_listing.mapping import perp_base_of, pick_pair, resolve_spot_base, spot_base_candidates

import pytest


def test_exact_name_wins_over_stripped_prefix():
    assert spot_base_candidates("1000SATS") == ["1000SATS", "SATS"]
    assert resolve_spot_base("1000SATS", {"1000SATS", "SATS"}) == "1000SATS"
    assert resolve_spot_base("1000SHIB", {"SHIB"}) == "SHIB"
    assert resolve_spot_base("1000000MOG", {"MOG"}) == "MOG"
    assert resolve_spot_base("1MBABYDOGE", {"BABYDOGE"}) == "BABYDOGE"


def test_aliases_and_misses():
    assert resolve_spot_base("LUNA2", {"LUNA"}) == "LUNA"
    assert resolve_spot_base("BTC", {"BTC"}) == "BTC"
    assert resolve_spot_base("BTCDOM", {"BTC"}) is None
    assert resolve_spot_base("1000", {"1000"}) == "1000"


def test_perp_base_of():
    assert perp_base_of("1000CHEEMSUSDT") == "1000CHEEMS"
    assert perp_base_of("BTCUSDC") is None
    assert perp_base_of("BTCUSDT_260327") is None
    assert perp_base_of("USDT") is None


def test_pick_pair_prefers_usdt():
    assert pick_pair([("ABCBTC", "BTC"), ("ABCUSDT", "USDT"), ("ABCTRY", "TRY")]) == "ABCUSDT"
    assert pick_pair([("ABCTRY", "TRY"), ("ABCEUR", "EUR")]) == "ABCEUR"


def test_config_parsers():
    assert parse_quotes("") == frozenset()
    assert parse_quotes(" usdt, USDC ") == frozenset({"USDT", "USDC"})
    with pytest.raises(ValueError):
        parse_quotes("US-DT")
    assert parse_offset("150") == 150
    assert parse_offset("1") == 1 and parse_offset("209") == 209
    for bad in ("0", "210", "299", "300"):
        with pytest.raises(ValueError):
            parse_offset(bad)
