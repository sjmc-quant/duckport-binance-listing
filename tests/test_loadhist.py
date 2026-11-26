from binance_listing.loadhist import collect_perps, map_perps


def row(symbol, base, quote, status="TRADING"):
    return {"symbol": symbol, "base": base, "quote": quote, "status": status}


def test_collect_perps_keeps_usdt_perpetuals_only():
    vision = {"BTCUSDT", "BTCUSDC", "ETHUSDT_260327", "1000SHIBUSDT", "OLDUSDT"}
    perps = collect_perps(vision, {"NEWUSDT": "NEW", "BTCUSDT": "BTC"})
    assert perps == {"BTCUSDT": "BTC", "1000SHIBUSDT": "1000SHIB", "OLDUSDT": "OLD", "NEWUSDT": "NEW"}


def test_map_perps_uses_delisted_pairs_and_quote_filter():
    spot = [row("BTCUSDT", "BTC", "USDT"), row("SHIBUSDT", "SHIB", "USDT"),
            row("OLDBTC", "OLD", "BTC", "BREAK"), row("ZZZUSDT", "ZZZ", "USDT")]
    perps = {"BTCUSDT": "BTC", "1000SHIBUSDT": "1000SHIB", "OLDUSDT": "OLD", "NEWUSDT": "NEW"}
    mapped, pairs = map_perps(perps, spot, frozenset())
    assert mapped == {"BTCUSDT": "BTC", "1000SHIBUSDT": "SHIB", "OLDUSDT": "OLD"}
    assert sorted(pairs) == ["BTC", "OLD", "SHIB"]
    mapped, pairs = map_perps(perps, spot, frozenset({"USDT"}))
    assert "OLDUSDT" not in mapped and "OLD" not in pairs
