#!/usr/bin/env python3
"""Rebuild the history: every `backfill` row is dropped and computed again.

Sources: spot exchangeInfo (every pair, delisted ones included), the first and last
1m bar of each pair, and the list of USDT perpetuals that ever existed (data.binance.vision).
Rows written by the live sampler are never touched, and nothing is written at or after the
first live sample.

    loadhist             rebuild
    loadhist --dry-run   compute and print, write nothing
"""

from __future__ import annotations

import collections
import logging
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

os.environ["TZ"] = "UTC"
time.tzset()

from binance_listing.config import (  # noqa: E402
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    PROXY_URL,
    SAMPLE_OFFSET_SECONDS,
    SPOT_QUOTES,
    setup_logging,
)
from binance_listing.duckport_client import DuckportClient  # noqa: E402
from binance_listing.events import (  # noqa: E402
    STATE_TRADING,
    Event,
    PairSpan,
    boundary_anchors,
    from_ms,
    history_events,
    sample_at_or_before,
    utcnow,
)
from binance_listing.mapping import perp_base_of, quote_rank, resolve_spot_base  # noqa: E402
from binance_listing.spot import RateLimited, SpotApi, SpotApiError  # noqa: E402

logger = logging.getLogger(__name__)

VISION_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
VISION_PERP_PREFIX = "data/futures/um/daily/klines/"
WORKERS = 4
TRIES = 4


def vision_perp_symbols(proxy: str = "") -> set[str]:
    """Every USD-M symbol that ever had daily K-line files, delisted ones included."""
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
    opener = urllib.request.build_opener(*handlers)
    found: set[str] = set()
    marker = ""
    while True:
        url = f"{VISION_LIST_URL}?delimiter=/&prefix={VISION_PERP_PREFIX}"
        if marker:
            url += f"&marker={marker}"
        with opener.open(url, timeout=30) as response:
            text = response.read().decode("utf-8")
        prefixes = re.findall(r"<Prefix>" + re.escape(VISION_PERP_PREFIX) + r"([^/<]+)/</Prefix>", text)
        found.update(prefixes)
        if "<IsTruncated>true</IsTruncated>" not in text:
            return found
        match = re.search(r"<NextMarker>([^<]+)</NextMarker>", text)
        if not match:
            raise RuntimeError("vision 列表被截断但没有 NextMarker")
        marker = match.group(1)


def collect_perps(vision: set[str], current: dict[str, str], tradfi: frozenset[str] = frozenset()) -> dict[str, str]:
    """symbol -> base asset for every USDT perpetual, past and present, except today's TradFi ones.

    A TradFi contract (a stock, a commodity) has no spot coin, but its name can match an old,
    unrelated spot coin (the stock BOTUSDT and the delisted coin BOT). Live never samples TradFi
    contracts, so a state backfilled for one would stay forever. A name that was a crypto
    perpetual before it became TradFi (ANTUSDT) loses its crypto-era history the same way.
    """
    perps: dict[str, str] = {}
    for symbol in vision:
        base = perp_base_of(symbol)
        if base and symbol not in tradfi:
            perps[symbol] = base
    perps.update(current)
    return perps


def map_perps(perps: dict[str, str], spot_rows: list[dict], quotes: frozenset[str]):
    """Returns ({perp symbol: spot base}, {spot base: [spot rows]}) for the perps that have a spot coin."""
    by_base: dict[str, list[dict]] = collections.defaultdict(list)
    for row in spot_rows:
        if quotes and row["quote"] not in quotes:
            continue
        by_base[row["base"]].append(row)
    mapped: dict[str, str] = {}
    for symbol, perp_base in perps.items():
        base = resolve_spot_base(perp_base, by_base)
        if base is not None:
            mapped[symbol] = base
    used = set(mapped.values())
    return mapped, {base: rows for base, rows in by_base.items() if base in used}


def fetch_span(api: SpotApi, row: dict) -> Optional[PairSpan]:
    """First and last 1m bar of one pair. None if it never traded."""
    for attempt in range(TRIES):
        try:
            api.pause_if_busy()
            first = api.first_bar_ms(row["symbol"])
            if first is None:
                return None
            rank = quote_rank(row["quote"])
            if row["status"] == "TRADING":
                return PairSpan(row["symbol"], from_ms(first), None, rank)
            last = api.last_bar_ms(row["symbol"])
            return PairSpan(row["symbol"], from_ms(first), from_ms(last if last is not None else first), rank)
        except RateLimited:
            raise
        except SpotApiError as exc:
            if attempt == TRIES - 1:
                raise
            logger.warning("%s 重试: %s", row["symbol"], exc)
    return None


def build_events(api: SpotApi, perps: dict[str, str], before) -> tuple[list[Event], dict]:
    spot_rows = api.symbols(trading_only=False)
    mapped, pairs_by_base = map_perps(perps, spot_rows, SPOT_QUOTES)
    pair_rows = [row for rows in pairs_by_base.values() for row in rows]
    logger.info("现货交易对 %s 个；USDT 永续 %s 个，其中 %s 个对得上现货币；要查 %s 个交易对的首末 K 线",
                len(spot_rows), len(perps), len(mapped), len(pair_rows))

    spans: dict[str, Optional[PairSpan]] = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for done, (row, span) in enumerate(
            zip(pair_rows, pool.map(lambda r: fetch_span(api, r), pair_rows)), 1
        ):
            spans[row["symbol"]] = span
            if done % 500 == 0:
                logger.info("  已查 %s / %s", done, len(pair_rows))

    events: list[Event] = []
    for symbol in sorted(mapped):
        coin_spans = [spans[row["symbol"]] for row in pairs_by_base[mapped[symbol]]]
        events += history_events(symbol, [s for s in coin_spans if s is not None],
                                 SAMPLE_OFFSET_SECONDS, before)
    final: dict[str, int] = {}
    for event in sorted(events, key=lambda e: e.known_at):
        final[event.symbol] = event.state
    stats = {
        "perps": len(perps),
        "mapped": len(mapped),
        "events": len(events),
        "trading_now": sum(1 for state in final.values() if state == STATE_TRADING),
        "delisted_now": sum(1 for state in final.values() if state != STATE_TRADING),
        "unmapped": sorted(set(perps) - set(mapped)),
    }
    return events, stats


def main() -> None:
    setup_logging()
    args = sys.argv[1:]
    if any(arg not in ("--dry-run",) for arg in args):
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    dry_run = "--dry-run" in args

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)
    try:
        client.ping()
        if not dry_run:
            client.init_schema()
        try:
            live_since = client.read_live_since()
        except Exception:
            live_since = None
        now_slot = sample_at_or_before(utcnow(), SAMPLE_OFFSET_SECONDS)
        # History stops where live starts; before the first live sample it runs up to now.
        before = live_since or now_slot
        logger.info("重建 %s 之前的历史（%s）", before,
                    "实盘第一次采样" if live_since else "还没有实盘采样，取当前采样点")

        current = client.read_perps()
        if not current:
            logger.error("exginfo 里没有 usdt_perp 合约（K 线采集插件没在跑？），分不出 TradFi 合约，不重建")
            sys.exit(1)
        tradfi = frozenset(client.read_tradfi_perps())
        perps = collect_perps(vision_perp_symbols(PROXY_URL), current, tradfi)
        # What live saw at its first sample, read before the rebuild replaces the rows under it
        observed = client.read_last_states(live_since) if live_since else {}
        events, stats = build_events(SpotApi(proxy=PROXY_URL), perps, before)
        # Live never samples TradFi contracts, so there is nothing of theirs to keep; their old rows just go
        observed = {symbol: state for symbol, state in observed.items() if symbol not in tradfi}
        anchors = boundary_anchors(events, observed, live_since) if live_since else []
        if anchors:
            logger.warning("重建后的历史在实盘开始时（%s）与实盘当时看到的不同，%s 个合约在该时刻补一行保持实盘不变: %s",
                           live_since, len(anchors), " ".join(f"{a.symbol}={a.state}" for a in anchors))
        logger.info("合约 %(perps)s，对得上现货币 %(mapped)s，事件 %(events)s 行；"
                    "截止时有现货 %(trading_now)s，现货已下架 %(delisted_now)s", stats)
        logger.info("没有现货币的合约 %s 个: %s", len(stats["unmapped"]), " ".join(stats["unmapped"]))
        if dry_run:
            for event in sorted(events, key=lambda e: e.known_at)[-15:]:
                print(event)
            for anchor in anchors:
                print("anchor", anchor)
            logger.info("dry-run：没有写入")
            return
        client.replace_backfill(events + anchors, None if live_since else now_slot)
        logger.info("已写入 %s 行 backfill（含实盘起点补行 %s）", len(events) + len(anchors), len(anchors))
    finally:
        client.close()


if __name__ == "__main__":
    main()
