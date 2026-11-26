"""The live sampler: every 5 minutes compare spot's trading list with the table."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

from binance_listing.config import PROXY_URL, SAMPLE_OFFSET_SECONDS, SPOT_QUOTES, WRITE_DEADLINE_SECONDS
from binance_listing.duckport_client import DuckportClient
from binance_listing.events import (
    MARKET_SPOT,
    SOURCE_LIVE,
    STATE_DELISTED,
    STATE_TRADING,
    Change,
    Event,
    from_ms,
    next_sample_after,
    plan_changes,
    to_ms,
    utcnow,
)
from binance_listing.mapping import quote_rank, resolve_spot_base
from binance_listing.spot import RateLimited, SpotApi, SpotApiError, trading_by_base

logger = logging.getLogger(__name__)

# A row says "known at the sample time", so a sample must be written soon after it.
# Bar lookups stop at LOOKUP_BUDGET; a sample not written by WRITE_DEADLINE is dropped.
LOOKUP_BUDGET = timedelta(seconds=60)
WRITE_DEADLINE = timedelta(seconds=WRITE_DEADLINE_SECONDS)
FETCH_RETRY_SECONDS = 5
# If fewer than this share of the coins that had spot are still in the list, the list is
# taken as bad (exchange maintenance, truncated response) and the sample is dropped.
MIN_STILL_TRADING = 0.8
MIN_STILL_TRADING_BASE = 20


class ListingJobs:
    def __init__(self, client: DuckportClient, api: Optional[SpotApi] = None, start: bool = True):
        self.client = client
        self.api = api or SpotApi(proxy=PROXY_URL)
        self.offset = SAMPLE_OFFSET_SECONDS
        self.quotes = SPOT_QUOTES
        self._missing: dict[str, datetime] = {}
        self._stop = threading.Event()
        self._now = utcnow
        if start:
            self.thread = threading.Thread(target=self._loop, name="binance-listing", daemon=True)
            self.thread.start()

    def request_stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            slot = next_sample_after(self._now(), self.offset)
            if self._stop.wait(max(0.0, (slot - self._now()).total_seconds())):
                return
            try:
                self.sample(slot)
            except Exception:
                logger.exception("采样 %s 失败，本次不写、不推进水位", slot)

    def _fetch_trading(self, slot: datetime) -> Optional[list[dict]]:
        while True:
            left = (slot + LOOKUP_BUDGET - self._now()).total_seconds()
            if left <= 1:
                return None
            try:
                return self.api.symbols(trading_only=True, timeout=min(20.0, left))
            except RateLimited as exc:
                logger.warning("现货 exchangeInfo 被限流: %s", exc)
                return None
            except SpotApiError as exc:
                logger.warning("现货 exchangeInfo 拉取失败: %s", exc)
            if self._stop.wait(FETCH_RETRY_SECONDS):
                return None

    def _event_time(self, change: Change, slot: datetime) -> Optional[datetime]:
        """First 1m bar of a listing; None if it cannot be had in time."""
        if self._now() > slot + LOOKUP_BUDGET:
            return None
        previous = change.previous
        # After a delisting the coin was not trading at that row's known_at (live wrote the row
        # because the sample missed it), so the relisting's first bar is at or after it. Not the
        # pair's first bar ever, and still bounded when the delisting's event_time is NULL.
        start = to_ms(previous.known_at) if previous and previous.known_at else 0
        try:
            ms = self.api.first_bar_ms(change.venue_symbol, start)
        except SpotApiError as exc:
            logger.warning("%s 查 1 分钟 K 线失败，event_time 留空: %s", change.venue_symbol, exc)
            return None
        return from_ms(ms) if ms is not None else None

    def _stopped_pairs(self, slot: datetime) -> Optional[dict[str, list[tuple[str, str]]]]:
        """base asset -> [(pair, quote)] of pairs in BREAK; None if the list cannot be had."""
        if self._now() > slot + LOOKUP_BUDGET:
            return None
        try:
            rows = self.api.break_symbols(timeout=min(20.0, (slot + LOOKUP_BUDGET - self._now()).total_seconds()))
        except SpotApiError as exc:
            logger.warning("现货 BREAK 名单拉取失败，下架只查原交易对: %s", exc)
            return None
        out: dict[str, list[tuple[str, str]]] = {}
        for row in rows:
            if self.quotes and row["quote"] not in self.quotes:
                continue
            out.setdefault(row["base"], []).append((row["symbol"], row["quote"]))
        return out

    def _delisting(self, change: Change, stopped: Optional[dict], slot: datetime) -> tuple[Optional[datetime], str]:
        """(last 1m bar, pair) of the coin's pair that stopped last.

        The pair noted at listing may have stopped long before the coin's last pair (USDT gone,
        BTC still trading), so every stopped pair of the coin is looked at, as history does.
        """
        pairs = {change.venue_symbol: quote_rank("")}   # quote unknown here; the BREAK list may give it
        base = resolve_spot_base(change.perp_base, stopped) if stopped and change.perp_base else None
        for pair, quote in (stopped or {}).get(base, []):
            pairs[pair] = quote_rank(quote)
        best: Optional[tuple[int, int, str]] = None
        for pair in sorted(pairs):
            if self._now() > slot + LOOKUP_BUDGET:
                break
            try:
                ms = self.api.last_bar_ms(pair)
            except SpotApiError as exc:
                logger.warning("%s 查最后一根 1 分钟 K 线失败: %s", pair, exc)
                continue
            if ms is not None and (best is None or (ms, -pairs[pair]) > (best[0], -best[1])):
                best = (ms, pairs[pair], pair)
        if best is None:
            return None, change.venue_symbol
        return from_ms(best[0]), best[2]

    def sample(self, slot: datetime) -> bool:
        """Run one sample for `slot`. Returns True if it was written (watermark moved)."""
        rows = self._fetch_trading(slot)
        if rows is None:
            logger.warning("采样 %s 跳过：没拿到现货名单", slot)
            return False
        trading = trading_by_base(rows, self.quotes)
        perps = self.client.read_perps()
        if not perps:
            logger.error("采样 %s 跳过：exginfo 里没有 usdt_perp 合约（K 线采集插件没在跑？）", slot)
            return False
        last = self.client.read_last_states()

        had_spot = [s for s in perps if s in last and last[s].state == STATE_TRADING]
        still = sum(1 for s in had_spot if resolve_spot_base(perps[s], trading) is not None)
        if len(had_spot) >= MIN_STILL_TRADING_BASE and still < MIN_STILL_TRADING * len(had_spot):
            logger.warning("采样 %s 跳过：有现货的 %s 个合约里只剩 %s 个在名单中，名单可疑",
                           slot, len(had_spot), still)
            return False

        changes, missing = plan_changes(perps, trading, last, self._missing, slot)
        stopped = self._stopped_pairs(slot) if any(c.state == STATE_DELISTED for c in changes) else None
        events = []
        for change in changes:
            if change.state == STATE_DELISTED:
                event_time, venue = self._delisting(change, stopped, slot)
            else:
                event_time, venue = self._event_time(change, slot), change.venue_symbol
            events.append(Event(slot, event_time, MARKET_SPOT, change.symbol, venue, change.state, SOURCE_LIVE))
        if self._now() > slot + WRITE_DEADLINE:
            logger.warning("采样 %s 跳过：超过 %s 秒还没写完", slot, int(WRITE_DEADLINE.total_seconds()))
            return False
        self.client.write_sample(events, slot)
        self._missing = missing
        for event in events:
            logger.info("%s %s -> %s (%s, event_time=%s)", slot, event.symbol,
                        "trading" if event.state == STATE_TRADING else "delisted",
                        event.venue_symbol, event.event_time)
        logger.info("采样 %s 完成：现货交易对 %s，合约 %s，变化 %s，待确认下架 %s",
                    slot, len(rows), len(perps), len(events), len(missing))
        return True
