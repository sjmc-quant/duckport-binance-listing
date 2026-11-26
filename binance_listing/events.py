"""Pure logic: the sampling grid, history rebuild and the live diff. No I/O here."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Optional

from binance_listing.config import SAMPLE_PERIOD_SECONDS
from binance_listing.mapping import pick_pair, resolve_spot_base

MARKET_SPOT = "spot"
STATE_NEVER = 0      # never had spot; not written, it is what "no row" means
STATE_TRADING = 1
STATE_DELISTED = 2
SOURCE_LIVE = "live"
SOURCE_BACKFILL = "backfill"

COLUMNS = ("known_at", "event_time", "market", "symbol", "venue_symbol", "state", "source")

_EPOCH = datetime(1970, 1, 1)
_PERIOD = timedelta(seconds=SAMPLE_PERIOD_SECONDS)
_MINUTE = timedelta(minutes=1)
# Live needs two samples in a row to call a coin delisted, so it cannot see a gap shorter
# than this between two pairs of one coin. History merges such gaps the same way.
MERGE_GAP = 2 * _PERIOD


@dataclass(frozen=True)
class Event:
    known_at: datetime
    event_time: Optional[datetime]
    market: str
    symbol: str
    venue_symbol: str
    state: int
    source: str


@dataclass(frozen=True)
class PairSpan:
    """One spot pair's trading life. `last` is None while it still trades."""
    symbol: str
    first: datetime
    last: Optional[datetime]
    rank: int = 0   # tie-break when several pairs open or close in the same minute: lowest wins


@dataclass(frozen=True)
class LastState:
    state: int
    venue_symbol: str
    event_time: Optional[datetime]
    source: str = ""


@dataclass(frozen=True)
class Change:
    """A state change seen by a live sample; event_time is still to be looked up."""
    symbol: str
    venue_symbol: str
    state: int
    previous: Optional[LastState]


def from_ms(ms: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=int(ms))


def to_ms(ts: datetime) -> int:
    return int((ts - _EPOCH).total_seconds() * 1000)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def sample_at_or_before(ts: datetime, offset: int) -> datetime:
    seconds = (ts - _EPOCH).total_seconds() - offset
    return _EPOCH + timedelta(seconds=(seconds // SAMPLE_PERIOD_SECONDS) * SAMPLE_PERIOD_SECONDS + offset)


def next_sample_after(ts: datetime, offset: int) -> datetime:
    """The first sample strictly after ts."""
    return sample_at_or_before(ts, offset) + _PERIOD


def merge_spans(spans: Iterable[PairSpan]) -> list[tuple[PairSpan, Optional[PairSpan]]]:
    """Merge the pairs of one coin into periods during which at least one pair traded.

    Returns (opener, closer): the pair that opened the period and the one that closed it
    (None while the period is still open).
    """
    periods: list[tuple[PairSpan, Optional[PairSpan]]] = []
    for span in sorted(spans, key=lambda s: (s.first, s.rank, s.symbol)):
        if span.last is not None and span.last < span.first:
            continue
        if periods:
            opener, closer = periods[-1]
            if closer is None:
                continue
            if span.first <= closer.last + _MINUTE + MERGE_GAP:
                if span.last is None:
                    periods[-1] = (opener, None)
                elif (span.last, -span.rank) > (closer.last, -closer.rank):
                    periods[-1] = (opener, span)
                continue
        periods.append((span, None if span.last is None else span))
    return periods


def history_events(
    symbol: str,
    spans: Iterable[PairSpan],
    offset: int,
    before: datetime,
) -> list[Event]:
    """Events a live sampler would have written for this perpetual, up to `before` (exclusive).

    known_at is when a sampler running then would have known: a listing at the first
    sample after the first 1m bar opens, a delisting one sample later than the first
    sample that misses the pair (live waits for two misses in a row).
    """
    events: list[Event] = []
    floor: Optional[datetime] = None
    for opener, closer in merge_spans(spans):
        known = next_sample_after(opener.first, offset)
        if floor is not None and known <= floor:
            known = floor + _PERIOD
        events.append(Event(known, opener.first, MARKET_SPOT, symbol, opener.symbol,
                            STATE_TRADING, SOURCE_BACKFILL))
        floor = known
        if closer is None:
            break
        known = next_sample_after(closer.last + _MINUTE, offset) + _PERIOD
        if known <= floor:
            known = floor + _PERIOD
        events.append(Event(known, closer.last, MARKET_SPOT, symbol, closer.symbol,
                            STATE_DELISTED, SOURCE_BACKFILL))
        floor = known
    return [event for event in events if event.known_at < before]


def boundary_anchors(
    events: Iterable[Event],
    observed: Mapping[str, LastState],
    live_since: datetime,
) -> list[Event]:
    """Rows at live_since that keep the live timeline as it was when the history under it is rebuilt.

    events:   the rebuilt history, all before live_since
    observed: perpetual symbol -> the table's last row at or before live_since, read before the rebuild

    The first live sample wrote a row only where it disagreed with the table, so for every other
    perpetual "what live saw" is the row the table had at live_since. If the rebuilt history ends
    in another state, that state would show from live_since until live next writes for the
    perpetual, and backtests would disagree with what live knew. An anchor restores the old state.

    State 0 cannot be written: where the old table had no row but the rebuilt history ends in
    trading, the anchor says delisted (2); 0 against 2 (neither has spot) is left alone.
    """
    final: dict[str, Event] = {}
    for event in sorted(events, key=lambda e: e.known_at):
        final[event.symbol] = event
    anchors: list[Event] = []
    for symbol in sorted(set(final) | set(observed)):
        seen = observed.get(symbol)
        if seen is not None and seen.source == SOURCE_LIVE:
            continue    # a live row at live_since survives the rebuild and still decides
        want = seen.state if seen is not None else STATE_NEVER
        rebuilt = final.get(symbol)
        have = rebuilt.state if rebuilt is not None else STATE_NEVER
        if want == have:
            continue
        if want == STATE_NEVER:
            if have != STATE_TRADING:
                continue
            want = STATE_DELISTED
        venue = seen.venue_symbol if seen is not None else rebuilt.venue_symbol
        event_time = seen.event_time if seen is not None else None
        anchors.append(Event(live_since, event_time, MARKET_SPOT, symbol, venue, want, SOURCE_BACKFILL))
    return anchors


def plan_changes(
    perps: Mapping[str, str],
    trading: Mapping[str, list[tuple[str, str]]],
    last: Mapping[str, LastState],
    missing: Mapping[str, datetime],
    slot: datetime,
) -> tuple[list[Change], dict[str, datetime]]:
    """Compare one live sample with the table.

    perps:   perpetual symbol -> its base asset
    trading: spot base asset -> [(pair symbol, quote asset)] trading now
    last:    perpetual symbol -> last row in the table
    missing: perpetual symbol -> the sample that first found its spot gone
    Returns the changes to write and the new `missing` map (to keep only if the write succeeds).
    """
    changes: list[Change] = []
    still_missing: dict[str, datetime] = {}
    for symbol in sorted(perps):
        previous = last.get(symbol)
        state = previous.state if previous is not None else STATE_NEVER
        base = resolve_spot_base(perps[symbol], trading)
        if base is not None:
            if state != STATE_TRADING:
                changes.append(Change(symbol, pick_pair(trading[base]), STATE_TRADING, previous))
            continue
        if state != STATE_TRADING:
            continue
        first_miss = missing.get(symbol)
        if first_miss is not None and first_miss < slot:
            changes.append(Change(symbol, previous.venue_symbol, STATE_DELISTED, previous))
        else:
            still_missing[symbol] = first_miss or slot
    return changes, still_missing
