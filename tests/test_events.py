from datetime import datetime as dt

from binance_listing.events import (
    LastState,
    PairSpan,
    history_events,
    merge_spans,
    next_sample_after,
    plan_changes,
    sample_at_or_before,
)


def test_sampling_grid():
    assert sample_at_or_before(dt(2026, 1, 1, 10, 2, 30), 150) == dt(2026, 1, 1, 10, 2, 30)
    assert sample_at_or_before(dt(2026, 1, 1, 10, 2, 29), 150) == dt(2026, 1, 1, 9, 57, 30)
    # opens on the hour or the half hour -> known 2.5 minutes later
    assert next_sample_after(dt(2026, 1, 1, 10, 0), 150) == dt(2026, 1, 1, 10, 2, 30)
    assert next_sample_after(dt(2026, 1, 1, 10, 30), 150) == dt(2026, 1, 1, 10, 32, 30)
    assert next_sample_after(dt(2026, 1, 1, 10, 2, 30), 150) == dt(2026, 1, 1, 10, 7, 30)


def test_history_listing_then_delisting():
    spans = [PairSpan("ABCUSDT", dt(2024, 3, 1, 10, 0), dt(2025, 6, 1, 2, 59))]
    listed, delisted = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert (listed.known_at, listed.event_time, listed.state) == (
        dt(2024, 3, 1, 10, 2, 30), dt(2024, 3, 1, 10, 0), 1)
    # last bar 02:59 -> trading stops 03:00 -> first miss 03:02:30 -> confirmed 03:07:30
    assert (delisted.known_at, delisted.event_time, delisted.state) == (
        dt(2025, 6, 1, 3, 7, 30), dt(2025, 6, 1, 2, 59), 2)
    assert {listed.source, delisted.source} == {"backfill"}
    assert listed.market == "spot" and listed.venue_symbol == "ABCUSDT"


def test_history_any_pair_counts_and_names_the_right_pair():
    spans = [
        PairSpan("ABCBTC", dt(2019, 1, 1, 8, 0), dt(2022, 1, 1, 0, 0)),
        PairSpan("ABCUSDT", dt(2020, 1, 1, 8, 0), dt(2023, 1, 1, 0, 0)),
        PairSpan("ABCETH", dt(2019, 6, 1), dt(2019, 7, 1)),
    ]
    events = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert [(e.state, e.venue_symbol, e.event_time) for e in events] == [
        (1, "ABCBTC", dt(2019, 1, 1, 8, 0)),
        (2, "ABCUSDT", dt(2023, 1, 1, 0, 0)),
    ]


def test_history_relisting_and_open_period():
    spans = [
        PairSpan("ABCUSDT", dt(2020, 1, 1), dt(2021, 1, 1)),
        PairSpan("ABCUSDC", dt(2024, 5, 1, 12, 30), None),
        PairSpan("ABCTRY", dt(2025, 1, 1), dt(2025, 2, 1)),
    ]
    events = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert [(e.state, e.venue_symbol) for e in events] == [(1, "ABCUSDT"), (2, "ABCUSDT"), (1, "ABCUSDC")]
    assert events[2].known_at == dt(2024, 5, 1, 12, 32, 30)


def test_history_short_gap_is_not_a_delisting():
    spans = [
        PairSpan("ABCBTC", dt(2020, 1, 1), dt(2021, 1, 1, 0, 0)),
        PairSpan("ABCUSDT", dt(2021, 1, 1, 0, 8), None),
    ]
    assert len(merge_spans(spans)) == 1
    spans[1] = PairSpan("ABCUSDT", dt(2021, 1, 1, 0, 30), None)
    assert len(merge_spans(spans)) == 2


def test_history_stops_before_cutoff_and_known_at_strictly_increases():
    spans = [PairSpan("ABCUSDT", dt(2024, 3, 1, 10, 0), dt(2024, 3, 1, 10, 0))]
    events = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert events[0].known_at < events[1].known_at
    assert history_events("ABCUSDT", spans, 150, dt(2024, 3, 1, 10, 2, 30)) == []
    assert len(history_events("ABCUSDT", spans, 150, dt(2024, 3, 1, 10, 2, 31))) == 1


PERPS = {"ABCUSDT": "ABC", "1000XYZUSDT": "1000XYZ", "NOSPOTUSDT": "NOSPOT"}
SLOT1 = dt(2026, 1, 1, 10, 2, 30)
SLOT2 = dt(2026, 1, 1, 10, 7, 30)


def test_plan_new_listing_and_never_listed():
    trading = {"ABC": [("ABCUSDT", "USDT")], "XYZ": [("XYZBTC", "BTC"), ("XYZUSDC", "USDC")]}
    changes, missing = plan_changes(PERPS, trading, {}, {}, SLOT1)
    assert [(c.symbol, c.venue_symbol, c.state) for c in changes] == [
        ("1000XYZUSDT", "XYZUSDC", 1), ("ABCUSDT", "ABCUSDT", 1)]
    assert missing == {}


def test_plan_delisting_needs_two_samples_in_a_row():
    last = {"ABCUSDT": LastState(1, "ABCUSDT", dt(2020, 1, 1))}
    changes, missing = plan_changes(PERPS, {}, last, {}, SLOT1)
    assert changes == [] and missing == {"ABCUSDT": SLOT1}
    # same sample again (a retry) does not count as the second one
    changes, _ = plan_changes(PERPS, {}, last, missing, SLOT1)
    assert changes == []
    changes, missing2 = plan_changes(PERPS, {}, last, missing, SLOT2)
    assert [(c.symbol, c.venue_symbol, c.state) for c in changes] == [("ABCUSDT", "ABCUSDT", 2)]
    assert missing2 == {}
    # it came back in between: forget the first miss
    changes, missing3 = plan_changes(PERPS, {"ABC": [("ABCUSDT", "USDT")]}, last, missing, SLOT2)
    assert changes == [] and missing3 == {}


def test_plan_relisting_and_no_change():
    last = {"ABCUSDT": LastState(2, "ABCUSDT", dt(2021, 1, 1))}
    changes, _ = plan_changes(PERPS, {"ABC": [("ABCUSDC", "USDC")]}, last, {}, SLOT1)
    assert [(c.symbol, c.venue_symbol, c.state) for c in changes] == [("ABCUSDT", "ABCUSDC", 1)]
    assert changes[0].previous.event_time == dt(2021, 1, 1)
    assert plan_changes(PERPS, {}, last, {}, SLOT1) == ([], {})


def test_history_same_minute_ties_prefer_the_ranked_pair():
    spans = [
        PairSpan("ABCTRY", dt(2024, 1, 1, 10), dt(2025, 1, 1, 2, 59), 4),
        PairSpan("ABCUSDT", dt(2024, 1, 1, 10), dt(2025, 1, 1, 2, 59), 0),
        PairSpan("ABCEUR", dt(2024, 1, 1, 10), dt(2024, 6, 1), 4),
    ]
    events = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert [(e.state, e.venue_symbol) for e in events] == [(1, "ABCUSDT"), (2, "ABCUSDT")]
