from datetime import datetime as dt

from binance_listing.events import (
    Event,
    LastState,
    PairSpan,
    boundary_anchors,
    delisting_known_at,
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
    # last bar 00:00 -> stops 00:01 -> first miss 00:02:30 -> delisting confirmed 00:07:30
    assert delisting_known_at(dt(2021, 1, 1, 0, 0), 150) == dt(2021, 1, 1, 0, 7, 30)
    spans = [
        PairSpan("ABCBTC", dt(2020, 1, 1), dt(2021, 1, 1, 0, 0)),
        PairSpan("ABCUSDT", dt(2021, 1, 1, 0, 7), None),
    ]
    # trading again by the confirming sample: live writes no delisting
    assert len(merge_spans(spans, 150)) == 1
    # opens after it: live has already written the delisting, then a listing
    spans[1] = PairSpan("ABCUSDT", dt(2021, 1, 1, 0, 8), None)
    assert len(merge_spans(spans, 150)) == 2
    events = history_events("ABCUSDT", spans, 150, dt(2026, 1, 1))
    assert [(e.state, e.known_at) for e in events] == [
        (1, dt(2020, 1, 1, 0, 2, 30)), (2, dt(2021, 1, 1, 0, 7, 30)), (1, dt(2021, 1, 1, 0, 12, 30))]
    # the window follows the sampling grid, not a fixed number of minutes
    spans = [PairSpan("ABCBTC", dt(2020, 1, 1), dt(2021, 1, 1, 0, 4)),
             PairSpan("ABCUSDT", dt(2021, 1, 1, 0, 12), None)]
    assert len(merge_spans(spans, 150)) == 1      # confirmed 00:12:30
    assert len(merge_spans(spans, 30)) == 2       # confirmed 00:10:30


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


LIVE_SINCE = dt(2026, 1, 1, 10, 2, 30)


def backfill(symbol, known_at, state, venue=None):
    return Event(known_at, known_at, "spot", symbol, venue or symbol, state, "backfill")


def test_boundary_anchors_keep_what_live_saw():
    rebuilt = [
        backfill("AAAUSDT", dt(2020, 1, 1, 0, 2, 30), 1), backfill("AAAUSDT", dt(2021, 1, 1, 0, 7, 30), 2),
        backfill("BBBUSDT", dt(2020, 1, 1, 0, 2, 30), 1),
        backfill("CCCUSDT", dt(2020, 1, 1, 0, 2, 30), 1),
        backfill("DDDUSDT", dt(2020, 1, 1, 0, 2, 30), 1), backfill("DDDUSDT", dt(2021, 1, 1, 0, 7, 30), 2),
        backfill("EEEUSDT", dt(2020, 1, 1, 0, 2, 30), 1),
    ]
    observed = {
        # old history said trading, the rebuilt one ends delisted: keep trading
        "AAAUSDT": LastState(1, "AAABTC", dt(2019, 1, 1), "backfill"),
        # same state: nothing to add
        "BBBUSDT": LastState(1, "BBBUSDT", dt(2020, 1, 1), "backfill"),
        # live wrote its own row at live_since: that row survives and decides
        "CCCUSDT": LastState(2, "CCCUSDT", None, "live"),
        # rebuilt history lost the coin, live saw it delisted: keep delisted
        "FFFUSDT": LastState(2, "FFFUSDT", dt(2022, 1, 1), "backfill"),
    }
    # DDD: no old row, rebuilt ends delisted -> 0 vs 2, left alone
    # EEE: no old row, rebuilt ends trading -> live saw no spot, anchor delisted
    anchors = boundary_anchors(rebuilt, observed, LIVE_SINCE)
    assert [(a.symbol, a.state, a.venue_symbol, a.event_time, a.known_at, a.source) for a in anchors] == [
        ("AAAUSDT", 1, "AAABTC", dt(2019, 1, 1), LIVE_SINCE, "backfill"),
        ("EEEUSDT", 2, "EEEUSDT", None, LIVE_SINCE, "backfill"),
        ("FFFUSDT", 2, "FFFUSDT", dt(2022, 1, 1), LIVE_SINCE, "backfill"),
    ]
    assert boundary_anchors(rebuilt, {}, LIVE_SINCE)[0].symbol == "BBBUSDT"
