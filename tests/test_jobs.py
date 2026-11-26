from datetime import datetime as dt, timedelta

from binance_listing.data_jobs import ListingJobs
from binance_listing.events import Event, to_ms
from binance_listing.spot import SpotApiError

S1 = dt(2026, 1, 1, 10, 2, 30)
S2 = S1 + timedelta(minutes=5)
S3 = S2 + timedelta(minutes=5)


def row(symbol, base, quote, status="TRADING"):
    return {"symbol": symbol, "base": base, "quote": quote, "status": status}


class FakeApi:
    def __init__(self, rows):
        self.rows = rows
        self.first = {}
        self.last = {}
        self.calls = []
        self.fail = False
        self.breaks = []

    def symbols(self, trading_only, timeout=None):
        if self.fail:
            raise SpotApiError("boom")
        return list(self.rows)

    def break_symbols(self, timeout=None):
        self.calls.append(("break",))
        return list(self.breaks)

    def first_bar_ms(self, symbol, start_ms=0):
        self.calls.append(("first", symbol, start_ms))
        return self.first.get(symbol)

    def last_bar_ms(self, symbol):
        self.calls.append(("last", symbol))
        return self.last.get(symbol)


def make(db, api, now):
    jobs = ListingJobs(db, api=api, start=False)
    jobs.offset, jobs.quotes = 150, frozenset()
    clock = {"now": now}
    jobs._now = lambda: clock["now"]
    jobs._stop.wait = lambda seconds: True   # a failed fetch gives up instead of sleeping
    return jobs, clock


def test_listing_is_written_with_first_bar_and_watermark_moves(db):
    db.set_perps({"ABCUSDT": "ABC", "NOSPOTUSDT": "NOSPOT"})
    api = FakeApi([row("ABCUSDT", "ABC", "USDT"), row("ZZZUSDT", "ZZZ", "USDT")])
    api.first["ABCUSDT"] = to_ms(dt(2026, 1, 1, 10, 0))
    jobs, clock = make(db, api, S1)

    assert jobs.sample(S1) is True
    assert db.rows() == [{
        "known_at": S1, "event_time": dt(2026, 1, 1, 10, 0), "market": "spot",
        "symbol": "ABCUSDT", "venue_symbol": "ABCUSDT", "state": 1, "source": "live",
    }]
    assert db.duck_time() == S1 and db.read_live_since() == S1

    # nothing changed: no row, watermark still moves, live_since stays
    clock["now"] = S2
    assert jobs.sample(S2) is True
    assert len(db.rows()) == 1 and db.duck_time() == S2 and db.read_live_since() == S1
    # the same sample written twice stays one row
    db.write_sample([Event(S1, None, "spot", "ABCUSDT", "ABCUSDT", 1, "live")], S1)
    assert len(db.rows()) == 1 and db.duck_time() == S2


def test_delisting_after_two_misses_then_relisting(db):
    db.set_perps({"ABCUSDT": "ABC"})
    db.replace_backfill([Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 1, "backfill")], None)
    api = FakeApi([])
    api.last["ABCUSDT"] = to_ms(dt(2026, 1, 1, 9, 59))
    jobs, clock = make(db, api, S1)

    assert jobs.sample(S1) is True and len(db.rows()) == 1
    clock["now"] = S2
    assert jobs.sample(S2) is True
    gone = db.rows()[-1]
    assert (gone["known_at"], gone["event_time"], gone["state"], gone["source"]) == (S2, dt(2026, 1, 1, 9, 59), 2, "live")

    api.rows = [row("ABCUSDC", "ABC", "USDC")]
    api.first["ABCUSDC"] = to_ms(dt(2026, 1, 1, 10, 10))
    clock["now"] = S3
    assert jobs.sample(S3) is True
    back = db.rows()[-1]
    assert (back["venue_symbol"], back["state"], back["event_time"]) == ("ABCUSDC", 1, dt(2026, 1, 1, 10, 10))
    # looked for the first bar after the delisting row was written, not the pair's first ever
    assert api.calls[-1] == ("first", "ABCUSDC", to_ms(S2))


def test_bad_samples_write_nothing_and_keep_the_watermark(db):
    perps = {f"C{i}USDT": f"C{i}" for i in range(30)}
    db.set_perps(perps)
    db.replace_backfill(
        [Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", s, s, 1, "backfill") for s in perps], None)
    full = [row(s, b, "USDT") for s, b in perps.items()]
    api = FakeApi(full)
    jobs, clock = make(db, api, S1)
    assert jobs.sample(S1) is True

    clock["now"] = S2
    api.fail = True                     # fetch failed
    assert jobs.sample(S2) is False
    api.fail, api.rows = False, full[:10]   # the list lost most coins
    assert jobs.sample(S2) is False
    api.rows = full
    clock["now"] = S2 + timedelta(seconds=120)   # too late to claim "known at S2"
    assert jobs.sample(S2) is False
    assert db.duck_time() == S1 and len(db.rows()) == 30 and jobs._missing == {}

    db.set_perps({})                    # K-line ingestor's exginfo is empty
    clock["now"] = S3
    assert jobs.sample(S3) is False


def test_event_time_is_null_when_the_bar_lookup_fails(db):
    db.set_perps({"ABCUSDT": "ABC"})
    api = FakeApi([row("ABCUSDT", "ABC", "USDT")])

    def boom(symbol, start_ms=0):
        raise SpotApiError("boom")

    api.first_bar_ms = boom
    jobs, _ = make(db, api, S1)
    assert jobs.sample(S1) is True
    assert db.rows()[0]["event_time"] is None and db.rows()[0]["state"] == 1


def test_backfill_can_be_rebuilt_without_touching_live_rows(db):
    old = [Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 1, "backfill"),
           Event(dt(2021, 1, 1, 0, 7, 30), dt(2021, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 2, "backfill")]
    db.replace_backfill(old, dt(2025, 1, 1, 0, 2, 30))
    db.write_sample([Event(S1, None, "spot", "XYZUSDT", "XYZUSDT", 1, "live")], S1)
    new = [Event(dt(2019, 5, 1, 0, 2, 30), dt(2019, 5, 1), "spot", "ABCUSDT", "ABCBTC", 1, "backfill")] * 2
    db.replace_backfill(new, None)
    assert [(r["symbol"], r["venue_symbol"], r["source"]) for r in db.rows()] == [
        ("ABCUSDT", "ABCBTC", "backfill"), ("XYZUSDT", "XYZUSDT", "live")]
    assert db.duck_time() == S1
    assert db.read_last_states()["ABCUSDT"].state == 1


def test_rebuild_after_live_keeps_the_live_timeline(db):
    """Live saw ABC trading at its first sample and wrote nothing; a rebuild that ends ABC delisted
    must not make the table say delisted from live_since on."""
    from binance_listing.events import boundary_anchors

    db.set_perps({"ABCUSDT": "ABC"})
    db.replace_backfill([Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 1, "backfill")], None)
    jobs, _ = make(db, FakeApi([row("ABCUSDT", "ABC", "USDT")]), S1)
    assert jobs.sample(S1) is True and len(db.rows()) == 1   # agreed with the table: no live row

    live_since = db.read_live_since()
    observed = db.read_last_states(live_since)
    assert observed["ABCUSDT"].source == "backfill"
    rebuilt = [Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 1, "backfill"),
               Event(dt(2021, 1, 1, 0, 7, 30), dt(2021, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 2, "backfill")]
    anchors = boundary_anchors(rebuilt, observed, live_since)
    db.replace_backfill(rebuilt + anchors, None)

    assert db.read_last_states()["ABCUSDT"].state == 1
    assert db.read_last_states(dt(2025, 1, 1))["ABCUSDT"].state == 2
    # a second rebuild from the same history adds the same anchor again, nothing more
    observed = db.read_last_states(live_since)
    db.replace_backfill(rebuilt + boundary_anchors(rebuilt, observed, live_since), None)
    assert [r["state"] for r in db.rows()] == [1, 2, 1]


def test_delisting_takes_the_pair_that_stopped_last(db):
    """USDT was noted at listing but stopped a year earlier; BTC kept trading until today."""
    db.set_perps({"1000ABCUSDT": "1000ABC"})
    db.replace_backfill([Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "1000ABCUSDT", "ABCUSDT", 1, "backfill")], None)
    api = FakeApi([row("ZZZUSDT", "ZZZ", "USDT")])
    api.breaks = [row("ABCUSDT", "ABC", "USDT", "BREAK"), row("ABCBTC", "ABC", "BTC", "BREAK"),
                  row("ZZZBTC", "ZZZ", "BTC", "BREAK")]
    api.last = {"ABCUSDT": to_ms(dt(2025, 1, 1)), "ABCBTC": to_ms(dt(2026, 1, 1, 9, 59))}
    jobs, clock = make(db, api, S1)
    assert jobs.sample(S1) is True
    assert ("break",) not in api.calls            # nothing delisted yet: no BREAK request
    clock["now"] = S2
    assert jobs.sample(S2) is True
    gone = db.rows()[-1]
    assert (gone["state"], gone["venue_symbol"], gone["event_time"]) == (2, "ABCBTC", dt(2026, 1, 1, 9, 59))
    assert sorted(c for c in api.calls if c[0] == "last") == [("last", "ABCBTC"), ("last", "ABCUSDT")]


def test_delisting_falls_back_to_the_noted_pair(db):
    db.set_perps({"ABCUSDT": "ABC"})
    db.replace_backfill([Event(dt(2020, 1, 1, 0, 2, 30), dt(2020, 1, 1), "spot", "ABCUSDT", "ABCUSDT", 1, "backfill")], None)
    api = FakeApi([])
    api.last["ABCUSDT"] = to_ms(dt(2026, 1, 1, 9, 59))

    def boom(timeout=None):
        raise SpotApiError("boom")

    api.break_symbols = boom
    jobs, clock = make(db, api, S1)
    jobs.sample(S1)
    clock["now"] = S2
    assert jobs.sample(S2) is True
    gone = db.rows()[-1]
    assert (gone["state"], gone["venue_symbol"], gone["event_time"]) == (2, "ABCUSDT", dt(2026, 1, 1, 9, 59))
