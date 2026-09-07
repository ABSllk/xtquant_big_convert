# coding: utf-8
"""get_divid_factors must answer a RANGE, not silently one day (#165).

@yucejade's report: the adapter collapsed any range to ``end_time or
start_time`` and asked for that single day. A given day is almost never an
ex-dividend day, so every range request returned ``{}`` -- and an empty dict
reads exactly like "no events in this range". Their nightly whole-market factor
sync ran that way for a month: every symbol empty, the factor table silently
stopped updating, nothing errored, K-lines kept arriving normally.

The old comment claimed "the xtdata SDK has the same 2-arg shape". It does not
-- the terminal's bundled SDK is
``get_divid_factors(stock_code, start_time, end_time)``.

Measured on the live terminal after the fix:

    000001.SZ 20240101-20260904 -> 5 events   (was {})
    600000.SH 20240101-20260904 -> 2 events   (was {})
    single day 20260612          -> 0.36, matching the reporter's own reading

and the ranges include the 2025-10-15 interim dividend (0.236) they had
confirmed independently.

Which path served it, also measured: neither the native SDK nor ContextInfo
accepts a range here, so the expansion below is what runs. 300750.SZ, which
this terminal has only one daily bar for, returned 0 events where 000001.SZ
returned 5 -- that is the candidate scan finding nothing to scan, and it is
why "no daily bars" now raises instead of answering {}.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider


DIVIDEND = [0.36, 0.0, 0.0, 0.0, 0.0, 0, 1.032906]


class Context(object):
    """ContextInfo as big QMT has it: get_divid_factors(code, ONE date)."""

    def __init__(self, events=None, accepts_range=False):
        self.events = dict(events or {})       # "YYYYMMDD" -> {ts: factors}
        self.accepts_range = accepts_range
        self.calls = []

    def get_divid_factors(self, code, *args):
        self.calls.append((code,) + tuple(args))
        if len(args) >= 2 and not self.accepts_range:
            raise TypeError("get_divid_factors takes at most 2 arguments")
        if len(args) >= 2:
            out = {}
            for day, payload in self.events.items():
                if args[0] <= day <= args[1]:
                    out.update(payload)
            return out
        if not args:
            return {}
        return dict(self.events.get(args[0], {}))


def _provider(context, bars=None, native=None):
    provider = BigQmtMarketDataProvider(context_info=context)
    provider._native = lambda: native
    frame = list(bars) if bars is not None else []
    provider.get_market_data_ex = (
        lambda **kw: {kw["stock_list"][0]: frame} if frame else {kw["stock_list"][0]: []})
    return provider


def _bars(rows):
    """rows: (day, close, preClose)."""
    return [{"stime": day, "close": close, "preClose": pre} for day, close, pre in rows]


# One ex-dividend day: 0612's preClose (10.0) differs from 0611's close (10.4).
BARS = _bars([
    ("20260610", 10.2, 10.1),
    ("20260611", 10.4, 10.2),
    ("20260612", 10.1, 10.0),      # ex-dividend
    ("20260613", 10.3, 10.1),
])


class RangeIsAnsweredTest(unittest.TestCase):
    def test_a_range_finds_the_event_the_old_code_missed(self):
        """The report in one assertion: end_time is not an ex-dividend day."""
        context = Context({"20260612": {"1781193600000": DIVIDEND}})
        provider = _provider(context, BARS)

        answer = provider.get_divid_factors("000001.SZ", "20260101", "20260904")

        self.assertEqual(answer, {"1781193600000": DIVIDEND})

    def test_it_probes_only_the_candidate_days(self):
        """Walking every trading day would be hundreds of RPCs per symbol."""
        context = Context({"20260612": {"1781193600000": DIVIDEND}})
        provider = _provider(context, BARS)

        provider.get_divid_factors("000001.SZ", "20260101", "20260904")

        probed = [c[1] for c in context.calls if len(c) == 2]
        self.assertIn("20260612", probed)
        self.assertLess(len(probed), len(BARS),
                        "probed every bar instead of the candidates")

    def test_several_events_are_merged(self):
        bars = _bars([
            ("20260610", 10.2, 10.1),
            ("20260611", 10.4, 10.2),
            ("20260612", 10.1, 10.0),      # ex-div
            ("20260613", 10.3, 10.1),
            ("20261015", 11.0, 10.6),      # ex-div
        ])
        context = Context({
            "20260612": {"1781193600000": DIVIDEND},
            "20261015": {"1760457600000": [0.236, 0, 0, 0, 0, 0, 1.021182]},
        })
        provider = _provider(context, bars)

        answer = provider.get_divid_factors("000001.SZ", "20260101", "20261231")

        self.assertEqual(len(answer), 2)

    def test_a_range_with_no_ex_dividend_day_is_legitimately_empty(self):
        """Bars exist and none of them is an ex-dividend day -- {} is a real
        answer here, and must not raise."""
        bars = _bars([("20260610", 10.2, 10.1), ("20260611", 10.4, 10.2)])
        bars[0]["preClose"] = 10.2          # no jump anywhere
        bars[1]["preClose"] = 10.2
        provider = _provider(Context(), bars)

        self.assertEqual(
            provider.get_divid_factors("000001.SZ", "20260101", "20260904"), {})


class SingleDateStillWorksTest(unittest.TestCase):
    def test_one_date_goes_straight_through(self):
        context = Context({"20260612": {"1781193600000": DIVIDEND}})
        provider = _provider(context, BARS)

        answer = provider.get_divid_factors("000001.SZ", "20260612", "20260612")

        self.assertEqual(answer, {"1781193600000": DIVIDEND})
        self.assertEqual(context.calls, [("000001.SZ", "20260612")])

    def test_only_start_time_is_treated_as_that_day(self):
        context = Context({"20260612": {"1781193600000": DIVIDEND}})
        provider = _provider(context, BARS)

        provider.get_divid_factors("000001.SZ", "20260612", "")

        self.assertEqual(context.calls, [("000001.SZ", "20260612")])

    def test_no_dates_at_all_asks_for_everything(self):
        context = Context()
        provider = _provider(context, BARS)

        provider.get_divid_factors("000001.SZ")

        self.assertEqual(context.calls, [("000001.SZ",)])


class PrefersARealRangeWhenOneExistsTest(unittest.TestCase):
    def test_a_context_that_takes_a_range_is_used_directly(self):
        """Another build may accept it; then no scanning is needed at all."""
        context = Context({"20260612": {"1781193600000": DIVIDEND}},
                          accepts_range=True)
        provider = _provider(context, BARS)

        answer = provider.get_divid_factors("000001.SZ", "20260101", "20260904")

        self.assertEqual(answer, {"1781193600000": DIVIDEND})
        self.assertEqual(len(context.calls), 1, "should not have expanded")

    def test_the_native_sdk_range_form_wins_when_reachable(self):
        class Native(object):
            def __init__(self):
                self.calls = []

            def get_divid_factors(self, code, start, end):
                self.calls.append((code, start, end))
                return {"1781193600000": DIVIDEND}

        native = Native()
        context = Context()
        provider = _provider(context, BARS, native=native)

        answer = provider.get_divid_factors("000001.SZ", "20240101", "20260904")

        self.assertEqual(answer, {"1781193600000": DIVIDEND})
        self.assertEqual(native.calls, [("000001.SZ", "20240101", "20260904")])
        self.assertEqual(context.calls, [], "context should not be touched")


class MissingDailyBarsSelfHealTest(unittest.TestCase):
    """#222: with <2 daily bars the bridge downloads the window itself, then
    rescans; only a STILL-short scan raises. The reporter's loop had daily
    bars in their own DuckDB but none on the terminal, so "the library already
    has them" never helped -- the scan reads the terminal's local bars.
    """

    def _provider_with_download(self, bars_before, bars_after):
        provider = _provider(Context({"20260612": {"x": DIVIDEND}}), bars=bars_before)
        calls = []

        def fake_download(stock_code, period, start_time, end_time):
            calls.append((stock_code, period, start_time, end_time))
            # The terminal now has the daily bars.
            provider.get_market_data_ex = (
                lambda **kw: {kw["stock_list"][0]: bars_after})
            return True

        provider.qmt_api = {"download_history_data": fake_download}
        return provider, calls

    def test_it_downloads_then_rescans_instead_of_raising(self):
        provider, calls = self._provider_with_download([], BARS)

        answer = provider.get_divid_factors("600036.SH", "20260401", "20260904")

        self.assertEqual(calls, [("600036.SH", "1d", "20260401", "20260904")])
        self.assertIn("x", answer)   # the event the empty scan would have lost

    def test_still_short_after_the_download_raises_with_the_honest_note(self):
        provider, calls = self._provider_with_download([], [])

        with self.assertRaises(RuntimeError) as caught:
            provider.get_divid_factors("600036.SH", "20260401", "20260904")

        self.assertEqual(len(calls), 1)
        message = " ".join(str(caught.exception).split())
        self.assertIn("The bridge downloaded the daily window itself", message)
        self.assertIn("#165", message)

    def test_a_failing_download_keeps_the_original_error(self):
        provider = _provider(Context(), bars=[])
        provider.qmt_api = {
            "download_history_data": lambda *a: (_ for _ in ()).throw(OSError("down"))}

        with self.assertRaises(RuntimeError) as caught:
            provider.get_divid_factors("600036.SH", "20260401", "20260904")

        self.assertIn("Download the daily history first", str(caught.exception))

    def test_no_download_channel_raises_the_original_error(self):
        provider = _provider(Context(), bars=[])
        provider.qmt_api = {}

        with self.assertRaises(RuntimeError) as caught:
            provider.get_divid_factors("600036.SH", "20260401", "20260904")

        self.assertIn("Download the daily history first", str(caught.exception))


class NoDailyBarsMustNotLookEmptyTest(unittest.TestCase):
    """The failure this issue is about, reintroduced in miniature if we let it.

    With no daily bars the candidate scan finds nothing, so {} would again mean
    both "no dividends" and "could not tell".
    """

    def test_a_single_bar_is_not_a_scan_either(self):
        """Measured live: 300750.SZ has exactly one daily bar here, and the
        first version of this guard only checked for zero -- so it still
        answered {} for the very code that exposed the gap."""
        one = _bars([("20260610", 10.2, 10.1)])
        provider = _provider(Context(), one)

        with self.assertRaises(RuntimeError):
            provider.get_divid_factors("300750.SZ", "20240101", "20260904")

    def test_it_raises_instead_of_answering_empty(self):
        provider = _provider(Context({"20260612": {"x": DIVIDEND}}), bars=[])

        with self.assertRaises(RuntimeError) as caught:
            provider.get_divid_factors("300750.SZ", "20240101", "20260904")

        message = " ".join(str(caught.exception).split())
        self.assertIn("too few to compare", message)
        self.assertIn("#165", message)

    def test_the_error_says_how_to_fix_it(self):
        provider = _provider(Context(), bars=[])

        with self.assertRaises(RuntimeError) as caught:
            provider.get_divid_factors("300750.SZ", "20240101", "20260904")

        self.assertIn("download_history_data", str(caught.exception))

    def test_a_single_date_still_answers_without_bars(self):
        """Only the range path needs bars; one date goes straight to QMT."""
        context = Context({"20260612": {"1781193600000": DIVIDEND}})
        provider = _provider(context, bars=[])

        self.assertEqual(
            provider.get_divid_factors("300750.SZ", "20260612", "20260612"),
            {"1781193600000": DIVIDEND})


class ProbeBudgetTest(unittest.TestCase):
    def test_it_stops_after_the_cap(self):
        """A filter that fails to narrow must not fire hundreds of RPCs."""
        rows = []
        for i in range(200):
            day = "2026%04d" % (1000 + i)
            rows.append((day, 10.0, 9.0))      # every bar looks like a jump
        provider = _provider(Context(), _bars(rows))

        provider.get_divid_factors("000001.SZ", "20260101", "20261231")

        # Single-date probes only: the one range attempt that TypeErrors on the
        # way in is not a probe and is not what the cap governs.
        probes = [c for c in provider.context_info.calls if len(c) == 2]
        self.assertEqual(len(probes), provider._DIVID_MAX_PROBES)


if __name__ == "__main__":
    unittest.main()
