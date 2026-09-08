# coding: utf-8
"""A window big QMT has no data for must come back empty, not as an epoch row (#228).

Asked for a `1w` window the terminal has nothing for, big QMT does not answer
with zero rows -- it answers with ONE row stamped at the epoch (`stime`
`19700101`, OHLC and volume all zero). The client turned that into index
`19700101` and `time = -28800000` (epoch zero in CST) and handed it to the
caller as if it were a bar.

miniQMT returns an empty DataFrame for the same window, and this bridge
promises miniQMT's semantics. The gap is not cosmetic: a caller that walks rows
to derive period boundaries reads the epoch row as a real bar, asks the trading
calendar for `1969-12-29~1970-01-04`, gets nothing, and dies -- one downstream
production instance crashed at 09:11 pre-open this way and then idled silently
until someone stopped it. The same rows were persisted as `bar_time=1970-01-01`
dirt that had to be cleaned by hand.

Measured on the live terminal at 0.3.26 (2026-09-08, pre-open), window
`20261012~20261018`, all columns:

    time       open  high  low  close  volume
    -28800000  0.0   0.0   0.0  0.0    0

for 000001.SZ, 600519.SH and 300750.SZ alike. The explicit-field path
(FormulaServer) answered the same window with an empty frame, so only the
all-columns RPC path produces these -- which is why the reporter saw a mix of
real rows and placeholders across a basket of 13 symbols.

The floor is the market's own start rather than a magic constant: the Shanghai
exchange opened in December 1990, so no A-share bar can be stamped earlier. A
zero row carrying a *plausible* date is left alone on purpose -- a suspended
day legitimately has zero volume, and `fill_data=True` fills gaps deliberately.
"""

import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.xtquant_compat import (
    _normalize_market_data_frame,
    _normalize_market_data_result,
)


ZERO_BAR = {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0}
REAL_BAR = {"open": 9.43, "high": 9.46, "low": 9.19, "close": 9.23, "volume": 867113}


def frame(rows):
    """A frame shaped like the one the RPC path hands to normalization."""
    import pandas as pd

    columns = ["stime", "open", "high", "low", "close", "volume", "time"]
    data = dict((name, []) for name in columns)
    for stime, bar in rows:
        data["stime"].append(stime)
        data["time"].append(0)
        for name in ("open", "high", "low", "close", "volume"):
            data[name].append(bar[name])
    return pd.DataFrame(data, columns=columns)


class EpochPlaceholderRows(unittest.TestCase):
    def test_the_lone_epoch_row_normalizes_to_an_empty_frame(self):
        """The exact shape measured live: one row, epoch stime, all zeros."""
        out = _normalize_market_data_frame(frame([("19700101", ZERO_BAR)]))
        self.assertEqual(len(out), 0, "an epoch placeholder row reached the caller")

    def test_epoch_row_with_a_full_timestamp_is_dropped_too(self):
        out = _normalize_market_data_frame(frame([("19700101000000", ZERO_BAR)]))
        self.assertEqual(len(out), 0)

    def test_a_real_bar_beside_a_placeholder_survives_alone(self):
        out = _normalize_market_data_frame(
            frame([("19700101", ZERO_BAR), ("20260907", REAL_BAR)])
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(list(out.index), ["20260907"])
        self.assertEqual(out["close"].iloc[0], 9.23)

    def test_the_time_column_still_belongs_to_the_row_it_is_on(self):
        """Dropping rows must not shift the derived epoch-ms column off by one."""
        out = _normalize_market_data_frame(
            frame([("19700101", ZERO_BAR), ("20260907", REAL_BAR)]),
            field_list=["time", "close"],
        )
        self.assertEqual(len(out), 1)
        self.assertGreater(out["time"].iloc[0], 0)
        self.assertEqual(out["close"].iloc[0], 9.23)

    def test_real_bars_are_left_exactly_as_they_were(self):
        out = _normalize_market_data_frame(
            frame([("20260831", REAL_BAR), ("20260907", REAL_BAR)])
        )
        self.assertEqual(list(out.index), ["20260831", "20260907"])

    def test_a_zero_volume_bar_on_a_plausible_date_is_kept(self):
        """A suspended day is zero-volume and real; only the epoch marks 'no data'."""
        out = _normalize_market_data_frame(frame([("20260907", ZERO_BAR)]))
        self.assertEqual(len(out), 1)
        self.assertEqual(list(out.index), ["20260907"])

    def test_the_dict_path_drops_them_per_code(self):
        data = {
            "000001.SZ": frame([("19700101", ZERO_BAR)]),
            "600000.SH": frame([("20260907", REAL_BAR)]),
        }
        out = _normalize_market_data_result(data)
        self.assertEqual(len(out["000001.SZ"]), 0)
        self.assertEqual(len(out["600000.SH"]), 1)


if __name__ == "__main__":
    unittest.main()
