import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider


class RawMarketContext:
    def __init__(self, payload=None):
        self.payload = payload or {}
        self.calls = []

    def get_market_data_ex_ori(
        self,
        fields=None,
        stock_code=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
    ):
        self.calls.append(
            {
                "fields": fields,
                "stock_code": stock_code,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
                "dividend_type": dividend_type,
            }
        )
        return self.payload

    def get_market_data_ex(self, *args, **kwargs):
        raise AssertionError("DataFrame-producing QMT API must not be called")


class BigQmtRawMarketBridgeTest(unittest.TestCase):
    def test_market_data_ex_uses_raw_context_api(self):
        rows = [[1784014200000, 55.1], [1784014260000, 55.2]]
        context = RawMarketContext({"600276.SH": rows})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=["close"], stock_list=["600276.SH"], period="1m", count=2
        )

        self.assertEqual("DataFrame", data["600276.SH"]["__bigqmt_type__"])
        self.assertEqual(["stime", "close"], data["600276.SH"]["columns"])
        self.assertEqual(rows, data["600276.SH"]["records"])
        self.assertEqual(["close"], context.calls[0]["fields"])
        self.assertEqual(["600276.SH"], context.calls[0]["stock_code"])

    def test_market_data_ex_returns_empty_frame_for_requested_symbol(self):
        context = RawMarketContext({})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=["close"], stock_list=["600276.SH"], period="1m", count=2
        )

        self.assertEqual([], data["600276.SH"]["records"])
        self.assertEqual(["stime", "close"], data["600276.SH"]["columns"])


class _PlainFrame(object):
    """Duck-typed stand-in for the plain path's per-code DataFrame."""

    def __init__(self, rows):
        self.index = list(rows)


class PlainMarketContext(object):
    """No get_market_data_ex_ori -- the plain shape path, per-period answers."""

    def __init__(self, empty_periods=()):
        self.empty_periods = set(empty_periods)
        self.calls = []

    def get_market_data_ex(self, fields=None, stock_code=None, period="1d", **kwargs):
        self.calls.append({"fields": fields, "period": period})
        code = list(stock_code or ["000001.SZ"])[0]
        if period in self.empty_periods:
            return {code: _PlainFrame([])}
        return {code: _PlainFrame([[1, 2]])}


class SynthPeriodAllFieldsRetryTest(unittest.TestCase):
    """#219: a terminal answered 0 rows for 1mon+ when field_list=[] while the
    same bars read fine with an explicit field list. Retry once with the
    explicit K-line fields; an empty retry still means empty."""

    def test_empty_all_fields_synth_period_retries_with_explicit_fields(self):
        context = RawMarketContext({"000001.SZ": []})
        provider = BigQmtMarketDataProvider(context)
        # First call (fields=[]) empty, second (explicit) has rows: sequence.
        answers = [{"000001.SZ": []}, {"000001.SZ": [[1784014200000, 55.1]]}]
        context.payload = None
        original = context.get_market_data_ex_ori

        def sequenced(*args, **kwargs):
            context.calls.append(kwargs)
            return answers.pop(0) if answers else {"000001.SZ": []}

        context.get_market_data_ex_ori = sequenced

        data = provider.get_market_data_ex(
            field_list=[], stock_list=["000001.SZ"], period="1mon", count=10)

        self.assertEqual(len(context.calls), 2)
        self.assertEqual(context.calls[1]["fields"],
                         list(BigQmtMarketDataProvider._KLINE_ALL_FIELDS))
        self.assertEqual(data["000001.SZ"]["records"], [[1784014200000, 55.1]])
        self.assertNotEqual(data["000001.SZ"]["columns"], [])
        context.get_market_data_ex_ori = original

    def test_empty_retry_keeps_the_original_empty_answer(self):
        context = RawMarketContext({"000001.SZ": []})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=[], stock_list=["000001.SZ"], period="1q", count=10)

        self.assertEqual(data["000001.SZ"]["records"], [])
        self.assertEqual(len(context.calls), 2, "exactly one retry")

    def test_explicit_fields_never_retry(self):
        context = RawMarketContext({"000001.SZ": []})
        provider = BigQmtMarketDataProvider(context)

        provider.get_market_data_ex(
            field_list=["close"], stock_list=["000001.SZ"], period="1mon", count=10)

        self.assertEqual(len(context.calls), 1)

    def test_daily_period_never_retries(self):
        context = RawMarketContext({"000001.SZ": []})
        provider = BigQmtMarketDataProvider(context)

        provider.get_market_data_ex(
            field_list=[], stock_list=["000001.SZ"], period="1d", count=10)

        self.assertEqual(len(context.calls), 1)

    def test_a_nonempty_first_answer_never_retries(self):
        rows = [[1784014200000, 55.1]]
        context = RawMarketContext({"000001.SZ": rows})
        provider = BigQmtMarketDataProvider(context)

        data = provider.get_market_data_ex(
            field_list=[], stock_list=["000001.SZ"], period="1mon", count=10)

        self.assertEqual(len(context.calls), 1)
        self.assertEqual(data["000001.SZ"]["records"], rows)

    def test_plain_shape_path_retries_too(self):
        context = PlainMarketContext(empty_periods=("1mon",))
        provider = BigQmtMarketDataProvider(context)

        # fields=[] empties only when the period is empty AND fields are empty
        # on this fake; emulate "all-fields broken, explicit fine" by keying
        # on the fields actually passed.
        def by_fields(fields=None, stock_code=None, period="1d", **kwargs):
            context.calls.append({"fields": fields, "period": period})
            code = list(stock_code or ["000001.SZ"])[0]
            if period == "1mon" and not fields:
                return {code: _PlainFrame([])}
            return {code: _PlainFrame([[1, 2]])}

        context.get_market_data_ex = by_fields
        data = provider.get_market_data_ex(
            field_list=[], stock_list=["000001.SZ"], period="1mon", count=10)

        self.assertEqual(len(context.calls), 2)
        self.assertEqual(context.calls[1]["fields"],
                         list(BigQmtMarketDataProvider._KLINE_ALL_FIELDS))

    def test_trading_date_context_fallback_maps_market_to_representative_stock(self):
        """ContextInfo 路径必须把市场代码转换为其要求的证券代码。"""
        class CalendarContext:
            def __init__(self):
                self.calls = []

            def get_trading_dates(self, stock_code, start_time, end_time, count):
                self.calls.append((stock_code, start_time, end_time, count))
                return ["20260826"]

        class UnavailableNativeXtData:
            def get_trading_dates(self, *_args):
                raise RuntimeError("native quote service unavailable")

        context = CalendarContext()
        provider = BigQmtMarketDataProvider(context, native_xtdata=UnavailableNativeXtData())

        for market, expected_stock in (("SH", "000001.SH"), ("sz", "399001.SZ"), ("000300.SH", "000300.SH")):
            with self.subTest(market=market):
                self.assertEqual(["20260826"], provider.get_trading_dates(market, "20260801", "20260826", 3))
                self.assertEqual((expected_stock, "20260801", "20260826", 3), context.calls[-1])


if __name__ == "__main__":
    unittest.main()
