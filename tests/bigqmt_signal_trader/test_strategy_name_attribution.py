# coding: utf-8
"""QMT does not put the strategy name on query rows at all (#133).

The obvious reading of "strategy_name 恒为空" is that the bridge forgot to
forward a field. It is not. describe_trade_detail_fields against the live
terminal listed every attribute on a real row:

    ORDER  120 attributes -- m_strStrategyName is NOT among them
    DEAL    47 attributes -- m_strStrategyName is NOT among them

get_trade_detail_data takes a strategy_name and filters by it, but it never
reports it back. The order path was reading m_strStrategyName and had been
getting "" from a name that does not exist; the deal path was echoing the query
filter, which defaults to "" (everything). Two different bugs, one symptom.

Neither can be fixed by reading harder. What the bridge does have is its own
record: every order it submits is remembered at submit time under the
user_order_id that rides out as the order remark. So orders this bridge placed
can be named after the fact, and orders placed by hand in the terminal cannot
-- they carry no remark, and there is nothing anywhere to recover.

The same live run also settled two smaller questions:

  * m_strInstrumentName IS on both row types -- instrument_name works.
  * DEAL rows carry BOTH m_dComssion (QMT's own misspelling) and m_dCommission,
    so "first attribute that exists" can stop on a 0.0 while the other holds
    the real number. Hence _first_nonzero.
  * No shareholder id on either row type, so secu_account stays "".
"""

import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.order_bigqmt import _first_nonzero
from bigqmt_signal_trader.exec_events import order_identity_key, order_identity_map
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


ACCOUNT = "8886800503"


class Row(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeRedis(object):
    def __init__(self, values=None, fail=False):
        self.values = dict(values or {})
        self.fail = fail
        self.mgets = []
        self.gets = []

    def mget(self, keys):
        if self.fail:
            raise RuntimeError("no")
        self.mgets.append(list(keys))
        return [self.values.get(key) for key in keys]

    def get(self, key):
        self.gets.append(key)
        return self.values.get(key)


def _identity(strategy_name, user_order_id):
    return json.dumps({"account_id": ACCOUNT, "user_order_id": user_order_id,
                       "strategy_name": strategy_name, "stock_code": "600000.SH"})


class CommissionTest(unittest.TestCase):
    """A live DEAL row has both spellings; one of them can be the zero."""

    def test_the_first_nonzero_wins(self):
        row = Row(m_dComssion=0.0, m_dCommission=1.36)

        self.assertEqual(_first_nonzero(row, ("m_dComssion", "m_dCommission")), 1.36)

    def test_the_first_candidate_wins_when_it_has_a_value(self):
        row = Row(m_dComssion=0.41, m_dCommission=9.99)

        self.assertEqual(_first_nonzero(row, ("m_dComssion", "m_dCommission")), 0.41)

    def test_all_zero_is_zero_not_an_error(self):
        row = Row(m_dComssion=0.0, m_dCommission=0.0)

        self.assertEqual(_first_nonzero(row, ("m_dComssion", "m_dCommission")), 0.0)

    def test_a_missing_attribute_is_skipped(self):
        """ORDER rows have neither spelling."""
        row = Row()

        self.assertEqual(_first_nonzero(row, ("m_dComssion", "m_dCommission")), 0.0)

    def test_junk_is_skipped_rather_than_raising(self):
        row = Row(m_dComssion="", m_dCommission=2.5)

        self.assertEqual(_first_nonzero(row, ("m_dComssion", "m_dCommission")), 2.5)


class IdentityMapTest(unittest.TestCase):
    def setUp(self):
        self.key = order_identity_key(ACCOUNT, "bq:abc:sig-1")
        self.redis = FakeRedis({self.key: _identity("alpha", "bq:abc:sig-1")})

    def test_it_finds_a_remembered_order(self):
        found = order_identity_map(self.redis, ACCOUNT, ["bq:abc:sig-1"])

        self.assertEqual(found["bq:abc:sig-1"]["strategy_name"], "alpha")

    def test_one_round_trip_for_the_whole_result_set(self):
        """This runs on the main strategy thread; N gets would be charged to
        every other request behind it."""
        order_identity_map(self.redis, ACCOUNT,
                           ["bq:abc:sig-1", "bq:def:sig-2", "bq:ghi:sig-3"])

        self.assertEqual(len(self.redis.mgets), 1)
        self.assertEqual(self.redis.gets, [])

    def test_duplicates_are_asked_for_once(self):
        order_identity_map(self.redis, ACCOUNT, ["bq:abc:sig-1"] * 4)

        self.assertEqual(self.redis.mgets, [[self.key]])

    def test_blank_ids_are_dropped(self):
        """Hand-placed orders have no remark at all."""
        order_identity_map(self.redis, ACCOUNT, ["", None, "   ", "bq:abc:sig-1"])

        self.assertEqual(self.redis.mgets, [[self.key]])

    def test_nothing_to_look_up_touches_redis_not_at_all(self):
        order_identity_map(self.redis, ACCOUNT, ["", None])

        self.assertEqual(self.redis.mgets, [])

    def test_it_is_bounded(self):
        order_identity_map(self.redis, ACCOUNT,
                           ["id-%d" % index for index in range(1000)], limit=10)

        self.assertEqual(len(self.redis.mgets[0]), 10)

    def test_a_client_without_mget_still_works(self):
        class OldRedis(FakeRedis):
            mget = None

        redis = OldRedis({self.key: _identity("alpha", "bq:abc:sig-1")})

        found = order_identity_map(redis, ACCOUNT, ["bq:abc:sig-1"])

        self.assertEqual(found["bq:abc:sig-1"]["strategy_name"], "alpha")

    def test_a_failing_mget_falls_back_to_gets(self):
        redis = FakeRedis({self.key: _identity("alpha", "bq:abc:sig-1")}, fail=True)

        found = order_identity_map(redis, ACCOUNT, ["bq:abc:sig-1"])

        self.assertEqual(found["bq:abc:sig-1"]["strategy_name"], "alpha")
        self.assertEqual(redis.gets, [self.key])

    def test_no_redis_is_an_empty_answer_not_a_crash(self):
        self.assertEqual(order_identity_map(None, ACCOUNT, ["bq:abc:sig-1"]), {})

    def test_unparsable_json_is_skipped(self):
        redis = FakeRedis({self.key: b"not json"})

        self.assertEqual(order_identity_map(redis, ACCOUNT, ["bq:abc:sig-1"]), {})

    def test_bytes_and_str_both_decode(self):
        payload = _identity("alpha", "bq:abc:sig-1")
        for stored in (payload, payload.encode("utf-8")):
            redis = FakeRedis({self.key: stored})

            found = order_identity_map(redis, ACCOUNT, ["bq:abc:sig-1"])

            self.assertEqual(found["bq:abc:sig-1"]["strategy_name"], "alpha")


class IdentityClientWiringTest(unittest.TestCase):
    """The identity store must not depend on which transport is configured.

    Only a redis TRANSPORT builds the download-job clients. This deployment
    runs zmq, so that attribute is None -- which silently meant orders were
    never remembered at submit time either, and attribution could never work
    no matter how the query side was written. Verified against the live
    terminal: the RPC transport is zmq and redis is reachable and configured.
    """

    def _handlers(self, **attributes):
        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        for name, value in attributes.items():
            setattr(handlers, name, value)
        return handlers

    def test_the_dedicated_client_is_used(self):
        redis_client = FakeRedis()

        handlers = self._handlers(order_identity_redis_client=redis_client,
                                  download_job_redis_client=None)

        self.assertIs(handlers._identity_redis(), redis_client)

    def test_it_falls_back_to_the_download_job_client(self):
        """Deployments that predate the dedicated attribute."""
        redis_client = FakeRedis()

        handlers = self._handlers(download_job_redis_client=redis_client)

        self.assertIs(handlers._identity_redis(), redis_client)

    def test_the_dedicated_one_wins(self):
        dedicated, download = FakeRedis(), FakeRedis()

        handlers = self._handlers(order_identity_redis_client=dedicated,
                                  download_job_redis_client=download)

        self.assertIs(handlers._identity_redis(), dedicated)

    def test_neither_is_none_not_an_error(self):
        handlers = self._handlers()

        self.assertIsNone(handlers._identity_redis())

    def test_the_strategy_reuses_its_cached_exec_event_client(self):
        """Not a second builder of its own.

        _exec_event_redis already exists for exactly this case -- "the RPC
        service has none, which is the zmq-transport case" -- and it caches.
        Its docstring records why: a fresh client per call leaked a connection
        pool per event, and redis-py's __del__ then raised an AttributeError
        that Python swallows as "Exception ignored in", visible only in the QMT
        panel.
        """
        import bigqmt_signal_trader_strategy as strategy
        import inspect

        source = inspect.getsource(strategy._build_rpc_service)

        self.assertIn("order_identity_redis_client", source)
        self.assertIn("_exec_event_redis(config)", source)

    def test_no_redis_config_means_no_client(self):
        import bigqmt_signal_trader_strategy as strategy

        strategy._exec_event_redis_client = None
        try:
            self.assertIsNone(strategy._exec_event_redis({}))
            self.assertIsNone(strategy._exec_event_redis({"redis": {}}))
        finally:
            strategy._exec_event_redis_client = None


class AttributionTest(unittest.TestCase):
    def _handlers(self, redis_client):
        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        handlers.order_identity_redis_client = redis_client
        handlers.download_job_redis_client = None
        return handlers

    def setUp(self):
        self.key = order_identity_key(ACCOUNT, "bq:abc:sig-1")
        self.redis = FakeRedis({self.key: _identity("alpha", "bq:abc:sig-1")})

    def test_a_bridge_order_gets_its_name_back(self):
        rows = [Row(user_order_id="bq:abc:sig-1", strategy_name="")]

        named = self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(named[0].strategy_name, "alpha")

    def test_a_hand_placed_order_stays_unnamed(self):
        """No remark, nothing remembered, nothing to recover -- and inventing
        one would be worse than the empty string."""
        rows = [Row(user_order_id="", strategy_name="")]

        named = self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(named[0].strategy_name, "")

    def test_the_submit_time_identity_wins_over_the_rows_name(self):
        """#216: the row's strategy-name field carries the QMT-side strategy's
        registered name (the bridge process, e.g. '大QMT桥接器'), not the
        caller's strategy_name. The submit-time identity record is the
        authoritative one and must WIN over the row's value."""
        rows = [Row(user_order_id="bq:abc:sig-1", strategy_name="大QMT桥接器")]

        named = self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(named[0].strategy_name, "alpha")

    def test_an_unknown_remark_keeps_the_rows_name(self):
        """An order this bridge never submitted (no identity record) keeps
        whatever the row says -- overriding only on a hit."""
        rows = [Row(user_order_id="someone-elses", strategy_name="beta")]

        named = self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(named[0].strategy_name, "beta")

    def test_rows_without_remarks_mean_no_redis_traffic(self):
        """Only remarked rows can have an identity; blank ones never ask."""
        rows = [Row(user_order_id="", strategy_name="beta")]

        self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(self.redis.mgets, [])
        self.assertEqual(self.redis.gets, [])

    def test_a_zmq_deployment_without_redis_returns_the_rows_unchanged(self):
        rows = [Row(user_order_id="bq:abc:sig-1", strategy_name="")]

        named = self._handlers(None)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(named[0].strategy_name, "")
        self.assertEqual(len(named), 1)

    def test_an_exploding_redis_does_not_lose_the_rows(self):
        """Attribution is a nicety; the query result is not."""
        class Exploding(object):
            def mget(self, keys):
                raise RuntimeError("down")

            def get(self, key):
                raise RuntimeError("down")

        rows = [Row(user_order_id="bq:abc:sig-1", strategy_name="")]

        named = self._handlers(Exploding())._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual(len(named), 1)
        self.assertEqual(named[0].strategy_name, "")

    def test_an_empty_result_set_is_fine(self):
        self.assertEqual(self._handlers(self.redis)._attribute_to_strategies(
            ACCOUNT, []), [])

    def test_none_is_fine(self):
        self.assertEqual(self._handlers(self.redis)._attribute_to_strategies(
            ACCOUNT, None), [])

    def test_a_mixed_result_set_names_only_what_it_can(self):
        rows = [Row(user_order_id="bq:abc:sig-1", strategy_name=""),
                Row(user_order_id="", strategy_name=""),
                Row(user_order_id="bq:zzz:sig-9", strategy_name="")]

        named = self._handlers(self.redis)._attribute_to_strategies(ACCOUNT, rows)

        self.assertEqual([row.strategy_name for row in named], ["alpha", "", ""])


if __name__ == "__main__":
    unittest.main()
