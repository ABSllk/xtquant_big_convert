# coding: utf-8
"""issue #174: callbacks never carry strategy_name.

@sumo225270 reported that both `on_stock_order` and `on_stock_trade` hand back
an empty strategy_name while the order was submitted with a non-empty one.
`instrument_name` and `order_remark` were fine, so it is not the callback
plumbing in general.

Two separate causes, and the trade one is unconditional:

**Trades had no strategy_name at all.** `normalize_trade_event` never emitted
the key -- not empty, absent -- so the client's
``item.get("strategy_name") or ""`` could only ever answer "". And the publish
path enriched only the order branch::

    if kind == "trade":
        event = normalize_trade_event(...)      # no enrichment
    else:
        event = normalize_order_event(...)
        event = enrich_order_identity(...)      # only here

**Orders depend entirely on the identity store**, because big QMT does not put
the strategy name on the row. Listing every attribute on a live terminal:
ORDER carries 120 and DEAL 47, and `m_strStrategyName` is in neither -- the
same shape as `m_strShareholderID` in #133. The live query path confirms it:
14 orders, `strategy_name` empty on every one. So the only way back is what
the bridge remembered at submit time, keyed by the remark.

**Superseded in part** -- see test_order_source_strategy_name.py. "big QMT
does not put the strategy name on the row" was the wrong conclusion from a
true observation: m_strStrategyName is indeed absent, but the name comes back
in 报单来源 (m_strSource), which is what passorder was handed as strategyName.
The reporter's own raw_fields carried it on both callbacks. Everything below
about the missing trade key and the identity-store fallback still holds; it is
just no longer the only way back.

That store was read from Redis only. On a zmq deployment whose Redis is
configured but not reachable -- which is exactly the "configured is not
reachable" trap in #145 -- the lookup fails silently and the name stays empty.
The in-process journal written at submit time (#156, already used by the query
path) is now the fallback, so a deployment with no working Redis still names
what this process submitted.
"""

import os
import sys
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

import bigqmt_signal_trader_strategy as strategy
from bigqmt_signal_trader import exec_events


def _deal_obj(**overrides):
    attrs = dict(
        m_strInstrumentID="601388", m_strExchangeID="SH", m_nOffsetFlag=49,
        m_nVolume=700, m_dPrice=35.36, m_dTradeAmount=24752.0,
        m_strOrderSysID="2209", m_strTradeID="t-1", m_strRemark="sig-1",
        m_strTradeTime="093013", m_strAccountID="acct", m_nDirection=49,
    )
    attrs.update(overrides)
    return type("FakeDeal", (), attrs)


def _order_obj(**overrides):
    attrs = dict(
        m_strInstrumentID="601388", m_strExchangeID="SH", m_nOffsetFlag=49,
        m_nVolumeTotalOriginal=158200, m_nVolumeTotal=158200, m_nVolumeTraded=0,
        m_strOrderSysID="2209", m_strRemark="sig-1", m_nOrderStatus=50,
        m_dLimitPrice=35.62, m_dTradedPrice=0.0, m_strAccountID="acct",
    )
    attrs.update(overrides)
    return type("FakeOrder", (), attrs)


# ------------------------------------------------------- the normalizer


class TradeEventCarriesStrategyNameTest(unittest.TestCase):
    def test_the_key_exists_even_when_qmt_says_nothing(self):
        """Absent, not empty, was the bug: .get() could not tell them apart."""
        event = exec_events.normalize_trade_event(_deal_obj(), "acct")

        self.assertIn("strategy_name", event)
        self.assertEqual(event["strategy_name"], "")

    def test_it_is_read_off_the_object_when_qmt_does_supply_it(self):
        """This terminal has no such attribute, but another build might."""
        event = exec_events.normalize_trade_event(
            _deal_obj(m_strStrategyName="alpha"), "acct")

        self.assertEqual(event["strategy_name"], "alpha")

    def test_the_camelcase_spelling_is_accepted_too(self):
        event = exec_events.normalize_trade_event(
            _deal_obj(strategyName="alpha"), "acct")

        self.assertEqual(event["strategy_name"], "alpha")


class EnrichWorksOnATradeEventTest(unittest.TestCase):
    """enrich_order_identity keys off remark, which trades carry as well."""

    def test_a_trade_event_is_named_from_the_store(self):
        import json

        class Store(object):
            def get(self, key):
                return json.dumps({"strategy_name": "alpha"}).encode("utf-8")

        event = exec_events.normalize_trade_event(_deal_obj(), "acct")
        enriched = exec_events.enrich_order_identity(Store(), "acct", event)

        self.assertEqual(enriched["strategy_name"], "alpha")

    def test_the_store_wins_over_the_rows_bridge_process_name(self):
        """#216: the row's strategy-name field is the QMT-side strategy's
        registered name (the bridge process), never the caller's. An order
        placed with strategy_name='DaBanStrategy' reported '大QMT桥接器'.
        The submit-time identity record must override the row."""
        import json

        class Store(object):
            def get(self, key):
                return json.dumps({"strategy_name": "alpha"}).encode("utf-8")

        event = exec_events.normalize_trade_event(
            _deal_obj(m_strSource="大QMT桥接器"), "acct")
        self.assertEqual(event["strategy_name"], "大QMT桥接器")  # the row's own answer

        enriched = exec_events.enrich_order_identity(Store(), "acct", event)

        self.assertEqual(enriched["strategy_name"], "alpha")


# ------------------------------------------------------- the publish path


class _Sink(object):
    def __init__(self):
        self.published = []


class _FakeExecEvents(object):
    """Real normalizers, fake transport; records enrichment calls."""

    normalize_order_event = staticmethod(exec_events.normalize_order_event)
    normalize_trade_event = staticmethod(exec_events.normalize_trade_event)
    normalize_order_error_event = staticmethod(exec_events.normalize_order_error_event)
    format_raw_snapshot = staticmethod(lambda *args: "")
    raw_field_snapshot = staticmethod(lambda *args: None)

    def __init__(self):
        self.enriched = []

    def enrich_order_identity(self, redis_client, account_id, event):
        self.enriched.append(event.get("event_type"))
        event["strategy_name"] = "from-redis"
        return event

    def publish_exec_event(self, sink, account_id, event):
        sink.published.append((account_id, event))


CFG = {"exec_events": {"enabled": True, "account_id": "acct",
                       "hold_presysid_order_seconds": 0.0}}


class _Journal(dict):
    pass


class _Handlers(object):
    _ORDER_IDENTITY_LOCAL_TTL_SECONDS = 86400.0

    def __init__(self, journal=None):
        self._order_identity_local = journal


class _Service(object):
    def __init__(self, handlers=None):
        self.handlers = handlers


class _PublishBase(unittest.TestCase):
    def setUp(self):
        self._saved = {name: getattr(strategy, name)
                       for name in ("_build_config", "_exec_event_sink",
                                    "_exec_event_redis", "_exec_events",
                                    "_rpc_service")}
        self._sink = _Sink()
        self._fake = _FakeExecEvents()
        strategy._build_config = lambda: CFG
        strategy._exec_event_sink = lambda config: self._sink
        strategy._exec_event_redis = lambda config: None
        strategy._exec_events = self._fake
        strategy._rpc_service = None
        strategy._held_presysid_orders.clear()
        strategy._instrument_name_cache.clear()

    def tearDown(self):
        for name, value in self._saved.items():
            setattr(strategy, name, value)
        strategy._held_presysid_orders.clear()
        strategy._instrument_name_cache.clear()

    def _events(self):
        return [event for _, event in self._sink.published]


class TradePublishIsEnrichedTest(_PublishBase):
    def test_the_trade_branch_now_enriches_like_the_order_branch(self):
        strategy._exec_event_redis = lambda config: object()   # a "working" store

        strategy._publish_exec_event("trade", _deal_obj(), None)

        self.assertEqual(self._fake.enriched, ["trade"])
        self.assertEqual(self._events()[0]["strategy_name"], "from-redis")

    def test_the_order_branch_still_enriches(self):
        strategy._exec_event_redis = lambda config: object()

        strategy._publish_exec_event("order", _order_obj(), None)

        self.assertEqual(self._fake.enriched, ["order"])


class LocalJournalFallbackTest(_PublishBase):
    """No Redis, or Redis configured but unreachable -- the #174 case."""

    def _wire_journal(self, name="alpha", age=0.0):
        journal = _Journal({("acct", "sig-1"): (time.time() - age, name)})
        strategy._rpc_service = _Service(_Handlers(journal))

    def test_a_trade_is_named_from_the_in_process_journal(self):
        self._wire_journal()

        strategy._publish_exec_event("trade", _deal_obj(), None)

        self.assertEqual(self._events()[0]["strategy_name"], "alpha")

    def test_an_order_is_named_from_the_in_process_journal(self):
        self._wire_journal()

        strategy._publish_exec_event("order", _order_obj(), None)

        self.assertEqual(self._events()[0]["strategy_name"], "alpha")

    def test_an_expired_entry_is_not_used(self):
        self._wire_journal(age=86400.0 * 2)

        strategy._publish_exec_event("trade", _deal_obj(), None)

        self.assertEqual(self._events()[0]["strategy_name"], "")

    def test_an_unknown_remark_stays_empty_rather_than_guessing(self):
        self._wire_journal()

        strategy._publish_exec_event("trade", _deal_obj(m_strRemark="other"), None)

        self.assertEqual(self._events()[0]["strategy_name"], "")

    def test_no_service_at_all_does_not_raise(self):
        strategy._rpc_service = None

        strategy._publish_exec_event("trade", _deal_obj(), None)

        self.assertEqual(self._events()[0]["strategy_name"], "")

    def test_the_journal_answers_without_touching_redis(self):
        """This runs on QMT's C++ callback thread. For an order this process
        submitted the journal already holds what redis holds, so a round trip
        per event buys nothing -- and costs the full timeout when redis is
        configured but unreachable, which is the reported deployment."""
        strategy._exec_event_redis = lambda config: object()
        self._wire_journal()

        strategy._publish_exec_event("trade", _deal_obj(), None)

        self.assertEqual(self._events()[0]["strategy_name"], "alpha")
        self.assertEqual(self._fake.enriched, [])   # redis never consulted

    def test_the_journal_wins_over_the_rows_bridge_process_name(self):
        """#216 publish-path: the row came pre-named with the bridge's own
        process name; the journal (this process's submit-time record) knows
        the caller's real strategy name and must override it."""
        self._wire_journal()

        strategy._publish_exec_event(
            "trade", _deal_obj(strategyName="大QMT桥接器"), None)

        self.assertEqual(self._events()[0]["strategy_name"], "alpha")

    def test_redis_is_the_fallback_when_the_journal_misses(self):
        """Another process's order: nothing local, so redis still answers."""
        strategy._exec_event_redis = lambda config: object()
        self._wire_journal()

        strategy._publish_exec_event("trade", _deal_obj(m_strRemark="other"), None)

        self.assertEqual(self._events()[0]["strategy_name"], "from-redis")
        self.assertEqual(self._fake.enriched, ["trade"])


if __name__ == "__main__":
    unittest.main()
