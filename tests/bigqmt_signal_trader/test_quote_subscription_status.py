# coding: utf-8
"""quote_subscription_status / quote_unsubscribe_all: the kill switch for a
lost seq.

Reported live: a user testing subscribe_whole_quote lost the seq, and
"unsubscribe won't work, restart of the server and closing the client did not
help". The 30s keepalive reaper is the designed self-heal (verified live:
pushes stop ~30s after the client dies), but a combo being fed by a leftover
keepalive needs an operator kill switch and a way to SEE what is alive.
"""
import os
import sys
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.quote_subscription_manager import QuoteSubscriptionManager


class _Source(object):
    """Recording subscription source: tracks engine-level subscribe/close."""

    def __init__(self):
        self.subscribed = []
        self.closed = []
        self._next = 0

    def subscribe(self, codes, on_push):
        self._next += 1
        handle = "handle-%d" % self._next
        self.subscribed.append((handle, list(codes)))
        return handle

    def unsubscribe(self, handle):
        self.closed.append(handle)


def _manager():
    source = _Source()
    manager = QuoteSubscriptionManager(source, heartbeat_timeout_seconds=30.0)
    return manager, source


class StatusTest(unittest.TestCase):
    def test_status_lists_combos_with_freshness(self):
        manager, source = _manager()
        manager.subscribe("client-a", "1", ["601398.SH"])
        manager.subscribe("client-a", "2", ["600519.SH", "000001.SZ"])

        status = manager.status()

        self.assertEqual(status["heartbeat_timeout_seconds"], 30.0)
        combos = {c["topic"]: c for c in status["combos"]}
        self.assertEqual(len(combos), 2)
        self.assertEqual(combos["601398.SH"]["clients"], 1)
        self.assertEqual(combos["000001.SZ,600519.SH"]["clients"], 1)
        self.assertLessEqual(combos["601398.SH"]["last_seen_seconds_ago"], 1.0)

    def test_empty_status_is_empty(self):
        manager, _source = _manager()
        self.assertEqual(manager.status()["combos"], [])


class UnsubscribeAllTest(unittest.TestCase):
    def test_unsubscribe_all_closes_every_combo(self):
        manager, source = _manager()
        manager.subscribe("client-a", "1", ["601398.SH"])
        manager.subscribe("client-a", "2", ["601398.SH"])
        manager.subscribe("client-b", "1", ["600519.SH"])

        count = manager.unsubscribe_all()

        self.assertEqual(count, 2)          # two combos, three subscriptions
        self.assertEqual(len(source.closed), 2)
        self.assertEqual(manager.status()["combos"], [])

    def test_keepalive_does_not_resurrect_a_force_cleared_combo(self):
        manager, source = _manager()
        manager.subscribe("client-a", "1", ["601398.SH"])
        manager.unsubscribe_all()

        manager.keepalive("client-a", "1")   # no-op on an unknown sub_id

        self.assertEqual(manager.status()["combos"], [])

    def test_unsubscribe_all_on_empty_is_a_no_op(self):
        manager, source = _manager()
        self.assertEqual(manager.unsubscribe_all(), 0)
        self.assertEqual(source.closed, [])


class HandlerWiringTest(unittest.TestCase):
    """The RPC handlers reach the same manager the subscriptions live in."""

    def test_handlers_expose_status_and_kill(self):
        from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
        from test_redis_rpc import FakeMarketData, FakePositionProvider

        manager, source = _manager()
        handlers = BigQmtRpcHandlers(
            account_id="acct", market_data=FakeMarketData(),
            position_provider=FakePositionProvider(),
            quote_subscription_manager=manager,
        )
        handlers.handle("subscribe_whole_quote", {
            "client_id": "client-a", "sub_id": "1", "codes": ["601398.SH"]})

        status = handlers.handle("quote_subscription_status", {})
        self.assertEqual(len(status["combos"]), 1)

        answer = handlers.handle("quote_unsubscribe_all", {})
        self.assertEqual(answer, {"unsubscribed": 1})
        self.assertEqual(len(source.closed), 1)
        self.assertEqual(manager.status()["combos"], [])

    def test_status_and_kill_are_allowed_methods(self):
        from bigqmt_signal_trader.redis_rpc import (
            QUOTE_SUBSCRIPTION_METHODS, READ_METHODS,
        )
        self.assertIn("quote_subscription_status", READ_METHODS)
        self.assertIn("quote_unsubscribe_all", QUOTE_SUBSCRIPTION_METHODS)


if __name__ == "__main__":
    unittest.main()
