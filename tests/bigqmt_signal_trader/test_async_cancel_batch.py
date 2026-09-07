# coding: utf-8
"""#224 follow-up: the async cancel batch must inherit the order batch's
hard-won rules (#148/#151/#195), which the contributed PR missed.

The native cancel return answers "the request went out", not "the order is
cancelled" -- it has been false while the cancel landed (#148) and true for
a nonexistent order (#151). So: per-item answers carry accepted/confirmed
vocabulary, failure wording never claims rejection, and a timed-out batch is
never silently resubmitted (#195).
"""
import os
import sys
import threading
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.models import CancelResult
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers
from bigqmt_signal_trader.xtquant_compat import (
    BigQmtXtTrader, RpcServerRepliedError, XtQuantTraderCallback,
)
from test_redis_rpc import FakeMarketData, FakePositionProvider


class _CancelGateway(object):
    """order_gateway stand-in: records cancels, answers what it's told."""

    def __init__(self, answer=True):
        self.cancelled = []
        self.answer = answer

    def cancel(self, order_ref, account_id=None):
        self.cancelled.append((account_id, order_ref.order_sys_id))
        return CancelResult(success=bool(self.answer), message="")


def _handlers(gateway):
    return BigQmtRpcHandlers(
        account_id="acct", market_data=FakeMarketData(),
        position_provider=FakePositionProvider(), order_gateway=gateway,
        allow_order_methods=True,
    )


class BatchCancelAnswerSemanticsTest(unittest.TestCase):
    """Server side: per-item answers must not claim what the native return
    cannot know."""

    def test_each_item_marks_accepted_and_never_confirmed(self):
        gateway = _CancelGateway()
        results = _handlers(gateway)._handle_cancel_orders_batch({
            "account_id": "acct",
            "items": [{"order_sysid": "sys-%d" % i} for i in range(3)],
        })

        self.assertEqual(len(gateway.cancelled), 3)
        for entry in results:
            self.assertTrue(entry["accepted"])
            self.assertFalse(entry["confirmed"],
                             "nothing in this path may claim the cancel landed")

    def test_a_native_false_still_marks_accepted(self):
        """#148's shape: the native return said false, the cancel landed."""
        gateway = _CancelGateway(answer=False)
        results = _handlers(gateway)._handle_cancel_orders_batch({
            "account_id": "acct", "items": [{"order_sysid": "sys-1"}],
        })

        self.assertFalse(results[0]["success"])   # the native value, kept
        self.assertTrue(results[0]["accepted"])   # the request went out
        self.assertFalse(results[0]["confirmed"])

    def test_a_missing_sysid_is_an_item_error_without_a_cancel(self):
        gateway = _CancelGateway()
        results = _handlers(gateway)._handle_cancel_orders_batch({
            "account_id": "acct",
            "items": [{"order_sysid": "sys-1"}, {"order_sysid": ""}],
        })

        self.assertEqual(len(gateway.cancelled), 1)
        self.assertFalse(results[1]["accepted"])
        self.assertIn("order_sysid", results[1]["error"])


class _CancelRec(XtQuantTraderCallback):
    def __init__(self):
        self.responses = []
        self.errors = []
        self.lock = threading.Lock()

    def on_cancel_order_stock_async_response(self, r):
        with self.lock:
            self.responses.append(r)

    def on_cancel_error(self, e):
        with self.lock:
            self.errors.append(e)


class _Client(object):
    def __init__(self, behavior):
        self.account_id = "acct"
        self.timeout_seconds = 30.0
        self.calls = []
        self.behavior = behavior

    def call(self, method, params=None, account_id=None, timeout_seconds=None):
        self.calls.append((method, params or {}, timeout_seconds))
        return self.behavior(method, params or {})


class AsyncCancelBatchClientTest(unittest.TestCase):
    def _trader(self, client):
        trader = BigQmtXtTrader(account_id="acct")
        trader.client = client
        rec = _CancelRec()
        trader.register_callback(rec)
        return trader, rec

    def test_a_backlog_goes_out_as_one_batch(self):
        client = _Client(lambda m, p: [
            {"index": i, "success": True, "accepted": True, "confirmed": False}
            for i in range(len(p["items"]))] if m == "cancel_order_stock_batch" else {})
        trader, rec = self._trader(client)
        seqs = [trader.cancel_order_stock_async("acct", "sys-%d" % i)
                for i in range(3)]

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        batches = [c for c in client.calls if c[0] == "cancel_order_stock_batch"]
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0][1]["items"]), 3)
        self.assertEqual([r.seq for r in rec.responses], seqs)

    def test_a_lone_cancel_takes_the_single_path(self):
        client = _Client(lambda m, p: {"success": True})
        trader, rec = self._trader(client)
        trader.cancel_order_stock_async("acct", "sys-1")

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        self.assertEqual([c[0] for c in client.calls], ["cancel_order_stock_sysid"])
        self.assertEqual(len(rec.responses), 1)

    def test_a_refused_batch_falls_back_to_singles(self):
        """A server-answered refusal means nothing ran, so singles are safe."""
        def behavior(method, params):
            if method == "cancel_order_stock_batch":
                raise RpcServerRepliedError("order_gateway is not configured")
            return {"success": True}

        client = _Client(behavior)
        trader, rec = self._trader(client)
        seqs = [trader.cancel_order_stock_async("acct", "sys-%d" % i)
                for i in range(3)]

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        singles = [c for c in client.calls if c[0] == "cancel_order_stock_sysid"]
        self.assertEqual(len(singles), 3)
        self.assertEqual([r.seq for r in rec.responses], seqs)

    def test_a_timed_out_batch_is_never_resubmitted(self):
        """#195 for cancels: a timeout means the cancels MAY BE RUNNING;
        resubmitting double-cancels. Report unknown-outcome per item."""
        def behavior(method, params):
            if method == "cancel_order_stock_batch":
                raise TimeoutError("redis rpc timeout: cancel_order_stock_batch")
            return {"success": True}

        client = _Client(behavior)
        trader, rec = self._trader(client)
        seqs = [trader.cancel_order_stock_async("acct", "sys-%d" % i)
                for i in range(3)]

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        singles = [c for c in client.calls if c[0] == "cancel_order_stock_sysid"]
        self.assertEqual(singles, [], "a timed-out batch must NOT be resubmitted")
        self.assertEqual([e.seq for e in rec.errors], seqs)
        for err in rec.errors:
            self.assertIn("MAY BE RUNNING", err.error_msg)

    def test_a_failed_item_errors_alone(self):
        def behavior(method, params):
            entries = [{"index": i, "success": True, "accepted": True,
                        "confirmed": False} for i in range(len(params["items"]))]
            entries[1] = {"index": 1, "success": False, "accepted": True,
                          "confirmed": False, "message": "native false"}
            return entries

        client = _Client(behavior)
        trader, rec = self._trader(client)
        seqs = [trader.cancel_order_stock_async("acct", "sys-%d" % i)
                for i in range(3)]

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        self.assertEqual([r.seq for r in rec.responses], [seqs[0], seqs[2]])
        self.assertEqual([e.seq for e in rec.errors], [seqs[1]])

    def test_the_failure_wording_never_claims_rejection(self):
        """Native false used to render as "rejected by server" -- the #148
        false-failure direction. The message must say unconfirmed, not dead."""
        client = _Client(lambda m, p: {"success": False})
        trader, rec = self._trader(client)
        trader.cancel_order_stock_async("acct", "sys-1")

        self.assertTrue(trader.wait_async_orders(timeout=5.0))

        self.assertEqual(len(rec.responses), 1)
        resp = rec.responses[0]
        self.assertNotEqual(resp.cancel_result, 0)
        self.assertNotIn("rejected", resp.error_msg.lower())
        self.assertIn("not confirmed", resp.error_msg)


if __name__ == "__main__":
    unittest.main()
