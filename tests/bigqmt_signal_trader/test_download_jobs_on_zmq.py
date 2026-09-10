# coding: utf-8
"""Download jobs must not need a redis TRANSPORT, only a redis.

On a zmq deployment the async download API answered

    RuntimeError: download jobs require a Redis client

for submit_download_history_data, get_download_status and wait_download --
because handlers.download_job_redis_client was built only when the transport
was redis.

The worker on the other side of that None was running the whole time.
_pump_download_jobs runs on every adjust tick regardless of transport and gets
its client from _exec_event_redis, which exists precisely because "_rpc_service
has none, which is the zmq-transport case". So queued jobs would have been
processed; there was simply no way to queue one.

Both stores now take the same client, and _exec_event_redis is the one that
builds it -- not a second builder, because it caches. Its docstring records
why: a fresh client per call leaked a connection pool per event, and redis-py's
__del__ then raised an AttributeError that Python swallows as "Exception
ignored in", visible only in the QMT panel.
"""

import inspect
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers

import bigqmt_signal_trader_strategy as strategy


class FakeRedis(object):
    pass


class WiringTest(unittest.TestCase):
    def setUp(self):
        self.source = inspect.getsource(strategy._build_rpc_service)

    def test_both_stores_take_the_same_client(self):
        self.assertIn("handlers.download_job_redis_client = _store_redis", self.source)
        self.assertIn("handlers.order_identity_redis_client = _store_redis", self.source)

    def test_optional_identity_reads_default_off_for_zmq(self):
        self.assertIn(
            "handlers.order_identity_remote_lookup_enabled = _config_bool(",
            self.source,
        )
        self.assertIn("_is_redis_transport(transport_name)", self.source)

    def test_the_client_falls_back_past_the_transport(self):
        self.assertIn(
            "_store_redis = response_redis_client or redis_client "
            "or _exec_event_redis(config)", self.source)

    def test_it_does_not_build_a_second_client_of_its_own(self):
        """_exec_event_redis caches; a private builder would not.

        (build_redis_client does appear in _build_rpc_service -- that is the
        redis-transport branch above, which is fine and predates this.)
        """
        self.assertNotIn("_store_redis = ", self.source.replace(
            "_store_redis = response_redis_client or redis_client "
            "or _exec_event_redis(config)", ""))
        self.assertFalse(hasattr(strategy, "_build_identity_redis_client"))

    def test_the_pump_uses_the_same_helper(self):
        """Which is why the worker was running while the door was locked."""
        pump = inspect.getsource(strategy._pump_download_jobs)

        self.assertIn("_exec_event_redis(config)", pump)

    def test_the_pump_runs_on_every_adjust_tick(self):
        adjust = inspect.getsource(strategy.adjust)

        self.assertIn("_pump_download_jobs", adjust)


class WorkerGateTest(unittest.TestCase):
    """Unlocking the door is only right if someone is behind it.

    _pump_download_jobs is OFF by default on Big QMT, and the runtime says why:
    the terminal's embedded xtdata SDK has no reachable data service, so the
    download would raise 无法连接行情服务 -- the same root cause as the
    download_* methods in #130. Verified live: after the client was wired,
    submit_download_history_data returned a job id and the job sat at
    state=pending done=0/1 for 12 seconds while the queue grew to 2 and the
    active key stayed empty. Nothing was ever going to run it.

    Accepting work into a queue no one drains looks like success and never
    finishes. That is worse than the refusal it replaced.
    """

    def _handlers(self, enabled):
        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        handlers.download_job_redis_client = FakeRedis()
        handlers.download_jobs_enabled = enabled
        return handlers

    def test_submitting_with_no_worker_is_refused(self):
        with self.assertRaises(RuntimeError) as caught:
            self._handlers(False)._handle_submit_download_history_data2({})

        self.assertIn("sit in the queue forever", str(caught.exception))

    def test_the_refusal_names_the_switch(self):
        with self.assertRaises(RuntimeError) as caught:
            self._handlers(False)._handle_submit_download_history_data2({})

        self.assertIn("download_jobs_enabled=True", str(caught.exception))

    def test_the_refusal_says_what_to_do_instead(self):
        with self.assertRaises(RuntimeError) as caught:
            self._handlers(False)._handle_submit_download_history_data2({})

        message = str(caught.exception)
        self.assertIn("get_market_data_ex", message)

    def test_an_absent_flag_is_treated_as_off(self):
        """An older deployment does not set it, and off is the safe reading."""
        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        handlers.download_job_redis_client = FakeRedis()

        with self.assertRaises(RuntimeError):
            handlers._require_download_worker()

    def test_with_a_worker_it_does_not_refuse(self):
        self._handlers(True)._require_download_worker()

    def test_reading_status_is_not_gated(self):
        """A job already queued should still be inspectable -- that is how you
        see it is stuck."""
        source = inspect.getsource(BigQmtRpcHandlers._handle_get_download_status)

        self.assertNotIn("_require_download_worker", source)

    def test_the_strategy_wires_the_flag_defaulting_off(self):
        source = inspect.getsource(strategy._build_rpc_service)

        self.assertIn("handlers.download_jobs_enabled = _config_bool(", source)
        self.assertIn('(config.get("download_jobs") or {}).get("enabled"), False)',
                      source)


class HandlerTest(unittest.TestCase):
    def _handlers(self, redis_client):
        handlers = BigQmtRpcHandlers.__new__(BigQmtRpcHandlers)
        handlers.download_job_redis_client = redis_client
        return handlers

    def test_a_client_is_handed_straight_back(self):
        redis_client = FakeRedis()

        self.assertIs(self._handlers(redis_client)._download_job_redis(), redis_client)

    def test_no_redis_at_all_still_says_so_plainly(self):
        """A deployment with no redis configured has no job store, and saying
        that is right -- what was wrong was saying it on zmq deployments that
        did have one."""
        with self.assertRaises(RuntimeError) as caught:
            self._handlers(None)._download_job_redis()

        self.assertIn("require a Redis client", str(caught.exception))

    def test_the_three_download_methods_all_go_through_it(self):
        source = inspect.getsource(BigQmtRpcHandlers)

        for method in ("_handle_get_download_status", "_handle_wait_download",
                       "_handle_submit_download_history_data2"):
            body = source.split("def %s" % method, 1)[1].split("\n    def ", 1)[0]
            self.assertIn("_download_job_redis()", body, method)


if __name__ == "__main__":
    unittest.main()
