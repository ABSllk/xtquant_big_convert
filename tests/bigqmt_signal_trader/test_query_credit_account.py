# coding: utf-8
"""#202: 信用账户「查柜台」通道 query_credit_account + credit_account_callback。

大 QMT 里信用账户的账户明细有两条路：

  同步 get_trade_detail_data(accId, 'CREDIT', 'ACCOUNT')  -> 终端缓存那份
  异步 query_credit_account(accId, seq, ContextInfo)      -> 柜台那份

第二条只从 credit_account_callback 出来，原来完全没接。这组用例钉住接进来之后
的三件事：真发得出去、回调落得下来、以及官方 6.13 的限流（「只能有一个查询」
「建议 30s 一次」）确实挡得住。
"""
import os
import sys
import threading
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.market_bigqmt import BigQmtMarketDataProvider
from bigqmt_signal_trader.adapters.order_dryrun import DryRunOrderGateway
from bigqmt_signal_trader.redis_rpc import BigQmtRpcHandlers


class _CreditResult(object):
    """回调交回来的对象是属性袋，不是 dict —— 和 QMT 一样。"""

    def __init__(self):
        self.m_strAccountID = "acct"
        self.m_nBrokerType = 3
        self.m_dPerAssurescaleValue = 2.87
        self.m_dTotalDebt = 12345.67       # 注意：回调这份是 Debt，不是 Debit
        self.m_dFinEnableQuota = 500000.0


class _Ctx(object):
    accid = "acct"

    def get_full_tick(self, codes):
        return {}


def _handlers(query_func=None, **kw):
    qmt_api = {}
    if query_func is not None:
        qmt_api["query_credit_account"] = query_func
    return BigQmtRpcHandlers(
        account_id="acct",
        market_data=BigQmtMarketDataProvider(_Ctx()),
        position_provider=None,
        order_gateway=DryRunOrderGateway(),
        qmt_api=qmt_api,
        **kw
    )


class QueryCreditAccountTest(unittest.TestCase):

    def test_issues_the_query_and_returns_the_callback_result(self):
        """回调在另一个线程落地，handler 的有界等待要等得到。"""
        seen = []
        handlers_box = {}

        def query_credit_account(account_id, seq, context_info):
            seen.append((account_id, seq, context_info))
            # QMT 从 C++ 回调线程调进来，这里用一个真线程模拟
            def fire():
                time.sleep(0.05)
                handlers_box["h"].note_credit_account(seq, _CreditResult())
            threading.Thread(target=fire).start()

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers
        # 显式要求等待：默认是 0（不堵 adjust 主线程），这条测的正是等待路径
        out = handlers.handle("query_credit_account", {"wait_seconds": 2})

        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][0], "acct")
        self.assertIsInstance(seen[0][1], int)
        self.assertIsNotNone(seen[0][2])            # ContextInfo 必须传下去

        self.assertTrue(out["query_issued"])
        self.assertTrue(out["fresh"])
        self.assertFalse(out["stale"])
        self.assertEqual(out["count"], 1)
        row = out["rows"][0]
        self.assertEqual(row["m_dPerAssurescaleValue"], 2.87)
        self.assertEqual(row["m_dTotalDebt"], 12345.67)
        self.assertEqual(row["m_nBrokerType"], 3)

    def test_rate_limited_second_call_serves_the_cache_and_says_so(self):
        """官方建议 30s 一次。第二次不该再打柜台，但也不该假装没数据。"""
        calls = []
        handlers_box = {}

        def query_credit_account(account_id, seq, context_info):
            calls.append(seq)
            handlers_box["h"].note_credit_account(seq, _CreditResult())

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers

        first = handlers.handle("query_credit_account", {})
        second = handlers.handle("query_credit_account", {})

        self.assertEqual(len(calls), 1)             # 只发了一次
        self.assertTrue(first["fresh"])
        self.assertFalse(second["query_issued"])
        self.assertIn("rate limited", second["not_issued_reason"])
        # 缓存照给，但明说是陈的
        self.assertEqual(second["count"], 1)
        self.assertTrue(second["stale"])
        self.assertFalse(second["fresh"])
        self.assertIsNotNone(second["age_seconds"])

    def test_min_interval_is_configurable_for_a_deliberate_refresh(self):
        calls = []
        handlers_box = {}

        def query_credit_account(account_id, seq, context_info):
            calls.append(seq)
            handlers_box["h"].note_credit_account(seq, _CreditResult())

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers
        handlers.credit_account_min_interval_seconds = 0.0

        handlers.handle("query_credit_account", {})
        handlers.handle("query_credit_account", {})
        self.assertEqual(len(calls), 2)

    def test_unbound_global_is_reported_not_silently_empty(self):
        """这台部署没注入 query_credit_account —— 要说出来，不能只回空。"""
        out = _handlers(None).handle("query_credit_account", {})

        self.assertFalse(out["query_issued"])
        self.assertFalse(out["callback_bound"])
        self.assertIn("not bound", out["not_issued_reason"])
        self.assertEqual(out["count"], 0)

    def test_a_throwing_global_is_reported_not_swallowed(self):
        def query_credit_account(account_id, seq, context_info):
            raise RuntimeError("counter refused")

        out = _handlers(query_credit_account).handle("query_credit_account", {})

        self.assertFalse(out["query_issued"])
        self.assertIn("counter refused", out["not_issued_reason"])

    def test_no_callback_within_the_wait_is_not_reported_as_fresh(self):
        """回调没来就是没来，不能把空/陈的当新数据报上去。"""
        def query_credit_account(account_id, seq, context_info):
            pass                                    # 永不回调

        out = _handlers(query_credit_account).handle(
            "query_credit_account", {"wait_seconds": 0.1})

        self.assertTrue(out["query_issued"])
        self.assertFalse(out["fresh"])
        self.assertEqual(out["count"], 0)

    def test_wait_seconds_is_clamped(self):
        """客户端不能靠一个大 wait_seconds 把 adjust 主线程按住。"""
        def query_credit_account(account_id, seq, context_info):
            pass                                    # 永不回调，只能靠上限收场

        handlers = _handlers(query_credit_account)
        handlers.credit_account_max_wait_seconds = 0.2

        t0 = time.time()
        handlers.handle("query_credit_account", {"wait_seconds": 3600})
        self.assertLess(time.time() - t0, 2.0)

    def test_default_ceiling_bounds_the_main_thread_hold(self):
        from bigqmt_signal_trader.redis_rpc import (
            CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS, CREDIT_ACCOUNT_MAX_WAIT_SECONDS,
        )
        self.assertLessEqual(CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS, 2.0)
        self.assertLessEqual(CREDIT_ACCOUNT_MAX_WAIT_SECONDS, 10.0)
        self.assertEqual(_handlers(None).credit_account_max_wait_seconds,
                         CREDIT_ACCOUNT_MAX_WAIT_SECONDS)

    def test_callback_never_raises_out_of_the_c_thread(self):
        """回调里抛异常在 QMT 那边是无堆栈的 SystemError，绝不能漏出去。"""
        class _Exploding(object):
            @property
            def m_dTotalDebt(self):
                raise RuntimeError("boom")

        handlers = _handlers(lambda *a: None)
        # 不抛，返回真假即可
        result = handlers.note_credit_account(1, _Exploding())
        self.assertIn(result, (True, False))

    def test_envelope_counts_callbacks_so_silence_is_diagnosable(self):
        """callbacks_seen=0 和 >0 指向完全不同的排查方向，必须报出来。

        实盘上出现过：绑定修好了、query_issued=true，但 fresh=false 没数据。
        光看这些分不清「QMT 压根没回调」和「回调来了但结果没落进来」。
        """
        handlers_box = {}

        def query_credit_account(account_id, seq, context_info):
            handlers_box["h"].note_credit_account(seq, _CreditResult())

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers

        before = handlers.handle("probe_capabilities", {})["credit_callback"]
        self.assertEqual(before["callbacks_seen"], 0)
        self.assertTrue(before["note_credit_account_available"])

        out = handlers.handle("query_credit_account", {})
        self.assertEqual(out["callbacks_seen"], 1)
        self.assertIsInstance(out["waited_seconds"], float)

        after = handlers.handle("probe_capabilities", {})["credit_callback"]
        self.assertEqual(after["callbacks_seen"], 1)
        self.assertEqual(after["cached_rows"], 1)

    def test_a_query_that_never_calls_back_reports_zero_callbacks(self):
        out = _handlers(lambda *a: None).handle(
            "query_credit_account", {"wait_seconds": 0.1})

        self.assertTrue(out["query_issued"])
        self.assertFalse(out["fresh"])
        self.assertEqual(out["callbacks_seen"], 0)      # QMT 没回调
        self.assertGreaterEqual(out["waited_seconds"], 0.05)

    def test_a_callback_that_cannot_be_normalised_still_counts(self):
        """回调来过就要记一笔 —— 哪怕结果没能用上，那也不是「没回调」。"""
        class _Exploding(object):
            @property
            def m_dTotalDebt(self):
                raise RuntimeError("boom")

        handlers = _handlers(lambda *a: None)
        handlers.note_credit_account(1, _Exploding())
        probe = handlers.handle("probe_capabilities", {})["credit_callback"]
        self.assertGreaterEqual(probe["callbacks_seen"], 1)

    def test_default_does_not_block_the_adjust_thread(self):
        """默认不等回调 —— handler 跑在 adjust 主线程上，等待可能把回调堵在门外。"""
        from bigqmt_signal_trader.redis_rpc import CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS

        self.assertEqual(CREDIT_ACCOUNT_DEFAULT_WAIT_SECONDS, 0.0)

        started = time.time()
        out = _handlers(lambda *a: None).handle("query_credit_account", {})
        self.assertLess(time.time() - started, 0.5)
        self.assertTrue(out["query_issued"])
        self.assertIn("下一次调用", out["note"])

    def test_result_is_available_on_the_next_call(self):
        """不等的代价只是多一次调用：回调随后落下，第二次就取到。"""
        handlers_box = {}
        pending = []

        def query_credit_account(account_id, seq, context_info):
            pending.append(seq)          # 模拟回调晚于 handler 返回

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers
        handlers.credit_account_min_interval_seconds = 0.0

        first = handlers.handle("query_credit_account", {})
        self.assertEqual(first["count"], 0)

        handlers.note_credit_account(pending[0], _CreditResult())   # 回调后到

        second = handlers.handle("query_credit_account", {})
        self.assertEqual(second["count"], 1)
        self.assertEqual(second["rows"][0]["m_dTotalDebt"], 12345.67)

    def test_a_lost_callback_does_not_wedge_the_channel_forever(self):
        """inflight 原来只在回调到达/抛异常时清 —— 超时没人清，一次丢失就永久卡死。"""
        calls = []
        handlers = _handlers(lambda a, s, c: calls.append(s))
        handlers.credit_account_min_interval_seconds = 0.0

        first = handlers.handle("query_credit_account", {"wait_seconds": 0.1})
        self.assertTrue(first["query_issued"])
        self.assertTrue(first["inflight_released"])

        second = handlers.handle("query_credit_account", {"wait_seconds": 0.1})
        self.assertTrue(second["query_issued"],
                        "回调丢一次就再也发不出去了：%s" % second["not_issued_reason"])
        self.assertEqual(len(calls), 2)

    def test_a_stuck_inflight_is_released_after_the_min_interval(self):
        """兜底：inflight 卡住超过一个最小间隔就当它丢了。"""
        calls = []
        handlers = _handlers(lambda a, s, c: calls.append(s))
        handlers.credit_account_min_interval_seconds = 0.0
        handlers._credit_account_inflight = True
        handlers._credit_account_asked = time.time() - 999

        out = handlers.handle("query_credit_account", {})
        self.assertTrue(out["query_issued"])
        self.assertEqual(len(calls), 1)

    def test_callback_thread_is_recorded_so_the_hypothesis_is_testable(self):
        """回调落在 adjust 线程还是 C++ 线程，是个事实问题 —— 记下来别猜。"""
        handlers = _handlers(lambda *a: None)
        handlers.note_credit_account(1, _CreditResult())

        probe = handlers.handle("probe_capabilities", {})["credit_callback"]
        self.assertTrue(probe["last_callback_thread"])
        out = handlers.handle("query_credit_account", {})
        self.assertTrue(out["callback_thread"])

    def test_wait_is_skipped_when_the_callback_lands_on_this_very_thread(self):
        """回调投递在本线程上时，等待永远等不到 —— 别白占 adjust 主线程。

        实盘实测（真两融账户）：callback_thread="MainThread"，handler 也在
        MainThread，等满 8 秒 callbacks_seen 纹丝不动；那几次回调全是在两次
        调用之间、handler 没占着线程的时候落下的。
        """
        handlers = _handlers(lambda *a: None)
        handlers.credit_account_min_interval_seconds = 0.0
        # 先记一次回调，让桥知道回调落在哪条线程上（就是本线程）
        handlers.note_credit_account(1, _CreditResult())

        started = time.time()
        out = handlers.handle("query_credit_account", {"wait_seconds": 5})

        self.assertLess(time.time() - started, 1.0, "明知等不到还等了")
        self.assertTrue(out["wait_skipped_same_thread"])
        self.assertEqual(out["waited_seconds"], 0.0)
        self.assertIn("等待永远等不到", out["note"])
        # 缓存照给
        self.assertEqual(out["count"], 1)

    def test_wait_is_honoured_when_the_callback_lands_elsewhere(self):
        """回调走别的线程时，等待是有意义的，不能一刀切砍掉。"""
        handlers = _handlers(lambda *a: None)
        handlers.credit_account_min_interval_seconds = 0.0
        handlers._credit_account_callback_thread = "SomeQmtCallbackThread"

        out = handlers.handle("query_credit_account", {"wait_seconds": 0.2})
        self.assertFalse(out["wait_skipped_same_thread"])
        self.assertGreaterEqual(out["waited_seconds"], 0.1)

    def test_nothing_observed_yet_still_honours_an_explicit_wait(self):
        """还没见过回调时不做假设 —— 照常等。"""
        handlers = _handlers(lambda *a: None)
        out = handlers.handle("query_credit_account", {"wait_seconds": 0.2})
        self.assertFalse(out["wait_skipped_same_thread"])
        self.assertGreaterEqual(out["waited_seconds"], 0.1)


class StaleCacheGuardTest(unittest.TestCase):
    """缓存不会自己刷新，所以太陈的数据宁可不给。

    真正的风险不是「30 秒 vs 0 秒」，而是一个只读 rows、不看 age_seconds 的
    调用方，拿几小时前的**维持担保比例**去做决策还毫无察觉 —— 那是强平线。
    """

    def _handlers_with_cached(self, age_seconds):
        handlers = _handlers(lambda *a: None)
        handlers.credit_account_min_interval_seconds = 0.0
        handlers.note_credit_account(1, _CreditResult())
        # 把缓存时间戳往回拨，模拟放了很久
        handlers._credit_account_stamp = time.time() - age_seconds
        return handlers

    def test_default_max_age_is_two_minutes(self):
        from bigqmt_signal_trader.redis_rpc import CREDIT_ACCOUNT_MAX_AGE_SECONDS
        self.assertEqual(CREDIT_ACCOUNT_MAX_AGE_SECONDS, 120.0)
        self.assertEqual(_handlers(None).credit_account_max_age_seconds, 120.0)

    def test_fresh_enough_cache_is_returned(self):
        out = self._handlers_with_cached(30).handle("query_credit_account", {})
        self.assertEqual(out["count"], 1)
        self.assertFalse(out["dropped_stale"])
        self.assertEqual(out["max_age_seconds"], 120.0)

    def test_too_old_cache_is_withheld_and_says_so(self):
        out = self._handlers_with_cached(3 * 3600).handle("query_credit_account", {})

        self.assertEqual(out["count"], 0)
        self.assertEqual(out["rows"], [])
        self.assertTrue(out["dropped_stale"])
        self.assertIn("不再交出", out["dropped_stale_reason"])
        self.assertIn("query_credit_detail", out["dropped_stale_reason"])
        # 年龄仍然如实报出来，别把「扣下了」伪装成「没数据」
        self.assertGreater(out["age_seconds"], 3000)

    def test_caller_can_opt_out_explicitly(self):
        out = self._handlers_with_cached(3 * 3600).handle(
            "query_credit_account", {"max_age_seconds": 0})
        self.assertEqual(out["count"], 1)
        self.assertFalse(out["dropped_stale"])

    def test_caller_can_ask_for_a_tighter_bound(self):
        out = self._handlers_with_cached(60).handle(
            "query_credit_account", {"max_age_seconds": 10})
        self.assertEqual(out["count"], 0)
        self.assertTrue(out["dropped_stale"])

    def test_a_fresh_callback_this_call_is_never_dropped(self):
        """这一次刚落下的回调不该被年龄规则误伤。"""
        handlers_box = {}

        def query_credit_account(account_id, seq, context_info):
            handlers_box["h"].note_credit_account(seq, _CreditResult())

        handlers = _handlers(query_credit_account)
        handlers_box["h"] = handlers
        out = handlers.handle("query_credit_account",
                              {"max_age_seconds": 1, "wait_seconds": 0})
        self.assertTrue(out["fresh"])
        self.assertEqual(out["count"], 1)
        self.assertFalse(out["dropped_stale"])

    def test_empty_cache_is_not_reported_as_dropped(self):
        """从没拿到过数据 ≠ 数据被扣下，两者的排查方向不同。"""
        out = _handlers(lambda *a: None).handle("query_credit_account", {})
        self.assertEqual(out["count"], 0)
        self.assertFalse(out["dropped_stale"])
        self.assertEqual(out["dropped_stale_reason"], "")


class MethodRegistrationTest(unittest.TestCase):

    def test_method_is_whitelisted_and_deferred_to_the_main_thread(self):
        from bigqmt_signal_trader.redis_rpc import (
            LISTENER_DEFERRED_METHODS, READ_METHODS,
        )
        # query_credit_account 需要 ContextInfo 和主线程上下文，必须走 drain
        self.assertIn("query_credit_account", READ_METHODS)
        self.assertIn("query_credit_account", LISTENER_DEFERRED_METHODS)
        self.assertIn("query_credit_account", _handlers(None).allowed_methods)


class CreditCallbackWiringTest(unittest.TestCase):
    """QMT 只往被挂载的那个文件回调，所以每个入口文件都要再导出一次。"""

    # 仓库里跟踪着的入口文件。
    ENTRY_FILES = (
        "src/BIGQMT_REDIS_DRYRUN.py",
        "src/BIGQMT_ZMQ_BACKTEST.py",
        "src/bigqmt_signal_trader_redis_rpc_runtime.py",
        "src/bigqmt_signal_trader_redis_dryrun.py",
        "src/bigqmt_signal_trader_dryrun.py",
        "bigqmt_no_redis/DRYRUN_no_redis.py",
    )

    # 单文件构建产物是 .gitignore 掉的生成物，干净检出里根本不存在，
    # 直接读它会让整条用例以 FileNotFoundError 挂掉。产物里那段导出是
    # 构建脚本里的纯文本模板（只有被嵌入的模块源码是 base64），而构建
    # 脚本是跟踪着的——所以事实来源是它们，不是产物。
    GENERATED_ENTRIES = (
        ("tools/build_single_file.py",
         "src/BIGQMT_REDIS_DRYRUN_ALL_IN_ONE.py"),
        ("tools/build_no_redis_single_file_flat.py",
         "src/BIGQMT_DRYRUN_NO_REDIS_FLAT_ALL_IN_ONE.py"),
    )

    def _missing_credit_export(self, rel):
        """rel 导出了 deal_callback 却没导出 credit_account_callback 就算漏。"""
        import io
        path = os.path.join(ROOT, rel.replace("/", os.sep))
        text = io.open(path, encoding="utf-8", errors="replace").read()
        return "deal_callback" in text and "credit_account_callback" not in text

    def _assert_present(self, rel):
        """文件不在了要响亮地挂，否则这条检查会静默地变成空跑。"""
        path = os.path.join(ROOT, rel.replace("/", os.sep))
        self.assertTrue(os.path.exists(path),
                        "名单里的文件不在了，名单该跟着改：%s" % rel)

    def test_every_entry_that_exports_deal_callback_exports_the_credit_one(self):
        for rel in self.ENTRY_FILES:
            self._assert_present(rel)
        missing = [rel for rel in self.ENTRY_FILES
                   if self._missing_credit_export(rel)]
        self.assertEqual(missing, [])

    def test_single_file_builders_export_the_credit_callback(self):
        """产物没生成时查构建脚本；本地生成过就顺手把产物也查了。"""
        missing = []
        for builder, generated in self.GENERATED_ENTRIES:
            self._assert_present(builder)
            if self._missing_credit_export(builder):
                missing.append(builder)
            out = os.path.join(ROOT, generated.replace("/", os.sep))
            if os.path.exists(out) and self._missing_credit_export(generated):
                missing.append(generated)
        self.assertEqual(missing, [])

    def test_strategy_defines_the_callback_and_binds_the_global(self):
        import io
        path = os.path.join(ROOT, "src", "bigqmt_signal_trader_strategy.py")
        text = io.open(path, encoding="utf-8", errors="replace").read()
        self.assertIn("def credit_account_callback(ContextInfo, seq, result):", text)
        # 全局函数要在捕获名单里，否则 handler 永远拿不到它
        self.assertIn('"query_credit_account",', text)


if __name__ == "__main__":
    unittest.main()
