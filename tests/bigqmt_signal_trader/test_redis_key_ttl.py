# coding: utf-8
"""#213：redis 里不留没用的键。

线上统计（账号 8886800503，休市期间）暴露三件事：

  1. position_events 以 **10 条/秒** 无条件写 —— 每个 adjust tick 一条，
     休市、持仓一动没动也照写，每天 86 万条。而 maxlen=2000 把可用历史
     压到了 200 秒：想回放持仓变动，三分半钟以前的已经被空转刷掉了。
  2. **七个事件流全是永久键**（position_events 843KB、order_events 1.41MB…）。
     maxlen 只挡单键膨胀，挡不住键本身永远不消失。
  3. 账号 ID 为空时照样建键，扫出过一个 "bigqmt:position_events:"（9 条）。
"""
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader import exec_events
from bigqmt_signal_trader.adapters.position_sync_redis import RedisPositionSyncSink
from bigqmt_signal_trader.adapters.redis_common import EVENT_STREAM_TTL_SECONDS
from bigqmt_signal_trader.adapters import signal_redis


class _Redis(object):
    """记下所有写操作，好断言「写了什么」和「没写什么」。"""

    def __init__(self):
        self.xadds = []
        self.setexs = []
        self.sets = []
        self.expires = []
        self.publishes = []

    def xadd(self, key, fields, maxlen=None, approximate=None):
        self.xadds.append((key, fields, maxlen))
        return "1-1"

    def setex(self, key, ttl, value):
        self.setexs.append((key, ttl, value))

    def set(self, key, value):
        self.sets.append((key, value))

    def expire(self, key, ttl):
        self.expires.append((key, ttl))

    def publish(self, key, raw):
        self.publishes.append((key, raw))


class _Asset(object):
    cash = 100.0
    total_asset = 200.0
    frozen_cash = 0.0
    market_value = 100.0


class _Position(object):
    def __init__(self, volume=100):
        self.stock_code = "600000.SH"
        self.volume = volume
        self.available = volume
        self.cost = 10.0
        self.stock_name = "浦发银行"


class _Snapshot(object):
    def __init__(self, account_id="acct", volume=100, reason="tick",
                 updated_at="2026-09-07 09:30:00"):
        self.account_id = account_id
        self.reason = reason
        self.updated_at = updated_at
        self.asset = _Asset()
        self.positions = {"600000.SH": _Position(volume)}


class UnchangedSnapshotIsNotRewrittenTest(unittest.TestCase):
    """publish() 每个 adjust tick 被调一次 —— 没变就不该写。"""

    def test_first_publish_writes(self):
        r = _Redis()
        RedisPositionSyncSink(r).publish(_Snapshot())
        self.assertEqual(len(r.setexs), 1)
        self.assertEqual(len(r.xadds), 1)

    def test_identical_snapshot_writes_nothing_more(self):
        r = _Redis()
        sink = RedisPositionSyncSink(r)
        for _ in range(50):
            sink.publish(_Snapshot())
        self.assertEqual(len(r.setexs), 1, "重复快照又写了 %d 次" % len(r.setexs))
        self.assertEqual(len(r.xadds), 1, "重复快照又写了 %d 条流" % len(r.xadds))

    def test_the_string_key_is_still_kept_alive_while_silent(self):
        """string 那份带 120 秒 TTL —— 静默期要续期，否则持仓查询会落空。"""
        r = _Redis()
        sink = RedisPositionSyncSink(r)
        sink.publish(_Snapshot())
        sink.publish(_Snapshot())
        self.assertIn(("bigqmt:positions:acct", 120), r.expires)

    def test_a_ticking_clock_alone_is_not_a_change(self):
        """updated_at 是秒级时间戳，每秒变一次。

        第一版拿整个 payload 去重，结果被它骗过：写入只从 10 条/秒降到
        1 条/秒。实测相邻两条之间唯一不同的字段就是 updated_at，
        account_id / reason / asset / positions 全部相同（#213）。
        """
        r = _Redis()
        sink = RedisPositionSyncSink(r)
        for second in range(30, 60):
            sink.publish(_Snapshot(updated_at="2026-09-07 09:30:%02d" % second))
        self.assertEqual(len(r.xadds), 1,
                         "只有时间戳在动，却写了 %d 条" % len(r.xadds))
        self.assertEqual(len(r.setexs), 1)

    def test_a_real_change_writes_again(self):
        r = _Redis()
        sink = RedisPositionSyncSink(r)
        sink.publish(_Snapshot(volume=100))
        sink.publish(_Snapshot(volume=100))
        sink.publish(_Snapshot(volume=200))
        self.assertEqual(len(r.xadds), 2)
        self.assertEqual(len(r.setexs), 2)

    def test_changing_back_and_forth_is_not_suppressed(self):
        """只比对上一次，不能因为「见过」就永久跳过。"""
        r = _Redis()
        sink = RedisPositionSyncSink(r)
        for volume in (100, 200, 100, 200):
            sink.publish(_Snapshot(volume=volume))
        self.assertEqual(len(r.xadds), 4)


class EmptyAccountIdTest(unittest.TestCase):
    """账号 ID 为空会拼出 "bigqmt:position_events:" 这种畸形键，而且永不过期。"""

    def test_empty_account_id_writes_nothing(self):
        for bad in ("", "   ", None):
            r = _Redis()
            RedisPositionSyncSink(r).publish(_Snapshot(account_id=bad))
            self.assertEqual(r.xadds, [], repr(bad))
            self.assertEqual(r.setexs, [], repr(bad))
            self.assertEqual(r.sets, [], repr(bad))

    def test_a_real_account_id_still_writes(self):
        r = _Redis()
        RedisPositionSyncSink(r).publish(_Snapshot(account_id="8886800503"))
        self.assertEqual(r.xadds[0][0], "bigqmt:position_events:8886800503")


class EveryStreamGetsATtlTest(unittest.TestCase):
    """maxlen 挡不住键本身永远不消失。"""

    def test_position_event_stream_is_given_a_ttl(self):
        r = _Redis()
        RedisPositionSyncSink(r).publish(_Snapshot())
        self.assertIn(("bigqmt:position_events:acct", EVENT_STREAM_TTL_SECONDS),
                      r.expires)

    def test_exec_event_streams_are_given_a_ttl(self):
        for publish in (exec_events.publish_order_event,
                        exec_events.publish_trade_event,
                        exec_events.publish_order_error_event,
                        exec_events.publish_cancel_error_event):
            r = _Redis()
            publish(r, "acct", {"kind": "x"})
            self.assertEqual(len(r.xadds), 1, publish.__name__)
            stream_key = r.xadds[0][0]
            self.assertIn((stream_key, EVENT_STREAM_TTL_SECONDS), r.expires,
                          "%s 写了 %s 却没给它过期时间" % (publish.__name__, stream_key))

    def test_signal_stream_is_given_a_ttl(self):
        r = _Redis()
        signal_redis.push_trade_signal(r, {"account_id": "acct", "code": "600000.SH"})
        self.assertEqual(len(r.xadds), 1)
        self.assertIn((r.xadds[0][0], EVENT_STREAM_TTL_SECONDS), r.expires)

    def test_ttl_is_a_day_not_a_minute(self):
        """太短会把回放窗口砍掉：一天覆盖「昨天收盘到今天开盘」。"""
        self.assertEqual(EVENT_STREAM_TTL_SECONDS, 86400)

    def test_a_failing_expire_never_breaks_the_write(self):
        """续期是清理，不是主路径 —— 它失败不能让事件丢掉。"""
        class _Exploding(_Redis):
            def expire(self, key, ttl):
                raise RuntimeError("no expire here")

        r = _Exploding()
        exec_events.publish_order_event(r, "acct", {"kind": "x"})
        self.assertEqual(len(r.xadds), 1)
        self.assertEqual(len(r.publishes), 1)


if __name__ == "__main__":
    unittest.main()
