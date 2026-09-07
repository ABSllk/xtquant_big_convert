# coding: utf-8
"""Futures/option callback rows resolved the wrong BUY/SELL direction.

``_extract_direction`` arbitrates direction/offset conflicts through
``_conflict_resolve``, which only recognized the stock opType domain
(23/24) plus the direction/offset enums (48/49/51/52). Some terminals
report futures/ETF-option callbacks with the futures opType table
(futures 0-15, ETF options 50-55) instead; on those the arbiter found
nothing it knew and fell back to offset -- but offset is open/close on
those accounts, so:

  - futures 卖出开空 arrived as direction=49 + offset=48(开仓) + op=3
    and resolved as BUY
  - ETF option 买入平仓 arrived as direction=48 + offset=49(平仓) + op=53
    and resolved as SELL

This change adds the passthrough opType side sets to the arbiter (same
sides as adapters.order_bigqmt._FUTURE_BUY_SIDE and friends, kept local
because order_bigqmt imports from this module). Terminals that report
the stock 23/24 domain for futures rows keep the existing behavior,
pinned below.
"""

import os
import sys
import types
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.exec_events import (
    _action_from_direction,
    _conflict_resolve,
    _extract_direction,
)


def _row(direction, offset, op_type):
    row = types.SimpleNamespace()
    if direction is not None:
        row.m_nDirection = direction
    if offset is not None:
        row.m_nOffsetFlag = offset
    if op_type is not None:
        row.m_nOpType = op_type
    return row


def _side(direction, offset, op_type):
    return _action_from_direction(_extract_direction(_row(direction, offset, op_type)))


class FuturesConflictResolvedByOptype(unittest.TestCase):
    def test_sell_to_open_no_longer_reads_buy(self):
        # 卖出开空: direction=49, offset=48(开仓), op=3(开空) -> SELL
        self.assertEqual(_side(49, 48, 3), "SELL")

    def test_buy_to_close_no_longer_reads_sell(self):
        # 平今空(买入动作): direction=48, offset=49(平仓), op=5 -> BUY
        self.assertEqual(_side(48, 49, 5), "BUY")

    def test_open_long_still_reads_buy(self):
        # 开多: direction=48, offset=48(开仓), op=0 -> BUY (agree, no conflict)
        self.assertEqual(_side(48, 48, 0), "BUY")


class EtfOptionConflictResolvedByOptype(unittest.TestCase):
    def test_sell_to_open_no_longer_reads_buy(self):
        # 卖出开仓: direction=49, offset=48(开仓), op=52 -> SELL
        self.assertEqual(_side(49, 48, 52), "SELL")

    def test_buy_to_close_no_longer_reads_sell(self):
        # 买入平仓: direction=48, offset=49(平仓), op=53 -> BUY
        self.assertEqual(_side(48, 49, 53), "BUY")

    def test_covered_open_still_reads_sell(self):
        # 备兑开仓: direction=49, offset=48, op=54 -> SELL
        self.assertEqual(_side(49, 48, 54), "SELL")


class StockDomainUnchanged(unittest.TestCase):
    def test_stock_sell_conflict_keeps_offset(self):
        # Stock sell: direction=48, offset=49, op=24 -> SELL (existing)
        self.assertEqual(_side(48, 49, 24), "SELL")

    def test_stock_buy_agree(self):
        self.assertEqual(_side(48, 48, 23), "BUY")

    def test_futures_row_on_stock_domain_terminal_keeps_behavior(self):
        # Terminals that report futures callbacks with op 23/24 (the live
        # diagnosis in _conflict_resolve's docstring) arbitrate as before.
        self.assertEqual(_side(49, 48, 24), "SELL")
        self.assertEqual(_side(48, 49, 23), "BUY")


class NoArbiterFallbackPinned(unittest.TestCase):
    def test_deal_row_without_optype_still_trusts_offset(self):
        # DEAL rows carry no m_nOpType: the arbiter has nothing to consult
        # and offset stays the fallback. A futures sell-to-open trade row
        # therefore still reads 48 -> BUY; documented limitation, unchanged
        # by this fix.
        self.assertEqual(_side(49, 48, None), "BUY")


class ConflictResolveDirect(unittest.TestCase):
    def test_futures_sell_side_prefers_direction(self):
        row = _row(49, 48, 3)
        self.assertEqual(_conflict_resolve(49, 48, row), 49)

    def test_futures_buy_side_prefers_direction(self):
        row = _row(48, 49, 5)
        self.assertEqual(_conflict_resolve(48, 49, row), 48)

    def test_etf_option_sell_side_prefers_direction(self):
        row = _row(49, 48, 52)
        self.assertEqual(_conflict_resolve(49, 48, row), 49)

    def test_no_known_optype_falls_back_to_offset(self):
        row = _row(49, 48, 999)
        self.assertEqual(_conflict_resolve(49, 48, row), 48)


if __name__ == "__main__":
    unittest.main()
