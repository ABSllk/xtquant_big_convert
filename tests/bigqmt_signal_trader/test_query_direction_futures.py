# coding: utf-8
"""Futures/option query rows reported the wrong BUY/SELL side.

``_action_from_offset_flag`` mapped m_nOffsetFlag straight to the side
(48=BUY, 49=SELL). That mapping is a stock fact: for futures and ETF
options, m_nOffsetFlag is open/close (48=开仓, 49=平仓), so on those
accounts the side must come from m_nOpType. Concretely, on a live
futures account:

  - 卖出开空 carries offset=48(开仓) and was reported as BUY
  - 买入平仓 (ETF option opType 53) carries offset=49(平仓) and was
    reported as SELL

Every passthrough opType already has a side recorded in
``passthrough_action_of`` (futures 0-15, ETF options 50-55); this change
makes the query path consult it for FUTURE / STOCK_OPTION requests and
keeps the offset fallback for rows without a usable opType (56/57
exercise rows have no side at all). Stock requests are untouched.
"""

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.adapters.order_bigqmt import _action_from_offset_flag


class StockMappingUntouched(unittest.TestCase):
    def test_stock_offset_is_the_side(self):
        self.assertEqual(_action_from_offset_flag(48, None, "STOCK"), "BUY")
        self.assertEqual(_action_from_offset_flag(49, None, "STOCK"), "SELL")

    def test_stock_ignores_optype(self):
        # A stock request keeps the offset mapping even if a stray op_type
        # is present -- opTypes 0-15/50-55 are not stock opTypes.
        self.assertEqual(_action_from_offset_flag(48, 24, "STOCK"), "BUY")
        self.assertEqual(_action_from_offset_flag(49, 23, "STOCK"), "SELL")

    def test_no_account_type_keeps_stock_mapping(self):
        self.assertEqual(_action_from_offset_flag(48, 3, None), "BUY")
        self.assertEqual(_action_from_offset_flag(49, 3, None), "SELL")


class FuturesSideFromOptype(unittest.TestCase):
    def test_sell_to_open_no_longer_reads_buy(self):
        # offset=48 means 开仓 for futures; opType 3 is 开空, a SELL action.
        self.assertEqual(_action_from_offset_flag(48, 3, "FUTURE"), "SELL")

    def test_open_long_still_buy(self):
        self.assertEqual(_action_from_offset_flag(48, 0, "FUTURE"), "BUY")

    def test_close_long_still_sell(self):
        # opType 1 平昨多 is a SELL action and offset=49 agrees by luck.
        self.assertEqual(_action_from_offset_flag(49, 1, "FUTURE"), "SELL")

    def test_close_short_reads_buy(self):
        # opType 5 平今空 is a BUY action but offset=49(平仓) reads SELL.
        self.assertEqual(_action_from_offset_flag(49, 5, "FUTURE"), "BUY")

    def test_sideless_optype_falls_back_to_offset(self):
        # 56 认购行权 has no side; the row keeps its offset mapping.
        self.assertEqual(_action_from_offset_flag(48, 56, "FUTURE"), "BUY")

    def test_missing_optype_falls_back_to_offset(self):
        self.assertEqual(_action_from_offset_flag(48, None, "FUTURE"), "BUY")


class EtfOptionSideFromOptype(unittest.TestCase):
    def test_sell_to_open_no_longer_reads_buy(self):
        # opType 52 卖出开仓: offset=48(开仓) read as BUY before this fix.
        self.assertEqual(_action_from_offset_flag(48, 52, "STOCK_OPTION"), "SELL")

    def test_buy_to_close_no_longer_reads_sell(self):
        # opType 53 买入平仓: offset=49(平仓) read as SELL before this fix.
        self.assertEqual(_action_from_offset_flag(49, 53, "STOCK_OPTION"), "BUY")

    def test_covered_open_still_sell(self):
        self.assertEqual(_action_from_offset_flag(48, 54, "STOCK_OPTION"), "SELL")


if __name__ == "__main__":
    unittest.main(verbosity=2)
