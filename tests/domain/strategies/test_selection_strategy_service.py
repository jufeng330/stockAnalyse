from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy


class StubFileUtils:
    def create_middle_file(self, file_name, df):
        return None


class StubStock:
    def get_stock_spot(self):
        return pd.DataFrame([
            {'代码': '600000', '股票代码': '600000', '总市值': 60000000000, '市盈率-动态': 10, '资产负债率': 0.4},
            {'代码': '600001', '股票代码': '600001', '总市值': 40000000000, '市盈率-动态': 20, '资产负债率': 0.8},
        ])


class StubStockUtils:
    def pd_convert_to_float(self, df, col):
        df = df.copy()
        df[col] = df[col].astype(float)
        return df


class StubDividendAnalyser:
    def __init__(self, market='SH') -> None:
        self.market = market

    def get_fh_codes(self, type='avg', threshold=0.03):
        df_stock_fh = pd.DataFrame([{'代码': '600000', '年份': 2024, '现金分红-股息率': 0.05}])
        df_summary = pd.DataFrame([{'代码': '600000', '平均股息率': 0.05}])
        return df_stock_fh, df_summary


class SelectionStrategyServiceTest(unittest.TestCase):
    def test_conservative_strategy_uses_src_dividend_analyser(self):
        service = SelectionStrategyService()
        selector = StockSelectStrategy(market='SH', strategy_type=3)
        selector.stock = StubStock()
        selector.stock_utils = StubStockUtils()
        selector.file_utils = StubFileUtils()

        with patch(
            'stock_analyse.domain.strategies.selection_strategy_service.StockFenHengAnalyser',
            StubDividendAnalyser,
        ):
            result = service.conservative_strategy(df_stock=None, market='SH', strategy_filter='avg', selector=selector)

        self.assertEqual(list(result['代码']), ['600000'])
        self.assertIn('平均股息率', result.columns)


if __name__ == '__main__':
    unittest.main()
