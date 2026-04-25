from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from scanner.stock_financial_analyser import StockFinancialAnalyser
from scanner.stock_report_analyser import StockReportAnalyser


class LegacyFinancialReportAnalyzersCompatTest(unittest.TestCase):
    @patch('scanner.stock_financial_analyser.execute_get_financial_codes')
    def test_financial_analyser_delegates_to_src_use_case(self, mock_execute):
        expected_filter = pd.DataFrame([{'股票代码': '600000', '净利润': 10_000_000}])
        expected_summary = pd.DataFrame([{'股票代码_': '600000', '净利润_2025': 10_000_000}])
        mock_execute.return_value = (expected_filter, expected_summary)

        analyzer = StockFinancialAnalyser(market='SH')
        actual_filter, actual_summary = analyzer.get_financial_codes(
            strategy_filter='continue',
            threshold_1=1,
            threshold_2=2,
            threshold_3=3,
        )

        mock_execute.assert_called_once_with(
            market='SH',
            strategy_filter='continue',
            threshold_1=1,
            threshold_2=2,
            threshold_3=3,
        )
        self.assertIs(actual_filter, expected_filter)
        self.assertIs(actual_summary, expected_summary)

    @patch('scanner.stock_report_analyser.execute_get_report_codes')
    def test_report_analyser_delegates_to_src_use_case(self, mock_execute):
        expected = (
            pd.DataFrame([{'股票代码': '600000', '资产负债率': 35}]),
            pd.DataFrame([{'股票代码': '600000', '净利润': 10_000_000}]),
            pd.DataFrame([{'股票代码': '600000', '净现金流-净现金流': 5_000_000}]),
            pd.DataFrame([{'股票代码_': '600000', '资产负债率_2025': 35}]),
            pd.DataFrame([{'股票代码_': '600000', '净利润_2025': 10_000_000}]),
            pd.DataFrame([{'股票代码_': '600000', '净现金流-净现金流_2025': 5_000_000}]),
        )
        mock_execute.return_value = expected

        analyzer = StockReportAnalyser(market='SH')
        actual = analyzer.get_report_codes(strategy_filter='avg')

        mock_execute.assert_called_once_with(market='SH', strategy_filter='avg')
        self.assertEqual(len(actual), 6)
        for index, frame in enumerate(expected):
            self.assertIs(actual[index], frame)


if __name__ == '__main__':
    unittest.main()
