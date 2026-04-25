from __future__ import annotations

import logging
import unittest

import pandas as pd

from stock_analyse.domain.strategies.financial_report_filter_service import FinancialReportFilterService


class StubStockStrategy:
    def __init__(self):
        self.calls: list[tuple[str, str, str, int, float, str]] = []

    def get_stock_continue_postive(self, df_financial, date, col_name, col_adjustment, continue_year=1, threshold=0, condition_type='>'):
        self.calls.append((date, col_name, col_adjustment, continue_year, threshold, condition_type))
        if col_name == '资产负债率':
            self._assert_report_date(df_financial)
            return {'600000', '600001'}
        if col_name == '资产-总资产':
            return {'600000'}
        if col_name == '资产-总资产同比':
            return {'600000', '600002'}
        return set()

    @staticmethod
    def _assert_report_date(df_financial):
        assert '报告期' in df_financial.columns
        assert list(df_financial['报告期']) == ['2025-03-31', '2024-03-31']


class FinancialReportFilterServiceTest(unittest.TestCase):
    def test_process_financial_metrics_uses_announcement_date_and_intersection(self):
        service = FinancialReportFilterService()
        stock_strategy = StubStockStrategy()
        df_report = pd.DataFrame([
            {'股票代码': '600000', '公告日期': '2025-03-31', '资产负债率': 35, '资产-总资产': 200000000000, '资产-总资产同比': 6},
            {'股票代码': '600001', '公告日期': '2024-03-31', '资产负债率': 45, '资产-总资产': 100000000000, '资产-总资产同比': 4},
        ])
        metrics = [
            ['资产负债率', '资产负债率要求', 85, '<'],
            ['资产-总资产', '总资产要求', 100000000000, '>'],
            ['资产-总资产同比', '总资产同比要求', 5, '>'],
        ]

        results, intersection = service.process_financial_metrics(
            df_financial=df_report,
            date_financial='2023-03-31',
            metrics_array=metrics,
            stock_strategy=stock_strategy,
            logger=logging.getLogger(__name__),
        )

        self.assertEqual(results, [{'600000', '600001'}, {'600000'}, {'600000', '600002'}])
        self.assertEqual(intersection, {'600000'})
        self.assertEqual(
            stock_strategy.calls,
            [
                ('2023-03-31', '资产负债率', '资产负债率要求', 1, 85, '<'),
                ('2023-03-31', '资产-总资产', '总资产要求', 1, 100000000000, '>'),
                ('2023-03-31', '资产-总资产同比', '总资产同比要求', 1, 5, '>'),
            ],
        )


if __name__ == '__main__':
    unittest.main()
