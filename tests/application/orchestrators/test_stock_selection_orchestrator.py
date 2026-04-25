from __future__ import annotations

import unittest

import pandas as pd

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator


class StubFileUtils:
    def __init__(self) -> None:
        self.saved_by_price = None

    def save_results_by_price(self, results):
        self.saved_by_price = results


class StubWorkflow:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.file_utils = StubFileUtils()

    def run(self, *, market: str, strategy_type: int, batch_size: int, strategy_filter: str):
        self.calls.append((market, strategy_type, batch_size, strategy_filter))
        return self.file_utils, [
            {
                '股票代码': '600000',
                '评分': '88.0',
                '当前价格': '¥10',
                '涨跌幅': '1.2%',
                '投资建议': '建议买入',
                '建议详情': 'ok',
            }
        ]


class StubBacktestWorkflow:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def run(self, *, market: str, high_score_stocks: list[dict], analysis_date: str):
        self.calls.append((market, high_score_stocks, analysis_date))
        return pd.DataFrame(high_score_stocks), 'stats'


class StockSelectionOrchestratorTest(unittest.TestCase):
    def test_run_full_market_scan_uses_src_workflow_instead_of_legacy_scanner(self):
        workflow = StubWorkflow()
        orchestrator = StockSelectionOrchestrator(full_market_scan_workflow=workflow)

        file_utils, high_score_stocks = orchestrator.run_full_market_scan(
            market='SH',
            strategy_type=2,
            batch_size=30,
            strategy_filter='avg',
        )

        self.assertEqual(workflow.calls, [('SH', 2, 30, 'avg')])
        self.assertIs(file_utils, workflow.file_utils)
        self.assertEqual(
            high_score_stocks,
            [
                {
                    '股票代码': '600000',
                    '评分': '88.0',
                    '当前价格': '¥10',
                    '涨跌幅': '1.2%',
                    '投资建议': '建议买入',
                    '建议详情': 'ok',
                }
            ],
        )

    def test_run_web_selection_saves_grouped_results_from_src_workflow(self):
        workflow = StubWorkflow()
        orchestrator = StockSelectionOrchestrator(full_market_scan_workflow=workflow)

        file_utils, high_score_stocks = orchestrator.run_web_selection(market='SH', strategy_type=1)

        self.assertEqual(workflow.calls, [('SH', 1, 20, 'avg')])
        self.assertEqual(file_utils.saved_by_price, high_score_stocks)

    def test_backtest_stocks_uses_src_backtest_workflow(self):
        workflow = StubBacktestWorkflow()
        orchestrator = StockSelectionOrchestrator(backtest_workflow=workflow)

        df_result, stats = orchestrator.backtest_stocks(
            market='SH',
            high_score_stocks=[{'股票代码': '600000', '评分': '88.0'}],
            analysis_date='2025-06-06',
        )

        self.assertEqual(workflow.calls, [('SH', [{'股票代码': '600000', '评分': '88.0'}], '2025-06-06')])
        self.assertEqual(list(df_result['股票代码']), ['600000'])
        self.assertEqual(stats, 'stats')


if __name__ == '__main__':
    unittest.main()
