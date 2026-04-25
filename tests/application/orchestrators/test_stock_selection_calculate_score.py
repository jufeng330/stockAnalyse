from __future__ import annotations

import unittest

from stock_analyse.application.orchestrators.stock_selection_orchestrator import StockSelectionOrchestrator


class StubTechnicalAnalysisWorkflow:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def analyze_stock(self, stock, market: str):
        self.calls.append((stock['代码'], market))
        return {'stock_code': stock['代码'], 'score': 77}


class StockSelectionCalculateScoreTest(unittest.TestCase):
    def test_calculate_score_uses_src_technical_analysis_workflow(self):
        workflow = StubTechnicalAnalysisWorkflow()
        orchestrator = StockSelectionOrchestrator(technical_analysis_workflow=workflow)

        result = orchestrator.calculate_score('SH', '600000')

        self.assertEqual(workflow.calls, [('600000', 'SH')])
        self.assertEqual(result, {'stock_code': '600000', 'score': 77})


if __name__ == '__main__':
    unittest.main()
