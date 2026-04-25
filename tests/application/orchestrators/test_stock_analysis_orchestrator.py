from __future__ import annotations

import unittest

from stock_analyse.application.orchestrators.stock_analysis_orchestrator import StockAnalysisOrchestrator


class StubTechnicalAnalysisWorkflow:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def run(self, *, stock_code: str, market: str):
        self.calls.append((stock_code, market))
        return 91, {'score': 91, 'stock_code': stock_code}


class StockAnalysisOrchestratorTest(unittest.TestCase):
    def test_get_stock_technical_analysis_uses_src_workflow(self):
        workflow = StubTechnicalAnalysisWorkflow()
        orchestrator = StockAnalysisOrchestrator(technical_analysis_workflow=workflow)

        score, summary = orchestrator.get_stock_technical_analysis('600000', 'SH')

        self.assertEqual(workflow.calls, [('600000', 'SH')])
        self.assertEqual(score, 91)
        self.assertEqual(summary, {'score': 91, 'stock_code': '600000'})


if __name__ == '__main__':
    unittest.main()
