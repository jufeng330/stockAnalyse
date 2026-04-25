from __future__ import annotations

import unittest

from stock_analyse.application.workflows.full_market_scan_workflow import FullMarketScanWorkflow
from stock_analyse.application.workflows.technical_analysis_workflow import TechnicalAnalysisWorkflow


class FullMarketScanWorkflowTest(unittest.TestCase):
    def test_build_runtime_uses_src_selector_and_file_utils(self):
        workflow = FullMarketScanWorkflow()

        file_utils, runtime = workflow.build_runtime(market='SH', strategy_type=1)

        self.assertEqual(file_utils.__class__.__module__, 'stock_analyse.infrastructure.persistence.stock_file_utils')
        self.assertEqual(runtime.selector.__class__.__module__, 'stock_analyse.domain.strategies.stock_select_strategy')
        self.assertIsInstance(runtime.analyzer, TechnicalAnalysisWorkflow)


if __name__ == '__main__':
    unittest.main()
