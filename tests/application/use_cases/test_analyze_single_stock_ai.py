from __future__ import annotations

import unittest

from stock_analyse.application.use_cases import analyze_single_stock_ai


class StubOrchestrator:
    def run(self, **kwargs):
        return {
            'stock_snapshot': {
                'trade_date': '2026-04-24',
                'technical': {'score': 90},
                'sentiment': {'sentiment_score': 60},
            },
            'decision': {
                'action': 'buy',
                'confidence': 0.8,
                'scores': {'composite': 78},
            },
            'final_state': {'analyst_outputs': {}},
            'meta': {'duration_ms': 1000},
        }


class AnalyzeSingleStockAITest(unittest.TestCase):
    def test_execute_returns_agentic_response_shape(self):
        result = analyze_single_stock_ai.execute(
            stock_code='600000',
            market='SH',
            orchestrator=StubOrchestrator(),
        )

        self.assertTrue(result['success'])
        self.assertEqual(result['data']['analysis_mode'], 'agentic')
        self.assertEqual(result['data']['decision']['action'], 'buy')
        self.assertEqual(result['data']['scores']['technical'], 90)
        self.assertEqual(result['data']['scores']['sentiment'], 60)


if __name__ == '__main__':
    unittest.main()
