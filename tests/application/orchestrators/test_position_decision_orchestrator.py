from __future__ import annotations

import unittest

from stock_analyse.application.orchestrators.position_decision_orchestrator import (
    PositionDecisionOrchestrator,
    PositionDecisionOutputError,
)


class StubAnalyzer:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def openai_api_call(
        self,
        symbol='',
        message='你好',
        instruction='',
        *,
        tools=None,
        tool_choice=None,
        response_format=None,
        require_tool_call=False,
    ):
        self.calls.append(
            {
                'symbol': symbol,
                'message': message,
                'instruction': instruction,
                'tools': tools,
                'tool_choice': tool_choice,
                'response_format': response_format,
                'require_tool_call': require_tool_call,
            }
        )
        return self.response


class PositionDecisionOrchestratorTest(unittest.TestCase):
    def _build_context(self):
        return {
            'holding_stock': {
                'id': 'HS-1',
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
            },
            'watch_stock': {
                'id': 'WS-1',
                'stock_code': '600519',
                'stock_name': '贵州茅台',
                'market': 'A股',
            },
            'request': {
                'trade_date': '2026-04-29',
                'analysis_depth': 'deep',
            },
            'financial_context': {'reports': {'income': 'ok'}},
            'trade_history_context': {'trades': [{'price': 1600}]},
            'holding_plan_context': {'latest_trade_plan': {'summary': '控制仓位'}},
            'supporting_context': {},
            'data_source': 'holding_snapshot',
        }

    def test_run_uses_json_output_and_returns_structured_result(self):
        analyzer = StubAnalyzer(
            {
                'recommended_action': 'watch',
                'decision_status': 'observe',
                'confidence': 'medium',
                'conclusion_summary': '继续观察，等待更明确的边际变化。',
                'tabs': [
                    {'id': 'trigger', 'title': '触发条件', 'summary': '暂无明确加减仓触发。', 'evidence': ['等待新的财报或价格触发']},
                    {'id': 'reason', 'title': '核心理由', 'summary': '赔率暂时一般。', 'evidence': ['估值尚未明显低估']},
                    {'id': 'execution', 'title': '执行注意事项', 'summary': '先跟踪不追价。', 'evidence': ['保留流动性和后手']},
                    {'id': 'risk', 'title': '风险分析', 'summary': '若基本面继续走弱需更谨慎。', 'evidence': ['增长预期可能下修']},
                    {'id': 'conclusion', 'title': '结论', 'summary': '继续观察。', 'evidence': ['前四项尚未形成明确动作共识']},
                ],
            }
        )
        orchestrator = PositionDecisionOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        result = orchestrator.run(context=self._build_context())

        self.assertTrue(result['success'])
        self.assertEqual(result['data']['decision']['action'], 'watch')
        self.assertEqual(result['data']['decision']['status'], 'observe')
        self.assertEqual(result['data']['decision']['confidence'], 'medium')
        self.assertEqual(result['data']['tabs'][4]['title'], '结论')
        self.assertEqual(result['data']['tabs'][0]['title'], '触发条件')
        self.assertEqual(len(analyzer.calls), 1)
        self.assertIsNone(analyzer.calls[0]['tool_choice'])
        self.assertFalse(analyzer.calls[0]['require_tool_call'])
        self.assertIsNone(analyzer.calls[0]['tools'])
        self.assertIn('JSON Schema', analyzer.calls[0]['message'])

    def test_run_rejects_invalid_tab_order(self):
        analyzer = StubAnalyzer(
            {
                'recommended_action': 'watch',
                'decision_status': 'observe',
                'confidence': 'medium',
                'conclusion_summary': '继续观察。',
                'tabs': [
                    {'id': 'risk', 'title': '风险分析', 'summary': '先说风险。', 'evidence': ['顺序错了']},
                    {'id': 'reason', 'title': '核心理由', 'summary': '赔率一般。', 'evidence': ['缺催化']},
                    {'id': 'execution', 'title': '执行注意事项', 'summary': '保持观察。', 'evidence': ['不追价']},
                    {'id': 'trigger', 'title': '触发条件', 'summary': '暂无。', 'evidence': ['等待信号']},
                    {'id': 'conclusion', 'title': '结论', 'summary': '继续观察。', 'evidence': ['顺序不合规']},
                ],
            }
        )
        orchestrator = PositionDecisionOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        with self.assertRaises(PositionDecisionOutputError):
            orchestrator.run(context=self._build_context())

    def test_run_rejects_non_object_response(self):
        analyzer = StubAnalyzer('not-json')
        orchestrator = PositionDecisionOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        with self.assertRaises(PositionDecisionOutputError):
            orchestrator.run(context=self._build_context())


if __name__ == '__main__':
    unittest.main()
