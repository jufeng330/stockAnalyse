from __future__ import annotations

import unittest

from stock_analyse.application.orchestrators.holding_review_orchestrator import (
    HoldingReviewOrchestrator,
    HoldingReviewOutputError,
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


class HoldingReviewOrchestratorTest(unittest.TestCase):
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
                'review_type': 'weekly',
                'period_key': '2026-W18',
                'analysis_depth': 'deep',
            },
            'trade_history_context': {'trades': [{'price': 1600}]},
            'entry_context': {'latest_entry_decision': {'summary': '原始买入逻辑'}},
            'reanalysis_context': {'latest_reanalysis': {'summary': '最近复核'}},
            'position_decision_context': {'latest_position_decision': {'summary': '最近减仓讨论'}},
            'financial_context': {'reports': {'income': 'ok'}},
            'market_context': {'technical': {'trend': 'up'}},
            'review_focus_context': {'unrealized_pnl_pct': 12.6},
            'data_source': 'holding_snapshot',
        }

    def test_run_uses_json_output_and_returns_structured_result(self):
        analyzer = StubAnalyzer(
            {
                'performance_summary': '结果尚可，但没有明显超额收益。',
                'execution_summary': '执行节奏基本合规。',
                'risk_summary': '短期波动放大，风险有所上升。',
                'discipline_summary': '总体遵守计划，但止盈纪律一般。',
                'next_action_summary': '继续跟踪冲高后的承接，必要时准备减仓。',
                'conclusion_tag': 'prepare_reduce',
                'tabs': [
                    {'id': 'execution_review', 'title': '执行与卖出复盘', 'summary': '执行节奏基本合规。', 'evidence': ['没有追涨加仓']},
                    {'id': 'result_review', 'title': '结果复盘', 'summary': '结果尚可。', 'evidence': ['维持浮盈']},
                    {'id': 'discipline_review', 'title': '方法与纪律', 'summary': '止盈纪律一般。', 'evidence': ['高位兑现不够果断']},
                    {'id': 'next_action', 'title': '后续动作', 'summary': '准备减仓。', 'evidence': ['波动放大且赔率下降']},
                ],
            }
        )
        orchestrator = HoldingReviewOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        result = orchestrator.run(context=self._build_context())

        self.assertTrue(result['success'])
        self.assertEqual(result['data']['conclusion_tag'], 'prepare_reduce')
        self.assertEqual(result['data']['tabs'][0]['title'], '执行与卖出复盘')
        self.assertEqual(result['data']['tabs'][3]['title'], '后续动作')
        self.assertEqual(len(analyzer.calls), 1)
        self.assertIsNone(analyzer.calls[0]['tool_choice'])
        self.assertFalse(analyzer.calls[0]['require_tool_call'])
        self.assertIsNone(analyzer.calls[0]['tools'])
        self.assertIn('JSON Schema', analyzer.calls[0]['message'])

    def test_run_rejects_invalid_tab_order(self):
        analyzer = StubAnalyzer(
            {
                'performance_summary': '结果尚可。',
                'execution_summary': '执行一般。',
                'risk_summary': '风险上升。',
                'discipline_summary': '纪律一般。',
                'next_action_summary': '继续观察。',
                'conclusion_tag': 'need_recheck',
                'tabs': [
                    {'id': 'result_review', 'title': '结果复盘', 'summary': '顺序错了。', 'evidence': ['第一个 tab 不对']},
                    {'id': 'execution_review', 'title': '执行与卖出复盘', 'summary': '执行一般。', 'evidence': ['存在偏差']},
                    {'id': 'discipline_review', 'title': '方法与纪律', 'summary': '纪律一般。', 'evidence': ['需改进']},
                    {'id': 'next_action', 'title': '后续动作', 'summary': '继续观察。', 'evidence': ['等待确认']},
                ],
            }
        )
        orchestrator = HoldingReviewOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        with self.assertRaises(HoldingReviewOutputError):
            orchestrator.run(context=self._build_context())

    def test_run_rejects_non_object_response(self):
        analyzer = StubAnalyzer('not-json')
        orchestrator = HoldingReviewOrchestrator(analyzer_factory=lambda **kwargs: analyzer)

        with self.assertRaises(HoldingReviewOutputError):
            orchestrator.run(context=self._build_context())


if __name__ == '__main__':
    unittest.main()
