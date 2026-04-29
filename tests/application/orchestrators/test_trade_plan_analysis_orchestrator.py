from __future__ import annotations

import unittest

from stock_analyse.application.orchestrators.trade_plan_analysis_orchestrator import TradePlanAnalysisOrchestrator


class StubAnalyzer:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def openai_api_call(self, symbol='', message='你好', instruction=''):
        self.calls.append({'symbol': symbol, 'message': message, 'instruction': instruction})
        return self.response


class TradePlanAnalysisOrchestratorTest(unittest.TestCase):
    def test_run_returns_markdown_and_structured_summary(self):
        analyzer = StubAnalyzer(
            '{'
            '"trade_plan_markdown":"## 一、计划摘要\\n\\n- 标的名称：测试股份\\n\\n---\\n\\n## 二、买前约束条件\\n\\n- 当前价值阶段是否仍成立：是\\n\\n---\\n\\n## 三、建仓计划\\n\\n- 最大目标仓位：20%\\n\\n---\\n\\n## 四、价格与下单执行设计\\n\\n- 当前价格：26.95\\n\\n---\\n\\n## 五、事件与风险窗口检查\\n\\n- 下次财报日期：待确认\\n\\n---\\n\\n## 六、失败预案与退出计划\\n\\n- 单笔最大允许亏损：2%",'
            '"decision":{'
            '"action":"buy",'
            '"summary":"先试错后确认。",'
            '"logic":"估值回归中枢，适合分笔推进。",'
            '"risk_level":"medium",'
            '"risks":["波动仍在"],'
            '"time_horizon":"5-15 trading days",'
            '"position_suggestion":{'
            '"target_position":"20%",'
            '"position_limit":"20%",'
            '"add_condition":"回踩不破支撑后加仓",'
            '"reduce_condition":"跌破观察位先减仓",'
            '"stop_loss_reference":"跌破关键支撑位退出"'
            '}'
            '},'
            '"plan_metadata":{'
            '"template_name":"持仓计划模板（买前执行版）",'
            '"data_source":"cache_first",'
            '"cache_hits":["A股_600900_测试股份_20260428_进场决策.md"]'
            '}'
            '}'
        )
        orchestrator = TradePlanAnalysisOrchestrator(analyzer_factory=lambda **kwargs: analyzer)
        result = orchestrator.run(
            context={
                'watch_stock': {
                    'id': 'WS-1',
                    'stock_code': '600900',
                    'stock_name': '测试股份',
                    'market': 'A股',
                },
                'request': {
                    'trade_date': '2026-04-28',
                    'plan_type': '三笔计划',
                    'risk_preference': '中高风险',
                },
                'template_markdown': '## 一、计划摘要\n## 二、买前约束条件\n## 三、建仓计划\n## 四、价格与下单执行设计\n## 五、事件与风险窗口检查\n## 六、失败预案与退出计划',
                'cache_context': {
                    'cache_hits': ['A股_600900_测试股份_20260428_进场决策.md'],
                    'entry_decision_markdown': '进场决策内容',
                    'stock_analysis_markdown': '股票分析内容',
                },
                'fallback_context': {'snapshot': {}},
                'data_source': 'cache_first',
            }
        )

        data = result['data']
        self.assertIn('## 一、计划摘要', data['trade_plan_markdown'])
        self.assertIn('## 二、买前约束条件', data['trade_plan_markdown'])
        self.assertIn('## 三、建仓计划', data['trade_plan_markdown'])
        self.assertIn('## 四、价格与下单执行设计', data['trade_plan_markdown'])
        self.assertIn('## 五、事件与风险窗口检查', data['trade_plan_markdown'])
        self.assertIn('## 六、失败预案与退出计划', data['trade_plan_markdown'])
        self.assertEqual(data['decision']['action'], 'buy')
        self.assertEqual(data['decision']['position_suggestion']['target_position'], '20%')
        self.assertEqual(data['meta']['data_source'], 'cache_first')
        self.assertEqual(len(analyzer.calls), 1)


if __name__ == '__main__':
    unittest.main()
