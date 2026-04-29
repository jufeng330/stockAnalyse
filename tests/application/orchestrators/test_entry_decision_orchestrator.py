from __future__ import annotations

import unittest

from stock_analyse.application.dto.entry_decision_state import EntryDecisionState
from stock_analyse.application.orchestrators.entry_decision_orchestrator import EntryDecisionOrchestrator


class StubAnalyzer:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def openai_api_call(self, symbol='', message='你好', instruction=''):
        self.calls.append({'symbol': symbol, 'message': message, 'instruction': instruction})
        return self.responses.pop(0)


class EntryDecisionOrchestratorTest(unittest.TestCase):
    def test_run_adds_template_aligned_summary_markdown(self):
        responses = [
            '{"macro_view":"中性","macro_conclusion":"适合研究","macro_reasoning":"风格偏防御","market_style":"红利","liquidity_signal":"宽松","risks":["波动"],"opportunities":["修复"]}',
            '{"asset_classification":"红利资产","classification_reasoning":"现金流稳定","upside_logic":["分红"],"risk_logic":["估值压缩"],"recommended_playbook":["分批定投"],"forbidden_playbook":["一次性重仓"]}',
            '{"current_stage":"B","stage_reasoning":"修复初期","revenue_growth_view":"持平","profit_growth_view":"改善","cashflow_view":"健康","margin_trend_view":"稳定","expectation_view":"中性偏乐观","stage_risks":["修复失败"]}',
            '{"price_zone":"合理区","zone_reasoning":"估值回到中枢","action_signal":"可以买第一笔","action_reasoning":"适合试错","valuation_comment":"合理","technical_comment":"企稳","cheap_reason":"回撤充分","danger_reason":"接近前高"}',
            '{"suggested_action":"适合买入","action_reasoning":"先小仓位试错","suggested_entry_leg":"第一笔","max_target_position":"15%","current_position":"0%","buy_plan":{"first":"5%"},"rise_plan":{"action":"持有"},"fall_plan":{"action":"观察"},"sell_rules":{"rule":"跌破关键位"},"execution_notes":["不要追高"]}',
            '{"risk_level":"medium","risk_reasoning":"需要分笔执行","key_risks":["回撤"],"invalidation_signals":["跌破支撑"],"position_constraints":{"max":"15%"},"decision_card":{"current_stage":"B","current_price_zone":"合理区","suggested_action":"适合买入","suggested_entry_leg":"第一笔","max_target_position":"15%","execution_summary":"先小仓位试错"},"conclusion_summary":"先小仓位试错"}',
            '## 一、标的基本信息\n\n- 标的名称：测试股份\n\n---\n\n## 二、Step 1：先看宏观，不要一上来就看个股\n\n- 我现在是否应该研究这个标的：是\n\n---\n\n## 十、最终一页决策卡\n\n- 当前阶段：B\n\n## 十一、复盘区（买后再填写）\n\n- 是否按计划执行：是\n\n## 十二、使用纪律\n\n1. 不跳步骤。'
        ]
        analyzer = StubAnalyzer(responses)
        orchestrator = EntryDecisionOrchestrator(
            data_facade=type('DataFacade', (), {
                'build_snapshot': lambda self, **kwargs: {
                    'market_context': {},
                    'reports': {},
                    'technical': {'summary': {}},
                    'sentiment': {},
                }
            })(),
            analyzer_factory=lambda **kwargs: analyzer,
        )
        state = EntryDecisionState(
            session_id='ED-1',
            watch_stock_id='WS-1',
            request={'trade_date': '2026-04-28', 'analysis_depth': 'standard'},
            watch_stock={
                'id': 'WS-1',
                'stock_code': '600900',
                'stock_name': '测试股份',
                'market': 'A股',
                'industry': '电力',
                'asset_type': '红利',
                'current_price': 26.95,
                'pe': 18.5,
            },
            manual_inputs={'position_input': {'current_position': '0%', 'max_target_position': '15%'}},
        )

        result = orchestrator.run(state=state)

        summary_markdown = result.final_result['data']['entry_decision_summary_markdown']
        self.assertIn('## 一、标的基本信息', summary_markdown)
        self.assertIn('## 二、Step 1：先看宏观，不要一上来就看个股', summary_markdown)
        self.assertIn('## 十、最终一页决策卡', summary_markdown)
        self.assertIn('## 十一、复盘区（买后再填写）', summary_markdown)
        self.assertIn('## 十二、使用纪律', summary_markdown)
        self.assertEqual(result.final_result['data']['entry_decision_summary_template'], '进场决策模板_空白实战版')
        self.assertEqual(len(analyzer.calls), 7)


if __name__ == '__main__':
    unittest.main()
