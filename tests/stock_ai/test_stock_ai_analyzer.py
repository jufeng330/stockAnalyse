from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from stock_analyse.infrastructure.config.settings import load_settings
from stock_analyse.infrastructure.llm.stock_ai_analyzer import StockAiAnalyzer


class StubStockCompanyInfo:
    def __init__(self, market, symbol):
        self.market = market
        self.symbol = symbol

    def get_stock_name(self):
        return '岳阳林纸'

    def get_stock_zyjs(self):
        return pd.DataFrame([{'主营业务': '造纸'}])

    def get_stock_individual_info_em(self):
        return pd.DataFrame([{'item': '行业', 'value': '造纸'}]), '2010-01-01', '造纸'

    def get_stock_fund_flow(self):
        return pd.DataFrame()

    def get_stock_industry_by_code(self, code):
        return pd.DataFrame([{'概念名称': '造纸概念'}])

    def get_stock_history_data(self, start_date_str, end_date_str):
        return pd.DataFrame([{'日期': '2026-04-22', '收盘': 10.5}])

    def get_stock_news(self):
        return pd.DataFrame([{'新闻标题': '示例新闻'}])

    def get_stock_individual_fund_flow(self):
        return pd.DataFrame([{'日期': '2026-04-22', '净流入': 1000}])

    def get_stock_financial_analysis_indicator(self, start_year='2024'):
        return pd.DataFrame([{'报告期': '2024', '净利润同比增长率': 0.1}])


class _StubOpenAIClient:
    def __init__(self, completion):
        self.calls = []
        completions = completion if isinstance(completion, list) else [completion]

        def create(**kwargs):
            self.calls.append(kwargs)
            index = min(len(self.calls) - 1, len(completions) - 1)
            return completions[index]

        self.chat = SimpleNamespace(
            completions=SimpleNamespace(
                create=create,
            )
        )


class StockAiAnalyzerTest(unittest.TestCase):
    @patch('stock_analyse.infrastructure.llm.stock_ai_analyzer.stockCompanyInfo', StubStockCompanyInfo)
    def test_stock_indicator_analyse_handles_empty_industry_fund_flow(self):
        analyzer = StockAiAnalyzer(system_prompt='system', prompt_template='{single_industry_df}')
        analyzer.openai_api_call = lambda symbol, message, instruction: 'ok'

        result = analyzer.stock_indicator_analyse('SH', '600963', '2026-01-01', '2026-04-23')

        self.assertEqual(result, 'ok')

    @patch('stock_analyse.infrastructure.llm.stock_ai_analyzer.OpenAI')
    def test_openai_api_call_accepts_json_content_directly(self, openai_cls):
        load_settings()
        completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content='{"performance_summary":"ok"}',
                        additional_kwargs={},
                    )
                )
            ]
        )
        stub_client = _StubOpenAIClient(completion)
        openai_cls.return_value = stub_client
        analyzer = StockAiAnalyzer(system_prompt='system')

        result = analyzer.openai_api_call(
            symbol='600519',
            message='test',
            instruction='system',
            require_tool_call=False,
        )

        self.assertEqual(result, {'performance_summary': 'ok'})
        self.assertEqual(len(stub_client.calls), 1)
        self.assertNotIn('tools', stub_client.calls[0])
        self.assertNotIn('tool_choice', stub_client.calls[0])
        self.assertNotIn('response_format', stub_client.calls[0])

    @patch('stock_analyse.infrastructure.llm.stock_ai_analyzer.OpenAI')
    def test_openai_api_call_reformats_non_json_text_with_second_pass(self, openai_cls):
        load_settings()
        first_completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content='结论如下：表现尚可，请整理成结构化输出。',
                        additional_kwargs={},
                    )
                )
            ]
        )
        second_completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content='{"performance_summary":"ok"}',
                        additional_kwargs={},
                    )
                )
            ]
        )
        stub_client = _StubOpenAIClient([first_completion, second_completion])
        openai_cls.return_value = stub_client
        analyzer = StockAiAnalyzer(system_prompt='system')

        result = analyzer.openai_api_call(
            symbol='600519',
            message='test',
            instruction='system',
            require_tool_call=False,
        )

        self.assertEqual(result, {'performance_summary': 'ok'})
        self.assertEqual(len(stub_client.calls), 2)
        self.assertIn('整理成一个合法 JSON 对象字符串', stub_client.calls[1]['messages'][0]['content'])
        self.assertIn('原始内容', stub_client.calls[1]['messages'][1]['content'])
        self.assertNotIn('tools', stub_client.calls[1])
        self.assertNotIn('tool_choice', stub_client.calls[1])

    @patch('stock_analyse.infrastructure.llm.stock_ai_analyzer.OpenAI')
    def test_openai_api_call_raises_when_no_text_content_returned(self, openai_cls):
        load_settings()
        completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content=None,
                        additional_kwargs={},
                    )
                )
            ]
        )
        openai_cls.return_value = _StubOpenAIClient(completion)
        analyzer = StockAiAnalyzer(system_prompt='system')

        with self.assertRaises(ValueError) as error:
            analyzer.openai_api_call(
                symbol='600519',
                message='test',
                instruction='system',
                require_tool_call=False,
            )

        self.assertIn('未返回文本内容', str(error.exception))
        self.assertIn('tool_calls=[]', str(error.exception))

    @patch('stock_analyse.infrastructure.llm.stock_ai_analyzer.OpenAI')
    def test_openai_api_call_raises_when_reformat_still_not_json(self, openai_cls):
        load_settings()
        first_completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content='这是一段无法直接解析的说明',
                        additional_kwargs={},
                    )
                )
            ]
        )
        second_completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[],
                        content='仍然不是 JSON',
                        additional_kwargs={},
                    )
                )
            ]
        )
        openai_cls.return_value = _StubOpenAIClient([first_completion, second_completion])
        analyzer = StockAiAnalyzer(system_prompt='system')

        with self.assertRaises(ValueError) as error:
            analyzer.openai_api_call(
                symbol='600519',
                message='test',
                instruction='system',
                require_tool_call=False,
            )

        self.assertIn('JSON 重整模式', str(error.exception))
        self.assertIn('tool_calls=[]', str(error.exception))

    def test_parse_json_content_extracts_object_from_wrapped_text(self):
        analyzer = StockAiAnalyzer(system_prompt='system')

        result = analyzer._parse_json_content('说明文字\n```json\n{"performance_summary":"ok"}\n```\n结束')

        self.assertEqual(result, {'performance_summary': 'ok'})

    def test_build_json_reformat_message_contains_raw_text(self):
        analyzer = StockAiAnalyzer(system_prompt='system')

        message = analyzer._build_json_reformat_message('原始输出')

        self.assertIn('原始输出', message)
        self.assertIn('合法 JSON 对象字符串', message)

    def test_summarize_choice_includes_additional_kwargs_tool_calls(self):
        analyzer = StockAiAnalyzer(system_prompt='system')
        choice = SimpleNamespace(
            tool_calls=[],
            content='',
            additional_kwargs={'tool_calls': [{'function': {'arguments': '{"performance_summary":"ok"}'}}]},
        )

        summary = analyzer._summarize_choice(choice)

        self.assertIn('tool_calls=', summary)
        self.assertIn('performance_summary', summary)


if __name__ == '__main__':
    unittest.main()
