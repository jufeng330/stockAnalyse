from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from stockAI.stockAgent.stock_ai_analyzer import StockAiAnalyzer


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


class StockAiAnalyzerTest(unittest.TestCase):
    @patch('stockAI.stockAgent.stock_ai_analyzer.stockCompanyInfo', StubStockCompanyInfo)
    def test_stock_indicator_analyse_handles_empty_industry_fund_flow(self):
        analyzer = StockAiAnalyzer(system_prompt='system', prompt_template='{single_industry_df}')
        analyzer.openai_api_call = lambda symbol, message, instruction: 'ok'

        result = analyzer.stock_indicator_analyse('SH', '600963', '2026-01-01', '2026-04-23')

        self.assertEqual(result, 'ok')


if __name__ == '__main__':
    unittest.main()
