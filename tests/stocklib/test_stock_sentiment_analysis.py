from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from stocklib.stock_sentiment_analysis import StockSentimentAnalysis


class StockSentimentAnalysisTest(unittest.TestCase):
    @patch('stocklib.stock_sentiment_analysis.ak.stock_news_main_cx')
    @patch('stocklib.stock_sentiment_analysis.ak.stock_research_report_em')
    @patch('stocklib.stock_sentiment_analysis.ak.stock_news_em')
    def test_get_comprehensive_news_data_returns_industry_news_list(
        self,
        mock_stock_news_em,
        mock_stock_research_report_em,
        mock_stock_news_main_cx,
    ):
        mock_stock_news_em.return_value = pd.DataFrame()
        mock_stock_research_report_em.return_value = pd.DataFrame()
        mock_stock_news_main_cx.return_value = pd.DataFrame([
            {'tag': '市场动态', 'summary': '中远海控所在航运行业景气度提升', 'url': 'https://example.com/news'}
        ])

        analyzer = StockSentimentAnalysis(market='SH', symbol='601919')
        analyzer.stock_service.get_stock_name = lambda: '中远海控'
        analyzer.news_cache_duration = None

        data = analyzer.get_comprehensive_news_data('601919', days=30)

        self.assertGreaterEqual(len(data['industry_news']), 1)
        self.assertEqual(data['industry_news'][0]['title'], '市场动态')
        self.assertIn('航运行业景气度提升', data['industry_news'][0]['content'])

    @patch('stocklib.stock_sentiment_analysis.ak.stock_notice_report')
    @patch('stocklib.stock_sentiment_analysis.ak.stock_news_main_cx')
    @patch('stocklib.stock_sentiment_analysis.ak.stock_research_report_em')
    @patch('stocklib.stock_sentiment_analysis.ak.stock_news_em')
    def test_get_comprehensive_news_data_uses_notice_report_for_announcements(
        self,
        mock_stock_news_em,
        mock_stock_research_report_em,
        mock_stock_news_main_cx,
        mock_stock_notice_report,
    ):
        mock_stock_news_em.return_value = pd.DataFrame()
        mock_stock_research_report_em.return_value = pd.DataFrame()
        mock_stock_news_main_cx.return_value = pd.DataFrame()
        mock_stock_notice_report.return_value = pd.DataFrame([
            {
                '代码': '601919',
                '名称': '中远海控',
                '公告标题': '关于年度分红的公告',
                '公告类型': '财务报告',
                '公告日期': '2026-04-23',
                '网址': 'https://example.com/notice',
            }
        ])

        analyzer = StockSentimentAnalysis(market='SH', symbol='601919')
        analyzer.stock_service.get_stock_name = lambda: '中远海控'
        analyzer.news_cache_duration = None

        data = analyzer.get_comprehensive_news_data('601919', days=30)

        self.assertEqual(len(data['announcements']), 1)
        self.assertEqual(data['announcements'][0]['title'], '关于年度分红的公告')
        self.assertEqual(data['announcements'][0]['type'], '财务报告')
        mock_stock_notice_report.assert_called_once()
        self.assertEqual(mock_stock_notice_report.call_args.kwargs['date'], '20260423')
        self.assertEqual(mock_stock_notice_report.call_args.kwargs['symbol'], '全部')
        self.assertNotIn('stock_zh_a_alerts_cls', dir(__import__('akshare')))


if __name__ == '__main__':
    unittest.main()
