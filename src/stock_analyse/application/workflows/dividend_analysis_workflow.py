from __future__ import annotations

import logging

import pandas as pd

from stock_analyse.infrastructure.persistence.stock_file_utils import StockFileUtils
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils
from stock_analyse.shared.report_date_utils import ReportDateUtils
from stock_analyse.shared.stock_utils import StockUtils


class StockFenHengAnalyser:
    """分红/股息率筛选工作流。

    用于按连续分红或平均股息率筛选候选股票，属于历史风格 workflow，当前仍服务于分红筛选类场景，先保留并补清用途说明。
    """

    def __init__(self, max_workers: int = 20, min_score: float = 30, market='SH'):
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.file_utils = StockFileUtils(market=self.market)
        self.cache_service = FileCacheUtils(market=self.market, cache_dir='history_' + market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()

    def get_fh_codes(self, type='continue', min_years=5, threshold=0.03) -> tuple[pd.DataFrame, pd.DataFrame]:
        stock = stockBorderInfo(market=self.market)
        df_fh = stock.get_stock_fhps_info()
        if df_fh is None or df_fh.empty:
            return pd.DataFrame(), pd.DataFrame()
        if type == 'continue':
            qualified_data, summary = self.filter_stocks_with_dividend(df_fh=df_fh, min_years=min_years, threshold=threshold)
        else:
            qualified_data, summary = self.filter_stocks_with_avg(df_fh=df_fh, threshold=threshold)

        qualified_data = qualified_data.sort_values(by=['年份', '现金分红-股息率'], ascending=[False, False])
        summary = summary.sort_values(by=['平均股息率'], ascending=[False])
        return qualified_data, summary

    def filter_stocks_with_dividend(self, df_fh, min_years=5, threshold=0.03):
        annual_dividend = df_fh.groupby(['代码', '年份']).agg({
            '现金分红-股息率': 'sum',
            '名称': 'first',
        }).reset_index()
        annual_dividend['达标'] = annual_dividend['现金分红-股息率'] > threshold
        qualified_years = annual_dividend.groupby('代码').agg({
            '名称': 'first',
            '达标': 'sum',
            '现金分红-股息率': 'mean',
        }).reset_index().rename(columns={'达标': '达标年数', '现金分红-股息率': '平均股息率'})
        df_qualified_stocks = qualified_years[qualified_years['达标年数'] >= min_years]
        qualified_codes = df_qualified_stocks['代码'].tolist()
        df_qualified_data = df_fh[df_fh['代码'].isin(qualified_codes)]
        dividend_pivot = annual_dividend.pivot_table(
            index='代码',
            columns='年份',
            values='现金分红-股息率',
            aggfunc='first',
        ).reset_index()
        dividend_pivot.columns = [str(col) for col in dividend_pivot.columns]
        df_qualified_stocks = df_qualified_stocks.merge(dividend_pivot, on='代码', how='left')
        df_qualified_stocks = df_qualified_stocks[df_qualified_stocks['代码'].isin(qualified_codes)]
        return df_qualified_data, df_qualified_stocks

    def filter_stocks_with_avg(self, df_fh, threshold=0.03):
        annual_dividend = df_fh.groupby(['代码', '年份']).agg({
            '现金分红-股息率': 'sum',
            '名称': 'first',
        }).reset_index()
        annual_dividend['达标'] = annual_dividend['现金分红-股息率'] > threshold
        stock_stats = annual_dividend.groupby('代码').agg({
            '名称': 'first',
            '现金分红-股息率': 'mean',
            '达标': 'sum',
        }).reset_index().rename(columns={'现金分红-股息率': '平均股息率', '达标': '达标年数'})
        qualified_stocks = stock_stats[stock_stats['平均股息率'] >= threshold]
        qualified_codes = qualified_stocks['代码'].tolist()
        qualified_data = df_fh[df_fh['代码'].isin(qualified_codes)]
        dividend_pivot = annual_dividend.pivot_table(
            index='代码',
            columns='年份',
            values='现金分红-股息率',
            aggfunc='first',
        ).reset_index()
        dividend_pivot.columns = [str(col) for col in dividend_pivot.columns]
        qualified_stocks = qualified_stocks.merge(dividend_pivot, on='代码', how='left')
        return qualified_data, qualified_stocks
