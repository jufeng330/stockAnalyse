
import os
import sys
import time
import random
import logging
import pandas as pd

import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import akshare as ak
from litellm import maritalk_key
from torch.ao.quantization.fx import convert
# from akshare import stock_individual_basic_info_hk_xq
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from .stock_analyzer import  StockAnalyzer


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from stocklib.stock_border import stockBorderInfo
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.stock_strategy import StockStrategy
from stocklib.utils_stock import StockUtils
from stocklib.stock_company import stockCompanyInfo

from .stock_result_utils import  StockFileUtils
class StockFenHengAnalyser:
    """全盘筛选高打分股票的扫描器"""

    def __init__(self, max_workers: int = 20, min_score: float = 30,market = 'SH'):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.file_utils = StockFileUtils(market = self.market)
        self.cache_service = FileCacheUtils(market=self.market, cache_dir='history_' + market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()

    def get_fh_codes(self,type = 'continue',min_years=5, threshold=0.03) -> pd.DataFrame:
        stock = stockBorderInfo(market=self.market)
        # "名称","代码", "现金分红 - 股息率"  年份
        df_fh = stock.get_stock_fhps_info()
        # 使用示例
        if type == 'continue':
            qualified_data, summary = self.filter_stocks_with_dividend(df_fh = df_fh, min_years=min_years, threshold=threshold)
        else:
            qualified_data, summary = self.filter_stocks_with_avg(df_fh = df_fh, threshold=threshold)

        qualified_data = qualified_data.sort_values(
            by=['年份', '现金分红-股息率'],  # 指定排序的列
            ascending=[False, False]  # 两列均降序排列
        )
        summary = summary.sort_values(
            by=['平均股息率'],  # 指定排序的列
            ascending=[False]  # 两列均降序排列
        )
        return qualified_data,summary

    def filter_stocks_with_dividend(self, df_fh, min_years=5, threshold=0.03):
        """
        筛选股息率超过threshold的年数 >= min_years的股票

        参数:
        df_fh: 包含股票分红数据的DataFrame
        min_years: 最小达标年数，默认5年
        threshold: 股息率阈值，默认3% (0.03)

        返回:
        符合条件的股票在原始DataFrame中的所有记录
        """
        # 按年份和代码分组，计算每年的股息率总和
        annual_dividend = df_fh.groupby(['代码', '年份']).agg({
            '现金分红-股息率': 'sum',
            '名称': 'first'
        }).reset_index()

        # 标记每年是否达标
        annual_dividend['达标'] = annual_dividend['现金分红-股息率'] > threshold

        # 统计每个股票达标的总年数和平均股息率
        qualified_years = annual_dividend.groupby('代码').agg({
            '名称': 'first',
            '达标': 'sum',
            '现金分红-股息率': 'mean'  # 新增：计算平均股息率
        }).reset_index().rename(columns={
            '达标': '达标年数',
            '现金分红-股息率': '平均股息率'  # 重命名列
        })

        # 筛选达标年数 >= min_years 的股票
        df_qualified_stocks = qualified_years[qualified_years['达标年数'] >= min_years]

        # 获取符合条件的股票代码
        qualified_codes = df_qualified_stocks['代码'].tolist()

        # 从原始数据中筛选符合条件的记录
        df_qualified_data = df_fh[df_fh['代码'].isin(qualified_codes)]

        # 新增：将年份转为列名，股息率转为列值
        dividend_pivot = annual_dividend.pivot_table(
            index='代码',
            columns='年份',
            values='现金分红-股息率',
            aggfunc='first'
        ).reset_index()

        # 将年份列转为字符串格式（可选）
        dividend_pivot.columns = [str(col) for col in dividend_pivot.columns]

        # 合并到结果中（通过代码列）
        df_qualified_stocks = df_qualified_stocks.merge(
            dividend_pivot,
            on='代码',
            how='left'
        )
        df_qualified_stocks = df_qualified_stocks[df_qualified_stocks['代码'].isin(qualified_codes)]
        return df_qualified_data, df_qualified_stocks

    def filter_stocks_with_avg(self, df_fh, threshold=0.03):
        """
        筛选平均股息率超过threshold的股票

        参数:
        df_fh: 包含股票分红数据的DataFrame
        threshold: 平均股息率阈值，默认3% (0.03)

        返回:
        符合条件的股票在原始DataFrame中的所有记录及统计信息
        """
        # 按年份和代码分组，计算每年的股息率总和
        annual_dividend = df_fh.groupby(['代码', '年份']).agg({
            '现金分红-股息率': 'sum',
            '名称': 'first'
        }).reset_index()

        # 标记每年是否达标（可选，保留用于兼容性）
        annual_dividend['达标'] = annual_dividend['现金分红-股息率'] > threshold

        # 统计每个股票的平均股息率
        stock_stats = annual_dividend.groupby('代码').agg({
            '名称': 'first',
            '现金分红-股息率': 'mean',
            '达标': 'sum'  # 保留达标年数统计（可选）
        }).reset_index().rename(columns={
            '现金分红-股息率': '平均股息率',
            '达标': '达标年数'  # 保留该列用于兼容性
        })

        # 筛选平均股息率 >= threshold 的股票
        qualified_stocks = stock_stats[stock_stats['平均股息率'] >= threshold]

        # 获取符合条件的股票代码
        qualified_codes = qualified_stocks['代码'].tolist()

        # 从原始数据中筛选符合条件的记录
        qualified_data = df_fh[df_fh['代码'].isin(qualified_codes)]

        # 将年份转为列名，股息率转为列值
        dividend_pivot = annual_dividend.pivot_table(
            index='代码',
            columns='年份',
            values='现金分红-股息率',
            aggfunc='first'
        ).reset_index()

        # 将年份列转为字符串格式
        dividend_pivot.columns = [str(col) for col in dividend_pivot.columns]

        # 合并结果
        qualified_stocks = qualified_stocks.merge(
            dividend_pivot,
            on='代码',
            how='left'
        )

        return qualified_data, qualified_stocks