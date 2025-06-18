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
class StockReportAnalyser:
    """全盘财务指标高的股票的扫描器"""

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


    def get_report_codes(self,strategy_filter='avg') -> pd.DataFrame:
        """
        根据财务报表过滤  发展良好的股票公司
        :param strategy_filter:
        :return:

         # 获取所有股票的财务报表
        # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
        # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
        # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额  融资性现金流-净现金流占比       公告日期
        """
        stock_border = stockBorderInfo(market=self.market)
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = stock_border.get_stock_border_report(self.market,
                                                                                                   date='20241231',
                                                                                                   indicator='年报')

        date_str = self.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
        if self.market == 'H':
            date_str = self.reportUtils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')
        # 筛选最近三年的数据（利润率为正）

        df_report_filter_zcfz,df_pivot_zcfz = self.process_stock_zcfz(date_str, stock_zcfz_em_df)

        df_report_filter_lrb, df_pivot_lrb = self.process_stock_lrb(date_str, stock_lrb_em_df)

        df_report_filter_xjll, df_pivot_xjll = self.process_stock_xjll(date_str, stock_xjll_em_df)

        stock_code_col = '股票代码_'

        # 获取每个 DataFrame 的股票代码集合
        set_zcfz = set(df_pivot_zcfz[stock_code_col])
        set_lrb = set(df_pivot_lrb[stock_code_col])
        set_xjll = set(df_pivot_xjll[stock_code_col])
        # 计算合集（并集）
        set_stocks = set_zcfz.intersection(set_lrb, set_xjll)
        print(f'符合要求的股票数量{len(set_stocks)}')

        df_report_filter_zcfz = df_report_filter_zcfz[df_report_filter_zcfz['股票代码'].isin(set_stocks)]
        df_report_filter_lrb = df_report_filter_lrb[df_report_filter_lrb['股票代码'].isin(set_stocks)]
        df_report_filter_xjll = df_report_filter_xjll[df_report_filter_xjll['股票代码'].isin(set_stocks)]
        df_pivot_zcfz = df_pivot_zcfz[df_pivot_zcfz['股票代码_'].isin(set_stocks)]
        df_pivot_lrb = df_pivot_lrb[df_pivot_lrb['股票代码_'].isin(set_stocks)]
        df_pivot_xjll = df_pivot_xjll[df_pivot_xjll['股票代码_'].isin(set_stocks)]

        return  df_report_filter_zcfz,df_report_filter_lrb,df_report_filter_xjll,df_pivot_zcfz,df_pivot_lrb,df_pivot_xjll

    def process_stock_zcfz(self, date_str, df_report):
        """
        处理资产负债表
        :param date_str:
        :param df_report:
        :return:
        # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
        """
        # 使用示例
        metrics_array = [
            ['资产负债率', '资产负债率要求', 85,'<'],
            ['资产-总资产', '总资产要求', 10000*10000*10,'>'],
            ['资产-总资产同比', '总资产同比要求', 5,'>']
        ]
        # 负债 - 总负债同比
        # 调用通用函数处理多个指标
        results, set_stocks = self.process_financial_metrics(
            df_report,
            date_str,
            metrics_array
        )
        # 结果说明
        print(f"各指标合格股票集合: {results}")
        print(f"所有指标合格股票的并集数量: {len(set_stocks)}")
        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)]
        df_financial_filter_year = df_report_filter[
            (df_report_filter['报告期'] > date_str)].copy()
        # 假设 df_financial_filter 包含 '股票代码' 和 '年份' 列
        df_financial_filter_year['年份'] = df_financial_filter_year['报告期'].astype(str).str[:4]
        df_pivot = df_financial_filter_year.pivot_table(
            index='股票代码',  # 每只股票一行
            columns='年份',  # 年份转为列
            values=['资产负债率', '资产-总资产', '资产-总资产同比', '负债-总负债', '负债-总负债同比'],
            aggfunc='first'  # 如果同一年份有多个值，取第一个
        ).reset_index()
        # 重命名列，将年份合并到列名中
        df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]
        # 结果示例（列名）：
        # '股票代码', '净利润_2020', '净利润_2021', '净利润同比增长率_2020', ...
        df_pivot = df_pivot.sort_values(by=['资产负债率_2025'], ascending=True)
        return df_report_filter,df_pivot

    def process_stock_lrb(self, date_str, df_report):
        """
        处理利润表
        :param date_str:
        :param df_report:
        :return:
        # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
        """
        # 使用示例
        metrics_array = [
            ['净利润同比 ', '净利润同比p要求', 10,'>'],
            ['营业总收入同比', '营业总收入同比要求', 10,'>'],
            ['净利润', '净利润要求', 10000*10000,'>'],
            ['营业利润', '营业利润要求', 10000*10000,'>']
        ]
        # 负债 - 总负债同比
        # 调用通用函数处理多个指标
        results, set_stocks = self.process_financial_metrics(
            df_report,
            date_str,
            metrics_array
        )
        # 结果说明
        print(f"各指标合格股票集合: {results}")
        print(f"所有指标合格股票的并集数量: {len(set_stocks)}")
        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)]
        df_financial_filter_year = df_report_filter[
            (df_report_filter['报告期'] > date_str)].copy()
        df_financial_filter_year['年份'] = df_financial_filter_year['报告期'].astype(str).str[:4]
        # 假设 df_financial_filter 包含 '股票代码' 和 '年份' 列
        df_pivot = df_financial_filter_year.pivot_table(
            index='股票代码',  # 每只股票一行
            columns='年份',  # 年份转为列
            values=['净利润', '营业利润', '营业总收入', '净利润同比', '营业总收入同比'],
            aggfunc='first'  # 如果同一年份有多个值，取第一个
        ).reset_index()
        # 重命名列，将年份合并到列名中
        df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]
        # 结果示例（列名）：
        # '股票代码', '净利润_2020', '净利润_2021', '净利润同比增长率_2020', ...
        df_pivot = df_pivot.sort_values(by=['净利润_2025'], ascending=False)
        return df_report_filter,df_pivot


    def process_stock_xjll(self, date_str, df_report):
        """
        处理现金流量表
        :param date_str:
        :param df_report:
        :return:
        # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额  融资性现金流-净现金流占比       公告日期
        """
        # 使用示例
        metrics_array = [
            ['净现金流-同比增长  ', '净现金流率要求', 5,'>'],
            ['经营性现金流-净现金流占比', '净现金流占比要求', 50,'>']
        ]
        # 负债 - 总负债同比
        # 调用通用函数处理多个指标
        results, set_stocks = self.process_financial_metrics(
            df_report,
            date_str,
            metrics_array
        )
        # 结果说明
        print(f"各指标合格股票集合: {results}")
        print(f"所有指标合格股票的并集数量: {len(set_stocks)}")
        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        df_report_filter = df_report[df_report['股票代码'].isin(set_stocks)]
        df_financial_filter_year = df_report_filter[
            (df_report_filter['报告期'] > date_str)].copy()
        # 假设 df_financial_filter 包含 '股票代码' 和 '年份' 列
        df_financial_filter_year['年份'] = df_financial_filter_year['报告期'].astype(str).str[:4]
        df_pivot = df_financial_filter_year.pivot_table(
            index='股票代码',  # 每只股票一行
            columns='年份',  # 年份转为列
            values=['净现金流-净现金流', '净现金流-同比增长', '经营性现金流-净现金流占比', '投资性现金流-净现金流占比', '融资性现金流-净现金流占比'],
            aggfunc='first'  # 如果同一年份有多个值，取第一个
        ).reset_index()
        # 重命名列，将年份合并到列名中
        df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]
        # 结果示例（列名）：
        # '股票代码', '净利润_2020', '净利润_2021', '净利润同比增长率_2020', ...
        df_pivot = df_pivot.sort_values(by=['净现金流-净现金流_2025'], ascending=False)
        return df_report_filter,df_pivot

    def process_financial_metrics(self, df_financial, date_financial, metrics_array,data_type = 'continue'):
        """
        处理多个财务指标，返回结果集合及它们的交集

        参数:
        stock_strategy: 股票策略对象
        df_financial: 财务数据DataFrame
        date_financial: 财务数据日期
        metrics_array: 指标数组，每个元素为[指标名称, 重命名名称, 阈值]

        返回:
        results: 结果集合数组
        union_results: 结果集合的并集
        """
        results = []
        if ('报告期'  not in df_financial.columns) and ( '公告日期' in df_financial.columns):
            df_financial['报告期'] = df_financial['公告日期']
        # 处理每个财务指标
        for metric_name, metric_rename, threshold,condition_type in metrics_array:
            # 调用股票策略获取符合条件的股票集合
            if data_type == 'continue':
                stock_set = self.stock_strategy.get_stock_continue_postive(
                    df_financial=df_financial,
                    date=date_financial,
                    col_name=metric_name,
                    col_adjustment=metric_rename,
                    continue_year=1,
                    threshold=threshold,
                    condition_type = condition_type
                )
            else:
                stock_set = self.stock_strategy.get_stock_avg_postive(
                    df_financial=df_financial,
                    date=date_financial,
                    col_name=metric_name,
                    col_adjustment=metric_rename,
                    continue_year=1,
                    threshold=threshold,
                    condition_type = condition_type
                )

            results.append(stock_set)
            print(f"df_financial {metric_name} 合格股票数量：{len(stock_set)}")

        # 计算所有结果的并集
        union_results = set()
        if results:
            union_results = results[0].copy()
            for result in results[1:]:
                union_results = union_results & result

        return results, union_results

