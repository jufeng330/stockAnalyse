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
class StockFinancialAnalyser:
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

    def get_financial_codes_normal(self,strategy_filter='avg') -> pd.DataFrame:
        df_financial_filter,summary  = self.get_financial_codes(strategy_filter,threshold_1= 10000, threshold_2=0, threshold_3=0)
        return df_financial_filter

    def get_financial_codes_quality(self, strategy_filter='avg') -> pd.DataFrame:
        df_financial_filter,summary = self.get_financial_codes(strategy_filter, threshold_1=5000 * 10000, threshold_2=0.05, threshold_3=0.05)
        return df_financial_filter
    def get_financial_codes(self,strategy_filter='avg',threshold_1=5000 * 10000, threshold_2=0.05, threshold_3=0.05) -> pd.DataFrame:
        stock = stockBorderInfo(market=self.market)
        date = '20250331'
        df_financial = stock.get_stock_border_financial_indicator(market=self.market, date=date)
        #                 # 日期, 摊薄每股收益(元), 加权每股收益(元), 每股收益_调整后(元), 扣除非经常性损益后的每股收益(元), 每股净资产_调整前(元), 每股净资产_调整后(元), 每股经营性现金流(元), 每股资本公积金(元), 每股未分配利润(元), 调整后的每股净资产(元), 总资产利润率(%), 主营业务利润率(%), 总资产净利润率(%), 成本费用利润率(%), 营业利润率(%), 主营业务成本率(%), 销售净利率(%), 股本报酬率(%), 净资产报酬率(%), 资产报酬率(%), 销售毛利率(%), 三项费用比重, 非主营比重, 主营利润比重, 股息发放率(%), 投资收益率(%), 主营业务利润(元), 净资产收益率(%), 加权净资产收益率(%), 扣除非经常性损益后的净利润(元), 主营业务收入增长率(%), 净利润增长率(%), 净资产增长率(%), 总资产增长率(%), 应收账款周转率(次), 应收账款周转天数(天), 存货周转天数(天), 存货周转率(次), 固定资产周转率(次), 总资产周转率(次), 总资产周转天数(天), 流动资产周转率(次), 流动资产周转天数(天), 股东权益周转率(次), 流动比率, 速动比率, 现金比率(%), 利息支付倍数, 长期债务与营运资金比率(%), 股东权益比率(%), 长期负债比率(%), 股东权益与固定资产比率(%), 负债与所有者权益比率(%), 长期资产与长期资金比率(%), 资本化比率(%), 固定资产净值率(%), 资本固定化比率(%), 产权比率(%), 清算价值比率(%), 固定资产比重(%), 资产负债率(%), 总资产(元), 经营现金净流量对销售收入比率(%), 资产的经营现金流量回报率(%), 经营现金净流量与净利润的比率(%), 经营现金净流量对负债比率(%), 现金流量比率(%), 短期股票投资(元), 短期债券投资(元), 短期其它经营性投资(元), 长期股票投资(元), 长期债券投资(元), 长期其它经营性投资(元), 1年以内应收帐款(元), 1-2年以内应收帐款(元), 2-3年以内应收帐款(元), 3年以内应收帐款(元), 1年以内预付货款(元), 1-2年以内预付货款(元), 2-3年以内预付货款(元), 3年以内预付货款(元), 1年以内其它应收款(元), 1-2年以内其它应收款(元), 2-3年以内其它应收款(元), 3年以内其它应收款(元)
        date_financial = self.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
        if self.market == 'H':
            date_financial = self.reportUtils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')
        # 筛选最近三年的数据（利润率为正）
        set_stocks = self.find_financial_stock_data(date_financial, df_financial, data_type=strategy_filter,
                                                    threshold_1=threshold_1, threshold_2=threshold_2, threshold_3=threshold_3)

        # # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]

        df_financial_filter_year = df_financial_filter[
            (df_financial['报告期'] > date_financial) & (df_financial_filter['报告期'].str.endswith('03-31'))].copy()
        # 假设 df_financial_filter 包含 '股票代码' 和 '年份' 列
        df_pivot = df_financial_filter_year.pivot_table(
            index='股票代码',  # 每只股票一行
            columns='年份',  # 年份转为列
            values=['净利润', '净利润同比增长率', '营业总收入同比增长率', '净资产收益率', '资产负债率'],
            aggfunc='first'  # 如果同一年份有多个值，取第一个
        ).reset_index()

        # 重命名列，将年份合并到列名中
        df_pivot.columns = ['_'.join(map(str, col)).strip() for col in df_pivot.columns.values]

        # 结果示例（列名）：
        # '股票代码', '净利润_2020', '净利润_2021', '净利润同比增长率_2020', ...
        df_pivot = df_pivot.sort_values(by=['净利润_2025', '净利润_2024'], ascending=False)
        return df_financial_filter,df_pivot



    def find_financial_stock_data(self, date_financial, df_financial, data_type ='continue', threshold_1=0.0,threshold_2=0.0,threshold_3=0.0):
        # 筛选最近三年的数据（利润率为正） data_type 取值是 continue,avg
        if data_type == 'continue':
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_continue_postive(
                df_financial=df_financial,date=date_financial,col_name=col_lrl,col_adjustment=col_lrl_rename,continue_year=3,threshold=threshold_1)

            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_continue_postive(df_financial=df_financial,date=date_financial,col_name=col_lrl,col_adjustment=col_lrl_rename,continue_year=3,threshold=threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_continue_postive(df_financial=df_financial,date=date_financial,col_name=col_lrl,col_adjustment=col_lrl_rename,continue_year=3,threshold=threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        else:
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_avg_postive(df_financial=df_financial,date=date_financial,col_name=col_lrl,col_adjustment=col_lrl_rename,continue_year=3,threshold=threshold_1)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_avg_postive(df_financial=df_financial,date=date_financial,col_name=col_lrl,col_adjustment=col_lrl_rename,continue_year=3,threshold=threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                           col_lrl_rename,threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        set_stocks = set_stocks_lrl & set_stocks_yy & set_stocks_lrl_ratio  # 或使用 set_lrl.intersection(set_yy)
        print(f"df_financial  合格股票数量：{len(set_stocks)}")
        return set_stocks



