import os
import time
import random
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import akshare as ak
from tqdm import tqdm
from .stock_analyzer import StockAnalyzer
from .stock_select_strategy import StockSelectStrategy

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from stocklib.stock_border import stockBorderInfo
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.stock_strategy import StockStrategy
from stocklib.utils_stock import StockUtils
from stocklib.stock_company import stockCompanyInfo

from .stock_result_utils import StockFileUtils
from stock_analyse.application.workflows.backtest_stocks_workflow import BacktestStocksWorkflow
from stock_analyse.application.workflows.full_market_scan_workflow import FullMarketScanWorkflow


# -------------------------------
# **全盘股票扫描器**
# -------------------------------
class TopStockScanner:
    """兼容旧入口的全盘扫描器；主流程已迁移到 src workflows。"""

    def __init__(self, max_workers: int = 20, min_score: float = 30, market='SH', strategy_type='1'):
        self.analyzer = StockAnalyzer(market=market)
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)
        self.market = market
        self.strategy_type = strategy_type
        self.stockSelector = StockSelectStrategy(market=self.market, strategy_type=strategy_type)
        strategy_name = self.stockSelector.get_strategy_name(strategy_type)
        self.file_utils = StockFileUtils(min_score=self.min_score, market=self.market, name=strategy_name)
        self.cache_service = FileCacheUtils(market=self.market, cache_dir='history_' + market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()
        self.full_market_scan_workflow = FullMarketScanWorkflow(max_workers=max_workers, min_score=min_score)
        self.backtest_workflow = BacktestStocksWorkflow()

    def get_all_stocks(self) -> pd.DataFrame:
        return self.full_market_scan_workflow.get_all_stocks(market=self.market)

    def analyze_stock_safe(self, stock, max_retries: int = 3) -> Optional[Dict]:
        runtime = self.full_market_scan_workflow.build_runtime(
            market=self.market,
            strategy_type=int(self.strategy_type),
        )[1]
        return self.full_market_scan_workflow.analyze_stock_safe(runtime, stock, max_retries=max_retries)

    def process_batch(self, stock_codes) -> List[Dict]:
        runtime = self.full_market_scan_workflow.build_runtime(
            market=self.market,
            strategy_type=int(self.strategy_type),
        )[1]
        runtime.file_utils = self.file_utils
        runtime.analyzer = self.analyzer
        runtime.selector = self.stockSelector
        return self.full_market_scan_workflow.process_batch(runtime, stock_codes)

    def scan_high_score_stocks(self, batch_size: int = 20, type=1, strategy_filter='avg') -> List[Dict]:
        file_utils, high_score_stocks = self.full_market_scan_workflow.run(
            market=self.market,
            strategy_type=type,
            batch_size=batch_size,
            strategy_filter=strategy_filter,
            min_score=self.min_score,
        )
        self.file_utils = file_utils
        return high_score_stocks

    def scan_stock(self, batch_size, df_stocks_data):
        runtime = self.full_market_scan_workflow.build_runtime(
            market=self.market,
            strategy_type=int(self.strategy_type),
        )[1]
        runtime.file_utils = self.file_utils
        runtime.analyzer = self.analyzer
        runtime.selector = self.stockSelector
        return self.full_market_scan_workflow.scan_stock(runtime, batch_size=batch_size, df_stocks_data=df_stocks_data)

    def backtest_stocks(self, list_high_score_stocks, analysis_date='2025-06-06'):
        if isinstance(list_high_score_stocks, pd.DataFrame):
            high_score_stocks = list_high_score_stocks.to_dict(orient='records')
        else:
            high_score_stocks = list_high_score_stocks
        return self.backtest_workflow.run(
            market=self.market,
            high_score_stocks=high_score_stocks,
            analysis_date=analysis_date,
        )

    """
     下面是废弃的代码，暂时保留
    """

    def generate_statistics_report(self, df_result, type='all'):
        return self.backtest_workflow.generate_statistics_report(
            pd.DataFrame(df_result),
            recommendation_type=type,
        )

    def get_stock_normal_info(self, df_stock = None, strategy_filter ='avg') -> pd.DataFrame:
        """
        获取公司本身是合格公司的数据  主要条件如下：
         1、 市值百亿以上
         2、 最近3年盈利为正，营业额是正增长的
         3、 公司估值在合理区间内  PE<15或者ROE>10 或者股息率>3%
        :return:
        """
        date = self.reportUtils.get_current_report_year_st()
        stock = stockBorderInfo(market=self.market)
        if df_stock is None:
            df_stock_spot = stock.get_stock_spot()
        else:
            df_stock_spot = df_stock.copy()
        df_stock_spot['代码'] = df_stock_spot['代码'].astype(str)
        print(f"df_stock 股票数量：{len(df_stock_spot)}")

        df_stock_spot = df_stock_spot[df_stock_spot['总市值'] > 100 * 10000 * 10000]  if '总市值' in df_stock_spot.columns else df_stock_spot
        df_stock_spot = df_stock_spot[df_stock_spot['市盈率-动态'] < 50] if '市盈率-动态' in df_stock_spot.columns else df_stock_spot

        df_stock_spot.loc[:, '资产负债率_%'] = df_stock_spot['资产负债率'] if '资产负债率' in df_stock_spot.columns else None
        if '资产负债率_%' in df_stock_spot.columns:
            df_stock_spot =  self.stock_utils.pd_convert_to_float(df_stock_spot, '资产负债率_%')
            df_stock_spot.loc[:, '资产负债率_%'] = df_stock_spot['资产负债率_%'].astype(float) * 100
        df_stock_spot = df_stock_spot[df_stock_spot['资产负债率_%'] < 85] if '资产负债率_%' in df_stock_spot.columns else df_stock_spot
        print(f"df_stock 资产负债率合格股票数量：{len(df_stock_spot)}")

        df_financial = stock.get_stock_border_financial_indicator(market = self.market, date=date, df_stock_spot=df_stock_spot)
        date_financial = self.reportUtils.get_report_year_str(days=365*3,format='%Y-%m-%d')
        if self.market == 'H':
            date_financial = self.reportUtils.get_report_year_str(days=365 * 4, format='%Y-%m-%d')

        set_stocks = self.find_financial_stock_data(date_financial, df_financial, data_type = strategy_filter)
        df_filtered = df_stock_spot[df_stock_spot['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]
        self.file_utils.create_middle_file(file_name='股票基本信息',df =df_filtered)
        self.file_utils.create_middle_file(file_name='股票财务信息',df = df_financial_filter)
        return df_filtered

    def find_financial_stock_data(self, date_financial, df_financial, data_type ='continue', threshold_1=0.0,threshold_2=0.0,threshold_3=0.0):
        if data_type == 'continue':
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                            col_lrl_rename,threshold_1)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                                  col_lrl_rename,threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_continue_postive(df_financial, date_financial, col_lrl,
                                                                           col_lrl_rename,threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        else:
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                            col_lrl_rename,threshold_1)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")
            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                                  col_lrl_rename,threshold_2)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")
            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_avg_postive(df_financial, date_financial, col_lrl,
                                                                           col_lrl_rename,threshold_3)
            print(f"df_financial {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        set_stocks = set_stocks_lrl & set_stocks_yy & set_stocks_lrl_ratio
        print(f"df_financial  合格股票数量：{len(set_stocks)}")
        return set_stocks

    def get_stock_quality_info(self,df_stock = None,strategy_filter='avg'):
        """
        获取公司本身是合格公司的数据  主要条件如下：
         1、 市值百亿以上
         2、 最近3年盈利>5% ，营业额是正增长>5%
         3、 公司估值在合理区间内  PE<15或者ROE>10 或者股息率>3%
        :return:
        """
        date = self.reportUtils.get_current_report_year_st()
        stock = stockBorderInfo(market=self.market)
        if df_stock is None:
            df_stock = stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        print(f"df_stock 票数量：{len(df_stock)}")

        df_stock = df_stock[df_stock['总市值']>100*10000*10000]  if '总市值' in df_stock.columns else df_stock

        df_stock = df_stock[df_stock['市盈率-动态'] < 30] if '市盈率-动态' in df_stock.columns else df_stock
        df_stock = df_stock[df_stock['市净率'] < 20] if '市净率' in df_stock.columns else df_stock

        df_stock['资产负债率_%'] = df_stock['资产负债率'] if '资产负债率' in df_stock.columns else None
        if '资产负债率_%' in df_stock.columns:
            df_stock = self.stock_utils.pd_convert_to_float(df_stock, '资产负债率_%')
            df_stock['资产负债率_%'] = df_stock['资产负债率_%'].astype(float) * 100
        df_stock = df_stock[df_stock['资产负债率_%'] < 80] if '资产负债率_%' in df_stock.columns else df_stock
        print(f"df_stock 资产负债率合格股票数量：{len(df_stock)}")
        df_financial = stock.get_stock_border_financial_indicator(market=self.market, date=date, df_stock_spot=df_stock)
        date_financial = self.reportUtils.get_report_year_str(days=365 * 3, format='%Y-%m-%d')
        set_stocks =self.find_financial_stock_data(date_financial,df_financial,data_type=strategy_filter,threshold_1=5000*10000,threshold_2=0.05,threshold_3=0.05)
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]
        self.file_utils.create_middle_file(file_name='股票基本信息', df=df_filtered)
        self.file_utils.create_middle_file(file_name='股票财务信息', df=df_financial_filter)

        return df_filtered
