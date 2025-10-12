import akshare as ak
import datetime
import pandas as pd
from .stock_concept_data import stockConceptData
from .stock_news_data import stockNewsData
from .stock_ak_indicator import stockAKIndicator
from .stock_border import stockBorderInfo
from .utils_report_date import ReportDateUtils
from .utils_file_cache import FileCacheUtils
from .stock_concept_service import stockConcepService
import traceback
from .mysql_cache import MySQLCache
import time
from tqdm import tqdm

import logging

# 个股相关信息查询
"""
  初始化所有的股票数据，存储在mysql中，供系统使用 
  数据的种类:  1
"""
class stockCompanyInfo:
    def __init__(self, marker='sz', symbol="002624"):
        # 定义 current_date 并格式化
        self.market = marker
        self.symbol = symbol
        self.xq_a_token = 'a9afe36f1d53c5f7180395537db631d013033091'
        # 新增变量 usa 和 ETF
        self.usa = 'usa'
        self.ETF = 'zq'
        # 新增变量 HongKong
        self.HongKong = 'H'
        self.logger = logging.getLogger(__name__)
        # self.border = stockBorderInfo(self.market)
        self.report_util = ReportDateUtils()
        self.cache_service = FileCacheUtils(market=self.market)
        self.mysql = MySQLCache()

    #  获取概念板块名称
    def init_stock_by_day(self):
        """
        获取概念板块名称
        :return:
        """
        if self.market == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        concept_sectors = self.mysql.read_from_cache(date='20250331', report_type='stock_concept_data')
        # concept_sectors = self.cache_service.read_from_serialized(date='20250331', report_type='stock_concept_data');
        if concept_sectors is None or concept_sectors.empty:
            stock_concept_service = stockConcepService()
            concept_sectors, industry_sectors = stock_concept_service.get_all_sectors_and_stocks()
        return concept_sectors

