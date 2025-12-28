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
  数据的种类:  
  按日初始化的数据 
  1、 每天的股票实时数据
  2、 每天新闻数据
  3、 股票历史成绩数据
  4、 北向资金当日持仓排行 
  
  按年和季度的
  1、 财务指标数据
  2、 财务报表  年报 、季报 半年报
  3、 股息
  
  固定数据
  1、 板块数据
  2、 板块归属
  3、 概念数据
  4、 概念归属
  5、 公司介绍
  6、 获取知名股票数据    get_famous_stock_info()	
  
  
  
"""
class stockDataInit:
    def __init__(self, market='SH', symbol="002624"):
        # 定义 current_date 并格式化
        self.market = market
        self.symbol = symbol
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
        stockBorder = stockBorderInfo(market=self.market)

        # 每日股票数据
        df_stock = stockBorder.get_stock_spot()
        # 北向资金数据
        if self.market == 'SH':
            df_bx = stockBorder.get_stock_hsgt_hold_stock_em()
            return

        print("end")

    def init_stock_allmarket_by_day(self):
        """
        获取概念板块名称
        :return:
        """
        market=['SH','H','usa']
        for m in market:
            try:
                stock = stockDataInit(market=m)
                stock.init_stock_by_day();
            except Exception as e:
                self.logger.error(f"init_stock_allmarket_by_day 初始化市场 {m} 的股票数据时出错: {e}")
                traceback.print_exc()
        print("end")

    def init_stock_by_year(self,report_date = '20250630'):
        # 1. 初始化A股（SH）实例
        stock_sh = stockBorderInfo(market=self.market)

        # 2. 测试1：获取三大财务报表
        print("=== 测试 get_stock_border_report()：三大财务报表 ===  此数据是当前报告期，不包含历史 其他包含历史")
        zcfz_df, lrb_df, xjll_df = stock_sh.get_stock_border_report(market='SH', date=report_date)
        print(f"资产负债表行数：{len(zcfz_df)}，利润表行数：{len(lrb_df)}，现金流量表行数：{len(xjll_df)}")
        # 打印利润表核心字段
        print("利润表核心数据（前5行）：")
        print(lrb_df[['股票代码', '股票简称', '净利润', '营业总收入', '净利润同比', '营业总收入同比']].head(5))
        print("-" * 80)

        # 3. 测试2：获取财务分析指标
        print("=== 测试 get_stock_border_financial_indicator()：财务指标 === 此数据包含历史")
        fin_indicator_df = stock_sh.get_stock_border_financial_indicator(market='SH', date=report_date)
        print(f"财务指标数据行数：{len(fin_indicator_df)}")
        print(fin_indicator_df.head(5))
        print("-" * 80)

        # 4. 测试3：获取2024年度分红数据
        print("=== 测试 get_stock_fhps_info()：2024年度分红 === 此数据获取了历史")
        fh_2024_df = stock_sh.get_stock_fhps_info(date=report_date)
        print(f"2024年度分红数据行数：{len(fh_2024_df)}")
        print(fh_2024_df[['代码', '现金分红-股息率', '预案公告日', '除权除息日']].head(5))


    def init_stock_allmarket_by_year(self,report_date = '20250630'):
        market=['SH','H','usa']
        for m in market:
            try:
                stock = stockDataInit(market=m)
                stock.init_stock_by_year(report_date);
            except Exception as e:
                self.logger.error(f"init_stock_allmarket_by_year 初始化市场 {m} 的股票数据时出错: {e}")
                traceback.print_exc()



