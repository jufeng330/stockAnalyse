import akshare as ak
import os
import sys
import pandas as pd
import datetime
import re
import numpy as np
from .stock_concept_data import stockConceptData
from .stock_news_data import stockNewsData
from .stock_ak_indicator import stockAKIndicator

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from .stock_annual_report import stockAnnualReport
from .utils_report_date import ReportDateUtils
from .stock_company import stockCompanyInfo
from .utils_file_cache import FileCacheUtils
from .stock_strategy import StockStrategy
from .utils_stock import StockUtils
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import os
import pickle
from tqdm import tqdm
import logging
import time
import random
import traceback

# 个股相关信息查询
"""
  需要缓存的数据：
  获取所有股票数据  stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
  获取所有板块信息: stock_board = stock_concept_service.stock_board_concept_name_ths()
"""
class stockBorderInfo:
    def __init__(self, market='SZ'):
        # 定义 current_date 并格式化
        self.market = market
        self.xq_a_token = 'a9afe36f1d53c5f7180395537db631d013033091'
        # 新增变量 usa 和 ETF
        self.usa = 'usa'
        self.ETF = 'zq'
        # 新增变量 HongKong
        self.HongKong = 'H'
        self.cache_dir = os.path.join(os.path.dirname(__file__), 'cache', 'financial_reports')  # 缓存目录可自定义
        os.makedirs(self.cache_dir, exist_ok=True)  # 创建缓存目录（如果不存在）
        self.reportUtils = ReportDateUtils()
        self.max_workers = 20
        self.logger = logging.getLogger(__name__)
        self.cache_service = FileCacheUtils(market = self.market)
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()

    # 获取所有股票数据的实时信息 和资金情况
    # 字段详情：   序号   股票代码  股票简称     最新价     涨跌幅    换手率     流入资金     流出资金        净额      成交额
    def get_stock_all_info(self):
        """
        获取个股资金流
        :return:
        """
        stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
        return stock_fund_flow_individual_df

    def get_stock_zh_a_spot_em_df(self):
            """
            优先调用 ak.stock_zh_a_spot_em()，异常或空值时调用 ak.stock_zh_a_spot()
            :return: 股票实时行情 DataFrame
            """
            try:
                # 尝试调用第一个接口
                stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()

                # 检查数据是否为空（包括空DataFrame或全NaN）
                if stock_zh_a_spot_em_df.empty or stock_zh_a_spot_em_df.isna().all().all():
                    raise ValueError("接口返回空数据或无效数据")

                return stock_zh_a_spot_em_df

            except Exception as e:
                print(f"调用 ak.stock_zh_a_spot_em() 失败，错误信息：{str(e)}")
                try:
                    # 异常或空数据时调用备用接口
                    stock_zh_a_spot_df = ak.stock_zh_a_spot()
                    stock_zh_a_spot_df['代码'] = stock_zh_a_spot_df['代码'].apply(lambda x: re.sub(r'[^\d]', '', x))

                    # 检查备用接口数据有效性
                    if stock_zh_a_spot_df.empty or stock_zh_a_spot_df.isna().all().all():
                        raise ValueError("备用接口返回空数据或无效数据")

                    return stock_zh_a_spot_df

                except Exception as e:
                    print(f"调用 ak.stock_zh_a_spot() 失败，错误信息：{str(e)}")
                    return pd.DataFrame()  # 返回空DataFrame表示最终失败
        # 定义格式化函数，作为静态方法
        # 获取所有股票的实时行情
        # 字段包括  ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
    def get_stock_spot(self):
        """
        获取股票的实时成交数据
        stock_hk_spot_em 数据内容太多
        :return:
        """
        cache = True
        current_date = self.reportUtils.get_current__history_date_str()
        report_type = self.market+'_spot_em_zh_df'
        stock_zh_a_spot_em_df = self.cache_service.read_from_serialized(current_date, report_type)
        if stock_zh_a_spot_em_df is not None and cache:
            code_col = '代码'  # 替换为实际列名
            # 使用正则表达式移除前缀 (bj|sz|sh)，不区分大小写
            stock_zh_a_spot_em_df[code_col] = stock_zh_a_spot_em_df[code_col].str.replace(
                r'^(bj|sz|sh)', '', case=False, regex=True
            ).str.upper()
            if self.market == 'usa':
                stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码'].apply(
                    lambda x: self.reportUtils.get_stock_code(market=self.market, symbol=x))
            else:
                stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码']
            return stock_zh_a_spot_em_df
        if self.market == 'SH' or self.market == 'SZ':
            stock_zh_a_spot_em_df = self.get_stock_zh_a_spot_em_df()
            # stock_zh_a_spot_em_df = ak.stock_zh_ah_spot()
            code_col = '代码'  # 替换为实际列名
            # 使用正则表达式移除前缀 (bj|sz|sh)，不区分大小写
            stock_zh_a_spot_em_df[code_col] = stock_zh_a_spot_em_df[code_col].str.replace(
                r'^(bj|sz|sh)', '', case=False, regex=True
            ).str.upper()


        elif self.market == 'usa':
            # ['序号', '名称', '最新价', '涨跌额', '涨跌幅', '开盘价', '最高价', '最低价', '昨收价', '总市值', '市盈率','成交量', '成交额', '振幅', '换手率', '代码']
            stock_zh_a_spot_em_df = ak.stock_us_spot_em()
            stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码'].apply(
                lambda x: self.reportUtils.get_stock_code(market=self.market, symbol=x))
        elif self.market == 'H':
            # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
            # stock_zh_a_spot_em_df = ak.stock_hk_spot_em()
            stock_zh_a_spot_em_df = ak.stock_hk_main_board_spot_em()
            # stock_hk_valuation_baidu_df = ak.stock_hk_valuation_baidu(symbol="06969", indicator="总市值", period="近一年")
        else:
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()

        if self.market == 'usa':
            stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码'].apply(
                lambda x: self.reportUtils.get_stock_code(market=self.market, symbol=x))
        else:
            stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码']
        if cache:
            self.cache_service.write_to_cache_serialized(current_date, report_type,stock_zh_a_spot_em_df)

        return stock_zh_a_spot_em_df

        # 获取所有股票的财务报表
        # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
        # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
        # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额  融资性现金流-净现金流占比       公告日期
    def get_stock_border_report(self, market="SH", date='20240331', indicator='年报',fields_unification=False):
        """
                    获取所有股票的三大财务报表
                    :param self:
                    :param market:   市场 主要包括 SH SZ H usa
                    :param date:     年报日期 国内是0331 国外1231
                    :param indicator:
                    :return:
        """

        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = self.get_stock_border_report_by_market(market, date,indicator,fields_unification)
        stock_zcfz_em_df.apply(lambda x: x.map(self.format_float))
        stock_lrb_em_df.apply(lambda x: x.map(self.format_float))
        stock_xjll_em_df.apply(lambda x: x.map(self.format_float))
        return stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df

    def get_stock_border_report_by_market(self, market="H", date='20240331', indicator='年报',fields_unification=False):
        """
                            获取所有股票的三大财务报表
                            :param self:
                            :param market:   市场 主要包括 SH SZ H usa
                            :param date:     年报日期 国内是0331 国外1231
                            :param indicator:
                            :return:
                """
        report_service = stockAnnualReport()
        if market == 'SH' or market == 'SZ':
            # 资产负债表
            zcfz_key = f"zcfz_{date}"
            stock_zcfz_em_df = self.cache_service.read_from_csv(date, "zcfz")
            if stock_zcfz_em_df is None:
                stock_zcfz_em_df = ak.stock_zcfz_em(date=date)
                self.cache_service.write_to_csv(date, "zcfz", stock_zcfz_em_df)

            # 利润表
            lrb_key = f"lrb_{date}"
            stock_lrb_em_df = self.cache_service.read_from_csv(date, "lrb")
            if stock_lrb_em_df is None:
                stock_lrb_em_df = ak.stock_lrb_em(date=date)
                self.cache_service.write_to_csv(date, "lrb", stock_lrb_em_df)

            # 现金流量表
            xjll_key = f"xjll_{date}"
            stock_xjll_em_df = self.cache_service.read_from_csv(date, "xjll")
            if stock_xjll_em_df is None:
                stock_xjll_em_df = ak.stock_xjll_em(date=date)
                self.cache_service.write_to_csv(date, "xjll", stock_xjll_em_df)
            zcfz, lrb, xjll = stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df
            return zcfz, lrb, xjll
        elif market == 'H' or market == 'usa':
            stock_zcfz_em_df = self.cache_service.read_from_serialized(date, "zcfz")
            stock_lrb_em_df = self.cache_service.read_from_serialized(date, "lrb")
            stock_xjll_em_df = self.cache_service.read_from_serialized(date, "xjll")

            if stock_lrb_em_df is None or stock_zcfz_em_df is None or stock_xjll_em_df is None:
                # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
                if market == 'H':
                    df_stock_all = ak.stock_hk_spot_em()
                    df_stock = df_stock_all
                else:
                    df_stock_all = ak.stock_us_spot_em()
                    df_stock = df_stock_all

                def get_report(stock_code):
                    return report_service.get_stock_report(stock_code=stock_code, market=market,
                                                           indicator=indicator)

                zcfz_all, lrb_all, xjll_all = self.process_stock_reports(df_stock, get_report, batch_size=20)
                try:
                    self.cache_service.write_to_cache_serialized(date, "zcfz", zcfz_all)
                    self.cache_service.write_to_cache_serialized(date, "lrb", lrb_all)
                    self.cache_service.write_to_cache_serialized(date, "xjll", xjll_all)
                    zcfz_all = pd.concat(zcfz_all, ignore_index=True)
                    lrb_all = pd.concat(lrb_all, ignore_index=True)
                    xjll_all = pd.concat(xjll_all, ignore_index=True)

                except Exception as e:
                    print(f"[CACHE]合并失败: {e}")

                self.cache_service.write_to_csv(date, "zcfz", zcfz_all)
                self.cache_service.write_to_csv(date, "lrb", lrb_all)
                self.cache_service.write_to_csv(date, "xjll", xjll_all)

            else:
                zcfz_all = pd.concat(stock_zcfz_em_df, ignore_index=True)
                lrb_all = pd.concat(stock_lrb_em_df, ignore_index=True)
                xjll_all = pd.concat(stock_xjll_em_df, ignore_index=True)

                subset = ['SECURITY_CODE', 'REPORT_DATE', 'STD_ITEM_CODE']

                zcfz_all = zcfz_all.drop_duplicates(subset=subset, keep='first')
                lrb_all = lrb_all.drop_duplicates(subset=subset, keep='first')
                xjll_all = xjll_all.drop_duplicates(subset=subset, keep='first')
        else:
            zcfz_all, lrb_all, xjll_all = report_service.get_stock_border_report(market=market, date=date,
                                                                                 indicator=indicator)

        # 将报表的行转成列
        index_cols = ['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT',
                      '股票代码']
        item_col = 'ITEM_NAME'
        if (market == 'H'):
            index_cols = ['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'REPORT_DATE',
                          'DATE_TYPE_CODE', 'FISCAL_YEAR', '股票代码']
            item_col = 'STD_ITEM_NAME'
        zcfz_all = self.reportUtils.pivot_financial_usa_data(df=zcfz_all, index_cols=index_cols, item_col=item_col,
                                                             fill_value=0)
        lrb_all = self.reportUtils.pivot_financial_usa_data(df=lrb_all, index_cols=index_cols, item_col=item_col,
                                                            fill_value=0)
        xjll_all = self.reportUtils.pivot_financial_usa_data(df=xjll_all, index_cols=index_cols, item_col=item_col,
                                                             fill_value=0)

        if fields_unification:
            zcfz_all = self.reportUtils.map_zcfz_share_to_a_share(h_share_df=zcfz_all, market=market)
            lrb_all = self.reportUtils.map_lrb_share_to_a_share(h_share_df=lrb_all, market=market)
            xjll_all = self.reportUtils.map_xjll_share_to_a_share(h_share_df=xjll_all, market=market)

        return zcfz_all, lrb_all, xjll_all


    def get_stock_border_financial_indicator(self, market="H", date='20240331', indicator='年报',df_stock_spot=None):
        """
            获取所有股票的财务指标数据
                :param self:
                :param market:   市场 主要包括 SH SZ H usa
                :param date:     年报日期 国内是0331 国外1231
                :param indicator:
                # 日期, 摊薄每股收益(元), 加权每股收益(元), 每股收益_调整后(元), 扣除非经常性损益后的每股收益(元), 每股净资产_调整前(元), 每股净资产_调整后(元), 每股经营性现金流(元), 每股资本公积金(元), 每股未分配利润(元), 调整后的每股净资产(元), 总资产利润率(%), 主营业务利润率(%), 总资产净利润率(%), 成本费用利润率(%), 营业利润率(%), 主营业务成本率(%), 销售净利率(%), 股本报酬率(%), 净资产报酬率(%), 资产报酬率(%), 销售毛利率(%), 三项费用比重, 非主营比重, 主营利润比重, 股息发放率(%), 投资收益率(%), 主营业务利润(元), 净资产收益率(%), 加权净资产收益率(%), 扣除非经常性损益后的净利润(元), 主营业务收入增长率(%), 净利润增长率(%), 净资产增长率(%), 总资产增长率(%), 应收账款周转率(次), 应收账款周转天数(天), 存货周转天数(天), 存货周转率(次), 固定资产周转率(次), 总资产周转率(次), 总资产周转天数(天), 流动资产周转率(次), 流动资产周转天数(天), 股东权益周转率(次), 流动比率, 速动比率, 现金比率(%), 利息支付倍数, 长期债务与营运资金比率(%), 股东权益比率(%), 长期负债比率(%), 股东权益与固定资产比率(%), 负债与所有者权益比率(%), 长期资产与长期资金比率(%), 资本化比率(%), 固定资产净值率(%), 资本固定化比率(%), 产权比率(%), 清算价值比率(%), 固定资产比重(%), 资产负债率(%), 总资产(元), 经营现金净流量对销售收入比率(%), 资产的经营现金流量回报率(%), 经营现金净流量与净利润的比率(%), 经营现金净流量对负债比率(%), 现金流量比率(%), 短期股票投资(元), 短期债券投资(元), 短期其它经营性投资(元), 长期股票投资(元), 长期债券投资(元), 长期其它经营性投资(元), 1年以内应收帐款(元), 1-2年以内应收帐款(元), 2-3年以内应收帐款(元), 3年以内应收帐款(元), 1年以内预付货款(元), 1-2年以内预付货款(元), 2-3年以内预付货款(元), 3年以内预付货款(元), 1年以内其它应收款(元), 1-2年以内其它应收款(元), 2-3年以内其它应收款(元), 3年以内其它应收款(元)
            #
                :return:
        """
        cache_key = f"financial"
        df_stock_financial = self.cache_service.read_from_serialized(date, cache_key)
        if df_stock_spot is None:
            df_stock_spot = self.get_stock_spot()
        df_stock_all = df_stock_spot
        # df_stock_all = df_stock_all.head(15)
        df_stock = df_stock_all

        if df_stock_financial is None or df_stock_financial.empty:
                # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
            def get_report(stock_code):
                stock_service = stockCompanyInfo(marker=market, symbol=stock_code)
                start_year = self.reportUtils.get_report_last_five_year(date=date)
                return stock_service.get_stock_financial_analysis_indicator(start_year=start_year)

            df_stock_financial_all = self.process_stock_financial_reports(df_stock, get_report, batch_size=20)
            try:
                df_stock_financial_all = pd.concat(df_stock_financial_all, ignore_index=True)
                self.cache_service.write_to_cache_serialized(date, cache_key, df_stock_financial_all)
                self.cache_service.write_to_csv(date, cache_key, df_stock_financial_all)
            except Exception as e:
                print(f"[CACHE]合并失败: {e}")
        else:
            df_stock_financial_all = df_stock_financial

        if '报告期' not in df_stock_financial_all.columns and '报告日期' in df_stock_financial_all.columns :
            # 转换并填充缺失值
            df_stock_financial_all['报告日期'] = pd.to_datetime(df_stock_financial_all['报告日期'],errors='coerce')
            df_stock_financial_all['报告期'] = df_stock_financial_all['报告日期'].dt.strftime('%Y-%m-%d').fillna('')

        if market == 'H' or market == 'usa':
            """
                |    | 证券代码   |   股票代码 | 股票简称     |   机构代码 | 报告日期            |   DATE_TYPE_CODE |   经营活动每股净现金流量_hk |   每股经营活动现金流量_hk |
                每股净资产_hk |   基本每股收益 |   稀释每股收益 |    营业收入 |   营业收入同比增长率 |      毛利润 |   毛利润同比增长率 |   归属于母公司股东净利润 |   归属于母公司股东的净利润同比增长率_hk |   毛利率 |   滚动市盈率每股收益_hk |   
                营业收入环比增长率_hk |   净利率_hk |   平均净资产收益率 |    毛利润环比增长率_hk |   总资产收益率 |   归属于母公司股东的净利润环比增长率_hk |   
                 年度净资产收益率_hk |   年度投入资本回报率_hk |   息税前利润税负_hk |   销售商品、提供劳务收到的现金占营业收入比重_hk | 
                 资产负债率 |   流动比率 |   流动负债占总负债比重_hk | 起始日期_hk         | 会计年度_hk   | 货币类型_hk   |   是否人民币代码_hk | 报告期     
            """
            if  '归属于母公司股东净利润' in df_stock_financial_all.columns and '净利润' not in df_stock_financial_all.columns :
                df_stock_financial_all['净利润'] = df_stock_financial_all['归属于母公司股东净利润']
            if  '归属于母公司股东的净利润同比增长率_hk' in df_stock_financial_all.columns and '净利润同比增长率' not in df_stock_financial_all.columns :
                df_stock_financial_all['净利润同比增长率'] = df_stock_financial_all['归属于母公司股东的净利润同比增长率_hk']
            if '营业收入' in df_stock_financial_all.columns and '营业总收入' not in df_stock_financial_all.columns:
                df_stock_financial_all.rename(columns={'营业收入': '营业总收入'}, inplace=True)
            if '营业收入同比增长率' in df_stock_financial_all.columns and '营业总收入同比增长率' not in df_stock_financial_all.columns:
                df_stock_financial_all.rename(columns={'营业收入同比增长率': '营业总收入同比增长率'}, inplace=True)
            if  '资产负债率' in df_stock_financial_all.columns :
                df_stock_financial_all['资产负债率'] = df_stock_financial_all['资产负债率']/100

        if '报告期' in df_stock_financial_all.columns:
            dates = pd.to_datetime(df_stock_financial_all['报告期'], errors='coerce')
            df_stock_financial_all['年份'] = np.where(
                dates.notna(),
                dates.dt.year.astype(str),
                np.nan  # 处理无效日期
            )

        return df_stock_financial_all

    #获取所有的板块信息  日期         概念名称 成分股数量  网址     代码
    def get_stock_board_all_concept_name(self):
        """
        获取股票的板块信息数据
        :return:
        """
        if self.market  == self.HongKong or self.market == self.usa:
            return pd.DataFrame()
        stock_concept_service = stockConceptData()
        stock_board = stock_concept_service.stock_board_concept_name_ths()

        return stock_board

    # 获取所有股票的代码和名称
    def get_stock_all_code(self):
        """
            获取所有股票的代码和名称
        :return:
        """
        stock_info_a_code_name_df = ak.stock_info_a_code_name()

        return stock_info_a_code_name_df

    # 获取北向的持仓数据    序号     代码    名称   今日收盘价  今日涨跌幅   今日持股-股数     今日持股-市值  今日持股-占流通股比  今日持股-占总股本比  今日增持估计-股数  今日增持估计-市值  今日增持估计-市值增幅  今日增持估计-占流通股比  今日增持估计-占总股本比   所属板块         日期
    def get_stock_hsgt_hold_stock_em(self):
        """
        获取北向的数据
        :return:
        """
        stock_em_hsgt_hold_stock_df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
        print(stock_em_hsgt_hold_stock_df)
        return stock_em_hsgt_hold_stock_df
    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f"{x:.1f}"
        return x


    def filter_by_report_date(self, df, date_str):
        """
        根据REPORT_DATE列筛选大于等于指定日期的数据

        参数:
            df: 要筛选的DataFrame
            date_str: 筛选的日期，格式为 'YYYYMMDD' 或 'YYYY-MM-DD'

        返回:
            筛选后的DataFrame
        """
        if df is None or df.empty:
            print("警告: DataFrame为空，跳过筛选")
            return df

        try:
            # 确保REPORT_DATE列是日期类型
            if not pd.api.types.is_datetime64_any_dtype(df['REPORT_DATE']):
                df['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'])

            # 处理输入的日期（转换为datetime对象）
            if len(date_str) == 8:  # YYYYMMDD格式
                target_date = pd.to_datetime(date_str, format='%Y%m%d')
            elif len(date_str) == 10 and '-' in date_str:  # YYYY-MM-DD格式
                target_date = pd.to_datetime(date_str, format='%Y-%m-%d')
            else:
                raise ValueError(f"日期格式不正确，应提供YYYYMMDD或YYYY-MM-DD格式: {date_str}")

            # 筛选大于等于目标日期的行
            filtered_df = df[df['REPORT_DATE'] >= target_date].copy()

            print(f"筛选完成: 从 {len(df)} 行数据中筛选出 {len(filtered_df)} 行 >= {date_str} 的数据")
            return filtered_df

        except Exception as e:
            print(f"错误: 筛选数据时出错: {e}")
            return df  # 出错时返回原始DataFrame




    def fetch_company_report(self,date,indicator='年报'):
        zcfz, lrb, xjll = self.get_stock_border_report(market=self.market, date=date,indicator=indicator)
        if not zcfz.empty and not lrb.empty and not xjll.empty:
            self.calculate_financial_indicators(zcfz, lrb, xjll)
            return [zcfz, lrb, xjll]
        return zcfz, lrb, xjll

    #计算指标'净资产收益率（ROE）', '毛利率', '净利率', '存货周转率'
    #       '应收账款周转率', '流动比率', '速动比率'
    def calculate_financial_indicators(self, zcfz, lrb, xjll ):
        # 合并报表，假设股票代码和公告日期是合并的关键列
        # 假设 zcfz, lrb, xjll 是已经定义好的 DataFrame
        merged_report = pd.merge(zcfz, lrb, on=['股票代码'], suffixes=('_zcfz', '_lrb'))
        merged_report = pd.merge(merged_report, xjll, on=['股票代码'], suffixes=('', '_xjll'))
        date = ''
        zcfz, lrb, xjll = self.fetch_company_report(date)
        # 计算各指标
        merged_report['净资产收益率（ROE）'] = merged_report['净利润'] / merged_report['股东权益合计']
        merged_report['毛利率'] = (merged_report['营业总收入'] - merged_report['营业总支出-营业支出']) / merged_report[
            '营业总收入']
        merged_report['净利率'] = merged_report['净利润'] / merged_report['营业总收入']

        # 近似计算流动资产和流动负债
        current_assets = merged_report['资产-货币资金'] + merged_report['资产-应收账款'] + merged_report['资产-存货']
        current_liabilities = merged_report['负债-应付账款'] + merged_report['负债-预收账款']

        merged_report['流动比率'] = current_assets / current_liabilities
        inventory = merged_report['资产-存货']
        merged_report['速动比率'] = (current_assets - inventory) / (current_liabilities + inventory)
        merged_report['存货周转率'] = merged_report['营业总支出-营业支出'] / merged_report['资产-存货']
        merged_report['应收账款周转率'] = merged_report['营业总收入'] / merged_report['资产-应收账款']

        # 选择需要的指标列
        metrics_to_add_lrb = ['股票代码', '净资产收益率（ROE）', '毛利率', '净利率', '存货周转率']
        metrics_to_add_zcfz = ['股票代码', '应收账款周转率', '流动比率', '速动比率']

        # 将指标添加到对应的报表中
        lrb = pd.merge(lrb, merged_report[metrics_to_add_lrb], on='股票代码', how='left')
        zcfz = pd.merge(zcfz, merged_report[metrics_to_add_zcfz], on='股票代码', how='left')


    def process_stock_reports(self,df_stock,get_report,batch_size=20):
        """
        分批处理股票数据并获取报告

        参数:
        df_stock (pd.DataFrame): 包含股票信息的DataFrame，需包含'代码'列
        get_report (function): 处理单只股票的函数，接收股票代码作为参数
        max_workers (int): 线程池最大工作线程数
        batch_size (int): 每批次处理的股票数量
        """
        max_workers = self.max_workers
        stock_codes = df_stock['代码'].tolist()  # 将代码列转为列表
        total_batches = (len(stock_codes) + batch_size - 1) // batch_size  # 计算总批次数
        zcfz_all, lrb_all, xjll_all = [], [], []  # 存储结果的列表

        # 使用tqdm显示总批次进度
        for batch_idx in tqdm(range(total_batches), desc="批次进度", ncols=80):
            # 计算当前批次的股票代码范围
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(stock_codes))
            batch_codes = stock_codes[start_idx:end_idx]

            # 为当前批次创建线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交当前批次的所有任务
                futures = {executor.submit(get_report, code): code for code in batch_codes}

                # 等待并处理当前批次的所有结果
                for future in futures:
                    try:
                        zcfz_i, lrb_i, xjll_i = future.result()
                        if zcfz_i is not None:
                            zcfz_all.append(zcfz_i)
                        if lrb_i is not None:
                            lrb_all.append(lrb_i)
                        if xjll_i is not None:
                            xjll_all.append(xjll_i)
                    except Exception as e:
                        stock = futures[future]
                        self.logger.error(f"处理股票 {stock} 时出错：{str(e)}")
                time.sleep(random.uniform(3, 5))
                self.logger.info(f"已处理 {batch_idx}/{total_batches} 支股票的资产负债表")
        return zcfz_all, lrb_all, xjll_all


    def process_stock_financial_reports(self,df_stock,get_report,batch_size=20):
        """
        分批处理股票数据并获取报告

        参数:
        df_stock (pd.DataFrame): 包含股票信息的DataFrame，需包含'代码'列
        get_report (function): 处理单只股票的函数，接收股票代码作为参数
        max_workers (int): 线程池最大工作线程数
        batch_size (int): 每批次处理的股票数量
        """
        max_workers = self.max_workers
        stock_codes = df_stock['代码'].tolist()  # 将代码列转为列表
        total_batches = (len(stock_codes) + batch_size - 1) // batch_size  # 计算总批次数
        financial_all= [] # 存储结果的列表

        # 使用tqdm显示总批次进度
        for batch_idx in tqdm(range(total_batches), desc="批次进度", ncols=80):
            # 计算当前批次的股票代码范围
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(stock_codes))
            batch_codes = stock_codes[start_idx:end_idx]

            # 为当前批次创建线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交当前批次的所有任务
                futures = {executor.submit(get_report, code): code for code in batch_codes}

                # 等待并处理当前批次的所有结果
                for future in futures:
                    try:
                        zcfz_i = future.result()
                        if zcfz_i is not None:
                            financial_all.append(zcfz_i)
                    except Exception as e:
                        stock = futures[future]
                        self.logger.error(f"处理股票 {stock} 时出错：{str(e)}")
                time.sleep(random.uniform(3, 5))
                self.logger.info(f"已处理 {batch_idx}/{total_batches} 支股票的资产负债表")
        return financial_all


    def merge_dataframes(self, main_df, merge_df, left_key, right_key, how='left', suffix=''):
        """
        安全地合并两个DataFrame，并记录合并信息

        参数:
            main_df: 主DataFrame
            merge_df: 要合并的DataFrame
            left_key: 主DataFrame中的键
            right_key: 要合并的DataFrame中的键
            how: 合并方式，默认为'left'
            suffix: 合并列的后缀，用于区分重复列
        """
        if merge_df is None or merge_df.empty:
            print(f"警告: {right_key} 对应的DataFrame为空，跳过合并")
            return main_df

        try:
            print(f"开始合并 {right_key} 数据，行数: {len(merge_df)}")

            # 找出所有重叠的列名（除了合并键）
            overlapping_columns = set(main_df.columns) & set(merge_df.columns)
            overlapping_columns = [col for col in overlapping_columns if col not in [left_key, right_key]]

            if overlapping_columns:
                print(f"发现重叠列: {overlapping_columns}，将使用后缀 '{suffix}' 区分")

                # 如果提供了后缀，使用它；否则使用默认后缀
                if suffix:
                    merged_df = pd.merge(main_df, merge_df,
                                         left_on=left_key,
                                         right_on=right_key,
                                         how=how,
                                         suffixes=('', suffix))
                else:
                    # 如果没有提供后缀，使用默认后缀'_y'
                    merged_df = pd.merge(main_df, merge_df,
                                         left_on=left_key,
                                         right_on=right_key,
                                         how=how)
            else:
                # 没有重叠列，直接合并
                merged_df = pd.merge(main_df, merge_df,
                                     left_on=left_key,
                                     right_on=right_key,
                                     how=how)

            # 检查合并后的列
            if right_key in merged_df.columns and right_key != left_key:
                print(f"合并后存在重复列: {right_key}，将移除")
                merged_df = merged_df.drop(columns=[right_key])

            print(f"合并完成，结果行数: {len(merged_df)}")
            return merged_df

        except Exception as e:
            print(f"错误: 合并 {right_key} 数据时出错: {e}")
            # 发生错误时返回原始DataFrame
            return main_df

    def get_stock_border_indicator_data(self, market="H", date='20240331', indicator='年报',df_stock_spot=None):
        """
                 # 获取估值数据
        # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
        # trade_date,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv
         """
        cache_key = f"估值数据"
        df_stock_financial = self.cache_service.read_from_serialized(date, cache_key)
        if df_stock_spot is None:
            df_stock_spot = self.get_stock_spot()
        df_stock_all = df_stock_spot
        # df_stock_all = df_stock_all.head(15)
        df_stock = df_stock_all

        if df_stock_financial is None or df_stock_financial.empty:
                # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
            def get_report(stock_code):
                stock_service = stockCompanyInfo(marker=market, symbol=stock_code)
                df_indicator = stock_service.get_stock_indicator_data()
                return df_indicator

            df_stock_financial_all = self.process_stock_financial_reports(df_stock, get_report, batch_size=20)
            try:
                df_stock_financial_all = pd.concat(df_stock_financial_all, ignore_index=True)
                self._write_to_cache_serialized(date, cache_key, df_stock_financial_all)
                self._write_to_csv(date, cache_key, df_stock_financial_all)
            except Exception as e:
                print(f"[CACHE]合并失败: {e}")
        else:
            df_stock_financial_all = df_stock_financial
        return df_stock_financial_all
    # 获取股票的所有数据，聚合股票数据，包括如下字段
    #   ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
    #   【'市盈率-动态', '市净率', '总市值', '流通市值',ROE、】
    #   【营业增长率、利润增长率、负债率 】
    def get_stock_border_info(self):
        date = self.reportUtils.get_current_report_year_st()
        #实时数据行情
        # 字段包括[序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
        df_stock = self.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)

        df_merge = df_stock.copy()

        # 财务指标数据
        df_financial = self.get_stock_border_financial_indicator(market = self.market, date=date,df_stock_spot=df_stock)
        date_financial = self.reportUtils.get_current_report_year_st(format='%Y-%m-%d',market=self.market)
        df_financial_current = self.reportUtils.get_finnancial_report_by_latest(df_financial)

        df_merge = pd.merge(df_merge, df_financial_current, left_on='股票代码', right_on='股票代码', how='left')

        #财报数据
        # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
        # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
        # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额
        if self.market == 'H' or self.market == 'usa':
            date = self.reportUtils.get_report_hk_year_str()
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = self.get_stock_border_report(self.market, date=date,indicator='年报',fields_unification=True)

        # 获取估值数据
        # df_indicator =   self.get_stock_border_indicator_data(market = self.market, date=date,df_stock_spot=df_stock)
        # df_merge = pd.merge(df_merge, df_indicator, left_on='代码', right_on='股票代码', how='left')

        # 分红数据
        if self.market == 'SH' or self.market == 'SZ':
            # 获取估值数据
            # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
            # trade_date,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv
            # df_indicator = ak.stock_a_indicator_lg(symbol="all")
            # df_stock = pd.merge(df_stock, df_indicator, left_on='代码', right_on='代码', how='left')
            # 分红配送，"名称","代码","送转股份 - 送转总比例","送转股份 - 送转比例","送转股份 - 转股比例","现金分红 - 现金分红比例","预案公告日","股权登记日","除权除息日","方案进度","最新公告日期","每股收益","每股净资产","每股公积金","每股未分配利润","净利润同比增长","总股本","现金分红 - 股息率"
            try :
                df_fh = self.get_stock_fhps_info(date='20241231')
                df_merge = pd.merge(df_merge, df_fh, left_on='代码', right_on='代码', how='left')
            except Exception as e:
                self.logger.error(f"获取分红数据失败:{e}")


            # 板块数据
            # df_concept = self.get_stock_board_all_concept_name()
            # print(df_concept)

        if '净资产收益率' in df_merge.columns and  'ROE'  not in df_merge.columns:
            df_merge['ROE'] = df_merge['净资产收益率']
            df_merge = self.stock_utils.pd_convert_to_float(df = df_merge, col_name='ROE')
        elif '平均净资产收益率' in df_merge.columns and  'ROE'  not in df_merge.columns:
            df_merge['ROE'] = df_merge['平均净资产收益率']
            df_merge = self.stock_utils.pd_convert_to_float(df=df_merge, col_name='ROE')

        return df_merge

        def convert_code_to_string(df, column_name):
            if column_name in df.columns:
                df[column_name] = df[column_name].astype(str)
            return df

        # 对多个DataFrame和列名应用转换函数
        if not (stock_zcfz_em_df is None or stock_lrb_em_df is None or stock_xjll_em_df is None):

            stock_zcfz_em_df = convert_code_to_string(stock_zcfz_em_df, '股票代码')
            stock_lrb_em_df = convert_code_to_string(stock_lrb_em_df, '股票代码')
            stock_xjll_em_df = convert_code_to_string(stock_xjll_em_df, '股票代码')

            # 关联 df_stock 与资产负债表
            column = '代码'
            if(self.market == 'usa'):
                column = '股票代码'
            if stock_zcfz_em_df is not None and stock_zcfz_em_df.empty == False:
                df_stock = self.merge_dataframes(df_stock, stock_zcfz_em_df, column, '股票代码','left', suffix = '_zcfz')
            # 关联合并后的结果与利润表
            if stock_lrb_em_df is not None and stock_lrb_em_df.empty == False:
                df_stock = self.merge_dataframes(df_stock, stock_lrb_em_df, column, '股票代码','left', suffix = '_lrb')
            # 关联合并后的结果与现金流量表
            if stock_xjll_em_df is not None and stock_xjll_em_df.empty == False:
                df_stock = self.merge_dataframes(df_stock, stock_xjll_em_df, column, '股票代码','left', suffix = '_xjll')
            # 移除重复的股票代码列
            df_stock = df_stock.drop(columns=['股票代码_x', '股票代码_y', '股票代码'], errors='ignore')



        # elif self.market == 'usa':
            #df_indicator = ak.stock_us_indicator_lg(symbol="all")
        # elif self.market == 'H':
            #df_indicator = ak.stock_hk_indicator_lg(symbol="all")
        # 板块数据

        return df_stock


    def get_stock_fhps_info(self,date='20241231'):
        """
        分红数据
         # 分红配送，"名称","代码","送转股份 - 送转总比例","送转股份 - 送转比例","送转股份 - 转股比例","现金分红 - 现金分红比例","预案公告日","股权登记日","除权除息日","方案进度","最新公告日期","每股收益","每股净资产","每股公积金","每股未分配利润","净利润同比增长","总股本","现金分红 - 股息率"
        :return:
        """
        df_fh = self.get_stock_fhps_info()
        date_start_str = date
        date_end_str = self.reportUtils.get_report_date_add_str(date_str=date, days=365)
        date_start = datetime.datetime.strptime(date_start_str, "%Y%m%d").date()
        date_end = datetime.datetime.strptime(date_end_str, "%Y%m%d").date()
        df_fh = df_fh[df_fh['最新公告日期'].between(date_start, date_end, inclusive='left')]
        return df_fh;


    def get_stock_fhps_info(self):
        """

        分红数据
         # 分红配送，"名称","代码","送转股份 - 送转总比例","送转股份 - 送转比例","送转股份 - 转股比例","现金分红 - 现金分红比例","预案公告日","股权登记日","除权除息日","方案进度","最新公告日期","每股收益","每股净资产","每股公积金","每股未分配利润","净利润同比增长","总股本","现金分红 - 股息率"
        "名称","代码", "现金分红 - 股息率"
        :return:
        """
        date = '20241231'
        cache_key = f"stock_fhps"
        if self.market  == 'usa' or self.market == 'H':
            cache_key = f"stock_fhps_{self.market}"
        date_start_str = date
        date_end_str = self.reportUtils.get_report_date_add_str(date_str=date, days=365)
        date_start = datetime.datetime.strptime(date_start_str, "%Y%m%d").date()
        date_end = datetime.datetime.strptime(date_end_str, "%Y%m%d").date()
        df_fh = self.cache_service.read_from_serialized(date, cache_key)
        if df_fh is not None:
            try:
                df_fh['年份'] = df_fh['最新公告日期'].apply(lambda x: x.year)
                date_columns = ['预案公告日', '股权登记日', '除权除息日', '最新公告日期']
                for col in date_columns:
                    # 将列转换为字符串，再转换为 datetime，确保所有非日期值转为 NaT
                    df_fh[col] = pd.to_datetime(df_fh[col].astype(str), errors='coerce')
                numeric_cols = ['现金分红-股息率', '每股收益', '每股净资产', '每股公积金', '每股未分配利润',
                                '净利润同比增长']
                df_fh[numeric_cols] = df_fh[numeric_cols].fillna(0)
                grouped = df_fh.groupby(['年份', '代码']).agg({
                    '名称': 'first',
                    '送转股份-送转总比例': 'first',
                    '送转股份-送转比例': 'first',
                    '送转股份-转股比例': 'first',
                    '现金分红-现金分红比例': lambda x: x.dropna().tolist(),
                    '现金分红-股息率': 'sum',
                    '每股收益': 'sum',
                    '每股净资产': 'last',
                    '每股公积金': 'last',
                    '每股未分配利润': 'last',
                    '净利润同比增长': 'last',
                    '总股本': 'first',
                    '预案公告日': lambda x: x.min(),
                    '股权登记日': lambda x: x.max(),
                    '除权除息日': lambda x: x.max(),
                    '方案进度': lambda x: x.dropna().tolist(),
                    '最新公告日期': 'max'
                }).reset_index()
                return grouped
            except  Exception as e:
                self.logger.error(f"获取分红数据失败:{e}")
                traceback.print_exc()
                raise

        self.cache_service.read_from_serialized('stock_fhps_info',date)
        try:

            # date = self.reportUtils.get_current_report_year_st()
            #  date = date
            # 分红数据
            date_array = ['20250331','20241231','20240930','20240630','20240331',
                          '20231231','20230930','20230630','20230331'
                            ,'20221231','20220930','20220630','20220331'
                            ,'20211231','20210930','20210630','20210331'
                            ,'20201231','20200930','20200630','20200331','20191231']

            df_fh =  pd.DataFrame()
            if self.market == 'SH' or self.market == 'SZ':
                for date_item in date_array:
                    df_fh_1 = ak.stock_fhps_em(date=date_item)
                    if df_fh.empty :
                        df_fh = df_fh_1.copy()
                    else:
                        if df_fh_1 is not None and df_fh_1.empty == False:
                            df_fh = pd.concat([df_fh, df_fh_1], ignore_index=True)
                        else:
                            self.logger.info(f"获取分红数据是空的:{date_item}")
                self.cache_service.write_to_cache_serialized(date, cache_key,df_fh)
                df_fh =  df_fh[df_fh['最新公告日期'].between(date_start, date_end, inclusive='left')]
                df_fh['年份'] = df_fh['最新公告日期'].apply(lambda x: x.year)
                numeric_cols = ['现金分红-股息率', '每股收益', '每股净资产', '每股公积金', '每股未分配利润',
                                '净利润同比增长']
                df_fh[numeric_cols] = df_fh[numeric_cols].fillna(0)
                date_columns = ['预案公告日', '股权登记日', '除权除息日', '最新公告日期']
                for col in date_columns:
                    # 将列转换为字符串，再转换为 datetime，确保所有非日期值转为 NaT
                    df_fh[col] = pd.to_datetime(df_fh[col].astype(str), errors='coerce')
                grouped = df_fh.groupby(['年份', '代码']).agg({
                    '名称': 'first',
                    '送转股份-送转总比例': 'first',
                    '送转股份-送转比例': 'first',
                    '送转股份-转股比例': 'first',
                    '现金分红-现金分红比例': lambda x: x.dropna().tolist(),
                    '现金分红-股息率': 'sum',
                    '每股收益': 'sum',
                    '每股净资产': 'last',
                    '每股公积金': 'last',
                    '每股未分配利润': 'last',
                    '净利润同比增长': 'last',
                    '总股本': 'first',
                    '预案公告日': lambda x: x.min(),
                    '股权登记日': lambda x: x.max(),
                    '除权除息日': lambda x: x.max(),
                    '方案进度': lambda x: x.dropna().tolist(),
                    '最新公告日期': 'max'
                }).reset_index()
                return grouped
            else:
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"获取分红数据失败:{e}")
            return pd.DataFrame()
