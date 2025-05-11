import akshare as ak
import datetime
import pandas as pd
from .stock_concept_data import stockConceptData
from .stock_news_data import stockNewsData
from .stock_ak_indicator import stockAKIndicator
from .stock_annual_report import stockAnnualReport
from .stock_company import stockCompanyInfo
import concurrent.futures
import os
import pickle

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

    # 获取所有股票数据的实时信息 和资金情况
    # 字段详情：   序号   股票代码  股票简称     最新价     涨跌幅    换手率     流入资金     流出资金        净额      成交额
    def get_stock_all_info(self):
        stock_fund_flow_individual_df = ak.stock_fund_flow_individual(symbol="即时")
        return stock_fund_flow_individual_df

        # 定义格式化函数，作为静态方法

    @staticmethod
    def format_float(x):
        if isinstance(x, (float, int)):
            return f"{x:.1f}"
        return x

    # 获取所有股票的实时行情
    # 字段包括  ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
    def get_stock_spot(self):
        if self.market == 'SH' or self.market == 'SZ':
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
            print(stock_zh_a_spot_em_df)
        elif  self.market == 'usa':
            # ['序号', '名称', '最新价', '涨跌额', '涨跌幅', '开盘价', '最高价', '最低价', '昨收价', '总市值', '市盈率','成交量', '成交额', '振幅', '换手率', '代码']
            stock_zh_a_spot_em_df = ak.stock_us_spot_em()
        elif self.market == 'H':
            # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
            stock_zh_a_spot_em_df = ak.stock_hk_spot_em()
            # stock_hk_valuation_baidu_df = ak.stock_hk_valuation_baidu(symbol="06969", indicator="总市值", period="近一年")

        else:
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()

        return stock_zh_a_spot_em_df

    # 获取所有股票的财务报表
    # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
    # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
    # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额  融资性现金流-净现金流占比       公告日期
    def get_stock_border_report(self,  market="SH", date='20240331', indicator='年报'):
        if market == 'SH' or market == 'SZ':
            # 资产负债表
            zcfz_key = f"zcfz_{date}"
            stock_zcfz_em_df = self._read_from_cache(date, "zcfz")
            if stock_zcfz_em_df is None:
                stock_zcfz_em_df = ak.stock_zcfz_em(date=date)
                self._write_to_cache(date, "zcfz", stock_zcfz_em_df)

            # 利润表
            lrb_key = f"lrb_{date}"
            stock_lrb_em_df = self._read_from_cache(date, "lrb")
            if stock_lrb_em_df is None:
                stock_lrb_em_df = ak.stock_lrb_em(date=date)
                self._write_to_cache(date, "lrb", stock_lrb_em_df)

            # 现金流量表
            xjll_key = f"xjll_{date}"
            stock_xjll_em_df = self._read_from_cache(date, "xjll")
            if stock_xjll_em_df is None:
                stock_xjll_em_df = ak.stock_xjll_em(date=date)
                self._write_to_cache(date, "xjll", stock_xjll_em_df)
        elif market == 'H' or market == 'usa':
            stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = self.get_stock_border_report_by_market( market=market, date=date, indicator=indicator)

        else:
            ak.stock_financial_us_report_em(symbol="105.TSLA", indicator="资产负债表", period="报告期")
            return None,None,None
            # {"资产负债表", "综合损益表", "现金流量表"}
            # {"年报", "单季报", "累计季报"}

        stock_zcfz_em_df.apply(lambda x: x.map(self.format_float))
        stock_lrb_em_df.apply(lambda x: x.map(self.format_float))
        stock_xjll_em_df.apply(lambda x: x.map(self.format_float))
        return stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df
    #获取所有的板块信息  日期         概念名称 成分股数量  网址     代码
    def get_stock_board_all_concept_name(self):
        if self.market  == self.HongKong or self.market == self.usa:
            return self.get_default_df()
        stock_concept_service = stockConceptData()
        stock_board = stock_concept_service.stock_board_concept_name_ths()

        return stock_board

    # 获取所有股票的代码和名称
    def get_stock_all_code(self):
        stock_info_a_code_name_df = ak.stock_info_a_code_name()
        print(stock_info_a_code_name_df)
        return stock_info_a_code_name_df
    # 获取北向的持仓数据    序号     代码    名称   今日收盘价  今日涨跌幅   今日持股-股数     今日持股-市值  今日持股-占流通股比  今日持股-占总股本比  今日增持估计-股数  今日增持估计-市值  今日增持估计-市值增幅  今日增持估计-占流通股比  今日增持估计-占总股本比   所属板块         日期
    def get_stock_hsgt_hold_stock_em(self):
        stock_em_hsgt_hold_stock_df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
        print(stock_em_hsgt_hold_stock_df)
        return stock_em_hsgt_hold_stock_df

    def _get_cache_filepath(self, date, report_type,file_type='csv'):
        """生成缓存文件路径"""
        if file_type == 'csv':
            return os.path.join(self.cache_dir, f"{report_type}_{date}_{self.market}.csv")
        else:
            return os.path.join(self.cache_dir, f"{report_type}_{date}_{self.market}.pkl")


    def _read_from_cache(self, date, report_type):
        """从 CSV 缓存读取数据"""
        filepath = self._get_cache_filepath(date, report_type)
        if os.path.exists(filepath):
            try:
                return pd.read_csv(filepath, index_col=0)  # index_col=0 避免额外索引列
            except Exception as e:
                print(f"[CACHE] 读取缓存失败: {filepath}, 错误: {e}")
                return None
        return None

    def _write_to_cache(self, date, report_type, data):
        """将 DataFrame 写入 CSV 缓存"""
        filepath = self._get_cache_filepath(date, report_type)
        try:
            data.to_csv(filepath, index=False)  # index=False 避免保存无意义的索引
            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")


    def _write_to_cache_serialized(self, date, report_type, data):
        """将 DataFrame 使用 pickle 写入二进制缓存"""
        filepath = self._get_cache_filepath(date, report_type,file_type='pkl')
        try:
            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # 使用 pickle 写入二进制文件
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)

            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")

    def _read_from__serialized(self, date, report_type):
        """从 pickle 缓存中读取数据"""
        filepath = self._get_cache_filepath(date, report_type,file_type='pkl')
        try:
            if os.path.exists(filepath):
                # 从 pickle 文件中读取数据
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            return None  # 文件不存在返回 None
        except Exception as e:
            print(f"[CACHE] 读取缓存失败: {filepath}, 错误: {e}")
            return None



    #计算指标'净资产收益率（ROE）', '毛利率', '净利率', '存货周转率'
    #       '应收账款周转率', '流动比率', '速动比率'
    def calculate_financial_indicators(self, zcfz, lrb, xjll ):
        # 合并报表，假设股票代码和公告日期是合并的关键列
        # 假设 zcfz, lrb, xjll 是已经定义好的 DataFrame
        merged_report = pd.merge(zcfz, lrb, on=['股票代码'], suffixes=('_zcfz', '_lrb'))
        merged_report = pd.merge(merged_report, xjll, on=['股票代码'], suffixes=('', '_xjll'))

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

    def get_stock_border_report_by_market(self, market="H", date='20240331', indicator='年报'):
        report_service = stockAnnualReport()
        if market == 'SH' or market == 'SZ':
            zcfz, lrb, xjll = report_service.get_stock_border_report(market=market, date=date,indicator = indicator)
            return zcfz, lrb, xjll
        elif market == 'H' or  market == 'usa':
            stock_zcfz_em_df = self._read_from_cache(date, "zcfz")
            stock_lrb_em_df = self._read_from_cache(date, "lrb")
            stock_xjll_em_df = self._read_from_cache(date, "xjll")

            if stock_lrb_em_df is None or stock_zcfz_em_df is None or stock_xjll_em_df is None:
                # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
                if market == 'H':
                    df_stock_all = ak.stock_hk_spot_em()
                    df_stock = df_stock_all[0:1000]
                else:
                    df_stock_all = ak.stock_us_spot_em()
                    df_stock = df_stock_all[0:1000]
                zcfz_all = []
                lrb_all = []
                xjll_all = []

                def get_report(stock_code):
                    return report_service.get_stock_report(stock_code=stock_code, market=market,
                                                           indicator=indicator)

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    results = list(executor.map(get_report, df_stock['代码']))

                for zcfz_i, lrb_i, xjll_i in results:
                    if zcfz_i is not None :
                        zcfz_all.append(zcfz_i)
                    if  lrb_i is not None :
                        lrb_all.append(lrb_i)
                    if xjll_i is not None:
                        xjll_all.append(xjll_i)

                try:
                    self._write_to_cache_serialized(date, "zcfz", zcfz_all)
                    self._write_to_cache_serialized(date, "lrb", lrb_all)
                    self._write_to_cache_serialized(date, "xjll", xjll_all)
                    zcfz = pd.concat(zcfz_all, ignore_index=True)
                    lrb = pd.concat(lrb_all, ignore_index=True)
                    xjll = pd.concat(xjll_all, ignore_index=True)
                except Exception as e:
                    print(f"[CACHE]合并失败: {e}")

                self._write_to_cache(date, "zcfz", zcfz)
                self._write_to_cache(date, "lrb", lrb)
                self._write_to_cache(date, "xjll", xjll)
                return zcfz, lrb, xjll
            else:
                zcfz_all = stock_zcfz_em_df
                lrb_all = stock_lrb_em_df
                xjll_all = stock_xjll_em_df
                return zcfz_all, lrb_all, xjll_all
        else:
            zcfz, lrb, xjll = report_service.get_stock_border_report(market=market, date=date, indicator=indicator)
            return zcfz, lrb, xjll

