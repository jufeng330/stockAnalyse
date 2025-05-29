import akshare as ak
import datetime
import pandas as pd
from .stock_concept_data import stockConceptData
from .stock_news_data import stockNewsData
from .stock_ak_indicator import stockAKIndicator
from .stock_annual_report import stockAnnualReport
from .utils_report_date import ReportDateUtils
from .stock_company import stockCompanyInfo
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
            stock_zh_a_spot_em_df['股票代码'] = stock_zh_a_spot_em_df['代码'].apply(lambda x: self.reportUtils.get_stock_code(market=self.market, symbol=x))

        elif self.market == 'H':
            # ['序号', '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高', '最低', '昨收', '成交量', '成交额']
            # stock_zh_a_spot_em_df = ak.stock_hk_spot_em()
            stock_zh_a_spot_em_df = ak.stock_us_famous_spot_em(symbol='科技类')
            # stock_hk_valuation_baidu_df = ak.stock_hk_valuation_baidu(symbol="06969", indicator="总市值", period="近一年")

        else:
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()


        return stock_zh_a_spot_em_df

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
    # 获取所有股票的财务报表
    # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
    # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
    # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额  融资性现金流-净现金流占比       公告日期
    def get_stock_border_report(self,  market="SH", date='20240331', indicator='年报'):
        if market == 'SH' or market == 'SZ':
            # 资产负债表
            zcfz_key = f"zcfz_{date}"
            stock_zcfz_em_df = self._read_from_csv(date, "zcfz")
            if stock_zcfz_em_df is None:
                stock_zcfz_em_df = ak.stock_zcfz_em(date=date)
                self._write_to_csv(date, "zcfz", stock_zcfz_em_df)

            # 利润表
            lrb_key = f"lrb_{date}"
            stock_lrb_em_df = self._read_from_csv(date, "lrb")
            if stock_lrb_em_df is None:
                stock_lrb_em_df = ak.stock_lrb_em(date=date)
                self._write_to_csv(date, "lrb", stock_lrb_em_df)

            # 现金流量表
            xjll_key = f"xjll_{date}"
            stock_xjll_em_df = self._read_from_csv(date, "xjll")
            if stock_xjll_em_df is None:
                stock_xjll_em_df = ak.stock_xjll_em(date=date)
                self._write_to_csv(date, "xjll", stock_xjll_em_df)
        elif market == 'H' or market == 'usa':
            stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = self.get_stock_border_report_by_market( market=market, date=date, indicator=indicator)
            stock_zcfz_em_df = self.filter_by_report_date(stock_zcfz_em_df, date)
            stock_lrb_em_df = self.filter_by_report_date(stock_lrb_em_df, date)
            stock_xjll_em_df = self.filter_by_report_date(stock_xjll_em_df, date)
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


    def _read_from_csv(self, date, report_type):
        """从 CSV 缓存读取数据"""
        filepath = self._get_cache_filepath(date, report_type)
        if os.path.exists(filepath):
            try:
                return pd.read_csv(filepath, index_col=0)  # index_col=0 避免额外索引列
            except Exception as e:
                print(f"[CACHE] 读取缓存失败: {filepath}, 错误: {e}")
                return None
        return None

    def _write_to_csv(self, date, report_type, data,force=False):
        """将 DataFrame 写入 CSV 缓存"""
        filepath = self._get_cache_filepath(date, report_type)
        try:
            if not force and  os.path.exists(filepath):
                return
            data.to_csv(filepath, index=False)  # index=False 避免保存无意义的索引
            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")


    def write_to_csv_force(self, stock_zcfz_em_df,stock_lrb_em_df,stock_xjll_em_df,date):
        self._write_to_csv(date, "zcfz", stock_zcfz_em_df,force=True)
        self._write_to_csv(date, "lrb", stock_lrb_em_df,force=True)
        self._write_to_csv(date, "xjll", stock_xjll_em_df,force=True)

    def _write_to_cache_serialized(self, date, report_type, data,force=False):
        """将 DataFrame 使用 pickle 写入二进制缓存"""
        filepath = self._get_cache_filepath(date, report_type,file_type='pkl')
        try:
            if not force and os.path.exists(filepath) :
                return
            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            # 使用 pickle 写入二进制文件
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)

            print(f"[CACHE] 已缓存 {report_type} 数据到: {filepath}")
        except Exception as e:
            print(f"[CACHE] 写入缓存失败: {filepath}, 错误: {e}")

    def _read_from_serialized(self, date, report_type):
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
    def get_stock_border_report_by_market(self, market="H", date='20240331', indicator='年报'):
        report_service = stockAnnualReport()
        if market == 'SH' or market == 'SZ':
            # 资产负债表
            zcfz_key = f"zcfz_{date}"
            stock_zcfz_em_df = self._read_from_csv(date, "zcfz")
            if stock_zcfz_em_df is None:
                stock_zcfz_em_df = ak.stock_zcfz_em(date=date)
                self._write_to_csv(date, "zcfz", stock_zcfz_em_df)


            # 利润表
            lrb_key = f"lrb_{date}"
            stock_lrb_em_df = self._read_from_csv(date, "lrb")
            if stock_lrb_em_df is None:
                stock_lrb_em_df = ak.stock_lrb_em(date=date)
                self._write_to_csv(date, "lrb", stock_lrb_em_df)

            # 现金流量表
            xjll_key = f"xjll_{date}"
            stock_xjll_em_df = self._read_from_csv(date, "xjll")
            if stock_xjll_em_df is None:
                stock_xjll_em_df = ak.stock_xjll_em(date=date)
                self._write_to_csv(date, "xjll", stock_xjll_em_df)
            zcfz, lrb, xjll = stock_zcfz_em_df,stock_lrb_em_df,stock_xjll_em_df
            return zcfz, lrb, xjll
        elif market == 'H' or  market == 'usa':
            stock_zcfz_em_df = self._read_from_serialized(date, "zcfz")
            stock_lrb_em_df = self._read_from_serialized(date, "lrb")
            stock_xjll_em_df = self._read_from_serialized(date, "xjll")

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

                zcfz_all, lrb_all, xjll_all = self.process_stock_reports(df_stock, get_report,batch_size=20)
                try:
                    self._write_to_cache_serialized(date, "zcfz", zcfz_all)
                    self._write_to_cache_serialized(date, "lrb", lrb_all)
                    self._write_to_cache_serialized(date, "xjll", xjll_all)
                    zcfz_all = pd.concat(zcfz_all, ignore_index=True)
                    lrb_all = pd.concat(lrb_all, ignore_index=True)
                    xjll_all = pd.concat(xjll_all, ignore_index=True)

                except Exception as e:
                    print(f"[CACHE]合并失败: {e}")

                self._write_to_csv(date, "zcfz", zcfz_all)
                self._write_to_csv(date, "lrb", lrb_all)
                self._write_to_csv(date, "xjll", xjll_all)

            else:
                zcfz_all = pd.concat(stock_zcfz_em_df, ignore_index=True)
                lrb_all = pd.concat(stock_lrb_em_df, ignore_index=True)
                xjll_all = pd.concat(stock_xjll_em_df, ignore_index=True)

                subset = ['SECURITY_CODE', 'REPORT_DATE','STD_ITEM_CODE']

                zcfz_all = zcfz_all.drop_duplicates(subset=subset, keep='first')
                lrb_all = lrb_all.drop_duplicates(subset=subset, keep='first')
                xjll_all = xjll_all.drop_duplicates(subset=subset, keep='first')
        else:
            zcfz_all, lrb_all, xjll_all = report_service.get_stock_border_report(market=market, date=date, indicator=indicator)

        # 将报表的行转成列
        index_cols = ['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT',
                      '股票代码']
        item_col = 'ITEM_NAME'
        if( market == 'H'):
            index_cols = ['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'ORG_CODE', 'REPORT_DATE', 'DATE_TYPE_CODE', 'FISCAL_YEAR',  '股票代码']
            item_col = 'STD_ITEM_NAME'
        zcfz_all = self.reportUtils.pivot_financial_usa_data(df=zcfz_all, index_cols = index_cols,item_col=item_col,fill_value=0)
        lrb_all = self.reportUtils.pivot_financial_usa_data(df=lrb_all, index_cols = index_cols,item_col=item_col,fill_value=0)
        xjll_all = self.reportUtils.pivot_financial_usa_data(df=xjll_all, index_cols = index_cols,item_col=item_col,fill_value=0)

        zcfz_all = self.reportUtils.map_zcfz_share_to_a_share(h_share_df=zcfz_all,market=market)
        lrb_all = self.reportUtils.map_lrb_share_to_a_share(h_share_df=lrb_all,market=market)
        xjll_all = self.reportUtils.map_xjll_share_to_a_share(h_share_df=xjll_all,market=market)

        return zcfz_all, lrb_all, xjll_all

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

        #财报数据
        # 资产负债表   序号   股票代码  股票简称      资产-货币资金      资产-应收账款        资产-存货       资产-总资产    资产-总资产同比      负债-应付账款       负债-预收账款       负债-总负债    负债-总负债同比      资产负债率        股东权益合计       公告日期
        # 利润表   序号   股票代码  股票简称           净利润          净利润同比        营业总收入       营业总收入同比   营业总支出-营业支出    营业总支出-销售费用   营业总支出-管理费用    营业总支出-财务费用  营业总支出-营业总支出          营业利润          利润总额       公告日期
        # 现金流量表   序号   股票代码  股票简称     净现金流-净现金流      净现金流-同比增长  经营性现金流-现金流量净额  经营性现金流-净现金流占比  投资性现金流-现金流量净额  投资性现金流-净现金流占比  融资性现金流-现金流量净额
        if self.market == 'H' or self.market == 'usa':
            date = self.reportUtils.get_report_hk_year_str()
        stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = self.get_stock_border_report(self.market, date=date,indicator='年报')

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

        # 分红数据
        if self.market ==  'SH' or self.market == 'SZ':
            #获取估值数据
            # 交易日，市盈率，市盈率 TTM, 市净率，市销率，市销率 TTM, 股息率，股息率 TTM, 总市值
            # trade_date,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv
            #df_indicator = ak.stock_a_indicator_lg(symbol="all")
            # df_stock = pd.merge(df_stock, df_indicator, left_on='代码', right_on='代码', how='left')

            # 分红配送，"名称","代码","送转股份 - 送转总比例","送转股份 - 送转比例","送转股份 - 转股比例","现金分红 - 现金分红比例","预案公告日","股权登记日","除权除息日","方案进度","最新公告日期","每股收益","每股净资产","每股公积金","每股未分配利润","净利润同比增长","总股本","现金分红 - 股息率"
            df_fh = ak.stock_fhps_em(date=date)
            df_stock = pd.merge(df_stock, df_fh, left_on='代码', right_on='代码', how='left')

        # elif self.market == 'usa':
            #df_indicator = ak.stock_us_indicator_lg(symbol="all")
        # elif self.market == 'H':
            #df_indicator = ak.stock_hk_indicator_lg(symbol="all")
        # 板块数据

        return df_stock

