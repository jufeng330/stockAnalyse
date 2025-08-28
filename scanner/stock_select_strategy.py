import pandas as pd
from typing import Dict, List, Optional
import logging
import sys
import os
import numpy as np
from .stock_fh_analyser import  StockFenHengAnalyser
from .stock_financial_analyser import StockFinancialAnalyser
from .stock_report_analyser import  StockReportAnalyser
from .stock_result_utils import  StockFileUtils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from stocklib.stock_border import stockBorderInfo
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.utils_report_date import ReportDateUtils
from stocklib.stock_strategy import StockStrategy
from stocklib.utils_stock import StockUtils


class StockSelectStrategy:
    """股票筛选策略类，采用策略模式实现不同的股票筛选逻辑"""

    def __init__(self, market: str = 'SH',strategy_type='1'):
        """
        初始化策略类

        Args:
            market: 市场类型，默认'SH'表示上海市场
        """
        self.market = market
        self.stock = stockBorderInfo(market=self.market)
        self.reportUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy()
        self.stock_utils = StockUtils()
        strategy_name = self.get_strategy_name(strategy_type)
        self.file_utils = StockFileUtils(market=self.market,name = strategy_name)
        self.logger = logging.getLogger(__name__)
        self.strategy_name = strategy_name


    def get_strategy_name(self,strategy_type):
        if strategy_type == 1:
            return "高股息选股策略_1"
        elif strategy_type == 2:
            return "优质股筛选策略_2"
        elif strategy_type == 3:
            return "保守型筛选策略_3"
        elif strategy_type == 4:
            return "成长型筛选策略_4"
        elif strategy_type == 5:
            return "价值型筛选策略_5"
        elif strategy_type == 6:
            return "知名股票筛选策略_6"
        return "未知_"+strategy_type


    def  select_stock(self, df_stock,strategy_type = 1,strategy_filter = 'continnue') -> pd.DataFrame:
        """
        根据策略类型选择对应的筛选策略

        Args:
            strategy_type: 策略类型编号

        Returns:
            对应的策略方法

        Raises:
            ValueError: 当策略类型不存在时抛出
        """
        if strategy_type == 1:  # 高股息投资策略
            return self.normal_strategy(df_stock,strategy_filter)
        elif strategy_type == 2: # 优质股筛选策略
            return self.quality_strategy(df_stock,strategy_filter)
        elif strategy_type == 3:  # 保守型筛选策略 大盘蓝筹
            return self.conservative_strategy(df_stock)
        elif strategy_type == 4:  # 成长型筛选策略
            return self.growth_strategy(df_stock)
        elif strategy_type == 5:  # 价值型筛选策略
            return self.value_strategy(df_stock)
        elif strategy_type == 6:  # 价值型筛选策略
            return self.famous_stock_strategy(df_stock)
        return pd.DataFrame()

    def normal_strategy(self, df_stock: Optional[pd.DataFrame] = None,
                        strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        常规筛选策略：
        1、市值百亿以上
        2、最近3年盈利为正，营业额正增长
        3、公司估值在合理区间内 (股息率>3%)  市盈率-动态<15

        Args:
            df_stock: 输入股票数据，若为None则自动获取
            strategy_filter: 筛选方式，'continue'或'avg'

        Returns:
            筛选后的股票数据框
        """
        date = self.reportUtils.get_current_report_year_st()

        # 获取实时数据行情
        if df_stock is None:
            df_stock_spot = self.stock.get_stock_border_info() # get_stock_spot()
        else:
            df_stock_spot = df_stock.copy()

        df_stock_spot['代码'] = df_stock_spot['代码'].astype(str)
        self.logger.info(f"常规策略 - 初始股票数量：{len(df_stock_spot)}")

        # 市值筛选：100亿以上
        if '总市值' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['总市值'] > 100 * 10000 * 10000]

        # 市盈率筛选：动态市盈率<50
        if '市盈率-动态' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['市盈率-动态'] < 15]
        else:
            if '平均净资产收益率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['平均净资产收益率'] > 15] # 高股息投资策略
            if '营业总收入同比增长率' in df_stock_spot.columns:
                df_stock_spot = df_stock_spot[df_stock_spot['营业总收入同比增长率'] > 20] # 高股息投资策略

        if '现金分红-股息率' in df_stock_spot.columns:
            df_stock_spot = df_stock_spot[df_stock_spot['现金分红-股息率'] > 0.03]

        # 资产负债率筛选：<85%
        if '资产负债率' in df_stock_spot.columns:
            try:
                df = self.stock_utils.pd_convert_to_float(
                    df_stock_spot, '资产负债率'
                )
                df_stock_spot['资产负债率_%'] = df['资产负债率'].astype(float) * 100
                df_stock_spot = df_stock_spot[df_stock_spot['资产负债率_%'] < 70]
            except Exception as e:
                self.logger.error(f"资产负债率转换错误: {e}")

        self.logger.info(f"常规策略 - 资产负债率筛选后股票数量：{len(df_stock_spot)}")

        # 获取财务指标数据
        df_financial = self.stock.get_stock_border_financial_indicator(
            market=self.market, date=date, df_stock_spot=df_stock_spot
        )

        # 计算财务筛选日期
        date_financial = self.reportUtils.get_report_year_str(
            days=365 * 3, format='%Y-%m-%d'
        )
        if self.market == 'H':
            date_financial = self.reportUtils.get_report_year_str(
                days=365 * 4, format='%Y-%m-%d'
            )

        # 财务数据筛选
        # set_stocks = self._find_financial_stock_data(date_financial, df_financial, strategy_filter)

        set_stocks = set(df_stock_spot['股票代码'].tolist())

        # 应用筛选
        df_filtered = df_stock_spot[df_stock_spot['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]

        # 保存中间结果
        self.file_utils.create_middle_file(file_name='常规策略-股票基本信息', df=df_filtered)
        self.file_utils.create_middle_file(file_name='常规策略-股票财务信息', df=df_financial_filter)

        self.logger.info(f"策略{self.strategy_name} - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def quality_strategy(self, df_stock: Optional[pd.DataFrame] = None,
                         strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        优质股筛选策略：
        1、市值百亿以上
        2、最近3年盈利>5%，营业额增长>5%
        3、低估值(PE<15)、低负债率(<80%)

        Args:
            df_stock: 输入股票数据，若为None则自动获取
            strategy_filter: 筛选方式，'continue'或'avg'

        Returns:
            筛选后的股票数据框
        """
        date = self.reportUtils.get_current_report_year_st()

        # 获取实时数据行情
        if df_stock is None:
            df_stock = self.stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        self.logger.info(f"优质股策略 - 初始股票数量：{len(df_stock)}")

        # 市值筛选：100亿以上
        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 100 * 10000 * 10000]


        # 市盈率筛选：动态市盈率<30
        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 15]
        else:
            if '平均净资产收益率' in df_stock.columns:
                df_stock = df_stock[df_stock['平均净资产收益率'] > 15] # 高股息投资策略
            if '营业总收入同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['营业总收入同比增长率'] > 20] # 高股息投资策略
            if '净利润同比增长率' in df_stock.columns:
                df_stock = df_stock[df_stock['净利润同比增长率'] > 10]

        # 市净率筛选：<20
        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < 5]

        # 资产负债率筛选：<80%
        if '资产负债率' in df_stock.columns:
            df_stock = self.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < 80]

        self.logger.info(f"优质股策略 - 资产负债率筛选后股票数量：{len(df_stock)}")

        # 获取财务指标数据
        df_financial = self.stock.get_stock_border_financial_indicator(
            market=self.market, date=date, df_stock_spot=df_stock
        )

        # 计算财务筛选日期
        date_financial = self.reportUtils.get_report_year_str(
            days=365 * 3, format='%Y-%m-%d'
        )

        if '营业总收入' in df_financial.columns:
            df_financial = self.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > 10*10000*10000]

        # 财务数据筛选（提高盈利和增长阈值）
        set_stocks = self._find_financial_stock_data(
            date_financial, df_financial, strategy_filter,
            threshold_1=5000 * 10000, threshold_2=0.05, threshold_3=0.05
        )

        # 应用筛选
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]
        df_financial_filter = df_financial[df_financial['股票代码'].isin(set_stocks)]

        # 保存中间结果
        self.file_utils.create_middle_file(file_name='优质股策略-股票基本信息', df=df_filtered)
        self.file_utils.create_middle_file(file_name='优质股策略-股票财务信息', df=df_financial_filter)

        self.logger.info(f"优质股策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def conservative_strategy(self, df_stock: Optional[pd.DataFrame] = None,strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        保守型筛选策略：
        1、大盘蓝筹股(市值500亿以上)
        2、低市盈率(PE<15)
        3、高股息率(>3%)
        4、低负债率(<60%)
        """
        # 获取实时数据行情
        if df_stock is None:
            df_stock = self.stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        self.logger.info(f"保守型策略 - 初始股票数量：{len(df_stock)}")

        # 市值筛选：500亿以上
        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 500 * 10000 * 10000]

        # 市盈率筛选：动态市盈率<15
        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 15]

        # 资产负债率筛选：<60%
        if '资产负债率' in df_stock.columns:
            df_stock = self.stock_utils.pd_convert_to_float(df_stock, '资产负债率')
            df_stock['资产负债率_%'] = df_stock['资产负债率'] * 100
            df_stock = df_stock[df_stock['资产负债率_%'] < 60]

        # 股息率筛选：>3%
        fh_service = StockFenHengAnalyser(market = self.market)
        df_stock_fh,df_fh_summary = fh_service.get_fh_codes(type = strategy_filter, threshold=0.03)
        if df_fh_summary is not None and len(df_fh_summary) > 0:
            col_fh_code = '代码'
            set_fh = set(df_stock_fh[col_fh_code])
            df_stock = df_stock[df_stock['代码'].isin(set_fh)]
            df_stock = df_stock.merge(
                df_fh_summary[[col_fh_code, '平均股息率']],  # 从df_fh_summary中选择需要的列
                left_on='代码',  # df_stock的连接键
                right_on=col_fh_code,  # df_fh_summary的连接键
                how='left'  # 左连接，保留df_stock的所有行
            )

        # 保存中间结果
        self.file_utils.create_middle_file(file_name='常规策略-股票基本信息', df=df_stock)
        self.file_utils.create_middle_file(file_name='常规策略-股票股息率', df=df_stock_fh)

        self.logger.info(f"保守型策略 - 最终筛选股票数量：{len(df_stock)}")
        return df_stock

    def growth_strategy(self, df_stock: Optional[pd.DataFrame] = None,strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        成长型筛选策略：
        1、中等市值(50-200亿) 去掉
        2、高营收增长率(>30%)
        3、高净利润增长率(>10%)
        4、合理估值(PE<35)  去除
        """
        # 获取实时数据行情
        if df_stock is None:
            df_stock = self.stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        self.logger.info(f"成长型策略 - 初始股票数量：{len(df_stock)}")


        date = self.reportUtils.get_current_report_year_st()
        # 获取财务指标数据
        df_financial = self.stock.get_stock_border_financial_indicator(
            market=self.market, date=date, df_stock_spot=df_stock
        )

        # 计算财务筛选日期
        date_financial = self.reportUtils.get_report_year_str(
            days=365 * 2, format='%Y-%m-%d'
        )

        if '营业总收入' in df_financial.columns:
            df_financial = self.stock_utils.pd_convert_to_float(df_financial, '营业总收入')
            df_financial = df_financial[df_financial['营业总收入'] > 10*10000*10000]
        # 财务数据筛选：高增长率
        if self.market == 'usa':
            threshold_profit = 0
            threshold_profit_growth = 20
            threshold_revenue_growth = 30
        else:
            threshold_profit = 0
            threshold_profit_growth = 0.20
            threshold_revenue_growth = 0.30


        set_stocks = self._find_financial_stock_data(
            date_financial, df_financial, 'avg',
            threshold_1=threshold_profit, threshold_2=threshold_profit_growth, threshold_3=threshold_revenue_growth
        )

        # 应用筛选
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]


        self.logger.info(f"成长型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered

    def value_strategy(self, df_stock: Optional[pd.DataFrame] = None,strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        价值型筛选策略：
        1、低市盈率(PE<12)
        2、低市净率(PB<1.5)
        3、高ROE(>15%)
        4、盈利稳定
        """
        # 获取实时数据行情
        if df_stock is None:
            df_stock = self.stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        self.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")

        if '总市值' in df_stock.columns:
            df_stock = df_stock[df_stock['总市值'] > 500 * 10000 * 10000]

        # 市盈率筛选：动态市盈率<12
        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 12]

        # 市净率筛选：<1.5
        if '市净率' in df_stock.columns:
            df_stock = df_stock[df_stock['市净率'] < 1.5]

        date = self.reportUtils.get_current_report_year_st()
        # 获取财务指标数据
        df_financial = self.stock.get_stock_border_financial_indicator(
            market=self.market, date=date, df_stock_spot=df_stock
        )

        # 计算财务筛选日期
        date_financial = self.reportUtils.get_report_year_str(
            days=365 * 3, format='%Y-%m-%d'
        )

        # 财务数据筛选：高ROE
        set_stocks = self._find_financial_stock_data(
            date_financial, df_financial, 'avg',
            threshold_1=0, threshold_2=0, threshold_3=0
        )

        # 获取ROE数据并筛选
        if not df_financial.empty and '净资产收益率(%)' in df_financial.columns:
            df_roe = df_financial[['股票代码', '净资产收益率(%)']].dropna()
            df_roe = self.stock_utils.pd_convert_to_float(df_roe, '净资产收益率(%)')
            df_roe = df_roe[df_roe['净资产收益率(%)'] > 15]
            set_roe = set(df_roe['股票代码'])
            set_stocks = set_stocks & set_roe

        # 应用筛选
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        self.logger.info(f"价值型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered


    def famous_stock_strategy(self, df_stock: Optional[pd.DataFrame] = None,strategy_filter: str = 'avg') -> pd.DataFrame:
        """
        价值型筛选策略： 知名股票筛选
        1、低市盈率(PE<12)
        2、低市净率(PB<1.5)
        3、高ROE(>15%)
        4、盈利稳定
        """
        # 获取实时数据行情
        if df_stock is None:
            df_stock = self.stock.get_stock_spot()
        df_stock['代码'] = df_stock['代码'].astype(str)
        self.logger.info(f"价值型策略 - 初始股票数量：{len(df_stock)}")


        # 市盈率筛选：动态市盈率<12
        if '市盈率-动态' in df_stock.columns:
            df_stock = df_stock[df_stock['市盈率-动态'] < 50]

        df_famous_stock = self.stock.get_famous_stock_info()
        set_famous_stocks = set(df_famous_stock['股票代码'].tolist())

        date = self.reportUtils.get_current_report_year_st()
        # 获取财务指标数据
        df_financial = self.stock.get_stock_border_financial_indicator(
            market=self.market, date=date, df_stock_spot=df_stock
        )

        # 计算财务筛选日期
        date_financial = self.reportUtils.get_report_year_str(
            days=365 * 3, format='%Y-%m-%d'
        )

        if self.market == 'usa':
            threshold_profit = 0
            threshold_profit_growth = 5
            threshold_revenue_growth = 10
        else:
            threshold_profit = 0
            threshold_profit_growth = 0.05
            threshold_revenue_growth = 0.05


        set_stocks = self._find_financial_stock_data(
            date_financial, df_financial, 'avg',
            threshold_1=threshold_profit, threshold_2=threshold_profit_growth, threshold_3=threshold_revenue_growth
        )


        set_stocks = set_stocks & set_famous_stocks
        # 应用筛选
        df_filtered = df_stock[df_stock['股票代码'].isin(set_stocks)]

        self.logger.info(f"价值型策略 - 最终筛选股票数量：{len(df_filtered)}")
        return df_filtered



    def _find_financial_stock_data(self, date_financial: str, df_financial: pd.DataFrame,
                                   data_type: str = 'continue',
                                   threshold_1: float = 0.0,
                                   threshold_2: float = 0.0,
                                   threshold_3: float = 0.0) -> set:
        """
        内部方法：根据财务数据筛选股票

        Args:
            date_financial: 财务数据筛选日期
            df_financial: 财务数据框
            data_type: 筛选类型，'continue'或'avg'
            threshold_1: 净利润阈值
            threshold_2: 净利润同比增长率阈值
            threshold_3: 营业总收入同比增长率阈值

        Returns:
            符合条件的股票代码集合
        """
        if data_type == 'continue':
            # 连续增长筛选
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_1
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")

            col_lrl = '净利润同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_2
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")

            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_continue_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_3
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_yy)}")
        else:
            # 平均水平筛选
            col_lrl = '净利润'
            col_lrl_rename = '全年利润率为正'
            set_stocks_lrl = self.stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_1
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl)}")

            col_lrl = '净利润同比增长率'
            if self.market == 'usa' and col_lrl not in df_financial.columns:
                df_financial = self.compute_financial_lrl_ratio(df_financial,col_lrl)
                # col_lrl = '净利率同比增长率'
            col_lrl_rename = '利润率同比为正'
            set_stocks_lrl_ratio = self.stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_2
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_lrl_ratio)}")

            col_lrl = '营业总收入同比增长率'
            col_lrl_rename = '全年业务收入增长率为正'
            set_stocks_yy = self.stock_strategy.get_stock_avg_postive(
                df_financial, date_financial, col_lrl, col_lrl_rename, threshold_3
            )
            self.logger.info(f"财务筛选 - {col_lrl} 合格股票数量：{len(set_stocks_yy)}")

        # 取交集
        set_stocks = set_stocks_lrl & set_stocks_yy & set_stocks_lrl_ratio
        self.logger.info(f"财务筛选 - 最终合格股票数量：{len(set_stocks)}")
        return set_stocks

    def compute_financial_lrl_ratio(self, df_financial, col_lrl='净利润同比增长率', col_lr='净利润'):
        # 1. 确保数据按年份升序排列（关键：保证“上一年”正确对应）
        # 检查是否包含'年份'列，避免排序失败
        if '年份' not in df_financial.columns:
            raise ValueError("数据框必须包含'年份'列")
        df_financial = df_financial.sort_values(by='年份').reset_index(drop=True)

        # 2. 提取上一年的指标值（使用shift(1)获取上一行数据）
        prev_col = f'上一年{col_lr}'  # 动态生成临时列名，避免与现有列冲突
        df_financial[prev_col] = df_financial[col_lr].shift(1)

        # 3. 用向量化操作计算同比增长率（替代apply，提升效率）
        # 先计算基础增长率
        growth = (df_financial[col_lr] - df_financial[prev_col]) / df_financial[prev_col] * 100

        # 处理特殊情况：无上一年数据、上一年为0、上一年为负导致的异常
        mask_no_prev = pd.isna(df_financial[prev_col])  # 无上一年数据
        mask_prev_zero = df_financial[prev_col] == 0  # 上一年为0
        mask_prev_neg = df_financial[prev_col] < 0  # 上一年为负

        # 无上一年数据：设为NaN
        growth[mask_no_prev] = np.nan

        # 上一年为0：当年为0则NaN，否则用正负无穷表示极端变化
        growth[mask_prev_zero & (df_financial[col_lr] == 0)] = np.nan
        growth[mask_prev_zero & (df_financial[col_lr] > 0)] = np.inf
        growth[mask_prev_zero & (df_financial[col_lr] < 0)] = -np.inf

        # 上一年为负：若当年扭亏为盈或亏损收窄，可标记为特殊值（根据业务需求调整）
        # 示例：此处保留原始计算结果，也可改为np.nan或其他标识
        # growth[mask_prev_neg] = np.nan  # 如需忽略负基数的情况，取消注释此行

        # 4. 赋值到目标列
        df_financial[col_lrl] = growth

        # 5. 清理临时列
        df_financial = df_financial.drop(columns=[prev_col])

        return df_financial

