""""
Stock Analysis System
优化后的全盘股票技术分析系统——用于A股市场股票的全面分析，已加速分析并增加额外指标。
"""

import logging
import traceback
import sys
import os
from datetime import datetime
from typing import Dict, Optional, Tuple
from stock_analyse.stocklib.technical_params import TechnicalParams

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from stocklib.stock_company import stockCompanyInfo
from stocklib.utils_report_date import ReportDateUtils
from stocklib.utils_file_cache import FileCacheUtils
from stocklib.stock_strategy import StockStrategy


# -------------------------------
# **股票分析引擎**
# -------------------------------
class StockAnalyzer:
    """股票分析引擎，计算各类技术指标"""

    def __init__(self, params: Optional[TechnicalParams] = None,market='SH'):
        """
        初始化股票分析引擎

        Args:
            params: 技术指标配置参数
        """
        self._setup_logging()
        self.params = params or TechnicalParams.default()
        self.dateUtils = ReportDateUtils()
        self.stock_strategy = StockStrategy(market = market)
        self.cache_service = FileCacheUtils(market=market)
        self.market = market
        self.cache_switch = False


    def _setup_logging(self) -> None:
        """配置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def calculate_ema(series: pd.Series, period: int) -> pd.Series:
        """计算 EMA"""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int) -> pd.Series:
        """
        计算 RSI（指数加权移动平均法）
        """
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_macd(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """计算 MACD、信号线和直方图"""
        exp1 = series.ewm(span=12, adjust=False).mean()
        exp2 = series.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        return macd, signal, macd - signal

    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int, std_dev: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """计算 Bollinger 通道"""
        middle = series.rolling(window=period, min_periods=period).mean()
        std = series.rolling(window=period, min_periods=period).std()
        upper = middle + std * std_dev
        lower = middle - std * std_dev
        return upper, middle, lower

    def calculate_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        """计算 ATR"""
        high = df['最高']
        low = df['最低']
        prev_close = df['收盘'].shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def calculate_obv(series_close: pd.Series, series_volume: pd.Series) -> pd.Series:
        """计算 OBV（能量潮指标）"""
        diff = series_close.diff().fillna(0)
        obv = np.where(diff > 0, series_volume, np.where(diff < 0, -series_volume, 0))
        return pd.Series(obv, index=series_close.index).cumsum()

    @staticmethod
    def calculate_stochastic(series_close: pd.Series, window: int = 14) -> Tuple[pd.Series, pd.Series]:
        """
        计算随机指标（Stochastic Oscillator）
           %K = (close - lowest_low) / (highest_high - lowest_low)*100
           %D 为 %K 的3日简单移动平均
        """
        lowest = series_close.rolling(window=window, min_periods=window).min()
        highest = series_close.rolling(window=window, min_periods=window).max()
        percentK = (series_close - lowest) / (highest - lowest + 1e-10) * 100
        percentD = percentK.rolling(window=3, min_periods=3).mean()
        return percentK, percentD

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有技术指标，并增加 OBV 和随机指标"""
        try:
            # 移动均线
            for key, period in self.params.ma_periods.items():
                df[f'MA_{period}'] = self.calculate_ema(df['收盘'], period)
            df['RSI'] = self.calculate_rsi(df['收盘'], self.params.rsi_period)
            df['MACD'], df['Signal'], df['MACD_hist'] = self.calculate_macd(df['收盘'])
            df['BB_upper'], df['BB_middle'], df['BB_lower'] = self.calculate_bollinger_bands(
                df['收盘'], self.params.bollinger_period, self.params.bollinger_std)
            df['Volume_MA'] = df['成交量'].rolling(window=self.params.volume_ma_period,
                                                   min_periods=self.params.volume_ma_period).mean()
            df['Volume_Ratio'] = df['成交量'] / (df['Volume_MA'] + 1e-10)
            df['ATR'] = self.calculate_atr(df, self.params.atr_period)
            df['Volatility'] = df['ATR'] / df['收盘'] * 100
            df['ROC'] = df['收盘'].pct_change(periods=10) * 100

            # 增加 OBV 指标
            df['OBV'] = self.calculate_obv(df['收盘'], df['成交量'])
            df['OBV_MA10'] = df['OBV'].rolling(window=10, min_periods=10).mean()

            # 增加随机指标 Stochastic
            df['%K'], df['%D'] = self.calculate_stochastic(df['收盘'], window=14)

            return df

        except Exception as e:
            self.logger.error(f"指标计算出错：{str(e)}")
            raise

    def analyze_stock(self, stock,market="SH") -> Dict:
        """针对单只股票执行完整的技术分析流程"""
        if isinstance(stock, pd.DataFrame):
            # 如果是DataFrame且只有一行，转换为Series
            if len(stock) == 1:
                stock = stock.iloc[0]
        stock_code = stock['代码']
        market = stock['market']
        try:

            stock_service = stockCompanyInfo(marker=market, symbol=stock_code)
            end_date_str  = self.dateUtils.get_current_history_date_st()
            start_date_str = self.dateUtils.get_start_history_date_st()
            # end_date_str = '20250607'

            report_type = 'history_'+stock_code
            date = end_date_str
            df_history_data = None
            if self.cache_switch :
                df_history_data = self.cache_service.read_from_csv(date, report_type=report_type)
            if df_history_data is None:
                # # 日期，开盘，收盘，最高，最低，成交量，成交额，振幅，涨跌幅，涨跌额，换手率，股票代码
                df_history_data = stock_service.get_stock_history_data(start_date_str,end_date_str)
                if self.cache_switch:
                    self.cache_service.write_to_csv(date, report_type, df_history_data)

            result = self.compute_result(df_history_data, stock, stock_code)


            # print(result)
            return result

        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 失败：{str(e)}")
            traceback.print_exc()
            raise

    def compute_result(self, df_history_data, stock, stock_code):
        """ 计算出结果
        df_history_data: 历史成交数据和指标计算结果
        stock: 股票实时信息和股票财务信息汇总
        包含如下信息:  stock_code,suggestion,analysis_date,score,price,price_change,signal
        增加如下列 基本信息: stock_name 市值、'流通市值',股本数量,行业、
                 估值信息: 净资产收益率-ROE、市盈率-PE,'市净率-PB',动态市盈率-PE-D,  市销率 股息率  营业增长率  利润增长率  负债率 DCF
                 市场行情： [振幅', '换手率','60日涨跌幅', '年初至今涨跌幅']
                 技术指标:  均线策略、KDJ 策略、RSI 策略、MACD 策略、成交量策略、威廉指标、ADX 策略、突破策略、SAR 策略、均值回归策略
                 市场情绪:
                 分析结果:  买入建议、卖出建议、风险评估、市场趋势、技术分析、市场预测、投资策略

          ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅']
    #   【'市盈率-动态', '市净率', '总市值', '流通市值',ROE、】
    #   【营业增长率、利润增长率、负债率 】
        """
        if df_history_data is None or df_history_data.empty:
            result = {
                'stock_code': stock_code,
                'stock_name': stock_code,
                'suggestion': '不建议买入',
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'score': 0,
                'price': 0,
                'price_change': -1,
                'signal':''
            }
            return result
        df_summary_data = self.stock_strategy.calculate_stock_data(df_history_data,stock,stock_code)
        score, buy_signal_str = self.stock_strategy.calculate_score(df_history_data=df_history_data, df_stock=stock,df_summary_data = df_summary_data)

        score2, buy_signal_str2 = self.calculate_score_simple(df_history_data, stock_code)

        suggestion = self.stock_strategy.get_recommendation(score)
        latest = df_history_data.iloc[-1]
        # prev = df_history_data.iloc[-2]
        df_summary_data['signal'] = buy_signal_str
        df_summary_data['score'] = score
        df_summary_data['score_simple'] = score2
        df_summary_data['suggestion'] = suggestion

        result = df_summary_data.to_dict('records')[0]
        return result

    def calculate_score_simple(self, df_history_data, stock_code):
        score2 = -1
        try:
            if df_history_data is None or len(df_history_data) == 0:
                return score2, ''
            df_copy = df_history_data.copy(deep=True)
            self.calculate_indicators(df_copy)
            score2, score2_suggestion = self.stock_strategy.calculate_score_simple(df_copy)
            result_2 = self.stock_strategy.find_vol_inc_stock(df_copy, stock_code)
            print(f'find_vol_inc_stock:{result_2}')
            result_3 = self.stock_strategy.find_macd_inc_stock(df_copy, stock_code)
            print(f'find_macd_inc_stock:{result_3}')

            return score2, score2_suggestion
        except Exception as e:
            self.logger.error(f"分析股票score2 {stock_code} 失败：{str(e)}")
        return score2,''
