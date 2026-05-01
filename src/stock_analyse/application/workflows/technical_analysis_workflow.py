from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo
from stock_analyse.domain.services.stock_strategy_service import StockStrategy
from stock_analyse.domain.services.technical_params import TechnicalParams
from stock_analyse.infrastructure.persistence.file_cache import FileCacheUtils
from stock_analyse.shared.report_date_utils import ReportDateUtils


class TechnicalAnalysisWorkflow:
    """技术分析工作流。

    用于单只股票技术评分、指标计算与全市场扫描中的单票分析，是传统量化流程与 AI 快照构建都会复用的应用层工作流。
    """

    def __init__(self, params: Optional[TechnicalParams] = None, market: str = 'SH') -> None:
        self._setup_logging()
        self.params = params or TechnicalParams.default()
        self.date_utils = ReportDateUtils()
        self.stock_strategy = StockStrategy(market=market)
        self.cache_service = FileCacheUtils(market=market)
        self.market = market
        self.cache_switch = False

    def _setup_logging(self) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def calculate_ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_macd(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        exp1 = series.ewm(span=12, adjust=False).mean()
        exp2 = series.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        return macd, signal, macd - signal

    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int, std_dev: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        middle = series.rolling(window=period, min_periods=period).mean()
        std = series.rolling(window=period, min_periods=period).std()
        upper = middle + std * std_dev
        lower = middle - std * std_dev
        return upper, middle, lower

    def calculate_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        high = df['最高']
        low = df['最低']
        prev_close = df['收盘'].shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        return tr.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def calculate_obv(series_close: pd.Series, series_volume: pd.Series) -> pd.Series:
        diff = series_close.diff().fillna(0)
        obv = np.where(diff > 0, series_volume, np.where(diff < 0, -series_volume, 0))
        return pd.Series(obv, index=series_close.index).cumsum()

    @staticmethod
    def calculate_stochastic(series_close: pd.Series, window: int = 14) -> Tuple[pd.Series, pd.Series]:
        lowest = series_close.rolling(window=window, min_periods=window).min()
        highest = series_close.rolling(window=window, min_periods=window).max()
        percent_k = (series_close - lowest) / (highest - lowest + 1e-10) * 100
        percent_d = percent_k.rolling(window=3, min_periods=3).mean()
        return percent_k, percent_d

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            for _, period in self.params.ma_periods.items():
                df[f'MA_{period}'] = self.calculate_ema(df['收盘'], period)
            df['RSI'] = self.calculate_rsi(df['收盘'], self.params.rsi_period)
            df['MACD'], df['Signal'], df['MACD_hist'] = self.calculate_macd(df['收盘'])
            df['BB_upper'], df['BB_middle'], df['BB_lower'] = self.calculate_bollinger_bands(
                df['收盘'], self.params.bollinger_period, self.params.bollinger_std
            )
            df['Volume_MA'] = df['成交量'].rolling(
                window=self.params.volume_ma_period,
                min_periods=self.params.volume_ma_period,
            ).mean()
            df['Volume_Ratio'] = df['成交量'] / (df['Volume_MA'] + 1e-10)
            df['ATR'] = self.calculate_atr(df, self.params.atr_period)
            df['Volatility'] = df['ATR'] / df['收盘'] * 100
            df['ROC'] = df['收盘'].pct_change(periods=10) * 100
            df['OBV'] = self.calculate_obv(df['收盘'], df['成交量'])
            df['OBV_MA10'] = df['OBV'].rolling(window=10, min_periods=10).mean()
            df['%K'], df['%D'] = self.calculate_stochastic(df['收盘'], window=14)
            return df
        except Exception as exc:
            self.logger.error(f'指标计算出错：{exc}')
            raise

    def analyze_stock(self, stock, market: str = 'SH') -> dict:
        if isinstance(stock, pd.DataFrame) and len(stock) == 1:
            stock = stock.iloc[0]
        stock_code = stock['代码']
        market = stock['market']
        try:
            stock_service = stockCompanyInfo(marker=market, symbol=stock_code)
            end_date_str = self.date_utils.get_current_history_date_st()
            start_date_str = self.date_utils.get_start_history_date_st()
            report_type = 'history_' + stock_code
            date = end_date_str
            df_history_data = None
            if self.cache_switch:
                df_history_data = self.cache_service.read_from_csv(date, report_type=report_type)
            if df_history_data is None:
                df_history_data = stock_service.get_stock_history_data(start_date_str, end_date_str)
                if self.cache_switch:
                    self.cache_service.write_to_csv(date, report_type, df_history_data)
            return self.compute_result(df_history_data, stock, stock_code)
        except Exception as exc:
            self.logger.error(f'分析股票 {stock_code} 失败：{exc}')
            traceback.print_exc()
            raise

    def compute_result(self, df_history_data, stock, stock_code):
        if df_history_data is None or df_history_data.empty:
            return {
                'stock_code': stock_code,
                'stock_name': stock_code,
                'suggestion': '不建议买入',
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'score': 0,
                'price': 0,
                'price_change': -1,
                'signal': '',
            }
        df_summary_data = self.stock_strategy.calculate_stock_data(df_history_data, stock, stock_code)
        score, buy_signal_str = self.stock_strategy.calculate_score(
            df_history_data=df_history_data,
            df_stock=stock,
            df_summary_data=df_summary_data,
        )
        score2, _ = self.calculate_score_simple(df_history_data, stock_code)
        suggestion = self.stock_strategy.get_recommendation(score)
        df_summary_data['signal'] = buy_signal_str
        df_summary_data['score'] = score
        df_summary_data['score_simple'] = score2
        df_summary_data['suggestion'] = suggestion
        return df_summary_data.to_dict('records')[0]

    def calculate_score_simple(self, df_history_data, stock_code):
        score2 = -1
        try:
            if df_history_data is None or len(df_history_data) == 0:
                return score2, ''
            df_copy = df_history_data.copy(deep=True)
            self.calculate_indicators(df_copy)
            score2, score2_suggestion = self.stock_strategy.calculate_score_simple(df_copy)
            return score2, score2_suggestion
        except Exception as exc:
            self.logger.error(f'分析股票score2 {stock_code} 失败：{exc}')
        return score2, ''

    def run(self, *, stock_code: str, market: str) -> tuple[int, dict]:
        stock_border = stockBorderInfo(market=market)
        df_stock = stock_border.get_stock_spot()
        df_stock = df_stock[df_stock['股票代码'] == stock_code].copy()
        df_stock['market'] = market
        df_summary_data = self.analyze_stock(df_stock, market)
        score = df_summary_data['score']
        return score, df_summary_data
