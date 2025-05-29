from datetime import datetime
import logging
import traceback
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from .technical_params import TechnicalParams

import numpy as np
import pandas as pd
import akshare as ak

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from stocklib.stock_company import stockCompanyInfo
from stocklib.utils_report_date import ReportDateUtils


class StockStrategy:
    """股票分析器，用于计算股票的各种指标"""
    def __init__(self):
        now = datetime.now()
        self._setup_logging()
    def _setup_logging(self) -> None:
        """配置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

    def calculate_stock_data(self, df_history_data, df_stock_data, stock_code):
        """ 计算出结果
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
        market = df_stock_data['market']
        stock_company = stockCompanyInfo(marker=market, symbol=stock_code)
        stock_name = df_stock_data.loc['股票简称'] if '股票简称' in df_stock_data.index else ''

        # 处理股本数量（通过行索引获取，无需指定列名）
        total_shares = df_stock_data.loc['总股本'] if '总股本' in df_stock_data.index else -1
        total_shares = total_shares if pd.notna(total_shares) else -1  # 处理NaN

        # 处理市值相关字段
        market_cap = df_stock_data.loc['总市值'] if '总市值' in df_stock_data.index else -1
        circulating_cap = df_stock_data.loc['流通市值'] if '流通市值' in df_stock_data.index else -1

        # 处理行情数据（假设 df_history_data 按行索引存储）
        price = df_stock_data.loc['最新价'] if '最新价' in df_stock_data.index else -1
        price_change = df_stock_data.loc['涨跌幅'] if '涨跌幅' in df_stock_data.index else -1
        amplitude = df_stock_data.loc['振幅'] if '振幅' in df_stock_data.index else -1000
        turnover_rate = df_stock_data.loc['换手率'] if '换手率' in df_stock_data.index else -1
        change_60d = df_stock_data.loc['60日涨跌幅'] if '60日涨跌幅' in df_stock_data.index else -1000
        change_ytd = df_stock_data.loc['年初至今涨跌幅'] if '年初至今涨跌幅' in df_stock_data.index else -1000

        # 处理估值与财务指标（统一通过行索引访问）
        roe = df_stock_data.loc['ROE'] if 'ROE' in df_stock_data.index else -1
        pe = df_stock_data.loc['市盈率-动态'] if '市盈率-动态' in df_stock_data.index else -1
        pb = df_stock_data.loc['市净率'] if '市净率' in df_stock_data.index else -1
        dynamic_pe = df_stock_data.loc['市盈率-动态'] if '市盈率-动态' in df_stock_data.index else -1
        ps_ratio = df_stock_data.loc['市销率'] if '市销率' in df_stock_data.index else -1
        dividend_yield = df_stock_data.loc['股息率'] if '股息率' in df_stock_data.index else -1
        revenue_growth = df_stock_data.loc['营业总收入同比'] if '营业总收入同比' in df_stock_data.index else -1
        profit_growth = df_stock_data.loc['净利润同比'] if '净利润同比' in df_stock_data.index else -1
        debt_ratio = df_stock_data.loc['资产负债率'] if '资产负债率' in df_stock_data.index else -1
        dcf = -1  # 保持空值

        result = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            '市值': market_cap,
            '流通市值': circulating_cap,
            '股本数量': total_shares,
            '行业': '',  # 需额外数据填充，此处留空

            'price': price,
            'price_change': price_change,
            '振幅': amplitude,
            '换手率': turnover_rate,
            '60日涨跌幅': change_60d,
            '年初至今涨跌幅': change_ytd,

            'ROE': roe,
            'PE': pe,
            'PB': pb,
            '动态市盈率': dynamic_pe,
            '市销率': ps_ratio,
            '股息率': dividend_yield,
            '营业增长率': revenue_growth,
            '利润增长率': profit_growth,
            '负债率': debt_ratio,
            'DCF': dcf
        }

        df_result = pd.DataFrame([result])
        return df_result
    def calculate_score(self, df_history_data: pd.DataFrame, df_stock: pd.DataFrame, df_summary_data):
        """
        计算股票综合打分（基本打分满分100分），并根据 OBV 与随机指标调整±5分，
        使量化结果更全面。
        基本打分逻辑不变：
          趋势（30分）、RSI（20分）、MACD（20分）、成交量（30分）
        附加指标调整：
          OBV：OBV > OBV_MA10，+5分；反之，-5分；
          随机指标：%K < 20为超卖，+5分；%K > 80为超买，-5分。
        """
        try:
            score, buy_signal_str = self.calculate_score_indicate(df_history_data)
            return score, buy_signal_str
        except Exception as e:
            self.logger.error(f"计算打分失败：{str(e)}")
            traceback.print_exc()
            raise

    def calculate_score_indicate(self, df_history_data: pd.DataFrame):
        """
               计算股票综合打分（基本打分满分100分），并根据 OBV 与随机指标调整±5分，

                RSI 策略	15      rsi_signal,rsi_signal_position
                KDJ 策略	15      kdj_signal  kdj_signal_position
                MACD 策略	30  macd_signal_index  macd_signal_position
                均线策略	10    ma_signal  ma_signal_position
                布林带策略	10   bb_signal  bb_signal_position
                成交量策略	10   volume_signal volume_signal_position
                威廉指标	5               williams_signal,williams_signal_position
                ADX 策略	5          adx_signal adx_signal_position
                突破策略 策略	5      breakout_signal  breakout_position
                SAR 策略     5     'sar_signal', 'sar_position'
                均值回归策略  5       'mean_signal', 'mean_signal_position'

            输出：
        """
        score = 0
        buy_signal_str = '买入的信号'

        result = self.has_recent_buy_signal(df=df_history_data, signal_column='rsi_signal')
        if (result):
            score += 15
            buy_signal_str += f"RSI 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='kdj_signal')
        if (result):
            score += 20
            buy_signal_str += f"KDJ 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='macd_signal_index')
        if (result):
            score += 20
            buy_signal_str += f"MACD 信号 触发买入\n"
        result = self.has_recent_buy_signal(df = df_history_data, signal_column='breakout_signal')
        if(result):
            score += 5
            buy_signal_str += f"均线策略 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='bb_signal')
        if (result):
            score += 5
            buy_signal_str += f"布林带策略 信号 触发买入\n"

        result = self.has_recent_buy_signal(df=df_history_data, signal_column='volume_signal')
        if (result):
            score += 5
            buy_signal_str += f"成交量策略 信号 触发买入\n"

        result = self.has_recent_buy_signal(df=df_history_data, signal_column='williams_signal')
        if (result):
            score += 5
            buy_signal_str += f"威廉指标 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='adx_signal')
        if (result):
            score += 5
            buy_signal_str += f"ADX策略 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='breakout_signal')
        if (result):
            score += 5
            buy_signal_str += f"突破策略 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='mean_signal')
        if (result):
            score += 5
            buy_signal_str += f"均值回归策略 信号 触发买入\n"

        return score, buy_signal_str


    def has_recent_buy_signal(self, df: pd.DataFrame, date_column: str = '日期',signal_column: str = 'macd_signal') -> bool:
        """
           判断 DataFrame 中是否包含最近 3 天的买入信号（breakout_signal 等于 1）。

           Args:
               df (pd.DataFrame): 包含股票数据的 DataFrame。
               date_column (str): 日期列的列名，默认为 'date'。

           Returns:
               bool: 若包含最近 3 天的买入信号返回 True，否则返回 False。
        """
        try:
            # 将日期列转换为 datetime 类型
            df[date_column] = pd.to_datetime(df[date_column])
            # 获取最近 3 天的日期范围
            latest_date = df[date_column].max()
            three_days_ago = latest_date - pd.Timedelta(days=2)
            # 筛选 breakout_signal 等于 1 且日期在最近 3 天的数据
            buy_data = df[(df[signal_column] == 1) & (df[date_column] >= three_days_ago) & (df[date_column] <= latest_date)]
            return not buy_data.empty
        except Exception as e:
            self.logger.error(f"判断最近买入信号出错：{str(e)}")
            return False

    def calculate_score_simple(self, df: pd.DataFrame) :
        """
        计算股票综合打分（基本打分满分100分），并根据 OBV 与随机指标调整±5分，
        使量化结果更全面。
        基本打分逻辑不变：
          趋势（30分）、RSI（20分）、MACD（20分）、成交量（30分）
        附加指标调整：
          OBV：OBV > OBV_MA10，+5分；反之，-5分；
          随机指标：%K < 20为超卖，+5分；%K > 80为超买，-5分。
        """
        try:
            score = 0
            latest = df.iloc[-1]
            buy_signal_str = '买入的信号'
            # 趋势打分
            if latest['MA_5'] > latest['MA_20'] and latest['MA_20'] > latest['MA_50']:
                score += 30
                buy_signal_str += f"MA5> MA20 > MA50 信号 触发买入\n"
            else:
                if latest['MA_5'] > latest['MA_20']:
                    score += 15
                    buy_signal_str += f"MA5> MA20 触发买入\n"
                if latest['MA_20'] > latest['MA_50']:
                    score += 15
                    buy_signal_str += f"MA20 > MA60 信号 触发买入\n"
            # RSI 打分
            if 30 <= latest['RSI'] <= 70:
                score += 20
                buy_signal_str += f"RSI 信号 触发买入\n"
            elif latest['RSI'] < 30:
                score += 15
                buy_signal_str += f"RSI 信号 触发买入\n"
            # MACD 打分
            if latest['MACD'] > latest['Signal']:
                score += 20
                buy_signal_str += f"macd 信号 触发买入\n"
            # 成交量打分
            if latest['Volume_Ratio'] > 1.5:
                score += 30
                buy_signal_str += f"Volume_Ratio 信号 触发买入\n"
            elif latest['Volume_Ratio'] > 1:
                score += 15

            # 附加 OBV 调整
            if latest['OBV'] > latest['OBV_MA10']:
                score += 5
                buy_signal_str += f"OBV 信号 触发买入\n"
            else:
                score -= 5

            # 附加随机指标调整
            if latest['%K'] < 20:
                score += 5
            elif latest['%K'] > 80:
                score -= 5

            return score,buy_signal_str

        except Exception as e:
            self.logger.error(f"计算打分失败：{str(e)}")
            raise

    @staticmethod
    def get_recommendation(score: float) -> str:
        """根据最终打分给出投资建议"""
        if score >= 50:
            return '强烈推荐买入'
        elif score >= 30:
            return '建议买入'
        elif score >= 10:
            return '建议持有'
        elif score >= 5:
            return '建议观望'
        else:
            return '建议观望'
