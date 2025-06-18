import logging
import traceback
from datetime import datetime

import pandas as pd
import numpy as np


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
        stock_name = df_stock_data.loc['股票简称'] if '股票简称' in df_stock_data.index else ''
        if stock_name == '':
            stock_name = df_stock_data.loc['名称_x'] if '名称_x' in df_stock_data.index else ''

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
        price = float(df_stock_data.loc['今开'] if '今开' in df_stock_data.index else -1)
        # 处理估值与财务指标（统一通过行索引访问）
        roe = df_stock_data.loc['ROE'] if 'ROE' in df_stock_data.index else -1
        if roe == -1:
            roe = df_stock_data.loc['净资产收益率'] if '净资产收益率' in df_stock_data.index else -1
        pe = df_stock_data.loc['市盈率-动态'] if '市盈率-动态' in df_stock_data.index else -1
        if pe == -1:
            pe_price = float(df_stock_data.loc['基本每股收益'] if '基本每股收益' in df_stock_data.index else -1)
            pe = (price / pe_price) if (price > 0 and pe_price > 0) else -1
        pb = df_stock_data.loc['市净率'] if '市净率' in df_stock_data.index else -1
        if pb == -1:
            pb_price = float(df_stock_data.loc['每股净资产_x'] if '每股净资产_x' in df_stock_data.index else -1)
            pb = (price / pb_price) if (price > 0 and pb_price > 0) else -1
        dynamic_pe = df_stock_data.loc['市盈率-动态'] if '市盈率-动态' in df_stock_data.index else -1
        ps_ratio = df_stock_data.loc['市销率'] if '市销率' in df_stock_data.index else -1
        dividend_yield = df_stock_data.loc['股息率'] if '股息率' in df_stock_data.index else -1

        revenue_total = df_stock_data.loc['营业总收入'] if '营业总收入' in df_stock_data.index else -1
        revenue_growth = df_stock_data.loc['营业总收入同比'] if '营业总收入同比' in df_stock_data.index else -1
        if revenue_growth == -1:
            revenue_growth = df_stock_data.loc['营业总收入同比增长率'] if '营业总收入同比增长率' in df_stock_data.index else -1

        profit_growth = df_stock_data.loc['净利润同比'] if '净利润同比' in df_stock_data.index else -1
        if profit_growth == -1:
            profit_growth = df_stock_data.loc['净利润同比增长率'] if '净利润同比增长率' in df_stock_data.index else -1
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
            '营业总收入': revenue_total,
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
        result = self.calculate_vol_inc(df=df_history_data,  ratio=1.5)
        if score>0:
            score += result
            buy_signal_str += f"成交量放大:{result} 触发买入\n"
        return score, buy_signal_str

    def calculate_vol_inc(self, df: pd.DataFrame, ratio=1.5):
        # 确保DataFrame中有足够的数据计算10日平均
        if len(df) < 10:
            return 0  # 数据不足返回0分

        # 计算10日平均成交量（成交量_10）
        col_name = '成交量'
        col_name_avg = col_name+"_10"
        df[col_name_avg] = df[col_name].rolling(window=10).mean()

        # 获取今日和昨日的数据
        today = df.iloc[-1]
        yesterday = df.iloc[-2]

        # 初始化分数
        score = 0

        # 检查今日成交量与10日平均的关系
        ratio2 = ratio * 1.33
        if today[col_name] >= today[col_name_avg] * ratio2:
            score = 50
        elif today[col_name] >= today[col_name_avg] * ratio:
            score = 30
        # 检查今日成交量与昨日成交量的关系
        if today[col_name] >= yesterday[col_name] * ratio2:
            score = max(score, 40)  # 取最高分
        elif today[col_name] >= yesterday[col_name] * ratio:
            score = max(score, 20)  # 取最高分
        return score



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



    def get_stock_continue_postive(self,df_financial,date,col_name='总资产净利润率(%)',col_adjustment='全年利润率为正',continue_year=3,threshold=0.01,condition_type='>'):
        """
        判断股票是否持续盈利，连续三年利润率均为正，则返回True，否则返回False。
        """
        date_financial = date,
        # 筛选最近三年的数据（无论利润率正负）
        col_lrl = col_name
        if not (col_name in df_financial.columns):
            return set(df_financial['股票代码'])
        df_recent_three_years =  df_financial[(df_financial['报告期'] > date) ].copy()
        # 确保报告期为日期类型
        df_recent_three_years['报告期'] = pd.to_datetime(df_recent_three_years['报告期'])
        # 提取年份信息（用于分组）
        df_recent_three_years['年份'] = df_recent_three_years['报告期'].dt.year
        # df_recent_three_years[col_lrl] = df_recent_three_years[col_lrl].apply(lambda x: x if isinstance(x, str) else float('nan'))
        # 2. 移除百分比符号
        def convert_unit_vectorized(s):
            # 处理缺失值
            mask_na = s.isna()
            result = pd.Series(index=s.index, dtype=float)
            result[mask_na] = np.nan

            # 转为字符串并去除空格
            s_str = s.astype(str).str.strip()

            # 处理百分比
            mask_percent = s_str.str.contains('%', na=False)
            percent_values = s_str[mask_percent].str.replace('%', '', regex=False)
            result[mask_percent] = pd.to_numeric(percent_values, errors='coerce') / 100

            # 处理"万"单位
            mask_wan = s_str.str.contains('万', na=False) & ~mask_percent
            wan_values = s_str[mask_wan].str.replace('万', '', regex=False)
            result[mask_wan] = pd.to_numeric(wan_values, errors='coerce') * 10000

            # 处理"亿"单位
            mask_yi = s_str.str.contains('亿', na=False) & ~mask_percent
            yi_values = s_str[mask_yi].str.replace('亿', '', regex=False)
            result[mask_yi] = pd.to_numeric(yi_values, errors='coerce') * 100000000

            # 处理纯数值
            mask_numeric = ~mask_percent & ~mask_wan & ~mask_yi
            result[mask_numeric] = pd.to_numeric(s_str[mask_numeric], errors='coerce')

            return result

        # 应用向量化函数
        df_recent_three_years[col_lrl] = convert_unit_vectorized(df_recent_three_years[col_lrl])
        # 按股票和年份分组，检查每年是否所有记录的利润率均为正
        df_recent_three_years[col_lrl] = pd.to_numeric(df_recent_three_years[col_lrl], errors='coerce')
        if len(df_recent_three_years) == 0:
            return set(df_financial['股票代码'])
        if condition_type == '>':
            result = df_recent_three_years.groupby(['股票代码', '年份']).apply(
                lambda x: (x[col_lrl] > threshold).all()
            )
        elif condition_type == '<':
            result = df_recent_three_years.groupby(['股票代码', '年份']).apply(
                lambda x: (x[col_lrl] < threshold).all()
            )
        elif condition_type == '<=':
            result = df_recent_three_years.groupby(['股票代码', '年份']).apply(
                lambda x: (x[col_lrl] <= threshold).all()
            )
        elif condition_type == '>=':
            result = df_recent_three_years.groupby(['股票代码', '年份']).apply(
                lambda x: (x[col_lrl] >= threshold).all()
            )
        else:
            result = df_recent_three_years.groupby(['股票代码', '年份']).apply(
                lambda x: (x[col_lrl] > threshold).all()
            )

        print(f'df_recent_three_years:{type(result)}')
        if not isinstance(result, pd.Series):
            error_msg = (
                f"错误: groupby().apply() 返回了 DataFrame 而非 Series。\n"
                f"可能原因: 1) 分组键有重复值 2) lambda 函数返回了多列结果。\n"
                f"请检查数据结构: df_recent_three_years[['股票代码', '年份']].duplicated().any() 是否为 True"
            )
            raise TypeError(error_msg)
        # <class 'pandas.core.series.Series'> ✅ 可以用 name 参数
        positive_three_years = result.reset_index(name = col_adjustment)

        # 筛选出连续三年利润率均为正的股票
        # qualified_stocks = positive_three_years.groupby('股票代码').filter(lambda x: x[col_lrl_rename].all() and x['年份'].nunique() >= continue_year)['股票代码'].unique()

        # 1. 先按 col_adjustment = true 过滤数据
        filtered_data = positive_three_years[positive_three_years[col_adjustment]==True]
        stock_row_counts = filtered_data.groupby('股票代码').size().reset_index(name='行数')
        qualified_stocks = stock_row_counts[stock_row_counts['行数'] >= continue_year]['股票代码'].tolist()
        qualified_stocks_set = set(qualified_stocks)

        return qualified_stocks_set

    def get_stock_avg_postive(self, df_financial, date, col_name='总资产净利润率(%)',
                                   col_adjustment='全年利润率为正',threshold=0.01,condition_type='>'):
        """
        判断股票是连续三年的平均值超过ratio，则返回True，否则返回False。
        """
        date_financial = date,
        # 筛选最近三年的数据（无论利润率正负）
        col_lrl = col_name
        col_lrl_rename = col_adjustment
        if not (col_name in df_financial.columns):
            return set(df_financial['股票代码'])
        df_recent_three_years = df_financial[
            (df_financial['报告期'] > date) ].copy()
        # 确保报告期为日期类型
        df_recent_three_years['报告期'] = pd.to_datetime(df_recent_three_years['报告期'])
        # 提取年份信息（用于分组）
        df_recent_three_years['年份'] = df_recent_three_years['报告期'].dt.year

        # df_recent_three_years[col_lrl] = df_recent_three_years[col_lrl].apply(lambda x: x if isinstance(x, str) else float('nan'))
        # 2. 移除百分比符号
        def convert_unit_vectorized(s):
            # 处理缺失值
            mask_na = s.isna()
            result = pd.Series(index=s.index, dtype=float)
            result[mask_na] = np.nan

            # 转为字符串并去除空格
            s_str = s.astype(str).str.strip()

            # 处理百分比
            mask_percent = s_str.str.contains('%', na=False)
            percent_values = s_str[mask_percent].str.replace('%', '', regex=False)
            result[mask_percent] = pd.to_numeric(percent_values, errors='coerce') / 100

            # 处理"万"单位
            mask_wan = s_str.str.contains('万', na=False) & ~mask_percent
            wan_values = s_str[mask_wan].str.replace('万', '', regex=False)
            result[mask_wan] = pd.to_numeric(wan_values, errors='coerce') * 10000

            # 处理"亿"单位
            mask_yi = s_str.str.contains('亿', na=False) & ~mask_percent
            yi_values = s_str[mask_yi].str.replace('亿', '', regex=False)
            result[mask_yi] = pd.to_numeric(yi_values, errors='coerce') * 100000000

            # 处理纯数值
            mask_numeric = ~mask_percent & ~mask_wan & ~mask_yi
            result[mask_numeric] = pd.to_numeric(s_str[mask_numeric], errors='coerce')

            return result

        # 应用向量化函数
        df_recent_three_years[col_lrl] = convert_unit_vectorized(df_recent_three_years[col_lrl])

        # 按股票和年份分组，检查每年是否所有记录的利润率均为正
        df_recent_three_years[col_lrl] = pd.to_numeric(df_recent_three_years[col_lrl], errors='coerce')

        # 筛选出连续三年平均利润率均为正的股票
        avg_profit = df_recent_three_years.groupby('股票代码')[col_lrl].mean().reset_index()

        stock_array = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
        print(avg_profit[avg_profit['股票代码'].isin(stock_array)].to_markdown())
        print(df_recent_three_years[df_recent_three_years['股票代码'].isin(stock_array)][['股票代码', col_lrl]].to_markdown())

        # 筛选出平均利润大于阈值的股票
        if condition_type == '>':
            qualified_stocks = avg_profit[avg_profit[col_lrl] > threshold]['股票代码'].tolist()
        elif condition_type == '<':
            qualified_stocks = avg_profit[avg_profit[col_lrl] < threshold]['股票代码'].tolist()
        elif condition_type == '<=':
            qualified_stocks = avg_profit[avg_profit[col_lrl] <= threshold]['股票代码'].tolist()
        elif condition_type == '>=':
            qualified_stocks = avg_profit[avg_profit[col_lrl] >= threshold]['股票代码'].tolist()
        else:
            qualified_stocks = avg_profit[avg_profit[col_lrl] > threshold]['股票代码'].tolist()

        qualified_stocks_set = set(qualified_stocks)
        return qualified_stocks_set


    def calculate_stock_roe(self, df_stock, df_indicator_mv, stock_code) :
        df_indicator_mv_date = df_indicator_mv[df_indicator_mv['日期'] == '2025-06-06'].copy()

        # 如果同一股票在该日期有多行数据，选择最新记录（根据其他列判断，如时间戳）
        # 这里假设按 '更新时间' 列降序排序后取第一条
        df_indicator_mv_date = df_indicator_mv_date.sort_values(
            by='更新时间', ascending=False
        ).drop_duplicates(subset='股票代码', keep='first')

        # 3. 合并两个数据框
        df_merged = pd.merge(
            left=df_indicator_mv_date ,
            right=df_stock,
            left_on='股票代码',
            right_on='代码',
            how='inner'  # 只保留两边都有的股票
        )
        df_merged['ROE'] = df_merged['净资产收益率'] / df_merged['总市值'] * 100
        return df_merged

    def find_macd_stock(self, df_stock, stock_code):
        """
         根据macd指标，验证每个macd指标都是正常的。每个macd都能超过10天上涨或者下跌，macd指标90%都可以带来超5%的收益。

        :param df_stock:
        :param stock_code:
        :return:
        """

        return pd.DataFrame()

    def find_vol_inc_stock(self, df_stock, stock_code):
        """
         根据成交量指标，验证每次放量成交指标90%都可以带来超5%的收益。

        :param df_stock:
        :param stock_code:
        :return:
        """

        return pd.DataFrame()