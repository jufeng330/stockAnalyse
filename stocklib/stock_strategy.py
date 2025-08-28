import logging
import traceback
from datetime import datetime

import pandas as pd
import numpy as np
from .utils_report_date import ReportDateUtils


class StockStrategy:
    """股票分析器，用于计算股票的各种指标"""
    def __init__(self,market='SH'):
        now = datetime.now()
        self._setup_logging()
        self.date_utils = ReportDateUtils()
        self.market = market
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
        stock_name = next(
            (df_stock_data.loc[key] for key in ['股票简称', '名称_x', '名称'] if key in df_stock_data.index),
            ''
        )

        # 处理股本数量（通过行索引获取，无需指定列名）
        total_shares = df_stock_data.loc['总股本'] if '总股本' in df_stock_data.index else -1
        total_shares = total_shares if pd.notna(total_shares) else -1  # 处理NaN

        # 处理市值相关字段
        market_cap = df_stock_data.loc['总市值'] if '总市值' in df_stock_data.index else -1
        circulating_cap = df_stock_data.loc['流通市值'] if '流通市值' in df_stock_data.index else -1
        circulating_type = df_stock_data.loc['市值规模'] if '市值规模' in df_stock_data.index else -1

        concept_name = df_stock_data.loc['概念板块'] if '概念板块' in df_stock_data.index else ''
        border_name = df_stock_data.loc['行业板块'] if '行业板块' in df_stock_data.index else ''
        gx_ratio = df_stock_data.loc['现金分红-股息率'] if '现金分红-股息率' in df_stock_data.index else 0


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
            '行业': border_name,  # 需额外数据填充，此处留空
            '市值规模': circulating_type,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            '市值': market_cap,
            '流通市值': circulating_cap,
            '股本数量': total_shares,
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
            '平均股息率': gx_ratio,
            '营业总收入': revenue_total,
            '营业增长率': revenue_growth,
            '利润增长率': profit_growth,
            '负债率': debt_ratio,
            'DCF': dcf,
            '概念板块': concept_name
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
            score, buy_signal_str = self.calculate_score_indicate(df_history_data,df_stock = df_stock)
            return score, buy_signal_str
        except Exception as e:
            self.logger.error(f"计算打分失败：{str(e)}")
            traceback.print_exc()
            raise

    def calculate_score_indicate(self, df_history_data: pd.DataFrame,df_stock = None):
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
        stock_code = df_history_data['股票代码']

        from .stock_wave_analyser import StockWaveAnalyzer
        wave_service = StockWaveAnalyzer(market=self.market,symbol=stock_code)

        df_wave,total_trend,last_trend = wave_service.analysis_stock_trend(stock_df = df_history_data)
        wave_percent = df_wave['波动百分比'].iloc[-1]
        if total_trend == '上升':
            if last_trend == '翻转中':
                if wave_percent<3:
                    score += 70
                elif wave_percent<5:
                    score += 60
                elif wave_percent<10:
                    score += 40
                else:
                    score += 30
                buy_signal_str += f"股票趋势:{total_trend} 阶段:{last_trend}，score: {score} df_wave: {df_wave.iloc[-1]}\n"
            elif last_trend == '探底中':
                if wave_percent>20:
                    score += 40
                elif wave_percent>10:
                    score += 30
                else:
                    score += 20

                buy_signal_str += f"股票趋势{total_trend} 阶段{last_trend}，score: {score} df_wave: {df_wave.iloc[-1]}\n"
        elif total_trend == '下降':
            if last_trend == '翻转中':
                if wave_percent<3:
                    score += 30
                elif wave_percent<5:
                    score += 20
                else:
                    score += 10

                buy_signal_str += f"股票趋势{total_trend} 阶段{last_trend}，score: {score} df_wave: {df_wave.iloc[-1]}\n"
            elif last_trend == '探底中':
                score += 0
                buy_signal_str += f"股票趋势{total_trend} 阶段{last_trend}，score: {score} df_wave: {df_wave.iloc[-1]}\n"
        else:
            if last_trend == '翻转中':
                if wave_percent < 3:
                    score +=20
                else:
                    score += 10
                buy_signal_str += f"股票趋势{total_trend} 阶段{last_trend}，score: {score} df_wave: {df_wave.iloc[-1]}\n"

        result = self.has_recent_buy_signal(df=df_history_data, signal_column='macd_signal_index')
        if (result):
            df_result_accuracy = self.find_macd_inc_stock(df_history_data, stock_code)
            score_value = df_result_accuracy['score'].iloc[0]
            score += score_value
            buy_signal_str += f"MACD 信号 触发买入 {score_value}\n"

        result_inc = self.calculate_vol_inc(df=df_history_data, ratio=1.5,df_stock = df_stock)
        if (result_inc > 0):
            df_result_acc = self.find_vol_inc_stock(df_history_data, stock_code)
            score_inc = df_result_acc['成交量股价上涨得分'].iloc[0]
            score += score_inc+result_inc
            buy_signal_str += f"成交量放大:{result_inc} 触发买入  score: {score_inc}\n"

        result = self.has_recent_buy_signal(df=df_history_data, signal_column='rsi_signal')
        if (result):
            score += 10
            buy_signal_str += f"RSI 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='kdj_signal')
        if (result):
            score += 5
            buy_signal_str += f"KDJ 信号 触发买入\n"

        result = self.has_recent_buy_signal(df = df_history_data, signal_column='breakout_signal')
        if(result):
            score += 5
            buy_signal_str += f"均线策略 信号 触发买入\n"
        result = self.has_recent_buy_signal(df=df_history_data, signal_column='bb_signal')
        if (result):
            score += 10
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



        # stock = StockWaveAnalyzer()
        buy_signal_str = buy_signal_str.replace('\n', '    ')
        return score, buy_signal_str

    def calculate_vol_inc(self, df: pd.DataFrame, ratio=1.5,df_stock = None):
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
        current = df_stock
        # 初始化分数
        score = 0

        # 检查今日成交量与10日平均的关系
        ratio2 = ratio * 1.33

        if today[col_name] >= today[col_name_avg] * ratio2:
            score = 25
        elif today[col_name] >= today[col_name_avg] * ratio:
            score = 15
        # 检查今日成交量与昨日成交量的关系
        if today[col_name] >= yesterday[col_name] * ratio2:
            score = max(score, 20)  # 取最高分
        elif today[col_name] >= yesterday[col_name] * ratio:
            score = max(score, 10)  # 取最高分
        if current is not None and len(current)>0:
            process = self.date_utils.calculate_stock_progress()
            if process> 0:
                current_value = current['成交量'] /process
            else:
                current_value = current['成交量']
            if current_value >= today[col_name] * ratio2:
                score = max(score, 40)  # 取最高分
            elif current_value >= today[col_name]  * ratio:
                score = max(score, 25)  # 取最高分
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
            logging.warn(f'get_stock_avg_postive:{self.market}  col_name:{col_name} not in df_financial.columns')
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

        # stock_array = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
        # print(avg_profit[avg_profit['股票代码'].isin(stock_array)].to_markdown())
        # print(df_recent_three_years[df_recent_three_years['股票代码'].isin(stock_array)][['股票代码', col_lrl]].to_markdown())

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


    def find_vol_inc_stock(self, df_stock, stock_code,date = None):
        """
        根据成交量指标评估放量成交与后续收益的关系并评分

          根据成交量 指标，验证每次放量成交指标90 % 都可以带来超5 % 的收益。
            1、 某条成交量是上10日平均成交量的2倍并且 股票是上涨的
              、 后续3天涨幅超过5%
                如果该情况100%返回30分
              如果该情况平均涨幅找过5% fanh 10分
              如果下跌
            2 、 条成交量是上10日平均成交量的2倍并且 股票是下跌的
               后续3天涨幅超过5%
                如果该情况100%返回-30分
              如果该情况平均涨幅找过5% fanh -10分
        参数:
        df_stock: 包含股票数据的DataFrame
        stock_code: 股票代码

        返回:
        pd.DataFrame: 包含评估结果的DataFrame
        """
        # 确保数据按日期排序
        col_date = '日期'
        df_stock = df_stock.sort_values(col_date)

        # 计算前10日平均成交量
        df_stock['vol_avg_10'] = df_stock['成交量'].rolling(window=10).mean()

        # 找出放量且上涨的交易日（条件1）
        df_stock['is_vol_up'] = (df_stock['成交量'] > 2 * df_stock['vol_avg_10']) & \
                                (df_stock['收盘'] > df_stock['开盘'])

        # 找出放量且下跌的交易日（条件2）
        df_stock['is_vol_down'] = (df_stock['成交量'] > 2 * df_stock['vol_avg_10']) & \
                                  (df_stock['收盘'] < df_stock['开盘'])

        # 计算后续3天的涨幅
        df_stock['return_3d'] = df_stock['收盘'].pct_change(3).shift(-3) * 100  # 转换为百分比

        # 筛选出符合条件的交易日
        condition1_days = df_stock[df_stock['is_vol_up']].copy()
        condition2_days = df_stock[df_stock['is_vol_down']].copy()

        # 计算条件1的得分
        if len(condition1_days) > 0:
            condition1_all_over_5 = (condition1_days['return_3d'] > 5).all()
            condition1_avg_return = condition1_days['return_3d'].mean()

            if condition1_all_over_5:
                score1 = 50
            elif condition1_avg_return > 5:
                score1 = 30
            elif condition1_avg_return > 1:
                score1 = 10
            else:
                score1 = 0
        else:
            score1 = 0

        # 计算条件2的得分
        if len(condition2_days) > 0:
            condition2_all_over_5 = (condition2_days['return_3d'] > 5).all()
            condition2_avg_return = condition2_days['return_3d'].mean()

            if condition2_all_over_5:
                score2 = 30
            elif condition2_avg_return > 5:
                score2 = 10
            elif condition2_avg_return > 1:
                score2 = 5
            else:
                score2 = 0
        else:
            score2 = 0

        # 合并得分
        total_score = score1 + score2

        # 构建结果DataFrame
        result_df = pd.DataFrame({
            '股票代码': [stock_code],
            '成交量股价上涨符合天数': [len(condition1_days)],
            '成交量股价上涨平均涨幅': [condition1_days['return_3d'].mean() if len(condition1_days) > 0 else 0],
            '成交量股价上涨得分': [score1],
            '成交量股价下跌符合天数': [len(condition2_days)],
            '成交量股价下跌平均涨幅': [condition2_days['return_3d'].mean() if len(condition2_days) > 0 else 0],
            '成交量股价下跌得分': [score2],
            '总得分': [total_score]
        })

        return result_df



    def find_macd_inc_stock(self, df_stock,stock_code):
        """
        分析MACD指标的买点卖点，评估交易表现并评分
        根据成交量指标，验证每次放量成交指标90 % 都可以带来超5 % 的收益。
            1、 找出
            macd指标的买点，找出macd指标的卖点
            2、 计算macd指标相邻的买点和卖点的天数
            以及涨幅
            3、 如果100 % 超过10天，以及涨幅超过10 % 分数是50分
            如果100 % 超过5天，以及涨幅超过5 %
            其他10分
        参数:
        df_stock: 包含股票数据的DataFrame
        stock_code: 股票代码

        返回:
        dict: 包含评分和平均收益的字典
        """
        # 确保数据按日期排序
        col_date = '日期'
        df_stock = df_stock.sort_values(col_date)

        # 找出MACD买点和卖点
        buy_points = df_stock[df_stock['macd_signal_index'] == 1].copy()
        sell_points = df_stock[df_stock['macd_signal_index'] == -1].copy()

        # 如果没有买点或卖点，直接返回0分
        if buy_points.empty or sell_points.empty:
            return pd.DataFrame({
                '股票代码': [stock_code],
                'score': 0,
                'avg_return': 0
            })
        # 确保买点在卖点之前，找到相邻的买卖点对
        buy_sell_pairs = []
        last_buy_index = -1

        for _, buy in buy_points.iterrows():
            # 找到该买点之后的第一个卖点
            subsequent_sells = sell_points[sell_points[col_date] > buy[col_date]]
            if not subsequent_sells.empty:
                first_sell = subsequent_sells.iloc[0]
                buy_sell_pairs.append((buy, first_sell))

        # 如果没有有效的买卖点对，返回0分
        if not buy_sell_pairs:
            return  pd.DataFrame({
            '股票代码': [stock_code],
            'score': 0,
            'avg_return': 0
        })

        # 计算每对买卖点的涨幅
        returns = []
        for buy, sell in buy_sell_pairs:
            return_rate = (sell['收盘'] / buy['收盘'] - 1) * 100  # 转换为百分比
            returns.append(return_rate)

        # 计算平均收益
        avg_return = sum(returns) / len(returns)

        # 根据条件评分
        all_over_10_return = all(r > 10 for r in returns)

        all_over_5_return = all(r > 5 for r in returns)

        if all_over_10_return:
            score = 50
        elif all_over_5_return:
            score = 30
        else:
            score = 15

        result_df = pd.DataFrame({
            '股票代码': [stock_code],
            'score': score,
            'avg_return': avg_return
        })
        return  result_df