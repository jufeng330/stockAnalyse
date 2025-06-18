import datetime
import akshare as ak
import numpy as np
import pandas as pd
import talib
import logging
from sklearn.preprocessing import PolynomialFeatures
from sklearn import datasets,linear_model
from sklearn.model_selection import cross_val_predict,cross_val_score,train_test_split
from sklearn.preprocessing import StandardScaler
# from tensorflow.keras.models import Sequential
# from tensorflow.keras.layers import Dense
from .utils_stock import StockUtils
import traceback

class stockAKIndicator:
    """
    股票分析工具类，用于获取股票数据和计算技术指标。
    包含以下功能：
    - 获取实时股票数据
    - 获取历史股票数据
    - 计算技术指标
    - 计算移动平均线
    - 计算MACD指标
    - 计算RSI指标
    - 计算KDJ指标
    - 计算BOLL指标
    - 计算ATR指标
    - 计算OBV指标
    """
    def __init__(self):
        # 获取当前日期
        current_date = datetime.datetime.now()
        # 生成当前日期的指定格式字符串
        self.current_date_str = current_date.strftime("%Y%m%d")
        # 计算前一天的日期
        previous_date = current_date - datetime.timedelta(days=1)
        # 生成前一天日期的指定格式字符串
        self.previous_date_str = previous_date.strftime("%Y%m%d")

        previous_year = current_date - datetime.timedelta(days=100)
        self.previous_year_str = previous_year.strftime("%Y%m%d")
        self.logger = logging.getLogger(__name__)
        self.stockUtils = StockUtils()


    @staticmethod
    def get_stock_code(df):
        if '股票代码' in df.columns:
            stock_code = df['股票代码'].values[0]
            return stock_code
        else:
            print("DataFrame 中不包含 '股票代码' 列。")
            return "Unknown"

    # 根据code获取每日成交数据
    def stock_day_data_code(self, stock_code, market, start_date_str, end_date_str):
        df = None
        if market == 'usa':
            try:
                # 1. 获取美股实时行情数据
                #stock_us_spot_df = ak.stock_us_spot_em()
                #self.logger.debug("美股实时行情数据：")
                # 查找阿里巴巴的代码
                # baba_filtered = stock_us_spot_df[stock_us_spot_df["名称"] == stock_code]
                baba_filtered = pd.DataFrame()
                if not baba_filtered.empty:
                    baba_code = baba_filtered["代码"].values[0]
                    self.logger.debug(f"美股股票的代码: {baba_code}")
                    # 3. 获取阿里巴巴（BABA）的每日行情数据
                    stock_us_hist_df = ak.stock_us_hist(symbol=baba_code, start_date=start_date_str,
                                                        end_date=end_date_str)
                    df = stock_us_hist_df
                    # self.logger.debug(stock_us_hist_df)
                else:
                    stock_us_hist_df = ak.stock_us_hist(symbol=stock_code, start_date=start_date_str,
                                                        end_date=end_date_str)
                    df = stock_us_hist_df
                    df['股票代码'] = stock_code
                    # self.logger.debug(stock_us_hist_df)
            except Exception as e:
                self.logger.error(f"获取美股数据时出现错误: {e}")
        elif market == 'H':  # 港股数据获取
            try:
                # 获取港股历史数据
                stock_hk_hist_df = ak.stock_hk_hist(symbol=stock_code, period="daily", start_date=start_date_str,
                                                    end_date=end_date_str)
                # self.logger.debug(stock_hk_hist_df)
                df = stock_hk_hist_df
                df['股票代码'] = stock_code

            except Exception as e:
                self.logger.error(f"获取债券/ETF基金数据时出现错误: {e}")
        elif market == 'zq':  # 新增债券/ETF基金条件
            try:
                # 获取债券或ETF基金的历史数据
                stock_zh_a_hist_df = ak.fund_etf_hist_em(symbol=stock_code, period="daily", start_date=start_date_str,
                                                         end_date=end_date_str)
                # self.logger.debug(stock_zh_a_hist_df)
                df = stock_zh_a_hist_df
            except Exception as e:
                self.logger.error(f"获取债券/ETF基金数据时出现错误: {e}")
                traceback.print_exc()
        else:
            try:
                # stock_zh_a_hist_df = ak.fund_etf_hist_em(symbol=stock_code, period="daily", start_date=start_date_str,end_date=end_date_str)

                # 3.历史行情数据 - 前复权
                # 日期，开盘，收盘，最高，最低，成交量，成交额，振幅，涨跌幅，涨跌额，换手率，股票代码

                sina_code = self.stockUtils.get_stock_zh_code(code=stock_code)
                stock_zh_a_hist_df = ak.stock_zh_a_daily(symbol=sina_code, start_date=start_date_str,
                                                         end_date=end_date_str, adjust="qfq")

                df = self.stockUtils.format_history_stock_code(stock_zh_a_hist_df, stock_code)

            except Exception as e:
                self.logger.error(f"获取 A 股数据{stock_code}时出现错误: {e}")
                try:

                    code = stock_code
                    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date_str,
                                                            end_date=end_date_str,
                                                            adjust="qfq")

                    df = stock_zh_a_hist_df

                except Exception as e2:
                    self.logger.error(f"获取 A 股数据{stock_code}时出现错误: {e2}")
                    traceback.print_exc()
        if df is not None:
            def format_float(x):
                if isinstance(x, float):
                    return '{:.2f}'.format(x)
                return x

            # 应用格式化函数
            df = df.apply(lambda x: x.map(format_float))

            df = df.reset_index(drop=True)
            numeric_columns = ['开盘', '收盘', '最高', '最低', '成交量']

            # 遍历列名列表，仅处理存在的列
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    # self.logger.debug(f"成功转换列: {col}")
                else:
                    self.logger.info(f"警告: 列 '{col}' 不存在，已跳过")
        return df

    # 移动平均线算法
    """
       计算并绘制股票收盘价的简单移动平均线（SMA）。
       :param ticker: 股票代码
       :param window: 移动平均的窗口大小，默认为 20
       """

    def strategy_mac(self, data, window=20):
        if data is None or data.empty:
            self.logger.debug("输入的 DataFrame 为空或为 None，无法进行小波分析。")
            return
        try:
            stock_code = self.get_stock_code(data)
            # 计算短期和长期均线
            short_window = 10
            long_window = 30
            data['MA_10'] = data['收盘'].rolling(window=short_window, min_periods=1).mean()
            data['MA_30'] = data['收盘'].rolling(window=long_window, min_periods=1).mean()
            data['MA_5'] = data['收盘'].rolling(window=short_window).mean()
            # 生成交易信号
            data['ma_signal'] = 0
            # 使用 .loc 进行赋值，确保操作在原始 DataFrame 上进行
            data.loc[short_window:, 'ma_signal'] = np.where(
                data.loc[short_window:, 'MA_10'] > data.loc[short_window:, 'MA_30'],
                1, 0
            )
            data['ma_signal_position'] = data['ma_signal'].diff()
            # 输出交易信号
            # self.logger.debug(data[['收盘', 'MA_10', 'MA_30', 'ma_signal', 'ma_signal_position']].tail(20))
            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()

    # 绘制均线策略和布林带策略的图像
    def strategy_bollinger(self, data, short_window = 10,long_window = 30):
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return

        # 计算均线
        data['MA_10'] = data['收盘'].rolling(window=short_window, min_periods=1).mean()
        data['MA_30'] = data['收盘'].rolling(window=long_window, min_periods=1).mean()

        # 计算布林带
        window = 20  # 布林带窗口
        data['Middle_Band'] = data['收盘'].rolling(window=window).mean()
        data['Std_Dev'] = data['收盘'].rolling(window=window).std()
        data['Upper_Band'] = data['Middle_Band'] + (2 * data['Std_Dev'])
        data['Lower_Band'] = data['Middle_Band'] - (2 * data['Std_Dev'])

        # 生成均线策略信号
        data['MAC_Signal'] = 0
        data.loc[short_window:, 'MA_Signal'] = np.where(data['MA_10'][short_window:] > data['MA_30'][short_window:], 1, -1)


        data['MA_Position'] = data['MA_Signal'].diff()

        # 生成布林带策略信号
        data['bb_signal'] = 0
        data.loc[window:, 'bb_signal'] = np.where(data['收盘'][window:] > data['Upper_Band'][window:], -1,
                                              np.where(data['收盘'][window:] < data['Lower_Band'][window:], 1, 0))

        data['bb_signal_position'] = data['bb_signal'].diff()

        return data

    # 绘制动量策略的图像
    def strategy_macd(self, data,momentum_window = 20):
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return

        # 计算动量
        data['Momentum'] = data['收盘'].pct_change(periods=momentum_window)

        # 计算 MACD
        macd, signal, hist = talib.MACD(data['收盘'], fastperiod=12, slowperiod=26, signalperiod=9)
        data["macd_dif"] = macd
        data["macd_signal"] = signal
        data["hist"] = hist
        stock_code = self.get_stock_code(data)

        # 生成交易信号
        data['macd_signal_index'] = 0
        # 买入信号
        data.loc[(data["macd_dif"] < data["macd_signal"]) & (
                data["macd_dif"].shift(1) > data["macd_signal"].shift(1)), 'macd_signal_index'] = 1
        # 短期均线下穿长期均线，卖出信号
        data.loc[(data["macd_dif"] > data["macd_signal"]) & (
                data["macd_dif"].shift(1) < data["macd_signal"].shift(1)), 'macd_signal_index'] = -1

        data['macd_signal_position'] = data['macd_signal_index'].diff()

        # 输出交易信号
        # self.logger.debug(data[['收盘', 'Momentum', 'macd_signal_index', 'macd_signal_position']].tail(20))

        return data

    # 绘制突破策略的图像
    def strategy_breakout(self, data, window=20):
        """
        绘制突破策略的图像
        :param data: 包含股票价格数据的 DataFrame，需有 '收盘' 列
        :param window: 计算前 n 日高低价的窗口大小，默认为 20
        """
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return

        # 计算前 window 日的最高价和最低价
        data['Previous_High_20'] = data['收盘'].rolling(window=window).max()
        data['Previous_Low_20'] = data['收盘'].rolling(window=window).min()

        data['Signal2'] = 0
        data.loc[window:, 'Signal2']  = np.where(data['收盘'][window:] > data['Previous_High_20'].shift(1)[window:], 1,
                                            np.where(data['收盘'][window:] < data['Previous_Low_20'].shift(1)[window:],
                                                     -1,
                                                     0))

        short_window = 20
        long_window = 50
        data['MA_20'] = data['收盘'].rolling(window=short_window, min_periods=1).mean()
        data['MA_50'] = data['收盘'].rolling(window=long_window, min_periods=1).mean()
        data['residence'] = data['MA_20'] + 2 * (data['收盘'].rolling(window=short_window, min_periods=1).std())
        data['support'] = data['MA_20'] - 2 * (data['收盘'].rolling(window=short_window, min_periods=1).std())

        # 生成交易信号
        data['breakout_signal'] = 0
        data.loc[window:, 'breakout_signal'] = np.where(data['收盘'][window:] > data['residence'][window:], 1,
                                           np.where(data['收盘'][window:] < data['support'][window:], -1, 0))
        data['breakout_position'] = data['breakout_signal'].diff()

        stock_code = self.get_stock_code(data)
        # 标记买入信号
        buy_indices = data[data['breakout_signal'] == 1].index
        # 标记卖出信号
        sell_indices = data[data['breakout_signal'] == -1].index


        # 标记买入信号
        # buy_indices = data[data['breakout_position'] == 1].index
        # sell_indices = data[data['breakout_position'] == -1].index


        # 输出交易信号
        # self.logger.debug(data[['收盘', 'Previous_High_20', 'Previous_Low_20', 'Signal', 'Position']].tail(20))
        return data

    # SAR 策略
    def strategy_sar(self, data):
        """
        绘制 SAR 策略的图像
        :param data: 包含股票价格数据和 SAR 指标的 DataFrame
        """
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return
        # 使用 talib 计算 SAR
        high_prices = data['最高']
        low_prices = data['最低']
        acceleration = 0.02
        maximum_acceleration = 0.2
        data['SAR'] = talib.SAR(high_prices, low_prices, acceleration=acceleration, maximum=maximum_acceleration)

        # 生成交易信号
        data['sar_signal'] = 0
        # data['Signal'][1:] = np.where(data['SAR'][1:] < data['收盘'][1:], 1, -1)
        # 短期均线下穿长期均线，卖出信号
        data.loc[(data['收盘'] > data['SAR']) & (data['收盘'].shift(1) < data['SAR'].shift(1)), 'sar_signal'] = 1
        # 短期均线下穿长期均线，卖出信号
        data.loc[(data['收盘'] < data['SAR']) & (data['收盘'].shift(1) > data['SAR'].shift(1)), 'sar_signal'] = -1

        data['sar_position'] = data['sar_signal'].diff()
        stock_code = self.get_stock_code(data)

        # 输出交易信号
        # self.logger.debug(data[['收盘', 'SAR', 'sar_signal', 'sar_position']].tail(20))
        return data

    # 均值回归策略
    def mean_reversion_strategy(self, data, window=20, z_score_threshold=1):
        """
        绘制均值回归策略的图像
        :param data: 包含股票价格数据的 DataFrame，需有 '收盘' 列
        :param window: 计算均值和标准差的窗口大小，默认为 20
        :param z_score_threshold: Z 分数阈值，用于判断偏离程度，默认为 1
        """
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return

        stock_code = self.get_stock_code(data)
        # 计算滚动均值和标准差
        data['Rolling_Mean'] = data['收盘'].rolling(window=window).mean()
        data['Rolling_Std'] = data['收盘'].rolling(window=window).std()

        # 计算 Z 分数
        data['Z_Score'] = (data['收盘'] - data['Rolling_Mean']) / data['Rolling_Std']

        # 生成交易信号
        data['mean_signal'] = 0
        data['mean_signal'] = np.where(data['Z_Score'] > z_score_threshold, -1,
                                  np.where(data['Z_Score'] < -z_score_threshold, 1, 0))
        data['mean_signal_position'] = data['mean_signal'].diff()

        # 标记买入信号
        buy_indices = data[data['mean_signal'] == 1].index

        # 标记卖出信号
        sell_indices = data[data['mean_signal'] == -1].index

        # 输出交易信号
        # self.logger.debug(data[['收盘', 'Rolling_Mean', 'Z_Score', 'mean_signal', 'mean_signal_position']].tail(20))

    # 次级结构套利
    def sub_structure_arbitrage(self, data, short_window=5, long_window=20, deviation_threshold=0.05):
        """
        绘制次级结构套利策略图像
        :param data: 包含股票数据的 DataFrame，需有 '收盘' 列
        :param short_window: 短期移动平均线窗口大小，默认为 5
        :param long_window: 长期移动平均线窗口大小，默认为 20
        :param deviation_threshold: 价格偏离阈值，默认为 0.05
        """
        if data is None or data.empty:
            self.logger.debug("数据为空，无法绘制图像。")
            return

        stock_code = self.get_stock_code(data)
        # 计算短期和长期移动平均线
        data['MA_5'] = data['收盘'].rolling(window=short_window).mean()
        data['MA_20'] = data['收盘'].rolling(window=long_window).mean()

        # 计算价格偏离程度
        data['Deviation'] = (data['收盘'] - data['MA_20']) / data['MA_5']

        # 生成交易信号
        data['arbitrage_signal'] = 0
        # 短期均线上穿长期均线且价格偏离超过阈值，买入信号
        data.loc[(data['MA_5'].shift(1) <= data['MA_20'].shift(1)) & (data['MA_5'] > data['MA_20']) & (
                data['Deviation'] > deviation_threshold), 'arbitrage_signal'] = 1
        # 短期均线下穿长期均线且价格偏离低于负阈值，卖出信号
        data.loc[(data['MA_5'].shift(1) >= data['MA_20'].shift(1)) & (data['MA_5'] < data['MA_20']) & (
                data['Deviation'] < -deviation_threshold), 'arbitrage_signal'] = -1
        data['arbitrage_signal_position'] = data['arbitrage_signal'].diff()
        # 输出交易信号
        # self.logger.debug(data[['收盘', 'MA_5', 'MA_20', 'Deviation', 'arbitrage_signal', 'arbitrage_signal_position']].tail(20))
        return data

    # RSI 择时策略
    def strategy_rsi(self, data, period=14, overbought=70, oversold=30):
        if data is None or data.empty:
            self.logger.debug("输入的 DataFrame 为空或为 None，无法进行 RSI 分析。")
            return None
        try:
            stock_code = self.get_stock_code(data)
            # 计算 RSI
            data['RSI'] = talib.RSI(data['收盘'], timeperiod=period)
            lastRSI = data['RSI'].shift(1)
            # 生成交易信号
            data['rsi_signal'] = 0
            data.loc[(data['RSI'] < oversold) & (lastRSI > oversold), 'rsi_signal'] = 1  # 买入信号
            data.loc[(data['RSI'] > overbought) & (lastRSI < overbought), 'rsi_signal'] = -1  # 卖出信号
            data['rsi_signal_position'] = data['rsi_signal'].diff()

            # 打印交易信号信息
            self.logger.debug("Signal 等于 1 的情况（买入信号）：")
            buy_indices = data[data['rsi_signal'] == 1].index
            for idx in buy_indices:
                rsi_value = data.loc[idx, 'RSI']
                self.logger.debug(f"索引: {idx}, RSI 值: {rsi_value}")

            self.logger.debug("\nSignal 等于 -1 的情况（卖出信号）：")
            sell_indices = data[data['rsi_signal'] == -1].index
            for idx in sell_indices:
                rsi_value = data.loc[idx, 'RSI']
                self.logger.debug(f"索引: {idx}, RSI 值: {rsi_value}")

            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    # KDJ 择时策略
    def strategy_kdj(self, data, fastk_period=9, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0):
        if data is None or data.empty:
            self.logger.debug("输入的 DataFrame 为空或为 None，无法进行 KDJ 分析。")
            return None
        try:
            stock_code = self.get_stock_code(data)
            # 计算 KDJ 指标
            high_prices = data['最高'].values
            low_prices = data['最低'].values
            close_prices = data['收盘'].values
            k, d = talib.STOCH(high_prices, low_prices, close_prices, fastk_period=fastk_period,
                               slowk_period=slowk_period, slowk_matype=slowk_matype,
                               slowd_period=slowd_period, slowd_matype=slowd_matype)
            j = 3 * k - 2 * d

            data['K'] = k
            data['D'] = d
            data['J'] = j

            # 获取上一个时间步的 K、D 值
            last_k = data['K'].shift(1)
            last_d = data['D'].shift(1)

            # 生成交易信号
            data['kdj_signal'] = 0
            data.loc[(data['K'] > data['D']) & (last_k < last_d), 'kdj_signal'] = 1  # 买入信号
            data.loc[(data['K'] < data['D']) & (last_k > last_d), 'kdj_signal'] = -1  # 卖出信号
            data['kdj_signal_position'] = data['kdj_signal'].diff()

            # 打印交易信号信息
            self.logger.debug("Signal 等于 1 的情况（买入信号）：")

            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    # 威廉R择时策略
    def strategy_williams_r(self, data, time_period=14, overbought=-20, oversold=-80):
        if data is None or data.empty:
            self.logger.debug("输入的 DataFrame 为空或为 None，无法进行威廉R分析。")
            return None
        try:
            stock_code = self.get_stock_code(data)
            # 计算威廉R指标
            high_prices = data['最高'].values
            low_prices = data['最低'].values
            close_prices = data['收盘'].values
            williams_r = talib.WILLR(high_prices, low_prices, close_prices, timeperiod=time_period)
            data['Williams_R'] = williams_r

            # 获取上一个时间步的威廉R值
            last_williams_r = data['Williams_R'].shift(1)

            # 生成交易信号
            data['williams_signal'] = 0
            data.loc[(data['Williams_R'] < oversold) & (last_williams_r >= oversold), 'williams_signal'] = 1  # 买入信号
            data.loc[(data['Williams_R'] > overbought) & (last_williams_r <= overbought), 'williams_signal'] = -1  # 卖出信号
            data['williams_signal_position'] = data['williams_signal'].diff()

            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    def strategy_adx(self, data, time_period=14, adx_threshold=25):
        if data is None or data.empty:
            self.logger.warn("输入的 DataFrame 为空或为 None，无法进行 ADX 分析。")
            return None
        try:
            # 假设数据包含 '最高', '最低', '收盘' 列，且存在一个获取股票代码的函数 get_stock_code
            stock_code = self.get_stock_code(data)

            # 计算 ADX、+DI 和 -DI
            high_prices = data['最高'].values
            low_prices = data['最低'].values
            close_prices = data['收盘'].values
            adx = talib.ADX(high_prices, low_prices, close_prices, timeperiod=time_period)
            plus_di = talib.PLUS_DI(high_prices, low_prices, close_prices, timeperiod=time_period)
            minus_di = talib.MINUS_DI(high_prices, low_prices, close_prices, timeperiod=time_period)

            data['ADX'] = adx
            data['+DI'] = plus_di
            data['-DI'] = minus_di

            # 获取上一个时间步的 +DI 和 -DI 值
            last_plus_di = data['+DI'].shift(1)
            last_minus_di = data['-DI'].shift(1)

            # 生成交易信号
            data['adx_signal'] = 0
            data.loc[(data['ADX'] > adx_threshold) & (data['+DI'] > data['-DI']) & (
                    last_plus_di <= last_minus_di), 'adx_signal'] = 1  # 买入信号
            data.loc[(data['ADX'] > adx_threshold) & (data['-DI'] > data['+DI']) & (
                    last_minus_di <= last_plus_di), 'adx_signal'] = -1  # 卖出信号
            data['adx_signal_position'] = data['adx_signal'].diff()

            # 打印交易信号信息
            self.logger.debug("Signal 等于 1 的情况（买入信号）：")
            buy_indices = data[data['adx_signal'] == 1].index
            for idx in buy_indices:
                adx_value = data.loc[idx, 'ADX']
                plus_di_value = data.loc[idx, '+DI']
                minus_di_value = data.loc[idx, '-DI']
                last_plus_di_value = last_plus_di[idx]
                last_minus_di_value = last_minus_di[idx]
                self.logger.debug(
                    f"索引: {idx}, 当前 ADX 值: {adx_value}, 当前 +DI 值: {plus_di_value}, 当前 -DI 值: {minus_di_value}, 上一周期 +DI 值: {last_plus_di_value}, 上一周期 -DI 值: {last_minus_di_value}")

            self.logger.debug("Signal 等于 -1 的情况（卖出信号）：")
            sell_indices = data[data['adx_signal'] == -1].index
            for idx in sell_indices:
                adx_value = data.loc[idx, 'ADX']
                plus_di_value = data.loc[idx, '+DI']
                minus_di_value = data.loc[idx, '-DI']
                last_plus_di_value = last_plus_di[idx]
                last_minus_di_value = last_minus_di[idx]
                self.logger.debug(f"索引: {idx}, 当前 ADX 值: {adx_value}, 当前 +DI 值: {plus_di_value}, 当前 -DI 值: {minus_di_value}, 上一周期 +DI 值: {last_plus_di_value}, 上一周期 -DI 值: {last_minus_di_value}")

            return data

        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    def strategy_volume(self, data, volume_threshold=1.5):
        """
        成交量指标策略函数

        :param data: 包含股票数据的 DataFrame，至少包含 '收盘' 和 '成交量' 列
        :param volume_threshold: 成交量变化的阈值，用于判断成交量是否异常放大
        :return: 保存交易信号图的图片路径，如果出现错误则返回 None
        """
        if data is None or data.empty:
            self.logger.debug("输入的 DataFrame 为空或为 None，无法进行成交量分析。")
            return None
        try:
            # 假设数据包含 '股票代码' 列，用于获取股票代码
            stock_code = self.get_stock_code(data)

            # 计算成交量的移动平均值（例如，过去 5 天的平均成交量）
            data['Volume_MA'] = data['成交量'].rolling(window=5).mean()

            # 计算成交量相对变化率
            data['Volume_Ratio'] = data['成交量'] / data['Volume_MA']

            # 获取上一个时间步的成交量相对变化率
            last_volume_ratio = data['Volume_Ratio'].shift(1)

            # 生成交易信号
            data['volume_signal'] = 0
            # 当成交量相对变化率大于阈值且上一周期小于等于阈值，同时收盘价上涨时，产生买入信号
            data.loc[(data['Volume_Ratio'] > volume_threshold) & (last_volume_ratio <= volume_threshold) & (
                    data['收盘'] > data['收盘'].shift(1)), 'volume_signal'] = 1
            # 当成交量相对变化率大于阈值且上一周期小于等于阈值，同时收盘价下跌时，产生卖出信号
            data.loc[(data['Volume_Ratio'] > volume_threshold) & (last_volume_ratio <= volume_threshold) & (
                    data['收盘'] < data['收盘'].shift(1)), 'volume_signal'] = -1

            data['volume_signal_position'] = data['volume_signal'].diff()

            # 打印交易信号信息
            self.logger.debug("Signal 等于 1 的情况（买入信号）：")
            buy_indices = data[data['volume_signal'] == 1].index
            for idx in buy_indices:
                volume_ratio = data.loc[idx, 'Volume_Ratio']
                last_volume_ratio_value = last_volume_ratio[idx]
                close_price = data.loc[idx, '收盘']
                prev_close_price = data.loc[idx - 1, '收盘'] if idx > 0 else None
                self.logger.debug(f"索引: {idx}, 当前成交量相对变化率: {volume_ratio}, 上一周期成交量相对变化率: {last_volume_ratio_value}, 当前收盘价: {close_price}, 上一周期收盘价: {prev_close_price}")

            self.logger.debug("Signal 等于 -1 的情况（卖出信号）：")
            sell_indices = data[data['volume_signal'] == -1].index
            for idx in sell_indices:
                volume_ratio = data.loc[idx, 'Volume_Ratio']
                last_volume_ratio_value = last_volume_ratio[idx]
                close_price = data.loc[idx, '收盘']
                prev_close_price = data.loc[idx - 1, '收盘'] if idx > 0 else None
                self.logger.debug(f"索引: {idx}, 当前成交量相对变化率: {volume_ratio}, 上一周期成交量相对变化率: {last_volume_ratio_value}, 当前收盘价: {close_price}, 上一周期收盘价: {prev_close_price}")

            return data

        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    # 线性回归模型
    def strategy_linear_regression(self, data, train_ratio=0.8):
        """
        机器学习线性回归模型策略函数

        :param data: 包含股票数据的 DataFrame，至少包含 '收盘' 列
        :param train_ratio: 训练数据所占的比例
        :return: 保存交易信号图的图片路径，如果出现错误则返回 None
        """
        if data is None or data.empty:
            self.logger.warn("输入的 DataFrame 为空或为 None，无法进行线性回归分析。")
            return None
        try:

            stock_code = self.get_stock_code(data)

            data['nclose'] = data['收盘'].shift(-1);
            data.fillna(method='pad', inplace=True)
            clst = ['开盘', '最高', '最低', '收盘']
            X0 = data[clst].values
            poly = PolynomialFeatures(degree=2, include_bias=False)
            x = poly.fit_transform(X0)
            y = data['nclose'].values
            mx = linear_model.LinearRegression()
            data['predict'] = cross_val_predict(mx, x, y, cv=20)
            cvscore = cross_val_score(mx, x, y, cv=6)
            self.logger.debug(cvscore)
            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    # 测试代码

    def strategy_kline_pattern(self, data):
        """
        K 线形态策略函数

        :param data: 包含股票数据的 DataFrame，至少包含 '开盘', '最高', '最低', '收盘' 列
        :return: 保存交易信号图的图片路径，如果出现错误则返回 None
        """
        if data is None or data.empty:
            self.logger.info("输入的 DataFrame 为空或为 None，无法进行 K 线形态分析。")
            return None
        try:
            stock_code = self.get_stock_code(data)

            # 计算看涨吞没和看跌吞没形态
            engulfing = talib.CDLENGULFING(data['开盘'].values, data['最高'].values, data['最低'].values,
                                           data['收盘'].values)
            data['Engulfing'] = engulfing

            whilte3 = talib.CDL3WHITESOLDIERS(data['开盘'].values, data['最高'].values, data['最低'].values,
                                              data['收盘'].values)
            data['whilte3'] = whilte3

            cdl = talib.CDLDRAGONFLYDOJI(data['开盘'].values, data['最高'].values, data['最低'].values,
                                         data['收盘'].values)
            data['CDL'] = cdl

            # 生成交易信号
            data['Signal'] = 0
            # 看涨吞没形态，产生买入信号
            data.loc[data['Engulfing'] == 100, 'Signal'] = 1
            # 看跌吞没形态，产生卖出信号
            data.loc[data['Engulfing'] == -100, 'Signal'] = -1

            data['Position'] = data['Signal'].diff()

            # 打印交易信号信息
            self.logger.debug("Signal 等于 1 的情况（买入信号）：")
            buy_indices = data[data['Signal'] == 1].index
            for idx in buy_indices:
                engulfing_value = data.loc[idx, 'Engulfing']
                self.logger.debug(f"索引: {idx}, 吞没形态值: {engulfing_value}")

            self.logger.debug("Signal 等于 -1 的情况（卖出信号）：")
            sell_indices = data[data['Signal'] == -1].index
            for idx in sell_indices:
                engulfing_value = data.loc[idx, 'Engulfing']
                self.logger.debug(f"索引: {idx}, 吞没形态值: {engulfing_value}")
            return data
        except Exception as e:
            self.logger.error(f"发生错误: {e}")
            traceback.print_exc()
            return None

    def strategy_mlp_regression(self, data, train_ratio=0.8, epochs=50, batch_size=32):
        """
        深度学习神经网络多层感知回归算法策略函数

        :param data: 包含股票数据的 DataFrame，至少包含 '收盘' 列
        :param train_ratio: 训练数据所占的比例
        :param epochs: 模型训练的轮数
        :param batch_size: 训练时的批次大小
        :return: 保存交易信号图的图片路径，如果出现错误则返回 None
        """

        """
        if data is None or data.empty:
            print("输入的 DataFrame 为空或为 None，无法进行多层感知回归分析。")
            return None
        try:

            stock_code = self.get_stock_code(data)

            df = data.copy();
            if '股票代码' in df.columns:
                df = df.drop(['股票代码'], axis=1)
            if '日期' in df.columns:
                df = df.drop(['日期'], axis=1)

            # 提取特征和目标变量
            X = df.drop(columns=['收盘']).values
            y = data['收盘'].values

            # 数据标准化
            scaler_X = StandardScaler()
            scaler_y = StandardScaler()
            X = scaler_X.fit_transform(X)
            y = scaler_y.fit_transform(y.reshape(-1, 1)).flatten()

            # 划分训练集和测试集
            train_size = int(train_ratio * len(data))
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]

            # 构建多层感知机模型
            model = Sequential()
            model.add(Dense(64, activation='relu', input_shape=(X_train.shape[1],)))
            model.add(Dense(32, activation='relu'))
            model.add(Dense(1))

            # 编译模型
            model.compile(optimizer='adam', loss='mse')

            # 训练模型
            model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, verbose=1)

            # 进行预测
            y_pred = model.predict(X)
            y_pred = scaler_y.inverse_transform(y_pred).flatten()

            # 生成交易信号
            data['Signal'] = 0
            data['Predicted'] = y_pred
            data['Signal'] = np.where(data['Predicted'].diff() > 0, 1, -1)
            data['Position'] = data['Signal'].diff()

            # 打印交易信号信息
            print("Signal 等于 1 的情况（买入信号）：")
            buy_indices = data[data['Signal'] == 1].index
            for idx in buy_indices:
                predicted_price = data.loc[idx, 'Predicted']
                print(f"索引: {idx}, 预测价格: {predicted_price}")

            print("Signal 等于 -1 的情况（卖出信号）：")
            sell_indices = data[data['Signal'] == -1].index
            for idx in sell_indices:
                predicted_price = data.loc[idx, 'Predicted']
                print(f"索引: {idx}, 预测价格: {predicted_price}")
            return data

        将项目中的py.文件 d.to_string(index=False) 修改成to to_markdown(index=False)except Exception as e:
            print(f"发生错误: {e}")
            traceback.print_exc()
            return None
        """
