import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import akshare as ak
import datetime
import pywt
import matplotlib.font_manager as fm
import mpld3


class stockIndicatorHtml:
    def __init__(self,current_date=None):
        # 定义 current_date 并格式化
        if current_date is None:
            current_date = datetime.datetime.now()
            current_date_str = current_date.strftime("%Y%m%d")
        self.current_date = current_date
        self.current_date_str = self.current_date.strftime("%Y%m%d")

    def get_stock_code(self, df):
        if '股票代码' in df.columns:
            stock_code = df['股票代码'].values[0]
            return stock_code
        else:
            print("DataFrame 中不包含 '股票代码' 列。")
            return "Unknown"

    # 移动平均线算法
    def plot_sma(self, data, window=20):
        if data is None or data.empty:
            print("输入的 DataFrame 为空或为 None，无法进行分析。")
            return None
        try:
            # 计算简单移动平均线
            data['SMA'] = data['收盘'].rolling(window=window).mean()
            stock_code = self.get_stock_code(data)

            # 可视化
            plt.figure(figsize=(12, 6))
            plt.plot(data['收盘'], label='Close Price')
            plt.plot(data['SMA'], label=f'{window}-day SMA')
            plt.title(f'{stock_code} Stock Price with {window}-day SMA')
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.legend()

            # 转换为 HTML 片段
            html = mpld3.fig_to_html(plt.gcf())
            plt.close()
            return html
        except Exception as e:
            print(f"发生错误: {e}")
            return None

    # 小波分析
    def plot_stock_wave(self, df):
        if df is None or df.empty:
            print("输入的 DataFrame 为空或为 None，无法进行小波分析。")
            return None
        try:
            # 设置 matplotlib 支持中文显示
            font_path = fm.findfont(fm.FontProperties(family='SimHei'))
            if font_path:
                plt.rcParams['font.family'] = fm.FontProperties(fname=font_path).get_name()
                plt.rcParams['axes.unicode_minus'] = False
            else:
                print("未找到支持中文的字体，可能无法正常显示中文。")

            data = pd.Series(df['收盘'].values, index=pd.to_datetime(df['日期']))
            stock_code = self.get_stock_code(df)

            # 进行小波分解
            wavelet = 'db4'
            coeffs = pywt.wavedec(data, wavelet, level=3)

            # 可视化各层细节系数
            plt.figure(figsize=(12, 8))
            plt.subplot(len(coeffs) + 1, 1, 1)
            plt.plot(data.index, data)
            plt.title(f'小波分析 - Original Stock Price - {stock_code}')

            for i, coeff in enumerate(coeffs):
                plt.subplot(len(coeffs) + 1, 1, i + 2)
                plt.plot(coeff)
                plt.title(f'小波分析 Wavelet Coefficients at Level {len(coeffs) - i}')

            plt.tight_layout()

            # 转换为 HTML 片段
            html = mpld3.fig_to_html(plt.gcf())
            plt.close()
            return html
        except Exception as e:
            print(f"发生错误: {e}")
            return None

    # 布林带
    def plot_stock_Bollinger(self, df):
        if df is None or df.empty:
            print("输入的 DataFrame 为空或为 None，无法进行分析。")
            return None
        try:
            data = pd.Series(df['收盘'].values, index=pd.to_datetime(df['日期']))
            stock_code = self.get_stock_code(df)

            # 计算布林带
            window = 20
            std_multiplier = 2

            # 计算中轨（简单移动平均线）
            middle_band = data.rolling(window=window).mean()
            # 计算标准差
            std_dev = data.rolling(window=window).std()
            # 计算上轨和下轨
            upper_band = middle_band + std_multiplier * std_dev
            lower_band = middle_band - std_multiplier * std_dev

            # 可视化
            plt.figure(figsize=(12, 6))
            plt.plot(data.index, data, label='Stock Price')
            plt.plot(data.index, middle_band, label='Middle Band', linestyle='--')
            plt.plot(data.index, upper_band, label='Upper Band', linestyle='--')
            plt.plot(data.index, lower_band, label='Lower Band', linestyle='--')
            plt.title(f'Bollinger Bands - {stock_code}')
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.legend()

            # 转换为 HTML 片段
            html = mpld3.fig_to_html(plt.gcf())
            plt.close()
            return html
        except Exception as e:
            print(f"发生错误: {e}")
            return None

    # 傅里叶变化图
    def plot_stock_fft(self, df):
        if df is None or df.empty:
            print("输入的 DataFrame 为空或为 None，无法进行分析。")
            return None
        try:
            data = pd.Series(df['收盘'].values, index=pd.to_datetime(df['日期']))
            stock_code = self.get_stock_code(df)
            # 进行傅里叶变换
            fft_values = np.fft.fft(data)
            frequencies = np.fft.fftfreq(len(data))
            # 取绝对值得到振幅谱
            amplitudes = np.abs(fft_values)

            # 可视化
            plt.figure(figsize=(12, 6))
            plt.subplot(2, 1, 1)
            plt.plot(data.index, data)
            plt.title(f'Stock {stock_code} Price Time Series')
            plt.xlabel('Date')
            plt.ylabel('Price')

            plt.subplot(2, 1, 2)
            plt.plot(frequencies[:len(frequencies) // 2], amplitudes[:len(amplitudes) // 2])
            plt.title(f'Fourier transform graph - {stock_code}')
            plt.xlabel('Frequency')
            plt.ylabel('Amplitude')

            plt.tight_layout()

            # 转换为 HTML 片段
            html = mpld3.fig_to_html(plt.gcf())
            plt.close()
            return html
        except Exception as e:
            print(f"发生错误: {e}")
            return None

    # 根据 code 获取每日成交数据
    def stock_day_data_code(self, stock_code, market, start_date_str, end_date_str):
        df = None
        if market == 'usa':
            try:
                # 1. 获取美股实时行情数据
                stock_us_spot_df = ak.stock_us_spot_em()
                print("美股实时行情数据：")
                # 查找对应的代码
                filtered = stock_us_spot_df[stock_us_spot_df["名称"] == stock_code]
                if not filtered.empty:
                    code = filtered["代码"].values[0]
                    print(f"美股股票的代码: {code}")
                    # 3. 获取对应的每日行情数据
                    stock_us_hist_df = ak.stock_us_hist(symbol=code, start_date=start_date_str, end_date=end_date_str)
                    df = stock_us_hist_df
                    print(stock_us_hist_df)
                else:
                    print("未找到对应的代码。")
            except Exception as e:
                print(f"获取美股数据时出现错误: {e}")
        elif market == 'H':  # 港股数据获取
            try:
                # 获取港股历史数据
                stock_hk_hist_df = ak.stock_hk_hist(symbol=stock_code, period="daily", start_date=start_date_str,
                                                     end_date=end_date_str)
                print(stock_hk_hist_df)
                df = stock_hk_hist_df
            except Exception as e:
                print(f"获取港股数据时出现错误: {e}")
        elif market == 'zq':  # 新增债券/ETF基金条件
            try:
                # 获取债券或ETF基金的历史数据
                stock_zh_a_hist_df = ak.fund_etf_hist_em(symbol=stock_code, period="daily", start_date=start_date_str,
                                                         end_date=end_date_str)
                print(stock_zh_a_hist_df)
                df = stock_zh_a_hist_df
            except Exception as e:
                print(f"获取债券/ETF基金数据时出现错误: {e}")
        else:
            try:
                # 3.历史行情数据 - 前复权
                stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_str,
                                                        end_date=end_date_str,
                                                        adjust="qfq")
                print(stock_zh_a_hist_df)
                df = stock_zh_a_hist_df
            except Exception as e:
                print(f"获取 A 股数据时出现错误: {e}")
        return df
