
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib as matplotlib
import matplotlib.dates as mdates
import logging
import traceback
from .stock_company import stockCompanyInfo


class StockWaveAnalyzer:
    """股票分析引擎，计算各类技术指标"""

    def __init__(self,market='SH',symbol="601919"):
        """
        初始化股票分析引擎

        Args:
            params: 技术指标配置参数
        """
        self._setup_logging()
        self.market = market
        self.symbol = symbol

        type = 2
        if type == 1:
            self.min_period = 5
            self.price_threshold = 0.05
        else:
            self.min_period = 4
            self.price_threshold = 0.01

    def _setup_logging(self) -> None:
        """配置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
    # 获取特斯拉(TSLA)最近200天的股票数据
    def get_stock_data(self,days=200):
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')
            stock_service = stockCompanyInfo(marker=self.market, symbol=self.symbol)
            stock_df = stock_service.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)
            # 只保留需要的列
            # 假设日期列名为 '日期'，若实际列名不同（如 'date'），需替换为实际列名
            stock_df = stock_df.sort_values(by='日期', ascending=True).reset_index(drop=True)
            df = stock_df[['日期', '收盘']].reset_index(drop=True)
            return df
        except Exception as e:
            print(f"获取股票数据失败: {e}")
            traceback.print_exc()
            return None

    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    def show_waves(self, stock_df, peaks, troughs):
        plt.figure(figsize=(14, 8))
        print(matplotlib.get_backend())  # 输出当前后端

        # 1. 处理日期和收盘价（关键修正：y轴用收盘价列）
        dates = pd.to_datetime(stock_df['日期'], format='%Y-%m-%d')  # 确保日期格式正确
        close_prices = stock_df['收盘']  # 提取收盘价列
        # 2. 绘制收盘价曲线（修正：用收盘价列作为y轴）
        plt.plot(dates, close_prices, label='收盘', color='blue', linewidth=2)

        # 3. 标记波峰（确保peaks是有效的索引）
        if len(peaks) > 0:  # 避免空索引报错
            plt.scatter(dates.iloc[peaks], close_prices.iloc[peaks],
                        color='red', s=100, marker='^', label='波峰')

        # 4. 标记波谷（同理）
        if len(troughs) > 0:
            plt.scatter(dates.iloc[troughs], close_prices.iloc[troughs],
                        color='green', s=100, marker='v', label='波谷')

        # 5. 其他绘图设置
        plt.title('股票收盘价与波峰波谷标记', fontsize=16)
        plt.xlabel('日期', fontsize=14)
        plt.ylabel('收盘价', fontsize=14)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        plt.grid(True, linestyle='--', alpha=0.7)


        plt.legend()
        plt.tight_layout()
        plt.show()

    # 识别波浪
    def identify_waves(self,close_prices,stock_df, min_period=5, price_threshold=0.05):
        # 计算价格变动阈值
        min_prominence = np.mean(close_prices) * price_threshold
        # 识别峰和谷
        peaks, _ = find_peaks(
            close_prices,
            distance=min_period,
            prominence=min_prominence
        )

        troughs, _ = find_peaks(
            -close_prices,
            distance=min_period,
            prominence=min_prominence
        )

        # self.show_waves(stock_df, peaks, troughs)

        # 合并峰谷并排序
        turning_points = np.union1d(peaks, troughs)
        turning_points.sort()

        # 筛选有效的交替峰谷序列
        valid_points = []
        last_type = None

        for idx in turning_points:
            is_peak = idx in peaks
            current_type = 'peak' if is_peak else 'trough'

            if not valid_points:
                valid_points.append((idx, current_type))
                last_type = current_type
            else:
                if current_type != last_type:
                    valid_points.append((idx, current_type))
                    last_type = current_type
                else:
                    # 保留更显著的转折点
                    last_idx = valid_points[-1][0]
                    if current_type == 'peak' and close_prices[idx] > close_prices[last_idx]:
                        valid_points[-1] = (idx, current_type)
                    elif current_type == 'trough' and close_prices[idx] < close_prices[last_idx]:
                        valid_points[-1] = (idx, current_type)

        return valid_points

    def analysis_stock_trend(self, stock_df=None):
        try  :
            df_wave = self.analysis_stock_wave(stock_df)
            if df_wave is None:
                return None, None, None
            total_trend = self.get_stock_trend(df_wave)
            last_trend = self.get_last_trend(df_wave)
            return df_wave, total_trend, last_trend
        except Exception as e:
            logging.warn(f"股票{self.symbol} analysis_stock_trend  error: {e}")
            return pd.DataFrame (),'None','None'
    def get_stock_trend(self,df_wave):
        """
        判断股票的整体趋势

        参数:
            df_wave: 包含股票趋势数据的DataFrame，需包含'类型'和'结束价格'列

        返回:
            str: 股票整体趋势（股票上升趋势/下降趋势/波动上升趋势/波动下降趋势/波动趋势）
        """
        # 分离上升和下降序列（按时间顺序）
        ups = df_wave[df_wave['类型'] == '上升']
        downs = df_wave[df_wave['类型'] == '下降']

        # 判断上升序列是否一直上升（结束价格单调递增）
        is_ups_continuous = len(ups) <= 1 or ups['结束价格'].is_monotonic_increasing

        # 判断下降序列是否一直下降（结束价格单调递减）
        is_downs_continuous = len(downs) <= 1 or downs['结束价格'].is_monotonic_decreasing

        # 计算上升/下降趋势占比
        up_ratio = 0
        if len(ups) > 0:
            up_num = 0
            for i in range(len(ups) - 1):
                if ups.iloc[i + 1]['结束价格'] > ups.iloc[i]['结束价格']:
                    up_num += 1
            up_ratio = up_num/len(ups)

        down_ratio = 0
        if len(downs) > 0:
            downs_num = 0
            for i in range(len(downs) - 1):
                if downs.iloc[i + 1]['结束价格'] < downs.iloc[i]['结束价格']:
                    downs_num += 1
            down_ratio = downs_num / len(downs)

        # 按优先级判断趋势
        if len(ups) > 0 and is_ups_continuous:
            return "上升"
        elif len(downs) > 0 and is_downs_continuous:
            return "下降"
        elif up_ratio > 0.6:
            return "波动上升"
        elif down_ratio > 0.6:
            return "波动下降"
        else:
            return "波动"

    def get_last_trend(self,df_wave):
        """
        判断股票的最后趋势状态

        参数:
            df_wave: 包含股票趋势数据的DataFrame，需包含'类型'列

        返回:
            str: 最后趋势状态（翻转中/探底中）
        """
        last_type = df_wave.iloc[-1]['类型']
        if last_type == '上升':
            return "翻转中"
        elif last_type == '下降':
            return "探底中"
        else:
            return "未知趋势"

    # 主函数
    def analysis_stock_wave(self,stock_df=None):
        # 获取数据
        if stock_df is None:
            stock_df = self.get_stock_data()
        if len(stock_df) < 20:  # 确保有足够数据
            print("获取数据不足，无法进行分析")
            return
        # 判断整列是否为纯字符串（object dtype 且至少有一个 str 元素）
        is_str_col = (
                pd.api.types.is_object_dtype(stock_df['日期']) and
                stock_df['日期'].dropna().map(type).eq(str).any()
        )
        if is_str_col:
            stock_df['日期'] = pd.to_datetime(stock_df['日期'], errors='coerce')

        close_prices = stock_df['收盘'].values

        # 识别波浪转折点
        turning_points = self.identify_waves(close_prices,stock_df,min_period=self.min_period, price_threshold=self.price_threshold)

        # 生成波浪分析结果
        waves = []
        for i in range(len(turning_points) - 1):
            start_idx, start_type = turning_points[i]
            end_idx, end_type = turning_points[i + 1]

            # 确定波浪类型
            if start_type == 'trough' and end_type == 'peak':
                wave_type = '上升'
            elif start_type == 'peak' and end_type == 'trough':
                wave_type = '下降'
            else:
                continue

            # 获取波浪信息
            start_date = self.get_stock_df_date(start_idx, stock_df)
            start_price = round(stock_df.iloc[start_idx]['收盘'], 2)
            end_date = self.get_stock_df_date(end_idx, stock_df)
            end_price = round(stock_df.iloc[end_idx]['收盘'], 2)
            period_days = (stock_df.iloc[end_idx]['日期'] - stock_df.iloc[start_idx]['日期']).days + 1
            amplitude = round(end_price - start_price, 2)

            waves.append({
                '开始时间': start_date,
                '开始价格': start_price,
                '结束时间': end_date,
                '结束价格': end_price,
                '周期天数': period_days,
                '波动幅度': amplitude,
                '波动百分比': amplitude / end_price * 100,
                '类型': wave_type
            })

        if turning_points:
            last_turn_idx, last_turn_type = turning_points[-1]
            latest_idx = len(stock_df) - 1
            last_price = round(stock_df.iloc[last_turn_idx]['收盘'], 2)
            latest_price = round(stock_df.iloc[latest_idx]['收盘'], 2)

            if latest_idx-last_turn_idx>=2:
                # 判断当前趋势
                if last_turn_type == 'peak' and latest_price < last_price:
                    current_trend = '下降'
                elif last_turn_type == 'trough' and latest_price > last_price:
                    current_trend = '上升'
                else:
                    # 如果最新价格突破了上一个转折点，则需要调整趋势判断
                    if latest_price > last_price:
                        current_trend = '上升'
                    else:
                        current_trend = '下降'


                last_turn_date = stock_df.iloc[last_turn_idx]['日期']
                latest_date = stock_df.iloc[latest_idx]['日期']
                period_days = (latest_date - last_turn_date).days + 1
                amplitude = round(latest_price - last_price, 2)
                # 转换日期格式
                last_turn_date = last_turn_date.strftime('%Y-%m-%d')
                latest_date = latest_date.strftime('%Y-%m-%d')

                waves.append({
                    '开始时间': last_turn_date,
                    '开始价格': last_price,
                    '结束时间': latest_date,
                    '结束价格': latest_price,
                    '周期天数': period_days,
                    '波动幅度': amplitude,
                    '波动百分比': amplitude/latest_price*100,
                    '类型': f'{current_trend}'
                })

        # 保存结果
        result_df = pd.DataFrame(waves)
        logging.info(f"波浪分析完成，{self.symbol}共识别出{len(result_df)}个有效波浪")
        # print(result_df)
        return result_df

    def get_stock_df_date(self, start_idx, stock_df):
        date_val = stock_df.iloc[start_idx]['日期']
        if isinstance(date_val, str):
            start_date = date_val
        else:
            start_date = date_val.strftime('%Y-%m-%d')
        return start_date
