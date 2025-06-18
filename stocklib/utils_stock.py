
import datetime
import logging
import pandas as pd

class StockUtils:
    def __init__(self):
        self.current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        # 定义 current_date 并格式化
        self.logger = logging.getLogger(__name__)



    # 根据股票代码获取带市场编号的股票代码
    def get_stock_zh_code(self, code):
        """

        :param code:
        :return:
        """
        if code.startswith('6'):
            return 'sh' + code
        elif code.startswith('0') or code.startswith('3'):
            return 'sz' + code
        elif code.startswith('8') or code.startswith('4') or code.startswith('9'):  # 新增北京科创板(北交所)代码处理
            return 'bj' + code  # 北交所代码通常以8开头，使用bj前缀
        else:
            return code

    def format_history_stock_code(self, stock_zh_a_hist_df,stock_code):
        """
        格式化股票历史数据，将列名映射为中文，并添加股票代码列。
        :param stock_zh_a_hist_df:
        :param stock_code:
        :return:
        """
        if stock_zh_a_hist_df is None or stock_zh_a_hist_df.empty:
            return stock_zh_a_hist_df
        column_mapping = {
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'turnover': '换手率'
        }
        # 执行列名映射
        stock_zh_a_hist_df = stock_zh_a_hist_df.rename(columns=column_mapping)
        stock_zh_a_hist_df['股票代码'] = stock_code
        return stock_zh_a_hist_df

    def pd_convert_to_float(self, df = None ,col_name='今开'):
        """
        将字段转换成float类型
        """


        # 2. 移除百分比符号
        def convert_unit(value):
            if pd.isna(value):
                return float('nan')

            value = str(value).strip()  # 转为字符串并去除空格

            # 处理百分比
            if '%' in value:
                value = value.replace('%', '')
                try:
                    return float(value) / 100  # 百分比转小数
                except ValueError:
                    return float('nan')

            # 处理万和亿
            if '万' in value:
                value = value.replace('万', '')
                try:
                    return float(value) * 10000  # 万转数值
                except ValueError:
                    return float('nan')
            elif '亿' in value:
                value = value.replace('亿', '')
                try:
                    return float(value) * 10000 * 10000  # 亿转数值
                except ValueError:
                    return float('nan')

            # 纯数值直接转换
            try:
                return float(value)
            except ValueError:
                return float('nan')

        # 应用函数处理列
        if col_name in df.columns:
            df.loc[:, col_name] = df[col_name].apply(convert_unit)
            df.loc[:, col_name] = pd.to_numeric(df[col_name], errors='coerce')
        return df