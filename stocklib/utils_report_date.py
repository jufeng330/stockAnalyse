
import sys
import os
import pandas as pd
import datetime
import gradio as gr
import logging
import numpy as np
import json
import traceback
# 添加 stock_analyse 目录到 Python 模块搜索路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

class ReportDateUtils:
    def __init__(self):
        self.current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        # 定义 current_date 并格式化
        self.logger = logging.getLogger(__name__)



    # 获取最近n年的日期列表
    def get_report_year_str_list(self, years=5):
        today = datetime.datetime.now()
        dates = []
        for i in range(years):
            date = self.get_report_year_str(days=(i) * 365)
            dates.append(date)
        return dates

    def get_current_report_year_st(self,format='%Y%m%d',market='SH'):
        if market  == 'SH' or market == 'SZ':
            date =  self.get_report_year_str(days=0,format=format,postfix_str='0331')
        elif market == 'H':
            date =  self.get_report_year_str(days=365,format= format,postfix_str = '1231')
        elif market == 'usa':
            date =  self.get_report_year_str(days=365,format= format,postfix_str = '1231')
        else:
            date = self.get_report_year_str(days=0, format= format,postfix_str='0331')

        return date

    def get_current__history_date_str(self, format='%Y%m%d',days = 0):
        current_date = datetime.datetime.now()
        if (days > 0):
            new_date = current_date - datetime.timedelta(days=days)
            current_date = new_date
        date = current_date.strftime(format)
        return date

    def get_report_date_add_str(self, date_str='20241231', days=365, format='%Y%m%d',postfix_str = '1231'):
        """
        获取指定日期加上指定天数后的年份，拼接"1231"

        参数:
        date_str: 输入的日期字符串，默认为'20241231'
        format: 输入日期的格式，默认为'%Y%m%d'
        days: 要增加的天数，默认为365

        返回:
        字符串：增加指定天数后的年份 + "1231"
        """
        # 将输入的日期字符串转换为datetime对象
        input_date = datetime.datetime.strptime(date_str, format)

        # 增加指定天数
        new_date = input_date + datetime.timedelta(days=days)

        # 获取新日期的年份并拼接"1231"
        result = f"{new_date.year}{postfix_str}"

        input_date = datetime.datetime.strptime(result, '%Y%m%d')
        result = input_date.strftime(format)

        return result

    def get_current_history_date_st(self):
        date =  datetime.datetime.now().strftime("%Y%m%d")
        return date

    def get_start_history_date_st(self,days=180):
        current_date =  datetime.datetime.now()
        if(days > 0):
            new_date = current_date - datetime.timedelta(days=days)
            current_date= new_date
        date = current_date.strftime("%Y%m%d")
        return date


    def get_report_year_str(self, days=0,format='%Y%m%d',postfix_str = '0331'):
        # 获取当前日期
        current_date = datetime.datetime.now()
        # 计算减去指定天数后的日期
        if days <= 0:
            new_date = current_date
        else:
            new_date = current_date - datetime.timedelta(days=days)
        # 获取减去指定天数后的年份
        target_year = new_date.year
        # 拼接年份和 '0331'
        target_date_str = str(target_year) + postfix_str
        if format == '%Y%m%d':
            target_date_str = str(target_year) + postfix_str
        elif format == '%Y-%m-%d':
            if len(postfix_str) == 4:
                postfix_str = postfix_str[:2] + '-' + postfix_str[2:]
            target_date_str = str(target_year) + '-'+postfix_str
        elif format == '%Y':
            target_date_str = str(target_year)
        else:
            target_date_str = str(target_year) + postfix_str
        return target_date_str
    def get_report_hk_year_str(self, days=0,postfix_str = '1231'):
        # 获取当前日期
        current_date = datetime.datetime.now()
        # 计算减去指定天数后的日期
        if days <= 0:
            new_date = current_date
        else:
            new_date = current_date - datetime.timedelta(days=days)
        # 获取减去指定天数后的年份
        target_year = new_date.year-1
        # 拼接年份和 '0331'
        target_date_str = str(target_year) + postfix_str
        return target_date_str

    from datetime import datetime, timedelta

    def get_history_date_str(self, days=0,format='%Y%m%d'):
        # 获取当前日期
        current_date = datetime.datetime.now()
        # 计算减去指定天数后的日期
        if days <= 0:
            new_date = current_date
        else:
            new_date = current_date - datetime.timedelta(days=days)
        target_date_str = new_date.strftime(format)
        return target_date_str
    def get_report_last_five_year(self, date=None):
        # 如果没有传入日期，使用当前日期
        if date is None:
            current_date = datetime.datetime.now()
        else:
            # 将输入的日期字符串转换为datetime对象
            current_date = datetime.datetime.strptime(date, '%Y%m%d')

        # 计算五年前的日期
        five_years_ago = current_date.replace(year=current_date.year - 5)

        # 将结果转换为字符串格式
        target_date_str = five_years_ago.strftime('%Y')

        return target_date_str

    def get_stock_code(self, market='usa',symbol='105.TSLA'):
        if market == 'usa':
            parts = symbol.split('.', 1)
            if len(parts) > 1:
                return parts[1]
            return symbol
        return symbol


    def pivot_financial_usa_data(self,df,
                             index_cols=['SECUCODE', 'SECURITY_CODE', 'SECURITY_NAME_ABBR', 'REPORT_DATE', 'REPORT_TYPE', 'REPORT', '股票代码'],
                             item_col='ITEM_NAME',
                             value_col='AMOUNT',
                             date_col='REPORT_DATE',
                             aggfunc='first',
                             fill_value=0,
                             sort_by=None,
                             ascending=True):
        """
        将财务数据从长格式转换为宽格式（透视表）

        参数:
            df: 输入的DataFrame，包含财务数据
            index_cols: 用作索引的列名列表，默认为['SECURITY_CODE', 'REPORT_DATE']
            item_col: 要转换为列的字段，默认为'ITEM_NAME'
            value_col: 作为值的字段，默认为'AMOUNT'
            date_col: 日期列，用于转换为datetime类型，默认为'REPORT_DATE'
            aggfunc: 聚合函数，默认为'first'（当存在重复值时取第一个）
            fill_value: 填充缺失值的值，默认为0
            sort_by: 排序的列名列表，默认为None（不排序）
            ascending: 是否升序排列，默认为True

        返回:
            转换后的DataFrame
        """
        if df is None or df.empty:
            print("警告: 输入的DataFrame为空，返回None")
            return None

        # 数据清洗
        print(f"开始数据清洗... 原始数据行数: {len(df)}")

        # 转换日期列
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])

        # 转换值列为数值类型
        if value_col in df.columns:
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
            # 统计无效值
            invalid_values = df[value_col].isna().sum()
            if invalid_values > 0:
                print(f"警告: {value_col} 列中有 {invalid_values} 个值无法转换为数值，已设为NaN")

        # 检查必要的列是否存在
        required_cols = index_cols + [item_col, value_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"输入的DataFrame缺少必要的列: {missing_cols}")

        # 创建透视表
        print(f"正在创建透视表... 索引列: {index_cols}，列字段: {item_col}，值字段: {value_col}")
        # print(df[df['SECURITY_CODE']=='BABA'].to_markdown())
        pivot_df = df.pivot_table(
            values=value_col,
            index=index_cols,
            columns=item_col,
            aggfunc=aggfunc,
            fill_value=fill_value
        ).reset_index()
        # print(pivot_df[pivot_df['SECURITY_CODE'] == 'BABA'].to_markdown())

        # 移除列名的层级索引名称
        pivot_df.columns.name = None

        # 排序（如果指定）
        if sort_by:
            print(f"按 {sort_by} 排序...")
            pivot_df = pivot_df.sort_values(sort_by, ascending=ascending)

        print(f"数据转换完成！转换后数据形状: {pivot_df.shape}")
        return pivot_df

    def map_lrb_share_to_a_share(self, h_share_df, market='usa'):
        """
        将H股数据框映射到A股格式

        参数:
        h_share_df (pd.DataFrame): H股数据框

        返回:
        pd.DataFrame: 转换后的A股格式数据框
        """
        # 创建一个新的数据框，用于存储映射后的A股数据

        us_to_a_share_mapping = {
            # 基础信息
            '股票代码': 'SECURITY_CODE',
            '股票简称': 'SECURITY_NAME_ABBR',
            '公告日期': 'REPORT_DATE',

            # 利润核心指标
            '净利润': '归属于母公司股东净利润',  # 或'净利润'
            '营业总收入': '营业收入',  # 美股'营业收入'通常包含主营+其他业务收入
            '营业利润': '营业利润',
            '利润总额': '持续经营税前利润',  # 美股可能需合并'税前利润其他项目'

            # 成本与费用
            '营业总支出-销售费用': '营销费用',
            '营业总支出-管理费用': '一般及行政费用',
            '营业总支出-财务费用': '利息支出合计',

            # 每股指标
            '每股基本盈利': '基本每股收益-普通股',
            '每股摊薄盈利': '摊薄每股收益-普通股',

            # 其他重要字段
            '营业总收入同比': None,  # 美股不直接披露同比，需计算
            '营业总支出-营业支出': '营业成本',  # 对应美股主营业务成本
            '净利润同比': None,  # 需通过历史数据计算

            # 可选映射（根据具体业务需求添加）
            '毛利': '毛利',
            '所得税': '所得税',
            '研发费用': '研发费用',
            '资产处置损益': '资产处置损益',
        }

        field_mapping_H = {
            '序号': None,  # H股数据中通常没有直接对应的序号，需要重新生成
            '股票代码': 'SECURITY_CODE',  # 或 '股票代码'，如果H股数据中存在这个字段
            '股票简称': 'SECURITY_NAME_ABBR',
            '净利润': '股东应占溢利',  # 或 '本公司拥有人应占全面收益总额'
            '净利润同比': None,  # H股数据通常不直接提供同比数据，需要计算
            '营业总收入': '营业额',  # 或 '经营收入总额'
            '营业总收入同比': None,  # H股数据通常不直接提供同比数据，需要计算
            '营业总支出-营业支出': None,  # H股中通常分解为多个明细项
            '营业总支出-销售费用': '銷售及分銷費用',  # 或 '销售及分销费用'
            '营业总支出-管理费用': '行政及一般费用',  # 或 '行政开支'
            '营业总支出-财务费用': '融资成本',  # 或 '利息支出'
            '营业总支出-营业总支出': '经营支出总额',
            '营业利润': '经营溢利',
            '利润总额': '除税前溢利',
            '公告日期': 'REPORT_DATE'
        }

        if market == 'usa':
            field_mapping = us_to_a_share_mapping
            # 美股财务字段映射
            value_mapping = {
                '营业总收入同比': '营业总收入',
                '净利润同比': '净利润'
            }
        elif market == 'H':
            field_mapping = field_mapping_H
            # H股财务字段映射
            value_mapping = {
                '营业总收入同比': '营业额',
                '净利润同比': '净利润'
            }
        else:
            return h_share_df

        a_share_df = pd.DataFrame()

        # 遍历A股的每个字段，进行映射
        for a_field, h_field in field_mapping.items():
            if h_field is not None and h_field in h_share_df.columns:
                # 如果H股中有对应的字段，则直接复制
                a_share_df[a_field] = h_share_df[h_field]
            elif a_field == '序号':
                # 为序号字段生成新的序列
                a_share_df[a_field] = range(1, len(h_share_df) + 1)
            elif a_field.endswith('同比'):
                # 对于同比字段，暂时填充为None，后续计算
                a_share_df[a_field] = None
            elif a_field == '营业总支出-营业支出':
                # 对于A股特有的汇总字段，尝试从H股明细项计算
                if '销售成本' in h_share_df.columns and '经营支出其他项目' in h_share_df.columns:
                    a_share_df[a_field] = h_share_df['销售成本'] + h_share_df['经营支出其他项目']
                else:
                    a_share_df[a_field] = None
            else:
                # 其他未映射的字段，填充为None
                a_share_df[a_field] = None
        a_share_df['REPORT_DATE'] = h_share_df['REPORT_DATE']
        # 计算同比数据
        if 'REPORT_DATE' in a_share_df.columns:
            # 确保公告日期为日期格式
            a_share_df['REPORT_DATE'] = pd.to_datetime(a_share_df['REPORT_DATE'])

            # 提取年份信息
            a_share_df['年份'] = a_share_df['REPORT_DATE'].dt.year

            # 按股票代码和年份排序，确保数据按时间顺序排列
            a_share_df = a_share_df.sort_values(by=['股票代码', '年份'])

            # 计算同比数据

            for a_field, h_field in value_mapping.items():
                if a_field in a_share_df.columns:
                    # 获取A股目标列名
                    # target_column = list(field_mapping.keys())[list(field_mapping.values()).index(h_field)]

                    # 按股票代码分组，计算同比
                    grouped = a_share_df.groupby('股票代码')[a_field]

                    # 计算同比增长率（保留两位小数）
                    # 计算同比增长率（保留两位小数）
                    a_share_df[a_field] = grouped.pct_change(periods=1, fill_method=None).map(
                        lambda x: f"{x:.2%}" if pd.notna(x) else None)

                    # 替换无穷大值为None
                    a_share_df[a_field] = a_share_df[a_field].replace([float('inf'), float('-inf')], None)

        return a_share_df

    def map_zcfz_share_to_a_share(self, h_share_df, market='H'):
        """
        将H股资产负债表数据映射到A股格式

        参数:
        h_share_df (pd.DataFrame): H股数据框
        market (str): 市场类型，默认为'H'（港股），也支持'usa'（美股）

        返回:
        pd.DataFrame: 转换后的A股格式数据框
        """
        # 创建映射字典
        us_to_a_share_mapping = {
            # 基础信息
            '序号': None,
            '股票代码': 'SECURITY_CODE',
            '股票简称': 'SECURITY_NAME_ABBR',
            '公告日期': 'REPORT_DATE',

            # 资产负债表核心指标
            '资产-货币资金': ['现金及现金等价物', '现金及存放同业款项'],
            '资产-应收账款': ['应收账款', '应收票据(流动)', '应收关联方款项'],
            '资产-存货': '存货',
            '资产-总资产': '总资产',
            '负债-应付账款': ['应付账款', '应付账款及其他应付款', '应付关联方款项(流动)'],
            '负债-预收账款': ['预收及预提费用', '递延收入(流动)'],
            '负债-总负债': '总负债',
            '股东权益合计': ['归属于母公司股东权益', '少数股东权益'],

            # 比率指标
            '资产-总资产同比': None,
            '负债-总负债同比': None,
            '资产负债率': None,
        }

        field_mapping_H = {
            # 基础信息
            '序号': None,
            '股票代码': 'SECURITY_CODE',
            '股票简称': 'SECURITY_NAME_ABBR',
            '公告日期': 'REPORT_DATE',

            # 资产负债表核心指标
            '资产-货币资金': ['现金及现金等价物', '库存现金及短期资金'],
            '资产-应收账款': ['应收帐款', '应收关联方款项'],
            '资产-存货': '存货',
            '资产-总资产': '总资产',
            '负债-应付账款': ['应付帐款', '应付关联方款项(流动)'],
            '负债-预收账款': ['预收款项', '合同负债'],
            '负债-总负债': '总负债',
            '股东权益合计': ['归属于母公司股东权益', '少数股东权益'],

            # 比率指标
            '资产-总资产同比': None,
            '负债-总负债同比': None,
            '资产负债率': None,
        }

        # 根据市场类型选择映射规则
        if market == 'usa':
            field_mapping = us_to_a_share_mapping
            value_mapping = {
                '资产-总资产同比': '资产-总资产',
                '负债-总负债同比': '负债-总负债',
                '资产负债率': ('负债-总负债', '资产-总资产'),
            }
        elif market == 'H':
            field_mapping = field_mapping_H
            value_mapping = {
                '资产-总资产同比': '资产-总资产',
                '负债-总负债同比': '负债-总负债',
                '资产负债率': ('负债-总负债', '资产-总资产'),
            }
        else:
            raise ValueError(f"不支持的市场类型: {market}，请使用'H'或'usa'")

        # 创建新的数据框用于存储映射结果
        a_share_df = pd.DataFrame()

        # 遍历A股字段，进行映射
        for a_field, h_field in field_mapping.items():
            if h_field is None:
                # 无映射字段的处理
                if a_field == '序号':
                    a_share_df[a_field] = range(1, len(h_share_df) + 1)
                elif a_field.endswith('同比') or a_field == '资产负债率':
                    a_share_df[a_field] = None  # 后续计算
                else:
                    a_share_df[a_field] = None  # 默认为None
            elif isinstance(h_field, list):
                # 多个H股字段合并的情况
                sum_value = None
                for sub_field in h_field:
                    if sub_field in h_share_df.columns:
                        if sum_value is None:
                            sum_value = h_share_df[sub_field]
                        else:
                            sum_value += h_share_df[sub_field]
                a_share_df[a_field] = sum_value if sum_value is not None else None
            else:
                # 单个H股字段映射
                if h_field in h_share_df.columns:
                    a_share_df[a_field] = h_share_df[h_field]
                else:
                    a_share_df[a_field] = None
        a_share_df['REPORT_DATE'] = h_share_df['REPORT_DATE']
        # 计算同比数据
        if 'REPORT_DATE' in a_share_df.columns and a_share_df['REPORT_DATE'].notna().any():
            # 确保公告日期为日期格式
            a_share_df['REPORT_DATE'] = pd.to_datetime(a_share_df['REPORT_DATE'])

            # 按股票代码和日期排序
            a_share_df = a_share_df.sort_values(['股票代码', 'REPORT_DATE'])

            # 计算同比
            for ratio_field, base_field in value_mapping.items():
                try:
                    if ratio_field.endswith('同比'):
                        if base_field in a_share_df.columns:
                            # 使用pct_change计算同比增长率
                            a_share_df[ratio_field] = a_share_df.groupby('股票代码')[base_field].pct_change(periods=4)
                            # 转换为百分比格式
                            a_share_df[ratio_field] = a_share_df[ratio_field].apply(
                                lambda x: f"{x:.2%}" if pd.notna(x) else None)
                    elif ratio_field == '资产负债率':
                        if isinstance(base_field, tuple) and all(f in a_share_df.columns for f in base_field):
                            # 计算资产负债率
                            a_share_df[ratio_field] = a_share_df[base_field[0]] / a_share_df[base_field[1]]
                            # 转换为百分比格式
                            a_share_df[ratio_field] = a_share_df[ratio_field].apply(
                                lambda x: f"{x:.2%}" if pd.notna(x) and x != float('inf') else None)
                except Exception as e:
                    print(f"计算{ratio_field}时出错: {e}")

        return a_share_df

    def map_xjll_share_to_a_share(self, h_share_df, market='H'):
        """
        将H股或美股现金流量表数据映射转换为A股现金流量表格式

        参数:
        h_share_df (pd.DataFrame): H股或美股现金流量表数据
        market (str): 市场类型，支持'H'(H股)和'usa'(美股)

        返回:
        pd.DataFrame: 转换后的A股现金流量表格式数据
        """
        # H股现金流量表字段映射
        h_to_a_mapping = {
            '序号': None,
            '股票代码': 'SECURITY_CODE',
            '股票简称': 'SECURITY_NAME_ABBR',
            '净现金流-净现金流': '现金净额',
            '净现金流-同比增长': None,
            '经营性现金流-现金流量净额': '经营业务现金净额',
            '经营性现金流-净现金流占比': None,
            '投资性现金流-现金流量净额': '投资业务现金净额',
            '投资性现金流-净现金流占比': None,
            '融资性现金流-现金流量净额': '融资业务现金净额',
            '融资性现金流-净现金流占比': None,
            '公告日期': 'REPORT_DATE'
        }

        # 美股现金流量表字段映射
        usa_to_a_mapping = {
            '序号': None,
            '股票代码': 'SECURITY_CODE',
            '股票简称': 'SECURITY_NAME_ABBR',
            '净现金流-净现金流': '现金及现金等价物增加(减少)额',
            '净现金流-同比增长': None,
            '经营性现金流-现金流量净额': '经营活动产生的现金流量净额',
            '经营性现金流-净现金流占比': None,
            '投资性现金流-现金流量净额': '投资活动产生的现金流量净额',
            '投资性现金流-净现金流占比': None,
            '融资性现金流-现金流量净额': '筹资活动产生的现金流量净额',
            '融资性现金流-净现金流占比': None,
            '公告日期': 'REPORT_DATE'
        }

        # 需要计算同比的字段映射
        value_mapping = {
            '净现金流-同比增长': {
                'H': '净现金流-净现金流',
                'usa': '净现金流-净现金流'
            }
        }

        # 选择对应的映射关系
        if market == 'H':
            field_mapping = h_to_a_mapping
        elif market == 'usa':
            field_mapping = usa_to_a_mapping
        else:
            # 不支持的市场类型，返回原始数据
            return h_share_df

        # 创建新DataFrame存储映射结果
        a_share_df = pd.DataFrame()

        # 遍历A股字段进行映射
        for a_field, h_field in field_mapping.items():
            if h_field is not None and h_field in h_share_df.columns:
                # 直接映射已有字段
                a_share_df[a_field] = h_share_df[h_field]
            elif a_field == '序号':
                # 生成序号
                a_share_df[a_field] = range(1, len(a_share_df) + 1)
            elif a_field.endswith('占比'):
                # 计算净现金流占比
                cash_flow_type = a_field.split('-')[0]
                cash_flow_field = f"{cash_flow_type}-现金流量净额"
                total_cash_field = '净现金流-净现金流'

                if cash_flow_field in a_share_df.columns and total_cash_field in a_share_df.columns:
                    a_share_df[a_field] = a_share_df[cash_flow_field] / a_share_df[total_cash_field]
                else:
                    a_share_df[a_field] = None
            elif a_field.endswith('同比增长'):
                # 同比数据后续计算
                a_share_df[a_field] = None
            else:
                # 其他未映射字段填充None
                a_share_df[a_field] = None
        a_share_df['REPORT_DATE'] = h_share_df['REPORT_DATE']
        # 计算同比增长率
        if 'REPORT_DATE' in a_share_df.columns and not a_share_df['REPORT_DATE'].empty:
            # 转换日期格式
            a_share_df['REPORT_DATE'] = pd.to_datetime(a_share_df['REPORT_DATE'])

            # 提取年份信息
            a_share_df['年份'] = a_share_df['REPORT_DATE'].dt.year

            # 按股票代码和年份排序
            a_share_df = a_share_df.sort_values(by=['股票代码', '年份'])

            # 获取对应市场的同比计算字段
            h_field = value_mapping['净现金流-同比增长'][market]

            if h_field in a_share_df.columns:
                # 按股票代码分组计算同比
                grouped = a_share_df.groupby('股票代码')[h_field]
                a_share_df['净现金流-同比增长'] = grouped.pct_change(periods=1)

                # 格式化为百分比
                a_share_df['净现金流-同比增长'] = a_share_df['净现金流-同比增长'].map(
                    lambda x: f"{x:.2%}" if pd.notna(x) else None)

                # 处理无穷大值
                a_share_df['净现金流-同比增长'] = a_share_df['净现金流-同比增长'].replace([float('inf'), float('-inf')],
                                                                                          None)

        return a_share_df

    def financial_indicator_map_usa_fields(self,df):
        """
        将美股数据字段映射到统一字段名
        :param df: 包含美股原始字段的DataFrame
        :return: 映射后的DataFrame
        """
        # 定义美股字段到统一字段的映射字典
        usa_field_mapping = {
            # 通用统一字段
            'SECUCODE': '证券代码',
            'SECURITY_CODE': '股票代码',
            'SECURITY_NAME_ABBR': '股票简称',
            'ORG_CODE': '机构代码',
            'REPORT_DATE': '报告日期',
            'OPERATE_INCOME': '营业收入',
            'OPERATE_INCOME_YOY': '营业收入同比增长率',
            'GROSS_PROFIT': '毛利润',
            'GROSS_PROFIT_YOY': '毛利润同比增长率',
            'BASIC_EPS': '基本每股收益',
            'DILUTED_EPS': '稀释每股收益',
            'GROSS_PROFIT_RATIO': '毛利率',
            'ROE_AVG': '平均净资产收益率',
            'ROA': '总资产收益率',
            'CURRENT_RATIO': '流动比率',
            'DEBT_ASSET_RATIO': '资产负债率',
            'PARENT_HOLDER_NETPROFIT': '归属于母公司股东净利润',
            'ACCOUNTS_RECE_TR': '应收账款周转率',
            'INVENTORY_TR': '存货周转率',
            'TOTAL_ASSETS_TR': '总资产周转率',
            'ACCOUNTS_RECE_TDAYS': '应收账款周转天数',
            'INVENTORY_TDAYS': '存货周转天数',
            'TOTAL_ASSETS_TDAYS': '总资产周转天数',
            'SPEED_RATIO': '速动比率',
            'OCF_LIQDEBT': '经营活动现金流净额与流动负债比率',
            'EQUITY_RATIO': '股东权益比率',
            'BASIC_EPS_YOY': '基本每股收益同比增长率',
            'GROSS_PROFIT_RATIO_YOY': '毛利率同比增长率',
            'NET_PROFIT_RATIO_YOY': '净利率同比增长率',
            'ROE_AVG_YOY': '平均净资产收益率同比增长率',
            'ROA_YOY': '总资产收益率同比增长率',
            'DEBT_ASSET_RATIO_YOY': '资产负债率同比增长率',
            'CURRENT_RATIO_YOY': '流动比率同比增长率',
            'SPEED_RATIO_YOY': '速动比率同比增长率',

            # 美股特有字段（统一字段名+_usa）
            'SECURITY_INNER_CODE': '证券内部代码_usa',
            'ACCOUNTING_STANDARDS': '会计准则_usa',
            'NOTICE_DATE': '公告日期_usa',
            'FINANCIAL_DATE': '财务日期_usa',
            'STD_REPORT_DATE': '标准报告日期_usa',
            'CURRENCY': '货币类型_usa',
            'DATE_TYPE': '日期类型_usa',
            'DATE_TYPE_CODE': '数据类型代码_usa',
            'REPORT_TYPE': '报告类型_usa',
            'REPORT_DATA_TYPE': '报告数据类型_usa',
            'ORGTYPE': '机构类型_usa',
        }

        # 过滤掉不存在的字段，避免KeyError
        valid_fields = {k: v for k, v in usa_field_mapping.items() if k in df.columns}

        # 执行字段映射并返回新DataFrame
        return df.rename(columns=valid_fields)

    def financial_indicator_map_hk_fields(self,df):
        """
        将港股数据字段映射到统一字段名
        :param df: 包含港股原始字段的DataFrame
        :return: 映射后的DataFrame
        """
        # 定义港股字段到统一字段的映射字典
        hk_field_mapping = {
            # 通用统一字段
            'SECUCODE': '证券代码',
            'SECURITY_CODE': '股票代码',
            'SECURITY_NAME_ABBR': '股票简称',
            'ORG_CODE': '机构代码',
            'REPORT_DATE': '报告日期',
            'OPERATE_INCOME': '营业收入',
            'OPERATE_INCOME_YOY': '营业收入同比增长率',
            'GROSS_PROFIT': '毛利润',
            'GROSS_PROFIT_YOY': '毛利润同比增长率',
            'BASIC_EPS': '基本每股收益',
            'DILUTED_EPS': '稀释每股收益',
            'GROSS_PROFIT_RATIO': '毛利率',
            'ROE_AVG': '平均净资产收益率',
            'ROA': '总资产收益率',
            'CURRENT_RATIO': '流动比率',
            'DEBT_ASSET_RATIO': '资产负债率',
            'HOLDER_PROFIT': '归属于母公司股东净利润',

            # 港股特有字段（统一字段名+_hk）
            'PER_NETCASH_OPERATE': '经营活动每股净现金流量_hk',
            'PER_OI': '每股经营活动现金流量_hk',
            'BPS': '每股净资产_hk',
            'HOLDER_PROFIT_YOY': '归属于母公司股东的净利润同比增长率_hk',
            'EPS_TTM': '滚动市盈率每股收益_hk',
            'OPERATE_INCOME_QOQ': '营业收入环比增长率_hk',
            'NET_PROFIT_RATIO': '净利率_hk',
            'GROSS_PROFIT_QOQ': '毛利润环比增长率_hk',
            'HOLDER_PROFIT_QOQ': '归属于母公司股东的净利润环比增长率_hk',
            'ROE_YEARLY': '年度净资产收益率_hk',
            'ROIC_YEARLY': '年度投入资本回报率_hk',
            'TAX_EBT': '息税前利润税负_hk',
            'OCF_SALES': '销售商品、提供劳务收到的现金占营业收入比重_hk',
            'CURRENTDEBT_DEBT': '流动负债占总负债比重_hk',
            'START_DATE': '起始日期_hk',
            'FISCAL_YEAR': '会计年度_hk',
            'CURRENCY': '货币类型_hk',
            'IS_CNY_CODE': '是否人民币代码_hk',
        }

        # 过滤掉不存在的字段，避免KeyError
        valid_fields = {k: v for k, v in hk_field_mapping.items() if k in df.columns}

        # 执行字段映射并返回新DataFrame
        return df.rename(columns=valid_fields)
    def financial_indicator_map_sh_fields(self,df):
        """
        将港股数据字段映射到统一字段名
        :param df: 包含港股原始字段的DataFrame
        :return: 映射后的DataFrame
        """
        # 定义港股字段到统一字段的映射字典
        a_share_field_mapping = {
            # 通用统一字段（与美股/港股含义一致）
            '日期': '报告日期',
            '摊薄每股收益(元)': '摊薄每股收益',  # 港股/美股无此指标，A股特有
            '加权每股收益(元)': '加权每股收益',  # A股特有
            '扣除非经常性损益后的每股收益(元)': '扣除非经常性损益后的每股收益',  # 统一字段
            '每股净资产_调整前(元)': '每股净资产调整前',  # A股特有
            '每股净资产_调整后(元)': '每股净资产调整后',  # A股特有
            '每股经营性现金流(元)': '每股经营性现金流',  # 统一字段（对应港股PER_OI）
            '每股资本公积金(元)': '每股资本公积金',  # A股特有
            '每股未分配利润(元)': '每股未分配利润',  # A股特有
            '调整后的每股净资产(元)': '调整后的每股净资产',  # A股特有
            '总资产利润率(%)': '总资产利润率',  # A股特有
            '主营业务利润率(%)': '主营业务利润率',  # A股特有
            '总资产净利润率(%)': '总资产净利润率',  # 对应港股/美股ROA
            '成本费用利润率(%)': '成本费用利润率',  # A股特有
            '营业利润率(%)': '营业利润率',  # A股特有
            '主营业务成本率(%)': '主营业务成本率',  # A股特有
            '销售净利率(%)': '销售净利率',  # 对应港股/美股NET_PROFIT_RATIO
            '股本报酬率(%)': '股本报酬率',  # A股特有
            '净资产报酬率(%)': '净资产报酬率',  # A股特有（类似港股ROE_AVG）
            '资产报酬率(%)': '资产报酬率',  # A股特有
            '销售毛利率(%)': '销售毛利率',  # 统一字段（对应港股/美股GROSS_PROFIT_RATIO）
            '股息发放率(%)': '股息发放率',  # A股特有
            '投资收益率(%)': '投资收益率',  # A股特有
            '主营业务利润(元)': '主营业务利润',  # A股特有
            '净资产收益率(%)': '净资产收益率',  # 统一字段（对应港股ROE_AVG）
            '加权净资产收益率(%)': '加权净资产收益率',  # A股特有
            '扣除非经常性损益后的净利润(元)': '扣除非经常性损益后的净利润',  # 统一字段
            '主营业务收入增长率(%)': '主营业务收入增长率',  # 统一字段（对应港股/美股OPERATE_INCOME_YOY）
            '净利润增长率(%)': '净利润增长率',  # A股特有
            '净资产增长率(%)': '净资产增长率',  # A股特有
            '总资产增长率(%)': '总资产增长率',  # A股特有
            '应收账款周转率(次)': '应收账款周转率',  # 统一字段（对应美股ACCOUNTS_RECE_TR）
            '应收账款周转天数(天)': '应收账款周转天数',  # 统一字段（对应美股ACCOUNTS_RECE_TDAYS）
            '存货周转天数(天)': '存货周转天数',  # 统一字段（对应美股INVENTORY_TDAYS）
            '存货周转率(次)': '存货周转率',  # 统一字段（对应美股INVENTORY_TR）
            '固定资产周转率(次)': '固定资产周转率',  # A股特有
            '总资产周转率(次)': '总资产周转率',  # 统一字段（对应美股TOTAL_ASSETS_TR）
            '总资产周转天数(天)': '总资产周转天数',  # 统一字段（对应美股TOTAL_ASSETS_TDAYS）
            '流动资产周转率(次)': '流动资产周转率',  # A股特有
            '流动资产周转天数(天)': '流动资产周转天数',  # A股特有
            '股东权益周转率(次)': '股东权益周转率',  # A股特有
            '流动比率': '流动比率',  # 统一字段（对应港股/美股CURRENT_RATIO）
            '速动比率': '速动比率',  # 统一字段（对应美股SPEED_RATIO）
            '现金比率(%)': '现金比率',  # A股特有
            '利息支付倍数': '利息支付倍数',  # A股特有
            '长期债务与营运资金比率(%)': '长期债务与营运资金比率',  # A股特有
            '股东权益比率(%)': '股东权益比率',  # 统一字段（对应美股EQUITY_RATIO）
            '长期负债比率(%)': '长期负债比率',  # A股特有
            '股东权益与固定资产比率(%)': '股东权益与固定资产比率',  # A股特有
            '负债与所有者权益比率(%)': '负债与所有者权益比率',  # A股特有
            '长期资产与长期资金比率(%)': '长期资产与长期资金比率',  # A股特有
            '资本化比率(%)': '资本化比率',  # A股特有
            '固定资产净值率(%)': '固定资产净值率',  # A股特有
            '资本固定化比率(%)': '资本固定化比率',  # A股特有
            '产权比率(%)': '产权比率',  # A股特有
            '清算价值比率(%)': '清算价值比率',  # A股特有
            '固定资产比重(%)': '固定资产比重',  # A股特有
            '资产负债率(%)': '资产负债率',  # 统一字段（对应港股/美股DEBT_ASSET_RATIO）
            '总资产(元)': '总资产',  # A股特有
            '经营现金净流量对销售收入比率(%)': '经营现金净流量对销售收入比率',  # A股特有
            '资产的经营现金流量回报率(%)': '资产的经营现金流量回报率',  # A股特有
            '经营现金净流量与净利润的比率(%)': '经营现金净流量与净利润的比率',  # A股特有
            '经营现金净流量对负债比率(%)': '经营现金净流量对负债比率',  # A股特有
            '现金流量比率(%)': '现金流量比率',  # A股特有
            '短期股票投资(元)': '短期股票投资',  # A股特有
            '短期债券投资(元)': '短期债券投资',  # A股特有
            '短期其它经营性投资(元)': '短期其它经营性投资',  # A股特有
            '长期股票投资(元)': '长期股票投资',  # A股特有
            '长期债券投资(元)': '长期债券投资',  # A股特有
            '长期其它经营性投资(元)': '长期其它经营性投资',  # A股特有
            '1年以内应收帐款(元)': '1年以内应收帐款',  # A股特有
            '1-2年以内应收帐款(元)': '1-2年以内应收帐款',  # A股特有
            '2-3年以内应收帐款(元)': '2-3年以内应收帐款',  # A股特有
            '3年以内应收帐款(元)': '3年以内应收帐款',  # A股特有
            '1年以内预付货款(元)': '1年以内预付货款',  # A股特有
            '1-2年以内预付货款(元)': '1-2年以内预付货款',  # A股特有
            '2-3年以内预付货款(元)': '2-3年以内预付货款',  # A股特有
            '3年以内预付货款(元)': '3年以内预付货款',  # A股特有
            '1年以内其它应收款(元)': '1年以内其它应收款',  # A股特有
            '1-2年以内其它应收款(元)': '1-2年以内其它应收款',  # A股特有
            '2-3年以内其它应收款(元)': '2-3年以内其它应收款',  # A股特有
            '3年以内其它应收款(元)': '3年以内其它应收款',  # A股特有
        }

        # 过滤掉不存在的字段，避免KeyError
        valid_fields = {k: v for k, v in a_share_field_mapping.items() if k in df.columns}

        # 执行字段映射并返回新DataFrame
        return df.rename(columns=valid_fields)

    def get_finnancial_report_by_year(self, date, df_financial, market='SH', indicator='年报'):
        """
        获取指定年份的财务报表数据
        :param date_financial: 财务报表的日期，格式为 'YYYY-MM-DD'
        :param df_financial: 包含财务报表数据的DataFrame
        :param date_financial:
        :param df_financial:
        :return:
        """
        date_financial = date  # self.get_current_report_year_st(format='%Y-%m-%d', market=market)
        if '报告期' in df_financial.columns:
            df_financial_current = df_financial[df_financial['报告期'] >= date_financial]
        elif '报告日期' in df_financial.columns:
            date_filter = datetime.datetime.strptime(date_financial, "%Y-%m-%d").date()
            df_financial_current = df_financial[df_financial['报告日期'] >= date_filter]
        else:
            df_financial_current = df_financial
        """
        if market == 'SH' or market == 'SZ':
        # date_financial = self.reportUtils.get_report_year_str(date=date_financial,market=market)
        elif market == 'usa':
        # date_financial = self.reportUtils.get_report_year_str(date=date_financial,market=market)
        elif market == 'H':
        # date_financial = self.reportUtils.get_report_hk_year_str(date=date_financial)
         """
        return df_financial_current

    def get_finnancial_report_by_latest(self, df_financial):
        """
        获取报告期最大的一条数据
        :param date_financial: 财务报表的日期，格式为 'YYYY-MM-DD'
        :param df_financial: 包含财务报表数据的DataFrame
        :param date_financial:
        :param df_financial:
        :return:
        """

        df_financial['报告期'] = pd.to_datetime(df_financial['报告期'])
        # 获取每个股票代码下报告期最大的记录索引
        max_indices = df_financial.groupby('股票代码')['报告期'].idxmax()
        # 根据索引选择对应的行
        df_financial_current = df_financial.loc[max_indices]
        return df_financial_current


    def convert_column_to_number(self,df,col_name = '利润率'):
        """
        金额类的转换
        :param df:
        :param col_name:
        :return:
        """

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
        if col_name in df.columns:
            df[col_name] = convert_unit_vectorized(df[col_name])
        return df
